//go:build linux

package route

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"sync"
	"syscall"
)

// ErrPortInUse 对应资源错误码 PORT_IN_USE（PrepareResult.Errors）。
var ErrPortInUse = errors.New("PORT_IN_USE")

// Reserved 是 Prepare 阶段预留的监听（ENG-031）：端口已占用，但不对外开放。
//   - TCP：只 bind 不 listen，到达的连接被内核直接拒绝（RST）；
//   - UDP：bind 后不读取，Commit 时先丢弃积压的数据报。
type Reserved struct {
	Bind     netip.AddrPort
	Network  string // "tcp" / "udp"
	fd       int    // TCP 预留 socket；Commit 后为 -1
	udp      *net.UDPConn
	released sync.Once
}

// releasePort 从进程内登记表移除（Abort 或已提交的监听关闭时）。幂等。
func (r *Reserved) releasePort() { r.released.Do(func() { ports.release(r.Network, r.Bind) }) }

// ReserveTCP 创建并 bind 一个不 listen 的 TCP socket。
// 本进程内的冲突由登记表判定（见 registry.go）；不设置 SO_REUSEPORT。
func ReserveTCP(bind netip.AddrPort) (*Reserved, error) {
	// 端口 0 表示由内核分配：内核保证唯一，bind 之后按实际端口登记
	explicit := bind.Port() != 0
	if explicit {
		if err := ports.acquire("tcp", bind); err != nil {
			return nil, err
		}
	}
	addr := bind.Addr().Unmap()
	family := syscall.AF_INET
	if addr.Is6() {
		family = syscall.AF_INET6
	}
	ok := false
	defer func() {
		if !ok && explicit {
			ports.release("tcp", bind)
		}
	}()
	fd, err := syscall.Socket(family, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC|syscall.SOCK_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer func() {
		if !ok {
			syscall.Close(fd)
		}
	}()
	// SO_REUSEADDR：节点重启后可立即重新 bind 处于 TIME_WAIT 的端口（AGT-010）
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		return nil, err
	}
	var sa syscall.Sockaddr
	if family == syscall.AF_INET6 {
		// IPv6 通配地址只覆盖 IPv6，与数据库中按地址族的冲突规则一致（00002 port_allocations）
		if err := syscall.SetsockoptInt(fd, syscall.IPPROTO_IPV6, syscall.IPV6_V6ONLY, 1); err != nil {
			return nil, err
		}
		sa = &syscall.SockaddrInet6{Port: int(bind.Port()), Addr: addr.As16()}
	} else {
		sa = &syscall.SockaddrInet4{Port: int(bind.Port()), Addr: addr.As4()}
	}
	if err := syscall.Bind(fd, sa); err != nil {
		if errors.Is(err, syscall.EADDRINUSE) {
			return nil, fmt.Errorf("%w: tcp %s", ErrPortInUse, bind)
		}
		return nil, err
	}
	if !explicit {
		sa, err := syscall.Getsockname(fd)
		if err != nil {
			return nil, err
		}
		switch a := sa.(type) {
		case *syscall.SockaddrInet4:
			bind = netip.AddrPortFrom(bind.Addr(), uint16(a.Port))
		case *syscall.SockaddrInet6:
			bind = netip.AddrPortFrom(bind.Addr(), uint16(a.Port))
		}
		if err := ports.acquire("tcp", bind); err != nil {
			return nil, err
		}
	}
	ok = true
	return &Reserved{Bind: bind, Network: "tcp", fd: fd}, nil
}

// ReserveUDP bind 一个 UDP socket，但在 Commit 之前不读取。
func ReserveUDP(bind netip.AddrPort) (*Reserved, error) {
	explicit := bind.Port() != 0
	if explicit {
		if err := ports.acquire("udp", bind); err != nil {
			return nil, err
		}
	}
	network := "udp4"
	if bind.Addr().Unmap().Is6() {
		network = "udp6"
	}
	c, err := net.ListenUDP(network, net.UDPAddrFromAddrPort(bind))
	if err != nil && explicit {
		ports.release("udp", bind)
	}
	if err != nil {
		if errors.Is(err, syscall.EADDRINUSE) {
			return nil, fmt.Errorf("%w: udp %s", ErrPortInUse, bind)
		}
		return nil, err
	}
	if !explicit {
		bind = netip.AddrPortFrom(bind.Addr(), uint16(c.LocalAddr().(*net.UDPAddr).Port))
		if err := ports.acquire("udp", bind); err != nil {
			c.Close()
			return nil, err
		}
	}
	return &Reserved{Bind: bind, Network: "udp", fd: -1, udp: c}, nil
}

// CommitTCP 开始 listen 并返回 net.Listener（ENG-032）。
func (r *Reserved) CommitTCP(backlog int) (net.Listener, error) {
	if r.Network != "tcp" || r.fd < 0 {
		return nil, errors.New("route: not a reserved TCP socket")
	}
	if err := syscall.Listen(r.fd, backlog); err != nil {
		if errors.Is(err, syscall.EADDRINUSE) {
			// 其他进程在预留之后抢先 listen 了同一端口
			return nil, fmt.Errorf("%w: tcp %s (listen)", ErrPortInUse, r.Bind)
		}
		return nil, err
	}
	f := os.NewFile(uintptr(r.fd), "vortex-listener")
	r.fd = -1
	defer f.Close() // FileListener 复制了 fd
	ln, err := net.FileListener(f)
	if err != nil {
		return nil, err
	}
	return &trackedListener{Listener: ln, r: r}, nil
}

// trackedListener 在关闭时释放端口登记。只包装监听本身，不包装接入的连接（ENG-004）。
type trackedListener struct {
	net.Listener
	r *Reserved
}

func (l *trackedListener) Close() error {
	err := l.Listener.Close()
	l.r.releasePort()
	return err
}

// CommitUDP 丢弃 Commit 之前积压在接收缓冲中的数据报（ENG-031），然后交出 socket 供读取。
// 返回被丢弃的数据报数。
func (r *Reserved) CommitUDP() (*net.UDPConn, int, error) {
	if r.Network != "udp" || r.udp == nil {
		return nil, 0, errors.New("route: not a reserved UDP socket")
	}
	// 注意：不能用“截止时间设为现在再读”的办法，Go 在截止时间已过时根本不会尝试读取。
	// 这里直接做非阻塞 recvfrom，直到 EAGAIN。
	rc, err := r.udp.SyscallConn()
	if err != nil {
		return nil, 0, err
	}
	dropped := 0
	buf := make([]byte, 64<<10)
	for {
		var rerr error
		if err := rc.Read(func(fd uintptr) bool {
			_, _, rerr = syscall.Recvfrom(int(fd), buf, syscall.MSG_DONTWAIT)
			return true // 只尝试一次，不等待
		}); err != nil {
			return nil, 0, err
		}
		if rerr != nil {
			break // EAGAIN：已清空
		}
		dropped++
	}
	c := r.udp
	r.udp = nil
	return c, dropped, nil
}

// ReleaseUDP 关闭已提交的 UDP 监听并释放端口登记。
func (r *Reserved) ReleaseUDP(c *net.UDPConn) error {
	err := c.Close()
	r.releasePort()
	return err
}

// Abort 释放尚未提交的预留（ENG-033）。幂等。
func (r *Reserved) Abort() error {
	var err error
	if r.fd >= 0 {
		err = syscall.Close(r.fd)
		r.fd = -1
		r.releasePort()
	}
	if r.udp != nil {
		err = r.udp.Close()
		r.udp = nil
		r.releasePort()
	}
	return err
}
