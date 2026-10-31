package route

import (
	"errors"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"vortex/engine/types"
)

func freePort(t *testing.T) uint16 {
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return uint16(l.Addr().(*net.TCPAddr).Port)
}

func TestReserveTCPRefusesUntilCommit(t *testing.T) {
	p := freePort(t)
	bind := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), p)
	r, err := ReserveTCP(bind)
	if err != nil {
		t.Fatal(err)
	}
	// 准备阶段：端口已被占用，但不接受连接（ENG-031）
	if c, err := net.DialTimeout("tcp", bind.String(), time.Second); err == nil {
		c.Close()
		t.Fatal("reserved port accepted a connection before Commit")
	}
	if _, err := ReserveTCP(bind); !errors.Is(err, ErrPortInUse) {
		t.Fatalf("second reservation should fail with PORT_IN_USE, got %v", err)
	}
	ln, err := r.CommitTCP(128)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		if c, err := ln.Accept(); err == nil {
			c.Close()
		}
	}()
	c, err := net.DialTimeout("tcp", bind.String(), time.Second)
	if err != nil {
		t.Fatalf("after Commit: %v", err)
	}
	c.Close()
}

func TestReserveWildcardConflict(t *testing.T) {
	p := freePort(t)
	r, err := ReserveTCP(netip.AddrPortFrom(netip.MustParseAddr("0.0.0.0"), p))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Abort()
	// 通配地址已预留时，同端口的具体地址必须冲突（PRT-002）
	if _, err := ReserveTCP(netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), p)); !errors.Is(err, ErrPortInUse) {
		t.Fatalf("specific address should conflict with wildcard, got %v", err)
	}
	// TCP 与 UDP 同号互不冲突（PRT-006）
	u, err := ReserveUDP(netip.AddrPortFrom(netip.MustParseAddr("0.0.0.0"), p))
	if err != nil {
		t.Fatalf("UDP with same port number must be independent: %v", err)
	}
	u.Abort()
}

func TestAbortReleasesPort(t *testing.T) {
	p := freePort(t)
	bind := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), p)
	for i := 0; i < 3; i++ {
		r, err := ReserveTCP(bind)
		if err != nil {
			t.Fatalf("round %d: %v（Abort 未释放端口）", i, err)
		}
		if err := r.Abort(); err != nil {
			t.Fatal(err)
		}
		_ = r.Abort() // 幂等
	}
}

func TestReserveUDPDropsBacklogOnCommit(t *testing.T) {
	r, err := ReserveUDP(netip.MustParseAddrPort("127.0.0.1:0"))
	if err != nil {
		t.Fatal(err)
	}
	addr := r.udp.LocalAddr().String()
	s, _ := net.Dial("udp", addr)
	defer s.Close()
	for i := 0; i < 5; i++ {
		_, _ = s.Write([]byte("stale")) // Commit 之前到达的数据报
	}
	time.Sleep(50 * time.Millisecond)
	c, dropped, err := r.CommitUDP()
	if err != nil {
		t.Fatal(err)
	}
	defer r.ReleaseUDP(c)
	if dropped != 5 {
		t.Fatalf("dropped %d stale datagrams, want 5", dropped)
	}
	_, _ = s.Write([]byte("fresh"))
	buf := make([]byte, 16)
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	n, _, err := c.ReadFromUDPAddrPort(buf)
	if err != nil || string(buf[:n]) != "fresh" {
		t.Fatalf("got %q, %v", buf[:n], err)
	}
}

func TestTableSwapMonotonicAndAtomic(t *testing.T) {
	var ts Tables
	id := types.UUID{1}
	if _, ok := ts.Swap(NewTable(1, []types.Rule{{RuleID: id, RuleVersion: 1}})); !ok {
		t.Fatal("first swap")
	}
	if _, ok := ts.Swap(NewTable(1, nil)); ok {
		t.Fatal("same version must be rejected")
	}
	if _, ok := ts.Swap(NewTable(0, nil)); ok {
		t.Fatal("older version must be rejected")
	}
	// 并发读写：读者总是看到完整的某个版本
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				tb := ts.Load()
				if r := tb.Rule(id); r == nil || r.RuleVersion != tb.Version {
					t.Error("torn read")
					return
				}
			}
		}()
	}
	for v := uint64(2); v < 2000; v++ {
		ts.Swap(NewTable(v, []types.Rule{{RuleID: id, RuleVersion: v}}))
	}
	close(stop)
	wg.Wait()
	if ts.Load().Version != 1999 {
		t.Fatal("final version")
	}
}

func TestClosedListenerReleasesPort(t *testing.T) {
	p := freePort(t)
	bind := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), p)
	r, err := ReserveTCP(bind)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := r.CommitTCP(16)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReserveTCP(bind); !errors.Is(err, ErrPortInUse) {
		t.Fatal("port must stay reserved while listening")
	}
	_ = ln.Close()
	r2, err := ReserveTCP(bind)
	if err != nil {
		t.Fatalf("port not released after listener close: %v", err)
	}
	_ = r2.Abort()
}

func TestKernelConflictWithForeignListener(t *testing.T) {
	// 不经过登记表的监听（相当于其他进程）：预留必须被内核拒绝
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	ap := netip.MustParseAddrPort(l.Addr().String())
	if r, err := ReserveTCP(ap); !errors.Is(err, ErrPortInUse) {
		if r != nil {
			r.Abort()
		}
		t.Fatalf("foreign listener not detected: %v", err)
	}
}

func TestRegistryV4V6Independent(t *testing.T) {
	p := freePort(t)
	r4, err := ReserveTCP(netip.AddrPortFrom(netip.MustParseAddr("0.0.0.0"), p))
	if err != nil {
		t.Fatal(err)
	}
	defer r4.Abort()
	r6, err := ReserveTCP(netip.AddrPortFrom(netip.MustParseAddr("::"), p))
	if err != nil {
		t.Skipf("IPv6 unavailable here: %v", err)
	}
	_ = r6.Abort()
}
