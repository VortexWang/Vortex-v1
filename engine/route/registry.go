package route

import (
	"fmt"
	"net/netip"
	"sync"
)

// 进程内端口登记表。
//
// 为什么需要它：Prepare 阶段 TCP 只 bind 不 listen（ENG-031）。Linux 上两个都设置了 SO_REUSEADDR
// 的 socket 只要都未 listen，就能 bind 到同一地址端口，内核不会报冲突；而去掉 SO_REUSEADDR 又会让
// 节点重启后无法立即重新 bind 处于 TIME_WAIT 的端口（AGT-010）。因此本进程内的预留冲突由登记表判定，
// 规则与数据库 port_allocations 相同（PRT-002、PRT-006）：同协议、同地址族、同端口，且任一方为通配
// 地址或两者地址相同即冲突。与其他进程的冲突仍由内核在 bind（对方已 listen）与 listen 时报告。
type portKey struct {
	network string // "tcp" / "udp"
	v6      bool
	port    uint16
}

type portRegistry struct {
	mu    sync.Mutex
	inUse map[portKey]map[netip.Addr]int
}

var ports = &portRegistry{inUse: map[portKey]map[netip.Addr]int{}}

func keyOf(network string, ap netip.AddrPort) (portKey, netip.Addr) {
	a := ap.Addr().Unmap()
	return portKey{network: network, v6: a.Is6(), port: ap.Port()}, a
}

// acquire 登记一个预留；冲突时返回 ErrPortInUse。
func (r *portRegistry) acquire(network string, ap netip.AddrPort) error {
	k, a := keyOf(network, ap)
	r.mu.Lock()
	defer r.mu.Unlock()
	for held := range r.inUse[k] {
		if held == a || held.IsUnspecified() || a.IsUnspecified() {
			return fmt.Errorf("%w: %s %s conflicts with %s", ErrPortInUse, network, ap, netip.AddrPortFrom(held, ap.Port()))
		}
	}
	if r.inUse[k] == nil {
		r.inUse[k] = map[netip.Addr]int{}
	}
	r.inUse[k][a]++
	return nil
}

func (r *portRegistry) release(network string, ap netip.AddrPort) {
	k, a := keyOf(network, ap)
	r.mu.Lock()
	defer r.mu.Unlock()
	if m := r.inUse[k]; m != nil {
		if m[a]--; m[a] <= 0 {
			delete(m, a)
		}
		if len(m) == 0 {
			delete(r.inUse, k)
		}
	}
}
