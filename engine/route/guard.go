package route

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"time"

	"vortex/engine/types"
)

// ErrTargetForbidden 对应原因码 target_unreachable（SEC-011 命中时以此拒绝，并产生 TargetRejected 事件）。
var ErrTargetForbidden = errors.New("target_unreachable: address forbidden by target policy")

// 引擎内置、不随配置下发的禁止类别（SEC-011，契约 C2a 决定 D9）。
var builtinDeny = mustPrefixes(
	"0.0.0.0/8", "127.0.0.0/8", "169.254.0.0/16", "224.0.0.0/4", "240.0.0.0/4", "255.255.255.255/32",
	"192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24",
	"::/128", "::1/128", "fe80::/10", "ff00::/8", "64:ff9b:1::/48", "2001:db8::/32", "100::/64",
)

// 私有网段：默认禁止，可按节点组放开（Q-08）。
var privateNets = mustPrefixes("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "100.64.0.0/10", "fc00::/7")

func mustPrefixes(ss ...string) []netip.Prefix {
	out := make([]netip.Prefix, len(ss))
	for i, s := range ss {
		out[i] = netip.MustParsePrefix(s)
	}
	return out
}

func inAny(a netip.Addr, ps []netip.Prefix) bool {
	for _, p := range ps {
		if p.Contains(a) {
			return true
		}
	}
	return false
}

// Guard 执行目标地址检查（SEC-011）。在每次建连前对**实际要连接的 IP** 检查，防止 DNS 重绑定。
type Guard struct {
	policy    types.TargetPolicy
	selfAddrs []netip.Addr // 本节点自身地址
	selfPorts []uint16     // 本节点 link / control / metrics 端口
	builtin   []netip.Prefix
}

// NewGuard 构造生产用 Guard。
func NewGuard(policy types.TargetPolicy, selfAddrs []netip.Addr, selfPorts []uint16) *Guard {
	return &Guard{policy: policy, selfAddrs: selfAddrs, selfPorts: selfPorts, builtin: builtinDeny}
}

// Check 返回 nil 表示允许连接。
func (g *Guard) Check(ap netip.AddrPort) error {
	a := ap.Addr().Unmap()
	port := ap.Port()
	for _, p := range g.policy.DenyPorts {
		if p == port {
			return ErrTargetForbidden
		}
	}
	if inAny(a, g.policy.DenyPrefixes) {
		return ErrTargetForbidden
	}
	self := a.IsLoopback() || a.IsUnspecified()
	for _, s := range g.selfAddrs {
		if s.Unmap() == a {
			self = true
		}
	}
	if self {
		// 本节点的 link / control / metrics 端口任何情况下都不可作为目标
		for _, p := range g.selfPorts {
			if p == port {
				return ErrTargetForbidden
			}
		}
		// 自托管节点作为出口时可以访问本机服务（SEC-011 例外）；平台节点不可以
		if g.policy.SelfHostedNode && !a.IsUnspecified() {
			return nil
		}
		return ErrTargetForbidden
	}
	if inAny(a, g.builtin) {
		return ErrTargetForbidden
	}
	if inAny(a, privateNets) && !inAny(a, g.policy.AllowPrivatePrefix) {
		return ErrTargetForbidden
	}
	return nil
}

// ---- 域名解析（ENG-T41） ----

// LookupFunc 与 net.Resolver.LookupNetIP 兼容，便于测试注入。
type LookupFunc func(ctx context.Context, network, host string) ([]netip.Addr, error)

// Resolver 解析目标域名并缓存。标准库不暴露 DNS 记录的 TTL，因此使用固定缓存时长，
// 默认 30 秒（不超过需求规定的 60 秒上限）。解析结果每次建连都重新经过 Guard 检查。
type Resolver struct {
	lookup LookupFunc
	ttl    time.Duration
	now    func() time.Time

	mu    sync.Mutex
	cache map[string]cacheEntry
}

type cacheEntry struct {
	addrs   []netip.Addr
	expires time.Time
}

func NewResolver(lookup LookupFunc, ttl time.Duration) *Resolver {
	if lookup == nil {
		lookup = net.DefaultResolver.LookupNetIP
	}
	if ttl <= 0 || ttl > 60*time.Second {
		ttl = 30 * time.Second
	}
	return &Resolver{lookup: lookup, ttl: ttl, now: time.Now, cache: map[string]cacheEntry{}}
}

// Resolve 返回 host 对应的地址；host 本身是 IP 时直接返回，不查询也不缓存。
func (r *Resolver) Resolve(ctx context.Context, host string) ([]netip.Addr, error) {
	if a, err := netip.ParseAddr(host); err == nil {
		return []netip.Addr{a}, nil
	}
	r.mu.Lock()
	if e, ok := r.cache[host]; ok && r.now().Before(e.expires) {
		r.mu.Unlock()
		return e.addrs, nil
	}
	r.mu.Unlock()
	addrs, err := r.lookup(ctx, "ip", host)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.cache[host] = cacheEntry{addrs: addrs, expires: r.now().Add(r.ttl)}
	r.mu.Unlock()
	return addrs, nil
}

// AllowedAddrs 解析并过滤出允许连接的地址；全部被禁止时返回 ErrTargetForbidden 与被拒的第一个地址。
func AllowedAddrs(ctx context.Context, r *Resolver, g *Guard, host string, port uint16) ([]netip.AddrPort, netip.Addr, error) {
	addrs, err := r.Resolve(ctx, host)
	if err != nil {
		return nil, netip.Addr{}, err
	}
	var ok []netip.AddrPort
	var rejected netip.Addr
	for _, a := range addrs {
		ap := netip.AddrPortFrom(a.Unmap(), port)
		if g.Check(ap) == nil {
			ok = append(ok, ap)
		} else if !rejected.IsValid() {
			rejected = a
		}
	}
	if len(ok) == 0 {
		return nil, rejected, ErrTargetForbidden
	}
	return ok, rejected, nil
}
