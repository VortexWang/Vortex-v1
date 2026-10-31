package tunnel

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"sync"
	"time"

	"vortex/engine/gate"
	"vortex/engine/internal/lifecycle"
	"vortex/engine/pipe"
	"vortex/engine/route"
	"vortex/engine/types"
	"vortex/engine/vlink"
)

// Router 把规则的下一跳映射为链路上的流，并处理入站流（中继与出口）。
type Router struct {
	Self      types.UUID
	Peers     *Peers
	Tables    *route.Tables
	Admission route.Admission
	Targets   *route.TargetDialer
	Emit      func(types.Event)

	mu   sync.Mutex
	sels map[types.UUID]memberSelector

	durations durations
}

type memberSelector struct {
	rule *types.Rule
	sel  *route.Selector
	ids  []types.UUID
}

func (r *Router) emit(ev types.Event) {
	if r.Emit != nil {
		r.Emit(ev)
	}
}

func (r *Router) selector(rule *types.Rule) memberSelector {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ms, ok := r.sels[rule.RuleID]; ok && ms.rule == rule {
		return ms
	}
	nh := rule.NextHop
	cands := make([]route.Candidate, len(nh.Members))
	ids := make([]types.UUID, len(nh.Members))
	for i, m := range nh.Members {
		cands[i] = route.Candidate{Key: string(m.NodeID[:]), Weight: m.Weight, Priority: m.Priority}
		ids[i] = m.NodeID
	}
	ms := memberSelector{rule: rule, sel: route.NewSelector(nh.MemberSchedule, cands), ids: ids}
	if r.sels == nil {
		r.sels = map[types.UUID]memberSelector{}
	}
	r.sels[rule.RuleID] = ms
	return ms
}

// Forget 删除不再存在的规则的选择器。
func (r *Router) Forget(keep func(types.UUID) bool) {
	r.durations.forget(keep)
	r.mu.Lock()
	defer r.mu.Unlock()
	for id := range r.sels {
		if !keep(id) {
			delete(r.sels, id)
		}
	}
}

// Open 在下一组中选择成员并开流（ENG-021、ENG-022）：排除已经过的节点与本节点，依次尝试；
// 下一组没有可用成员时返回 route.ErrNoRoute。visited 不含本节点，由这里追加。
func (r *Router) Open(ctx context.Context, rule *types.Rule, client netip.AddrPort, visited []types.UUID) (net.Conn, error) {
	pc, err := r.open(ctx, rule, client, visited, types.NetworkTCP)
	if err != nil {
		return nil, err
	}
	return pc, nil
}

// OpenUDP 为 UDP 会话开流（ENG-047：首个 DGRAM 之前发送网络类型为 udp 的 OPEN）。
func (r *Router) OpenUDP(ctx context.Context, rule *types.Rule, client netip.AddrPort, visited []types.UUID) (route.DatagramConn, error) {
	pc, err := r.open(ctx, rule, client, visited, types.NetworkUDP)
	if err != nil {
		return nil, err
	}
	return udpPath{pc}, nil
}

func (r *Router) open(ctx context.Context, rule *types.Rule, client netip.AddrPort, visited []types.UUID, network types.Network) (*pathConn, error) {
	if rule.HopIndex >= 255 {
		return nil, route.ErrNoRoute
	}
	path := append(slices.Clone(visited), r.Self)
	if len(path) > vlink.MaxVisited {
		return nil, route.ErrNoRoute
	}
	ms := r.selector(rule)
	tried := map[string]bool{}
	excluded := func(k string) bool {
		if tried[k] {
			return true
		}
		id := types.UUID([]byte(k))
		return id == r.Self || slices.Contains(visited, id)
	}
	healthy := func(k string) bool { return r.Peers.Healthy(types.UUID([]byte(k))) }
	o := &vlink.Open{RuleID: rule.RuleID, RuleVersion: rule.RuleVersion, PathGeneration: rule.PathGeneration,
		HopIndex: uint8(rule.HopIndex + 1), Network: network, Visited: path, Client: client}
	for {
		i := ms.sel.Pick(healthy, excluded)
		if i < 0 {
			return nil, route.ErrNoRoute
		}
		id := ms.ids[i]
		tried[string(id[:])] = true
		short := network == types.NetworkTCP && r.durations.short(rule.RuleID)
		s, err := r.Peers.OpenStream(ctx, id, o, short)
		if err == nil {
			return &pathConn{Stream: s, r: r, rule: rule, peer: id, entry: len(visited) == 0, tcp: network == types.NetworkTCP, start: time.Now()}, nil
		}
	}
}

// udpPath 以数据报方式使用出站 UDP 会话流。
type udpPath struct{ *pathConn }

func (u udpPath) Read(b []byte) (int, error) { return u.Stream.ReadDgram(b) }
func (u udpPath) Write(b []byte) (int, error) {
	if err := u.Stream.SendDgram(b); err != nil {
		return 0, err
	}
	return len(b), nil
}

// pathConn 是出站流；入口在关闭时把下游回传的原因码记为事件（ENG-015：按段指出原因）。
type pathConn struct {
	*vlink.Stream
	r     *Router
	rule  *types.Rule
	peer  types.UUID
	entry bool
	tcp   bool
	start time.Time
	once  sync.Once
}

func (c *pathConn) Close() error {
	c.once.Do(func() {
		if c.tcp {
			c.r.durations.record(c.rule.RuleID, time.Since(c.start))
		}
		var se *vlink.StreamError
		if c.entry && errors.As(c.Stream.Err(), &se) && se.Remote {
			c.r.emit(types.Event{Kind: types.EventOpenRejected, RuleID: c.rule.RuleID, TenantID: c.rule.TenantID,
				PeerID: c.peer, Detail: fmt.Sprintf("path: %v %s", se.Code, se.Text)})
		}
	})
	return c.Stream.Close()
}

// remoteError 返回下游流被对端终止的原因。
func remoteError(c any) *vlink.StreamError {
	var pc *pathConn
	switch x := c.(type) {
	case *pathConn:
		pc = x
	case udpPath:
		pc = x.pathConn
	default:
		return nil
	}
	var se *vlink.StreamError
	if errors.As(pc.Stream.Err(), &se) && se.Remote {
		return se
	}
	return nil
}

// gateCode 把门控拒绝映射为原因码（C1 第 6 节）。
func gateCode(err error) vlink.ErrorCode {
	switch {
	case errors.Is(err, gate.ErrQuota):
		return vlink.CodeQuotaExhausted
	case errors.Is(err, gate.ErrConnLimited):
		return vlink.CodeConnLimited
	case errors.Is(err, gate.ErrShare):
		return vlink.CodeRateLimited
	case errors.Is(err, gate.ErrRunGrant):
		return vlink.CodeNoRoute
	}
	return vlink.CodeNoRoute
}

// check 执行 ENG-020 ①～④；返回 0 表示通过。
func (r *Router) check(rule *types.Rule, peer types.UUID, o *vlink.Open) (vlink.ErrorCode, string) {
	switch {
	case rule == nil:
		return vlink.CodeNoRoute, "rule not active"
	case rule.RuleVersion != o.RuleVersion || rule.PathGeneration != o.PathGeneration:
		return vlink.CodeUnauthorized, fmt.Sprintf("version %d/%d, local %d/%d", o.RuleVersion, o.PathGeneration, rule.RuleVersion, rule.PathGeneration)
	case rule.Role == types.RoleEntry:
		return vlink.CodeUnauthorized, "rule is an entry on this node"
	case !slices.Contains(rule.AllowedUpstreams, peer):
		return vlink.CodeUnauthorized, "peer not an allowed upstream"
	case uint32(o.HopIndex) != rule.HopIndex:
		return vlink.CodeUnauthorized, fmt.Sprintf("hop_index %d, local %d", o.HopIndex, rule.HopIndex)
	case slices.Contains(o.Visited, r.Self):
		return vlink.CodeUnauthorized, "loop: this node already visited"
	case !slices.Contains(rule.Networks, o.Network):
		return vlink.CodeUnauthorized, "network not enabled"
	}
	return 0, ""
}

// HandleInbound 处理对端开的流（中继与出口）：开流检查 → 门控 → 下一跳或目标 → 管道。
func (r *Router) HandleInbound(ctx context.Context, sess *lifecycle.Scope, l *vlink.Link, s *vlink.Stream) {
	o := s.OpenInfo()
	peer := l.Peer().NodeID
	rule := r.Tables.Load().Rule(o.RuleID)
	reject := func(code vlink.ErrorCode, reason string) {
		s.Reject(code, "")
		ev := types.Event{Kind: types.EventOpenRejected, RuleID: o.RuleID, PeerID: peer,
			Detail: fmt.Sprintf("%v: %s", code, reason)}
		if rule != nil {
			ev.TenantID = rule.TenantID
		}
		r.emit(ev) // ENG-023：每次拒绝产生审计事件
	}
	if code, reason := r.check(rule, peer, o); code != 0 {
		reject(code, reason)
		return
	}
	hook, release, err := r.Admission.Admit(ctx, rule, o.Client, sess.Shutdown)
	if err != nil {
		reject(gateCode(err), err.Error())
		return
	}
	defer release()
	s.Accept()
	if o.Network == types.NetworkUDP {
		r.handleUDP(ctx, sess, rule, s, hook)
		return
	}

	var down net.Conn
	if rule.NextHop != nil {
		down, err = r.Open(ctx, rule, o.Client, o.Visited)
		if err != nil {
			s.Reset(vlink.CodeNoRoute, "next group has no available member")
			return
		}
	} else {
		down, err = r.Targets.Dial(ctx, rule)
		if err != nil {
			s.Reset(vlink.CodeTargetUnreachable, "")
			return
		}
		if rule.ProxyProtocol == types.ProxyV1 || rule.ProxyProtocol == types.ProxyV2 {
			dst, _ := netip.ParseAddrPort(down.LocalAddr().String()) // 隧道中没有入口监听地址，用出口本地地址
			h, err := route.ProxyHeader(rule.ProxyProtocol, false, o.Client, dst)
			if err == nil {
				_ = down.SetWriteDeadline(time.Now().Add(5 * time.Second))
				_, err = down.Write(h)
				_ = down.SetWriteDeadline(time.Time{})
			}
			if err != nil {
				_ = down.Close()
				s.Reset(vlink.CodeTargetUnreachable, "")
				return
			}
		}
	}
	if sess.Own(down) != nil {
		return
	}
	pipe.Run(ctx, s, down, pipe.Options{
		Hook:        hook,
		IdleTimeout: rule.TCPIdle,
		Go: func(f func()) {
			if sess.Go(func(context.Context) { f() }) != nil {
				f()
			}
		},
	})
	if se := remoteError(down); se != nil { // 下游的原因码逐段回传（ENG-015）
		s.Reset(se.Code, se.Text)
	}
}
