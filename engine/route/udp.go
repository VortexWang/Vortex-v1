package route

import (
	"context"
	"errors"
	"hash/maphash"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"vortex/engine/internal/lifecycle"
	"vortex/engine/pipe"
	"vortex/engine/types"
)

// UDP 直接转发（7.1 节、8.9 节）。
//
// goroutine 预算（ENG-045）：每个 UDP 监听 1 个读 goroutine；每个会话 1 个 goroutine 读取目标回包。
// 会话键为“监听 + 客户端地址”（ENG-040）；每个会话使用独立的已连接 UDP socket 访问目标，
// 保证回包正确归属（ENG-042）。计量按整包（ENG-038）：额度不足一个完整数据报时丢弃该包，不截断。

const (
	udpShards     = 64
	maxDatagram   = 64 << 10
	udpSockBuffer = 4 << 20 // ENG-043 默认 4 MiB
)

// UDPStats 供指标与测试使用。
type UDPStats struct {
	SessionsCreated atomic.Int64
	SessionsActive  atomic.Int64
	DroppedQuota    atomic.Int64 // 额度不足一个整包而丢弃
	DroppedNoRoute  atomic.Int64 // 无可用目标、准入拒绝、会话表满
	Forwarded       atomic.Int64
}

type udpSession struct {
	client  netip.AddrPort
	target  *net.UDPConn
	hook    pipe.Hook
	release func()
	scope   *lifecycle.Scope
	header  []byte       // PROXY v2 头部（每个数据报前加，RUL-001）
	last    atomic.Int64 // 最近一次任一方向有数据的单调时间
	closed  atomic.Bool
}

type udpShard struct {
	mu       sync.Mutex
	sessions map[netip.AddrPort]*udpSession
}

// UDPEntry 是直接转发规则在入口节点上的一个 UDP 监听。
type UDPEntry struct {
	RuleID      types.UUID
	Tables      *Tables
	Conn        *net.UDPConn // 已提交的监听（Reserved.CommitUDP）
	Scope       *lifecycle.Scope
	Admission   Admission
	Resolver    *Resolver
	Guard       *Guard
	Healthy     func(key string) bool
	Events      func(types.Event)
	MaxSessions int // 0 = 默认 100,000；超出时新会话的数据报被丢弃

	Stats UDPStats

	shards [udpShards]udpShard
	seed   maphash.Seed
	start  time.Time
	total  atomic.Int64

	selMu   sync.Mutex
	selRule *types.Rule
	sel     *Selector
}

func (e *UDPEntry) Start() error {
	if e.Admission == nil {
		e.Admission = AllowAll{}
	}
	if e.MaxSessions == 0 {
		e.MaxSessions = 100_000
	}
	e.seed = maphash.MakeSeed()
	e.start = time.Now()
	for i := range e.shards {
		e.shards[i].sessions = map[netip.AddrPort]*udpSession{}
	}
	_ = e.Conn.SetReadBuffer(udpSockBuffer)
	_ = e.Conn.SetWriteBuffer(udpSockBuffer)
	if err := e.Scope.Own(e.Conn); err != nil {
		return err
	}
	return e.Scope.Go(e.readLoop)
}

func (e *UDPEntry) mono() int64 { return int64(time.Since(e.start)) }

func (e *UDPEntry) shard(k netip.AddrPort) *udpShard {
	b := k.Addr().As16()
	var h maphash.Hash
	h.SetSeed(e.seed)
	_, _ = h.Write(b[:])
	_, _ = h.Write([]byte{byte(k.Port() >> 8), byte(k.Port())})
	return &e.shards[h.Sum64()%udpShards]
}

// readLoop 读取客户端数据报：查找或创建会话，整包计量后写往目标。单 goroutine、单缓冲，稳态无分配。
func (e *UDPEntry) readLoop(ctx context.Context) {
	buf := make([]byte, maxDatagram)
	local, _ := netip.ParseAddrPort(e.Conn.LocalAddr().String())
	for {
		n, client, err := e.Conn.ReadFromUDPAddrPort(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		client = netip.AddrPortFrom(client.Addr().Unmap(), client.Port())
		s := e.session(ctx, client, local)
		if s == nil {
			e.Stats.DroppedNoRoute.Add(1)
			continue
		}
		e.forward(s, buf[:n], types.DirUp, func(p []byte) error { _, err := s.target.Write(p); return err })
	}
}

// forward 对一个数据报做整包计量并发送。header 只在上行（发往目标）时前置。
func (e *UDPEntry) forward(s *udpSession, p []byte, d types.Direction, send func([]byte) error) {
	granted, err := s.hook.Acquire(s.scope.Context(), d, len(p))
	if err != nil || granted < len(p) {
		if err == nil {
			s.hook.Release(d, granted, 0) // 不足一个整包：退回，丢弃该包（ENG-038）
		}
		e.Stats.DroppedQuota.Add(1)
		return
	}
	out := p
	if d == types.DirUp && len(s.header) > 0 {
		out = append(append(make([]byte, 0, len(s.header)+len(p)), s.header...), p...)
	}
	if err := send(out); err != nil {
		s.hook.Release(d, granted, 0) // 发送失败整包退回
		return
	}
	s.hook.Release(d, granted, len(p)) // 只计业务载荷，不含 PROXY 头部
	s.last.Store(e.mono())
	e.Stats.Forwarded.Add(1)
}

func (e *UDPEntry) session(ctx context.Context, client, local netip.AddrPort) *udpSession {
	sh := e.shard(client)
	sh.mu.Lock()
	if s := sh.sessions[client]; s != nil {
		sh.mu.Unlock()
		return s
	}
	sh.mu.Unlock()

	// 新会话（RUL-008：UDP 按会话选择路径与目标）。创建过程在锁外进行，最后再插入；并发创建时保留先插入者。
	if e.total.Load() >= int64(e.MaxSessions) {
		return nil
	}
	rule := e.Tables.Load().Rule(e.RuleID)
	if rule == nil {
		return nil
	}
	var self atomic.Pointer[udpSession] // 会话建立后才有；门控在此之前不会调用 kill
	kill := func() {
		if p := self.Load(); p != nil {
			e.remove(p)
		}
	}
	hook, release, err := e.Admission.Admit(ctx, rule, client, kill)
	if err != nil {
		return nil
	}
	target, err := e.dial(ctx, rule)
	if err != nil {
		release()
		return nil
	}
	var header []byte
	if rule.ProxyProtocol == types.ProxyV2 {
		header, _ = ProxyHeader(types.ProxyV2, true, client, local)
	}
	_ = target.SetReadBuffer(udpSockBuffer)
	scope := e.Scope.Child()
	s := &udpSession{client: client, target: target, hook: hook, release: release, scope: scope, header: header}
	s.last.Store(e.mono())

	sh.mu.Lock()
	if existing := sh.sessions[client]; existing != nil {
		sh.mu.Unlock()
		_ = target.Close()
		release()
		scope.Shutdown()
		return existing
	}
	sh.sessions[client] = s
	sh.mu.Unlock()
	self.Store(s)
	e.total.Add(1)
	e.Stats.SessionsCreated.Add(1)
	e.Stats.SessionsActive.Add(1)

	idle := rule.UDPIdle
	if idle <= 0 {
		idle = 60 * time.Second // ENG-006 UDP 默认 60 秒
	}
	if scope.Own(target) != nil || scope.Go(func(ctx context.Context) { e.returnLoop(ctx, s, idle) }) != nil {
		e.remove(s)
	}
	return s
}

// returnLoop 是会话唯一的 goroutine：读取目标回包写回客户端，并负责空闲回收。
func (e *UDPEntry) returnLoop(ctx context.Context, s *udpSession, idle time.Duration) {
	defer e.remove(s)
	buf := make([]byte, maxDatagram)
	for {
		_ = s.target.SetReadDeadline(time.Now().Add(idle))
		n, err := s.target.Read(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			// 截止时间到：若上行最近仍有数据，则会话仍活跃，续期
			if time.Duration(e.mono()-s.last.Load()) < idle {
				continue
			}
			return // 空闲超时，回收会话
		}
		e.forward(s, buf[:n], types.DirDown, func(p []byte) error {
			_, err := e.Conn.WriteToUDPAddrPort(p, s.client)
			return err
		})
	}
}

func (e *UDPEntry) remove(s *udpSession) {
	if !s.closed.CompareAndSwap(false, true) {
		return
	}
	sh := e.shard(s.client)
	sh.mu.Lock()
	if sh.sessions[s.client] == s {
		delete(sh.sessions, s.client)
	}
	sh.mu.Unlock()
	e.total.Add(-1)
	e.Stats.SessionsActive.Add(-1)
	s.scope.Shutdown()
	s.release()
}

func (e *UDPEntry) selector(r *types.Rule) *Selector {
	e.selMu.Lock()
	defer e.selMu.Unlock()
	if e.selRule == r {
		return e.sel
	}
	cands := make([]Candidate, len(r.Targets))
	for i, t := range r.Targets {
		cands[i] = Candidate{Key: net.JoinHostPort(t.Host, itoa(t.Port)), Weight: t.Weight, Priority: t.Priority}
	}
	e.selRule, e.sel = r, NewSelector(r.TargetSchedule, cands)
	return e.sel
}

func itoa(p uint16) string {
	var b [5]byte
	i := len(b)
	for {
		i--
		b[i] = byte('0' + p%10)
		p /= 10
		if p == 0 {
			return string(b[i:])
		}
	}
}

// dial 选目标并建立已连接 UDP socket；目标地址经过 SEC-011 检查。
func (e *UDPEntry) dial(ctx context.Context, r *types.Rule) (*net.UDPConn, error) {
	sel := e.selector(r)
	tried := map[string]bool{}
	for len(tried) < sel.Len() {
		i := sel.Pick(e.Healthy, func(k string) bool { return tried[k] })
		if i < 0 {
			break
		}
		cand := sel.At(i)
		tried[cand.Key] = true
		t := r.Targets[i]
		aps, rejected, err := AllowedAddrs(ctx, e.Resolver, e.Guard, t.Host, t.Port)
		if errors.Is(err, ErrTargetForbidden) {
			if e.Events != nil {
				e.Events(types.Event{Kind: types.EventTargetRejected, RuleID: r.RuleID, TenantID: r.TenantID,
					Detail: cand.Key + " -> " + rejected.String()})
			}
			continue
		}
		if err != nil || len(aps) == 0 {
			continue
		}
		// UDP 无连接握手，无法据此判断目标是否存活；以探测结果（Healthy）为准，取第一个允许的地址
		c, err := net.DialUDP("udp", nil, net.UDPAddrFromAddrPort(aps[0]))
		if err == nil {
			return c, nil
		}
	}
	return nil, ErrNoRoute
}

// Sessions 返回当前会话数。
func (e *UDPEntry) Sessions() int { return int(e.total.Load()) }
