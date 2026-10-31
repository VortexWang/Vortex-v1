// Package tunnel 是隧道转发：节点间 VLink 链路的建立与管理、按下一组成员开流、入站流的开流检查
// （ENG-020～ENG-023）以及出口连接目标。
package tunnel

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math"
	"net"
	"strconv"
	"sync"
	"time"

	"vortex/engine/internal/lifecycle"
	"vortex/engine/types"
	"vortex/engine/vlink"
)

var (
	ErrUnknownPeer = errors.New("tunnel: peer has no address in runtime config")
	ErrPeerDown    = errors.New("tunnel: peer recently unreachable")
)

// InboundHandler 处理对端在链路上开的一条流；sess 拥有该流，handler 返回后 sess 被关闭。
type InboundHandler func(ctx context.Context, sess *lifecycle.Scope, l *vlink.Link, s *vlink.Stream)

// 链路模式参数（ENG-017、开发方案 01 §3.5）。
const (
	DefaultMinMux    = 2
	DefaultMaxMux    = 4
	DefaultPoolRatio = 0.5 // 池目标 = max(2, 近 1 分钟池并发峰值 × 比例)
	DefaultPoolMax   = 64
)

// Peers 管理到各对端节点的链路（ENG-017）：
//   - mux：每个主动连接的对端维持 2～4 条长链路，新流选在途字节最少的链路；全部链路的在途字节都超过
//     链路窗口一半时增加一条，空闲链路超过下限的部分在空闲一段时间后关闭；
//   - pool：预热若干空闲链路，每条同时只承载一个流，流结束后归还复用；池大小按近 1 分钟并发自适应；
//   - 池中没有空闲链路时，该会话改用 mux 链路，同时补充池：两种模式下新会话都不增加握手往返。
//
// 建链在配置提交时即开始（预热），会话到达时通常已有链路。链路建立后双向对等（ENG-018）。
type Peers struct {
	Local       func() *vlink.Local // 每次握手时取（代次随配置变化）
	Handle      InboundHandler
	Emit        func(types.Event)
	LinkOptions vlink.LinkOptions
	DialTimeout time.Duration // 默认 5 秒
	DownBackoff time.Duration // 连接失败后多久内视为不可用，默认 5 秒
	MinMux      int           // 默认 2
	MaxMux      int           // 默认 4
	PoolRatio   float64       // 默认 0.5
	PoolMax     int           // 默认 64
	IdleClose   time.Duration // 超过下限的空闲 mux 链路的关闭时间，默认 60 秒
	Tick        time.Duration // 维护周期，默认 1 秒

	root *lifecycle.Scope
	kick chan struct{}

	mu    sync.Mutex
	peers map[types.UUID]*peerState
}

type peerState struct {
	id             types.UUID
	addr           string // 非空：本节点主动连接（Peer.Outbound）
	mode           types.LinkMode
	known          bool
	links          []*linkInfo // 全部链路（含对端发起的）
	poolIdle       []*linkInfo
	poolBusy       int
	poolFallback   int // 想用 pool 但池空、改用 mux 的进行中会话：同样计入池需求
	peak, prevPeak int // 本分钟与上一分钟的池并发峰值
	peakAt         time.Time
	dialing        map[vlink.LinkMode]int
	syncDial       *dialCall     // 没有任何可用链路时，会话等待的那次建链
	ready          chan struct{} // 有 mux 链路建立或建链失败时关闭并替换，唤醒等待者
	downUntil      time.Time
}

func (ps *peerState) readyWait() <-chan struct{} {
	if ps.ready == nil {
		ps.ready = make(chan struct{})
	}
	return ps.ready
}

func (ps *peerState) wake() {
	if ps.ready != nil {
		close(ps.ready)
		ps.ready = nil
	}
}

type linkInfo struct {
	l         *vlink.Link
	pool      bool
	idleSince time.Time // NumStreams 为 0 的起始时刻；有流时为零值
}

type dialCall struct {
	done chan struct{}
	l    *linkInfo
	err  error
}

// Start 绑定生命周期并启动维护 goroutine；ln 非 nil 时在其上接受入站链路。
func (p *Peers) Start(root *lifecycle.Scope, ln net.Listener) error {
	p.root = root
	p.peers = map[types.UUID]*peerState{}
	p.kick = make(chan struct{}, 1)
	if p.DialTimeout == 0 {
		p.DialTimeout = 5 * time.Second
	}
	if p.DownBackoff == 0 {
		p.DownBackoff = 5 * time.Second
	}
	if p.MinMux == 0 {
		p.MinMux = DefaultMinMux
	}
	if p.MaxMux == 0 {
		p.MaxMux = DefaultMaxMux
	}
	if p.PoolRatio == 0 {
		p.PoolRatio = DefaultPoolRatio
	}
	if p.PoolMax == 0 {
		p.PoolMax = DefaultPoolMax
	}
	if p.IdleClose == 0 {
		p.IdleClose = 60 * time.Second
	}
	if p.Tick == 0 {
		p.Tick = time.Second
	}
	if p.Emit == nil {
		p.Emit = func(types.Event) {}
	}
	if err := root.Go(p.maintainLoop); err != nil {
		return err
	}
	if ln == nil {
		return nil
	}
	if err := root.Own(ln); err != nil {
		return err
	}
	return root.Go(func(ctx context.Context) { p.acceptLoop(ctx, ln) })
}

func (p *Peers) state(id types.UUID) *peerState {
	ps := p.peers[id]
	if ps == nil {
		ps = &peerState{id: id, dialing: map[vlink.LinkMode]int{}}
		p.peers[id] = ps
	}
	return ps
}

func (p *Peers) acceptLoop(ctx context.Context, ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			select {
			case <-time.After(50 * time.Millisecond):
			case <-ctx.Done():
				return
			}
			continue
		}
		// 握手失败时 Handshake 关闭连接；Scope 关闭时 ctx 取消使握手立即失败。成功后链路由自己的 Scope 拥有
		if p.root.Go(func(ctx context.Context) {
			l := p.Local()
			tc := tls.Server(c, vlink.ServerConfig(l.Identity, l.Trust))
			if est, err := vlink.Handshake(ctx, tc, l, vlink.Expect{}); err == nil {
				p.register(est, false, vlink.ModeMux)
			}
		}) != nil {
			_ = c.Close()
		}
	}
}

// register 启动链路并登记；每条链路一个接受入站流的 goroutine（另有链路自身的读写 goroutine）。
func (p *Peers) register(est *vlink.Established, initiator bool, mode vlink.LinkMode) *linkInfo {
	opt := p.LinkOptions
	opt.Initiator, opt.Mode = initiator, mode
	l := vlink.NewLink(est, opt)
	peer := est.Peer.NodeID
	ls := p.root.Child()
	if ls.Own(closer{l}) != nil {
		return nil
	}
	li := &linkInfo{l: l, pool: l.Mode() == vlink.ModePool, idleSince: time.Now()}
	p.mu.Lock()
	ps := p.state(peer)
	ps.links = append(ps.links, li)
	if li.pool && initiator {
		ps.poolIdle = append(ps.poolIdle, li)
	}
	ps.downUntil = time.Time{}
	if !li.pool {
		ps.wake()
	}
	p.mu.Unlock()
	if est.Peer.ServedMismatch {
		p.Emit(types.Event{Kind: types.EventPeerDown, PeerID: peer, Detail: "served_digest mismatch"})
	}
	err := ls.Go(func(ctx context.Context) {
		defer ls.Shutdown()
		defer p.unregister(peer, li)
		for {
			s, err := l.Accept(ctx)
			if err != nil {
				return
			}
			sess := ls.Child()
			if sess.Own(s) != nil {
				return
			}
			if sess.Go(func(ctx context.Context) {
				defer sess.Shutdown()
				p.Handle(ctx, sess, l, s)
			}) != nil {
				return
			}
		}
	})
	if err != nil {
		p.unregister(peer, li)
		return nil
	}
	return li
}

func remove(ls []*linkInfo, li *linkInfo) []*linkInfo {
	for i, x := range ls {
		if x == li {
			return append(ls[:i], ls[i+1:]...)
		}
	}
	return ls
}

func (p *Peers) unregister(peer types.UUID, li *linkInfo) {
	p.mu.Lock()
	if ps := p.peers[peer]; ps != nil {
		ps.links = remove(ps.links, li)
		ps.poolIdle = remove(ps.poolIdle, li)
	}
	p.mu.Unlock()
	p.Emit(types.Event{Kind: types.EventPeerDown, PeerID: peer, Detail: fmt.Sprint(li.l.Err())})
	p.poke()
}

type closer struct{ l *vlink.Link }

func (c closer) Close() error { return c.l.Close() }

func (p *Peers) poke() {
	select {
	case p.kick <- struct{}{}:
	default:
	}
}

// SetPeers 更新对端地址与链路模式，并立即开始预热；已不在运行配置中的对端的链路被关闭。
func (p *Peers) SetPeers(peers []types.Peer) {
	seen := map[types.UUID]bool{}
	p.mu.Lock()
	for _, pr := range peers {
		seen[pr.NodeID] = true
		ps := p.state(pr.NodeID)
		ps.known, ps.mode, ps.addr = true, pr.LinkMode, ""
		if pr.Outbound && pr.Address != "" {
			ps.addr = net.JoinHostPort(pr.Address, strconv.Itoa(int(pr.LinkPort)))
		}
	}
	var stale []*vlink.Link
	for id, ps := range p.peers {
		if !seen[id] {
			ps.known, ps.addr = false, ""
			for _, li := range ps.links {
				stale = append(stale, li.l)
			}
		}
	}
	p.mu.Unlock()
	for _, l := range stale {
		_ = l.Close()
	}
	p.poke()
}

// CloseRevoked 关闭对端证书或节点已被吊销的链路（GOAWAY revoked，ENG-010）。
func (p *Peers) CloseRevoked(rev *vlink.Revocations) {
	p.mu.Lock()
	var bad []*vlink.Link
	for id, ps := range p.peers {
		for _, li := range ps.links {
			cs := li.l.ConnectionState()
			if len(cs.PeerCertificates) > 0 && rev.Revoked(cs.PeerCertificates[0], id) {
				bad = append(bad, li.l)
			}
		}
	}
	p.mu.Unlock()
	for _, l := range bad {
		_ = l.Shutdown(vlink.GoawayRevoked)
	}
}

// Healthy 表示可以尝试该对端：最近没有连接失败（AGT-007 的探测结果在 M3 接入）。
func (p *Peers) Healthy(id types.UUID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	ps := p.peers[id]
	return ps == nil || time.Now().After(ps.downUntil)
}

// usePool 判定本会话是否使用 pool 链路：UDP 固定 mux；TCP 由对端模式决定，auto 时看规则的会话时长（short）。
func usePool(mode types.LinkMode, network types.Network, short bool) bool {
	if network == types.NetworkUDP {
		return false
	}
	switch mode {
	case types.LinkModePool:
		return true
	case types.LinkModeMux:
		return false
	}
	return short
}

// OpenStream 在到对端的链路上开流。short 表示该规则近期会话时长中位数低于 10 秒（auto 模式据此选 pool）。
func (p *Peers) OpenStream(ctx context.Context, id types.UUID, o *vlink.Open, short bool) (*vlink.Stream, error) {
	p.mu.Lock()
	ps := p.state(id)
	if usePool(ps.mode, o.Network, short) {
		for len(ps.poolIdle) > 0 {
			li := ps.poolIdle[len(ps.poolIdle)-1]
			ps.poolIdle = ps.poolIdle[:len(ps.poolIdle)-1]
			if !li.l.Usable() {
				continue
			}
			s, err := li.l.Open(o)
			if err != nil {
				continue
			}
			ps.poolBusy++
			ps.peak = max(ps.peak, ps.poolBusy+ps.poolFallback)
			p.mu.Unlock()
			s.OnClose(func() { p.poolReturn(id, li) })
			p.poke()
			return s, nil
		}
		// 池中没有空闲链路：改用 mux，避免为本会话等待建链；维护循环随即补充池
		ps.poolFallback++
		ps.peak = max(ps.peak, ps.poolBusy+ps.poolFallback)
		p.mu.Unlock()
		p.poke()
		s, err := p.openMux(ctx, id, o)
		p.mu.Lock()
		if err != nil {
			ps.poolFallback--
			p.mu.Unlock()
			return nil, err
		}
		p.mu.Unlock()
		s.OnClose(func() {
			p.mu.Lock()
			ps.poolFallback--
			p.mu.Unlock()
		})
		return s, nil
	}
	p.mu.Unlock()
	return p.openMux(ctx, id, o)
}

func (p *Peers) openMux(ctx context.Context, id types.UUID, o *vlink.Open) (*vlink.Stream, error) {
	p.mu.Lock()
	li := p.bestMuxLocked(p.state(id))
	p.mu.Unlock()
	if li == nil {
		var err error
		if li, err = p.waitDial(ctx, id); err != nil {
			return nil, err
		}
	}
	s, err := li.l.Open(o)
	if err != nil { // 刚收到 GOAWAY 等：等一条新链路再试一次
		if li, err = p.waitDial(ctx, id); err != nil {
			return nil, err
		}
		s, err = li.l.Open(o)
	}
	p.poke()
	return s, err
}

func (p *Peers) poolReturn(id types.UUID, li *linkInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ps := p.peers[id]
	if ps == nil {
		return
	}
	ps.poolBusy--
	if li.l.Usable() && li.l.NumStreams() == 0 {
		ps.poolIdle = append(ps.poolIdle, li)
	}
}

// bestMuxLocked 在可用的非 pool 链路中选在途字节最少者（相同时选流少者）。
func (p *Peers) bestMuxLocked(ps *peerState) *linkInfo {
	var best *linkInfo
	var bestIn int64 = math.MaxInt64
	bestN := math.MaxInt
	for _, li := range ps.links {
		if li.pool || !li.l.Usable() {
			continue
		}
		in, n := li.l.InFlight(), li.l.NumStreams()
		if in < bestIn || (in == bestIn && n < bestN) {
			best, bestIn, bestN = li, in, n
		}
	}
	return best
}

// waitDial 在没有可用 mux 链路时同步建链（同一对端的并发请求共用一次建链）。
func (p *Peers) waitDial(ctx context.Context, id types.UUID) (*linkInfo, error) {
	p.mu.Lock()
	ps := p.state(id)
	for {
		if li := p.bestMuxLocked(ps); li != nil {
			p.mu.Unlock()
			return li, nil
		}
		if ps.dialing[vlink.ModeMux] == 0 {
			break
		}
		// 预热建链正在进行：等它完成，不再另建一条
		w := ps.readyWait()
		p.mu.Unlock()
		select {
		case <-w:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		p.mu.Lock()
	}
	if c := ps.syncDial; c != nil {
		p.mu.Unlock()
		select {
		case <-c.done:
			return c.l, c.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if ps.addr == "" {
		p.mu.Unlock()
		return nil, ErrUnknownPeer
	}
	if time.Now().Before(ps.downUntil) {
		p.mu.Unlock()
		return nil, ErrPeerDown
	}
	c := &dialCall{done: make(chan struct{})}
	ps.syncDial = c
	addr := ps.addr
	ps.dialing[vlink.ModeMux]++
	p.mu.Unlock()
	// 建链不随单个会话取消：多个会话可能在等同一次建链
	c.l, c.err = p.dial(id, addr, vlink.ModeMux)
	p.mu.Lock()
	ps.syncDial = nil
	p.mu.Unlock()
	close(c.done)
	return c.l, c.err
}

// dial 建立一条链路；调用前已为该模式记入 dialing。
func (p *Peers) dial(id types.UUID, addr string, mode vlink.LinkMode) (*linkInfo, error) {
	li, err := p.dialOnce(id, addr, mode)
	p.mu.Lock()
	ps := p.state(id)
	ps.dialing[mode]--
	if err != nil {
		ps.downUntil = time.Now().Add(p.DownBackoff)
		ps.wake()
	}
	p.mu.Unlock()
	if err != nil {
		p.Emit(types.Event{Kind: types.EventPeerDown, PeerID: id, Detail: err.Error()})
	}
	return li, err
}

func (p *Peers) dialOnce(id types.UUID, addr string, mode vlink.LinkMode) (*linkInfo, error) {
	ctx, cancel := context.WithTimeout(p.root.Context(), p.DialTimeout)
	defer cancel()
	var d net.Dialer
	nc, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	l := p.Local()
	l.Mode = mode
	est, err := vlink.Handshake(ctx, tls.Client(nc, vlink.ClientConfig(l.Identity, l.Trust, id)), l, vlink.Expect{NodeID: id})
	if err != nil {
		return nil, err
	}
	li := p.register(est, true, mode)
	if li == nil {
		return nil, lifecycle.ErrClosed
	}
	return li, nil
}

// ---------------------------------------------------------------- 维护：预热、扩缩

func (p *Peers) maintainLoop(ctx context.Context) {
	t := time.NewTicker(p.Tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-p.kick:
		}
		p.maintain()
	}
}

type dialReq struct {
	id   types.UUID
	addr string
	mode vlink.LinkMode
}

// maintain 对每个主动连接的对端：mux 链路补足下限，全部繁忙时增加（不超过上限），超过下限的空闲链路
// 空闲 IdleClose 后关闭；pool 空闲链路补足或缩减到目标值。
func (p *Peers) maintain() {
	now := time.Now()
	var dials []dialReq
	var closeL []*vlink.Link
	p.mu.Lock()
	for _, ps := range p.peers {
		for _, li := range ps.links {
			if li.l.NumStreams() > 0 {
				li.idleSince = time.Time{}
			} else if li.idleSince.IsZero() {
				li.idleSince = now
			}
		}
		if ps.addr == "" || now.Before(ps.downUntil) {
			continue
		}
		// mux
		var mux []*linkInfo
		busy := 0
		half := int64(p.LinkOptions.LinkWindow)
		if half == 0 {
			half = vlink.DefaultLinkWindow
		}
		half /= 2
		for _, li := range ps.links {
			if !li.pool && li.l.Usable() {
				mux = append(mux, li)
				if li.l.InFlight() >= half {
					busy++
				}
			}
		}
		have := len(mux) + ps.dialing[vlink.ModeMux]
		switch {
		case have < p.MinMux:
			for i := have; i < p.MinMux; i++ {
				dials = append(dials, dialReq{ps.id, ps.addr, vlink.ModeMux})
				ps.dialing[vlink.ModeMux]++
			}
		case len(mux) > 0 && busy == len(mux) && have < p.MaxMux:
			dials = append(dials, dialReq{ps.id, ps.addr, vlink.ModeMux})
			ps.dialing[vlink.ModeMux]++
		case len(mux) > p.MinMux:
			extra := len(mux) - p.MinMux
			for _, li := range mux {
				if extra == 0 {
					break
				}
				if !li.idleSince.IsZero() && now.Sub(li.idleSince) >= p.IdleClose {
					closeL = append(closeL, li.l)
					extra--
				}
			}
		}
		// pool
		if now.Sub(ps.peakAt) >= time.Minute {
			ps.prevPeak, ps.peak, ps.peakAt = ps.peak, ps.poolBusy, now
		}
		target := 0
		if peak := max(ps.peak, ps.prevPeak); ps.mode == types.LinkModePool || peak > 0 {
			target = max(2, int(math.Ceil(float64(peak)*p.PoolRatio)))
			target = min(target, p.PoolMax)
		}
		var idle []*linkInfo
		for _, li := range ps.poolIdle {
			if li.l.Usable() {
				idle = append(idle, li)
			}
		}
		ps.poolIdle = idle
		switch n := len(idle) + ps.dialing[vlink.ModePool]; {
		case n < target:
			for i := n; i < target; i++ {
				dials = append(dials, dialReq{ps.id, ps.addr, vlink.ModePool})
				ps.dialing[vlink.ModePool]++
			}
		case len(idle) > target:
			for _, li := range idle[target:] {
				closeL = append(closeL, li.l)
			}
			ps.poolIdle = idle[:target]
		}
	}
	p.mu.Unlock()
	for _, l := range closeL {
		_ = l.Close()
	}
	for _, d := range dials {
		d := d
		if p.root.Go(func(context.Context) { _, _ = p.dial(d.id, d.addr, d.mode) }) != nil {
			p.mu.Lock()
			p.state(d.id).dialing[d.mode]--
			p.mu.Unlock()
		}
	}
}

// PeerStats 是到某对端的链路情况（测试与指标用）。
type PeerStats struct {
	Mux, PoolIdle, PoolBusy int
}

func (p *Peers) Stats(id types.UUID) PeerStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	ps := p.peers[id]
	if ps == nil {
		return PeerStats{}
	}
	st := PeerStats{PoolIdle: len(ps.poolIdle), PoolBusy: ps.poolBusy}
	for _, li := range ps.links {
		switch {
		case li.pool:
		case li.l.Usable():
			st.Mux++
		}
	}
	return st
}

// NumLinks 返回到对端的链路数（全部模式）。
func (p *Peers) NumLinks(id types.UUID) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if ps := p.peers[id]; ps != nil {
		return len(ps.links)
	}
	return 0
}

// PeerCert 是测试辅助：返回链路上对端的叶子证书。
func PeerCert(l *vlink.Link) *x509.Certificate {
	cs := l.ConnectionState()
	if len(cs.PeerCertificates) == 0 {
		return nil
	}
	return cs.PeerCertificates[0]
}
