// Package tunnel 是隧道转发：节点间 VLink 链路的建立与管理、按下一组成员开流、入站流的开流检查
// （ENG-020～ENG-023）以及出口连接目标。
package tunnel

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
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

// Peers 管理到各对端节点的链路（M1 第一阶段：每个对端一条 mux 链路，ENG-017 的多链路与 pool 在 ENG-T27）。
// 链路建立后双向对等：任一方都可以在上面开流（ENG-018）。
type Peers struct {
	Local       func() *vlink.Local // 每次握手时取（代次随配置变化）
	Handle      InboundHandler
	Emit        func(types.Event)
	LinkOptions vlink.LinkOptions
	DialTimeout time.Duration // 默认 5 秒
	DownBackoff time.Duration // 连接失败后多久内视为不可用，默认 5 秒

	root *lifecycle.Scope

	mu        sync.Mutex
	addrs     map[types.UUID]string // 本端主动连接的对端（Peer.Outbound）
	known     map[types.UUID]bool   // 运行配置中出现的全部对端
	links     map[types.UUID][]*vlink.Link
	dialing   map[types.UUID]*dialCall
	downUntil map[types.UUID]time.Time
}

type dialCall struct {
	done chan struct{}
	l    *vlink.Link
	err  error
}

// Start 绑定生命周期；ln 非 nil 时在其上接受入站链路。
func (p *Peers) Start(root *lifecycle.Scope, ln net.Listener) error {
	p.root = root
	p.addrs, p.known, p.links = map[types.UUID]string{}, map[types.UUID]bool{}, map[types.UUID][]*vlink.Link{}
	p.dialing, p.downUntil = map[types.UUID]*dialCall{}, map[types.UUID]time.Time{}
	if p.DialTimeout == 0 {
		p.DialTimeout = 5 * time.Second
	}
	if p.DownBackoff == 0 {
		p.DownBackoff = 5 * time.Second
	}
	if p.Emit == nil {
		p.Emit = func(types.Event) {}
	}
	if ln == nil {
		return nil
	}
	if err := root.Own(ln); err != nil {
		return err
	}
	return root.Go(func(ctx context.Context) { p.acceptLoop(ctx, ln) })
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
				p.register(est, false)
			}
		}) != nil {
			_ = c.Close()
		}
	}
}

// register 启动链路并登记；每条链路一个接受入站流的 goroutine（另有链路自身的读写 goroutine）。
func (p *Peers) register(est *vlink.Established, initiator bool) *vlink.Link {
	opt := p.LinkOptions
	opt.Initiator = initiator
	l := vlink.NewLink(est, opt)
	peer := est.Peer.NodeID
	ls := p.root.Child()
	if ls.Own(closer{l}) != nil {
		return nil
	}
	p.mu.Lock()
	p.links[peer] = append(p.links[peer], l)
	delete(p.downUntil, peer)
	p.mu.Unlock()
	if est.Peer.ServedMismatch {
		p.Emit(types.Event{Kind: types.EventPeerDown, PeerID: peer, Detail: "served_digest mismatch"})
	}
	err := ls.Go(func(ctx context.Context) {
		defer ls.Shutdown()
		defer p.unregister(peer, l)
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
		p.unregister(peer, l)
		return nil
	}
	return l
}

func (p *Peers) unregister(peer types.UUID, l *vlink.Link) {
	p.mu.Lock()
	ls := p.links[peer]
	for i, x := range ls {
		if x == l {
			ls = append(ls[:i], ls[i+1:]...)
			break
		}
	}
	if len(ls) == 0 {
		delete(p.links, peer)
	} else {
		p.links[peer] = ls
	}
	p.mu.Unlock()
	p.Emit(types.Event{Kind: types.EventPeerDown, PeerID: peer, Detail: fmt.Sprint(l.Err())})
}

type closer struct{ l *vlink.Link }

func (c closer) Close() error { return c.l.Close() }

// SetPeers 更新对端地址；已不在运行配置中的对端的链路被关闭。
func (p *Peers) SetPeers(peers []types.Peer) {
	addrs, known := map[types.UUID]string{}, map[types.UUID]bool{}
	for _, pr := range peers {
		known[pr.NodeID] = true
		if pr.Outbound && pr.Address != "" {
			addrs[pr.NodeID] = net.JoinHostPort(pr.Address, strconv.Itoa(int(pr.LinkPort)))
		}
	}
	p.mu.Lock()
	p.addrs, p.known = addrs, known
	var stale []*vlink.Link
	for id, ls := range p.links {
		if !known[id] {
			stale = append(stale, ls...)
		}
	}
	p.mu.Unlock()
	for _, l := range stale {
		_ = l.Close()
	}
}

// CloseRevoked 关闭对端证书或节点已被吊销的链路（GOAWAY revoked，ENG-010）。
func (p *Peers) CloseRevoked(rev *vlink.Revocations) {
	p.mu.Lock()
	var bad []*vlink.Link
	for id, ls := range p.links {
		for _, l := range ls {
			cs := l.ConnectionState()
			if len(cs.PeerCertificates) > 0 && rev.Revoked(cs.PeerCertificates[0], id) {
				bad = append(bad, l)
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
	return time.Now().After(p.downUntil[id])
}

// Link 返回到对端的可用链路；没有时建立（同一对端的并发请求共用一次建链）。
func (p *Peers) Link(ctx context.Context, id types.UUID) (*vlink.Link, error) {
	p.mu.Lock()
	ls := p.links[id]
	for i := len(ls) - 1; i >= 0; i-- {
		if ls[i].Usable() {
			l := ls[i]
			p.mu.Unlock()
			return l, nil
		}
	}
	if c := p.dialing[id]; c != nil {
		p.mu.Unlock()
		select {
		case <-c.done:
			return c.l, c.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	addr, ok := p.addrs[id]
	if !ok {
		p.mu.Unlock()
		return nil, ErrUnknownPeer
	}
	if time.Now().Before(p.downUntil[id]) {
		p.mu.Unlock()
		return nil, ErrPeerDown
	}
	c := &dialCall{done: make(chan struct{})}
	p.dialing[id] = c
	p.mu.Unlock()

	// 建链不随单个会话取消：多个会话可能在等同一次建链
	c.l, c.err = p.dial(id, addr)
	p.mu.Lock()
	delete(p.dialing, id)
	if c.err != nil {
		p.downUntil[id] = time.Now().Add(p.DownBackoff)
	}
	p.mu.Unlock()
	close(c.done)
	if c.err != nil {
		p.Emit(types.Event{Kind: types.EventPeerDown, PeerID: id, Detail: c.err.Error()})
	}
	return c.l, c.err
}

func (p *Peers) dial(id types.UUID, addr string) (*vlink.Link, error) {
	ctx, cancel := context.WithTimeout(p.root.Context(), p.DialTimeout)
	defer cancel()
	var d net.Dialer
	nc, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	l := p.Local()
	est, err := vlink.Handshake(ctx, tls.Client(nc, vlink.ClientConfig(l.Identity, l.Trust, id)), l, vlink.Expect{NodeID: id})
	if err != nil {
		return nil, err
	}
	link := p.register(est, true)
	if link == nil {
		return nil, lifecycle.ErrClosed
	}
	return link, nil
}

// NumLinks 返回到对端的链路数（测试与指标用）。
func (p *Peers) NumLinks(id types.UUID) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.links[id])
}

// PeerCert 是测试辅助：返回链路上对端的叶子证书。
func PeerCert(l *vlink.Link) *x509.Certificate {
	cs := l.ConnectionState()
	if len(cs.PeerCertificates) == 0 {
		return nil
	}
	return cs.PeerCertificates[0]
}
