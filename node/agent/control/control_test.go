package control

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"vortex/engine/types"
	"vortex/node/agent"
)

var (
	hA = types.UUID{0xa}
	hB = types.UUID{0xb}
	hC = types.UUID{0xc}
)

// memHub 是内存中的 Edge Hub：回 HubHello 声明 mode；记录每条流收到的消息。
type memHub struct {
	mu      sync.Mutex
	id      types.UUID
	mode    agent.Mode
	down    bool
	streams []*memConn
	dials   int
}

type memConn struct {
	hub    *memHub
	in     chan agent.In
	mu     sync.Mutex
	got    []agent.Out
	closed bool
	done   chan struct{}
}

func (c *memConn) Send(m agent.Out) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("closed")
	}
	c.got = append(c.got, m)
	if _, ok := m.(agent.Hello); ok {
		c.hub.mu.Lock()
		mode := c.hub.mode
		c.hub.mu.Unlock()
		c.in <- agent.HubHello{Hub: c.hub.id, Mode: mode}
	}
	return nil
}

func (c *memConn) Recv(ctx context.Context) (agent.In, error) {
	select {
	case m := <-c.in:
		return m, nil
	case <-c.done:
		return nil, errors.New("stream closed")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *memConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		close(c.done)
	}
	return nil
}

func (c *memConn) hellos() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, m := range c.got {
		if _, ok := m.(agent.Hello); ok {
			n++
		}
	}
	return n
}

func (c *memConn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// kill 结束该实例全部流；down 为 true 时之后的连接也被拒绝（实例宕机）。
func (h *memHub) kill(down bool) {
	h.mu.Lock()
	h.down = down
	ss := append([]*memConn(nil), h.streams...)
	h.mu.Unlock()
	for _, s := range ss {
		s.Close()
	}
}

func (h *memHub) setMode(m agent.Mode) {
	h.mu.Lock()
	h.mode = m
	ss := append([]*memConn(nil), h.streams...)
	h.mu.Unlock()
	for _, s := range ss {
		if !s.isClosed() {
			s.in <- agent.ModeChange{Mode: m}
		}
	}
}

type memDialer struct{ hubs map[types.UUID]*memHub }

func (d memDialer) Dial(_ context.Context, ep Endpoint) (Conn, error) {
	h := d.hubs[ep.ID]
	h.mu.Lock()
	defer h.mu.Unlock()
	h.dials++
	if h.down {
		return nil, errors.New("connection refused")
	}
	c := &memConn{hub: h, in: make(chan agent.In, 64), done: make(chan struct{})}
	h.streams = append(h.streams, c)
	return c, nil
}

// fakeAgent 记录 Controller 的调用。
type fakeAgent struct {
	mu        sync.Mutex
	connected int
	adopted   int
	hellos    []types.UUID // 收到的 HubHello 的实例
}

func (a *fakeAgent) Hello() agent.Hello { return agent.Hello{Node: types.UUID{1}} }
func (a *fakeAgent) Connected(s agent.Session) {
	a.mu.Lock()
	a.connected++
	a.mu.Unlock()
	_ = s.Send(a.Hello())
}
func (a *fakeAgent) Adopt(agent.Session) { a.mu.Lock(); a.adopted++; a.mu.Unlock() }
func (a *fakeAgent) Disconnected()       {}
func (a *fakeAgent) HandleHub(_ context.Context, m agent.In) {
	if hh, ok := m.(agent.HubHello); ok {
		a.mu.Lock()
		a.hellos = append(a.hellos, hh.Hub)
		a.mu.Unlock()
	}
}

func setup(modes map[types.UUID]agent.Mode) (*Controller, map[types.UUID]*memHub, *fakeAgent) {
	hubs := map[types.UUID]*memHub{}
	var eps []Endpoint
	for _, id := range []types.UUID{hA, hB, hC} {
		if m, ok := modes[id]; ok {
			hubs[id] = &memHub{id: id, mode: m}
			eps = append(eps, Endpoint{ID: id})
		}
	}
	fa := &fakeAgent{}
	c := &Controller{Agent: fa, Hubs: eps, Dialer: memDialer{hubs}, BackoffMin: 5 * time.Millisecond,
		BackoffMax: 40 * time.Millisecond, ProbeEvery: 20 * time.Millisecond, HelloTimeout: 100 * time.Millisecond}
	return c, hubs, fa
}

func waitFor(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func run(c *Controller) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	return func() { cancel(); <-done }
}

// 首个实例不可达：退避并轮换到下一个；该实例宕机后再次轮换。
func TestRotateOnFailure(t *testing.T) {
	c, hubs, _ := setup(map[types.UUID]agent.Mode{hA: agent.ModeOnline, hB: agent.ModeOnline})
	hubs[hA].down = true
	stop := run(c)
	defer stop()
	waitFor(t, "connected to B", func() bool { cur, _ := c.Current(); return cur == hB })
	hubs[hA].mu.Lock()
	hubs[hA].down = false
	hubs[hA].mu.Unlock()
	hubs[hB].kill(false) // 只断流、实例仍可连：断线后也轮换到下一个实例
	waitFor(t, "rotated back to A", func() bool { cur, _ := c.Current(); return cur == hA })
	hubs[hA].kill(true)
	waitFor(t, "rotated to B after A went down", func() bool { cur, _ := c.Current(); return cur == hB })
}

// HUB-014：当前实例声明自治、同区域有在线实例时切换；探测流转正，不重复 Hello；原连接关闭。
func TestSwitchToOnlineHub(t *testing.T) {
	c, hubs, fa := setup(map[types.UUID]agent.Mode{hA: agent.ModeOnline, hB: agent.ModeAutonomous, hC: agent.ModeOnline})
	stop := run(c)
	defer stop()
	waitFor(t, "connected to A", func() bool { cur, m := c.Current(); return cur == hA && m == agent.ModeOnline })
	time.Sleep(3 * c.ProbeEvery)
	if hubs[hB].dials+hubs[hC].dials != 0 {
		t.Fatal("probed while the current hub is online")
	}
	start := time.Now()
	hubs[hA].setMode(agent.ModeAutonomous)
	waitFor(t, "switched to C", func() bool { cur, m := c.Current(); return cur == hC && m == agent.ModeOnline })
	if d := time.Since(start); d > 10*c.ProbeEvery {
		t.Fatalf("switch took %v", d)
	}
	fa.mu.Lock()
	adopted, connected, last := fa.adopted, fa.connected, fa.hellos[len(fa.hellos)-1]
	fa.mu.Unlock()
	if adopted != 1 || connected != 1 || last != hC {
		t.Fatalf("adopted %d connected %d last hello %v", adopted, connected, last)
	}
	hubs[hC].mu.Lock()
	cs := hubs[hC].streams[0]
	hubs[hC].mu.Unlock()
	if cs.hellos() != 1 {
		t.Fatalf("adopted stream got %d hellos", cs.hellos())
	}
	waitFor(t, "old stream closed", func() bool { return hubs[hA].streams[0].isClosed() })
	for _, s := range hubs[hB].streams {
		if !s.isClosed() {
			t.Fatal("probe stream to an autonomous hub left open")
		}
	}
}

// 全部实例都在自治：保持当前连接，探测流都关闭。
func TestStayWhenNoOnlineHub(t *testing.T) {
	c, hubs, _ := setup(map[types.UUID]agent.Mode{hA: agent.ModeAutonomous, hB: agent.ModeAutonomous})
	stop := run(c)
	waitFor(t, "probed B", func() bool { hubs[hB].mu.Lock(); defer hubs[hB].mu.Unlock(); return hubs[hB].dials >= 2 })
	if cur, _ := c.Current(); cur != hA {
		t.Fatalf("left the current hub for an autonomous one: %v", cur)
	}
	if hubs[hA].streams[0].isClosed() || len(hubs[hA].streams) != 1 {
		t.Fatal("current stream closed")
	}
	stop() // 探测是同步的：停止后不会有进行中的探测
	hubs[hB].mu.Lock()
	defer hubs[hB].mu.Unlock()
	for _, s := range hubs[hB].streams {
		if !s.isClosed() {
			t.Fatal("probe stream left open")
		}
	}
}

func TestBackoffGrowsAndJitters(t *testing.T) {
	c, hubs, _ := setup(map[types.UUID]agent.Mode{hA: agent.ModeOnline})
	hubs[hA].down = true
	c.BackoffMin, c.BackoffMax = 10*time.Millisecond, 40*time.Millisecond
	stop := run(c)
	time.Sleep(300 * time.Millisecond)
	stop()
	hubs[hA].mu.Lock()
	n := hubs[hA].dials
	hubs[hA].mu.Unlock()
	// 10 + 20 + 40 + 40 + ... ≈ 7～9 次；无退避时会是数千次
	if n < 4 || n > 14 {
		t.Fatalf("%d dials in 300 ms", n)
	}
	c.defaults()
	for i := 0; i < 1000; i++ {
		if d := c.jitter(time.Second); d < 800*time.Millisecond || d > 1200*time.Millisecond {
			t.Fatalf("jitter %v outside ±20%%", d)
		}
	}
}
