package route

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"vortex/engine/internal/lifecycle"
	"vortex/engine/pipe"
	"vortex/engine/types"
)

// udpEcho 是回显目标；记录看到的源地址（每个会话应是不同的已连接 socket，ENG-042）。
type udpEcho struct {
	c       *net.UDPConn
	mu      sync.Mutex
	sources map[netip.AddrPort]int
	packets atomic.Int64
	last    []byte
}

func newUDPEcho(t *testing.T) *udpEcho {
	c, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadBuffer(4 << 20)
	u := &udpEcho{c: c, sources: map[netip.AddrPort]int{}}
	go func() {
		buf := make([]byte, 65536)
		for {
			n, from, err := c.ReadFromUDPAddrPort(buf)
			if err != nil {
				return
			}
			u.packets.Add(1)
			u.mu.Lock()
			u.sources[from]++
			u.last = append(u.last[:0], buf[:n]...)
			u.mu.Unlock()
			_, _ = c.WriteToUDPAddrPort(buf[:n], from)
		}
	}()
	t.Cleanup(func() { c.Close() })
	return u
}

func (u *udpEcho) tgt(w, p uint32) types.Target {
	ap := netip.MustParseAddrPort(u.c.LocalAddr().String())
	return types.Target{Host: ap.Addr().String(), Port: ap.Port(), Weight: w, Priority: p}
}

func (u *udpEcho) distinctSources() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.sources)
}

type udpFixture struct {
	e     *UDPEntry
	scope *lifecycle.Scope
	addr  string
	evs   chan types.Event
}

func newUDPEntry(t *testing.T, rule types.Rule, adm Admission) *udpFixture {
	rule.RuleID = types.UUID{0xd2}
	rule.RuleVersion = 1
	var ts Tables
	ts.Swap(NewTable(1, []types.Rule{rule}))
	r, err := ReserveUDP(netip.MustParseAddrPort("127.0.0.1:0"))
	if err != nil {
		t.Fatal(err)
	}
	conn, _, err := r.CommitUDP()
	if err != nil {
		t.Fatal(err)
	}
	root := lifecycle.NewRoot(context.Background())
	evs := make(chan types.Event, 64)
	e := &UDPEntry{RuleID: rule.RuleID, Tables: &ts, Conn: conn, Scope: root, Admission: adm,
		Resolver: NewResolver(nil, 0), Guard: NewGuard(types.TargetPolicy{SelfHostedNode: true}, nil, []uint16{7443}),
		Events: func(ev types.Event) { evs <- ev }}
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close(); r.releasePort() })
	return &udpFixture{e: e, scope: root, addr: conn.LocalAddr().String(), evs: evs}
}

func udpClient(t *testing.T, addr string) *net.UDPConn {
	c, err := net.Dial("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.(*net.UDPConn).SetReadBuffer(4 << 20)
	t.Cleanup(func() { c.Close() })
	return c.(*net.UDPConn)
}

// exchange 发送一个数据报并等待回显。
func exchange(c *net.UDPConn, p []byte) ([]byte, error) {
	_ = c.SetDeadline(time.Now().Add(time.Second))
	if _, err := c.Write(p); err != nil {
		return nil, err
	}
	buf := make([]byte, 65536)
	n, err := c.Read(buf)
	return buf[:n], err
}

func TestUDPEchoIntegrity(t *testing.T) {
	echo := newUDPEcho(t)
	f := newUDPEntry(t, types.Rule{Targets: []types.Target{echo.tgt(1, 0)}}, nil)
	c := udpClient(t, f.addr)
	for i, size := range []int{1, 1200, 1472, 8192, 60000} {
		p := bytes.Repeat([]byte{byte(i + 1)}, size)
		got, err := exchange(c, p)
		if err != nil || !bytes.Equal(got, p) {
			t.Fatalf("size %d: %v (got %d bytes)", size, err, len(got))
		}
	}
	if f.e.Sessions() != 1 {
		t.Fatalf("one client must be one session, got %d", f.e.Sessions())
	}
}

func TestUDPSessionPerClientAndWeights(t *testing.T) {
	a, b := newUDPEcho(t), newUDPEcho(t)
	adm := &hookAdmission{hook: pipe.NopHook{}}
	f := newUDPEntry(t, types.Rule{Targets: []types.Target{a.tgt(3, 0), b.tgt(1, 0)}}, adm)
	for i := 0; i < 40; i++ {
		c := udpClient(t, f.addr)
		for j := 0; j < 3; j++ { // 同一会话的多个数据报必须去往同一目标（RUL-009）
			if _, err := exchange(c, []byte("hi")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if f.e.Sessions() != 40 {
		t.Fatalf("sessions %d, want 40", f.e.Sessions())
	}
	// 每个会话一个独立的已连接 socket（ENG-042）；按会话而非按包选择目标（RUL-008）：3:1 → 30/10
	if a.distinctSources() != 30 || b.distinctSources() != 10 {
		t.Fatalf("sessions per target %d/%d, want 30/10", a.distinctSources(), b.distinctSources())
	}
	if a.packets.Load()+b.packets.Load() != 120 {
		t.Fatalf("packets %d", a.packets.Load()+b.packets.Load())
	}
	// 准入每个会话只做一次，不是每个数据报一次（门控开销与 ENG-024 语义）
	if adm.admitted.Load() != 40 || f.e.Stats.SessionsCreated.Load() != 40 {
		t.Fatalf("admitted %d, sessions created %d; want 40 each", adm.admitted.Load(), f.e.Stats.SessionsCreated.Load())
	}
}

func TestUDPIdleReclaimAndRecreate(t *testing.T) {
	echo := newUDPEcho(t)
	f := newUDPEntry(t, types.Rule{Targets: []types.Target{echo.tgt(1, 0)}, UDPIdle: 200 * time.Millisecond}, nil)
	c := udpClient(t, f.addr)
	// 持续有流量时不回收
	for i := 0; i < 6; i++ {
		if _, err := exchange(c, []byte("keep")); err != nil {
			t.Fatal(err)
		}
		time.Sleep(80 * time.Millisecond)
	}
	if f.e.Stats.SessionsCreated.Load() != 1 {
		t.Fatalf("active session reclaimed: created %d", f.e.Stats.SessionsCreated.Load())
	}
	time.Sleep(450 * time.Millisecond)
	if f.e.Sessions() != 0 {
		t.Fatalf("idle session not reclaimed (ENG-006): %d", f.e.Sessions())
	}
	if _, err := exchange(c, []byte("again")); err != nil {
		t.Fatal(err)
	}
	if f.e.Stats.SessionsCreated.Load() != 2 || echo.distinctSources() != 2 {
		t.Fatalf("new session expected after idle: created %d sources %d", f.e.Stats.SessionsCreated.Load(), echo.distinctSources())
	}
}

// packetHook 每次最多授予 limit 字节，统计写成功字节与退回次数。
type packetHook struct {
	limit    int
	up, down atomic.Int64
	refunds  atomic.Int64
}

func (h *packetHook) Acquire(_ context.Context, _ types.Direction, want int) (int, error) {
	if want > h.limit {
		return h.limit, nil
	}
	return want, nil
}

func (h *packetHook) Release(d types.Direction, granted, written int) {
	if written == 0 && granted > 0 {
		h.refunds.Add(1)
	}
	if d == types.DirUp {
		h.up.Add(int64(written))
	} else {
		h.down.Add(int64(written))
	}
}

type hookAdmission struct {
	hook     pipe.Hook
	deny     atomic.Bool
	admitted atomic.Int64
	released atomic.Int64
}

func (a *hookAdmission) Admit(context.Context, *types.Rule, netip.AddrPort) (pipe.Hook, func(), error) {
	if a.deny.Load() {
		return nil, nil, ErrNoRoute
	}
	a.admitted.Add(1)
	return a.hook, func() { a.released.Add(1) }, nil
}

func TestUDPWholePacketMetering(t *testing.T) {
	echo := newUDPEcho(t)
	h := &packetHook{limit: 1000}
	f := newUDPEntry(t, types.Rule{Targets: []types.Target{echo.tgt(1, 0)}}, &hookAdmission{hook: h})
	c := udpClient(t, f.addr)
	if got, err := exchange(c, make([]byte, 800)); err != nil || len(got) != 800 {
		t.Fatalf("800-byte packet: %v", err)
	}
	// 1200 字节超过单次授予额度：整包丢弃，不得截断发送（ENG-038）
	before := echo.packets.Load()
	_ = c.SetDeadline(time.Now().Add(300 * time.Millisecond))
	_, _ = c.Write(make([]byte, 1200))
	if _, err := c.Read(make([]byte, 2000)); err == nil {
		t.Fatal("oversized packet must not be forwarded")
	}
	if echo.packets.Load() != before {
		t.Fatal("target received a truncated or oversized packet")
	}
	if h.up.Load() != 800 || h.down.Load() != 800 || h.refunds.Load() != 1 || f.e.Stats.DroppedQuota.Load() != 1 {
		t.Fatalf("metered up %d down %d refunds %d dropped %d", h.up.Load(), h.down.Load(), h.refunds.Load(), f.e.Stats.DroppedQuota.Load())
	}
}

func TestUDPAdmissionDenyAndGuard(t *testing.T) {
	echo := newUDPEcho(t)
	adm := &hookAdmission{hook: pipe.NopHook{}}
	adm.deny.Store(true)
	f := newUDPEntry(t, types.Rule{Targets: []types.Target{echo.tgt(1, 0)}}, adm)
	c := udpClient(t, f.addr)
	_ = c.SetDeadline(time.Now().Add(200 * time.Millisecond))
	_, _ = c.Write([]byte("x"))
	if _, err := c.Read(make([]byte, 10)); err == nil || echo.packets.Load() != 0 {
		t.Fatal("denied client must not reach target")
	}
	// 目标指向本机 link 端口：SEC-011 拒绝并产生审计事件
	g := newUDPEntry(t, types.Rule{Targets: []types.Target{{Host: "127.0.0.1", Port: 7443, Weight: 1}}}, nil)
	c2 := udpClient(t, g.addr)
	_, _ = c2.Write([]byte("x"))
	select {
	case ev := <-g.evs:
		if ev.Kind != types.EventTargetRejected {
			t.Fatalf("event %v", ev.Kind)
		}
	case <-time.After(time.Second):
		t.Fatal("no TargetRejected event")
	}
	if g.e.Sessions() != 0 {
		t.Fatal("no session may be created for a forbidden target")
	}
}

func TestUDPShutdownBudgetAndLeak(t *testing.T) {
	g0 := lifecycle.LiveGoroutines()
	echo := newUDPEcho(t)
	adm := &hookAdmission{hook: pipe.NopHook{}}
	f := newUDPEntry(t, types.Rule{Targets: []types.Target{echo.tgt(1, 0)}}, adm)
	for i := 0; i < 25; i++ {
		if _, err := exchange(udpClient(t, f.addr), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	// 每个 UDP 监听 1 个 + 每个会话 1 个（ENG-045）
	if got := lifecycle.LiveGoroutines() - g0; got != 26 {
		t.Fatalf("goroutines %d, want 26", got)
	}
	_ = f.scope.Close()
	if lifecycle.LiveGoroutines() != g0 || f.e.Sessions() != 0 || adm.released.Load() != 25 {
		t.Fatalf("after shutdown: goroutines %d sessions %d released %d",
			lifecycle.LiveGoroutines()-g0, f.e.Sessions(), adm.released.Load())
	}
}

// TestUDPLossAtRate 在开发机回环上以 1,200 字节数据报、约 100 Mbps 发送 3 秒，测量往返丢包。
// 这不是 NFR-005 的验收（需 13.2 节环境与正式计量门控），只用于发现明显回归。
func TestUDPLossAtRate(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	echo := newUDPEcho(t)
	f := newUDPEntry(t, types.Rule{Targets: []types.Target{echo.tgt(1, 0)}}, nil)
	c := udpClient(t, f.addr)
	const size, pps, dur = 1200, 10_400, 3 * time.Second // 10,400 pps × 1,200 B × 8 ≈ 100 Mbps
	var recv atomic.Int64
	seen := make([]atomic.Bool, int(pps*dur/time.Second)+1)
	go func() {
		buf := make([]byte, 2048)
		for {
			_ = c.SetReadDeadline(time.Now().Add(time.Second))
			n, err := c.Read(buf)
			if err != nil {
				return
			}
			if n == size {
				seq := binary.BigEndian.Uint32(buf)
				if int(seq) < len(seen) && !seen[seq].Swap(true) {
					recv.Add(1)
				}
			}
		}
	}()
	p := make([]byte, size)
	sent := 0
	start := time.Now()
	for time.Since(start) < dur && sent < len(seen) {
		due := int(float64(time.Since(start)) / float64(time.Second) * pps)
		for ; sent < due && sent < len(seen); sent++ {
			binary.BigEndian.PutUint32(p, uint32(sent))
			_, _ = c.Write(p)
		}
		time.Sleep(200 * time.Microsecond)
	}
	time.Sleep(500 * time.Millisecond)
	loss := 1 - float64(recv.Load())/float64(sent)
	t.Logf("sent %d, received %d unique, round-trip loss %.4f%%（开发机回环）", sent, recv.Load(), loss*100)
	if loss > 0.01 {
		t.Fatalf("round-trip loss %.3f%% exceeds 1%% regression guard", loss*100)
	}
}
