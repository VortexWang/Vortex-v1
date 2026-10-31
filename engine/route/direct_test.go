package route

import (
	"context"
	"errors"
	"io"
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

// target 是一个计数的回显服务，模拟最终目标。
type target struct {
	ln    net.Listener
	conns atomic.Int64
}

func newTarget(t *testing.T) *target {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tg := &target{ln: ln}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			tg.conns.Add(1)
			go func() { defer c.Close(); _, _ = io.Copy(c, c) }()
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return tg
}

func (tg *target) tgt(weight, prio uint32) types.Target {
	ap := netip.MustParseAddrPort(tg.ln.Addr().String())
	return types.Target{Host: ap.Addr().String(), Port: ap.Port(), Weight: weight, Priority: prio}
}

type fixture struct {
	t      *testing.T
	entry  *DirectEntry
	scope  *lifecycle.Scope
	addr   string
	events chan types.Event
	tables *Tables
	ruleID types.UUID
}

// newEntry 按 Prepare → Commit 的顺序建立一个直接转发入口。
// Guard 使用自托管节点策略：允许回环目标（本测试的目标都在 127.0.0.1），但仍禁止本机 link 端口 7443。
func newEntry(t *testing.T, targets []types.Target, sched types.Schedule, adm Admission) *fixture {
	ruleID := types.UUID{0xd1}
	var ts Tables
	ts.Swap(NewTable(1, []types.Rule{{RuleID: ruleID, RuleVersion: 1, Role: types.RoleEntry, EnforcementPoint: true,
		Targets: targets, TargetSchedule: sched}}))
	res, err := ReserveTCP(netip.MustParseAddrPort("127.0.0.1:0"))
	if err != nil {
		t.Fatal(err)
	}
	_ = res // 端口 0 由内核分配；下面从 listener 取实际地址
	ln, err := res.CommitTCP(1024)
	if err != nil {
		t.Fatal(err)
	}
	root := lifecycle.NewRoot(context.Background())
	evs := make(chan types.Event, 64)
	e := &DirectEntry{
		RuleID: ruleID, Tables: &ts, Listener: ln, Scope: root, Admission: adm,
		Resolver: NewResolver(nil, 0),
		Guard:    NewGuard(types.TargetPolicy{SelfHostedNode: true}, nil, []uint16{7443}),
		Events:   func(ev types.Event) { evs <- ev },
	}
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, entry: e, scope: root, addr: ln.Addr().String(), events: evs, tables: &ts, ruleID: ruleID}
	t.Cleanup(func() { _ = root.Close() })
	return f
}

// roundTrip 建立一次会话并回显一段数据。
func (f *fixture) roundTrip(msg string) (string, error) {
	c, err := net.DialTimeout("tcp", f.addr, time.Second)
	if err != nil {
		return "", err
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := c.Write([]byte(msg)); err != nil {
		return "", err
	}
	_ = c.(*net.TCPConn).CloseWrite()
	b, err := io.ReadAll(c)
	return string(b), err
}

func TestDirectForwardEcho(t *testing.T) {
	tg := newTarget(t)
	f := newEntry(t, []types.Target{tg.tgt(1, 0)}, types.ScheduleWeightedRoundRobin, nil)
	got, err := f.roundTrip("hello vortex")
	if err != nil || got != "hello vortex" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestDirectTargetWeights(t *testing.T) {
	a, b := newTarget(t), newTarget(t)
	f := newEntry(t, []types.Target{a.tgt(3, 0), b.tgt(1, 0)}, types.ScheduleWeightedRoundRobin, nil)
	for i := 0; i < 400; i++ {
		if _, err := f.roundTrip("x"); err != nil {
			t.Fatal(err)
		}
	}
	if a.conns.Load() != 300 || b.conns.Load() != 100 {
		t.Fatalf("connections %d/%d, want 300/100 (RUL-007)", a.conns.Load(), b.conns.Load())
	}
}

func TestDirectFailoverToNextTarget(t *testing.T) {
	alive := newTarget(t)
	dead := newTarget(t)
	deadT := dead.tgt(1, 0)
	_ = dead.ln.Close() // 主目标端口已关闭：连接被拒
	f := newEntry(t, []types.Target{{Host: deadT.Host, Port: deadT.Port, Priority: 0, Weight: 1}, alive.tgt(1, 10)},
		types.SchedulePrimaryBackup, nil)
	for i := 0; i < 5; i++ {
		if got, err := f.roundTrip("ok"); err != nil || got != "ok" {
			t.Fatalf("failover: %q %v", got, err)
		}
	}
	if alive.conns.Load() != 5 {
		t.Fatalf("backup served %d", alive.conns.Load())
	}
}

func TestDirectAllTargetsDownClosesClient(t *testing.T) {
	dead := newTarget(t)
	tt := dead.tgt(1, 0)
	_ = dead.ln.Close()
	f := newEntry(t, []types.Target{tt}, types.ScheduleWeightedRoundRobin, nil)
	start := time.Now()
	got, _ := f.roundTrip("x")
	if got != "" || time.Since(start) > 2*time.Second {
		t.Fatalf("client should be closed quickly, got %q after %v", got, time.Since(start))
	}
}

func TestDirectGuardBlocksSelfPort(t *testing.T) {
	ok := newTarget(t)
	// 第一个目标指向本机 link 端口 7443（SEC-011），必须跳过并产生审计事件，再用下一个目标
	f := newEntry(t, []types.Target{{Host: "127.0.0.1", Port: 7443, Weight: 1}, ok.tgt(1, 10)}, types.SchedulePrimaryBackup, nil)
	if got, err := f.roundTrip("guarded"); err != nil || got != "guarded" {
		t.Fatalf("%q %v", got, err)
	}
	select {
	case ev := <-f.events:
		if ev.Kind != types.EventTargetRejected {
			t.Fatalf("event %v", ev.Kind)
		}
	case <-time.After(time.Second):
		t.Fatal("no TargetRejected event")
	}
}

// ---- 准入与计量钩子 ----

type countingHook struct{ up, down atomic.Int64 }

func (h *countingHook) Acquire(_ context.Context, _ types.Direction, want int) (int, error) {
	return want, nil
}

func (h *countingHook) Release(d types.Direction, _, written int) {
	if d == types.DirUp {
		h.up.Add(int64(written))
	} else {
		h.down.Add(int64(written))
	}
}

type testAdmission struct {
	deny     atomic.Bool
	hook     *countingHook
	admitted atomic.Int64
	released atomic.Int64
}

func (a *testAdmission) Admit(context.Context, *types.Rule, netip.AddrPort) (pipe.Hook, func(), error) {
	if a.deny.Load() {
		return nil, nil, errors.New("conn_limited")
	}
	a.admitted.Add(1)
	var once sync.Once
	return a.hook, func() { once.Do(func() { a.released.Add(1) }) }, nil
}

func TestDirectAdmissionAndMetering(t *testing.T) {
	tg := newTarget(t)
	adm := &testAdmission{hook: &countingHook{}}
	f := newEntry(t, []types.Target{tg.tgt(1, 0)}, types.ScheduleWeightedRoundRobin, adm)
	for i := 0; i < 10; i++ {
		if _, err := f.roundTrip("0123456789"); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for adm.released.Load() < 10 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if adm.admitted.Load() != 10 || adm.released.Load() != 10 {
		t.Fatalf("admitted %d released %d", adm.admitted.Load(), adm.released.Load())
	}
	if adm.hook.up.Load() != 100 || adm.hook.down.Load() != 100 {
		t.Fatalf("metered up %d down %d, want 100/100", adm.hook.up.Load(), adm.hook.down.Load())
	}
	// 准入拒绝：连接被关闭，目标没有收到任何连接
	adm.deny.Store(true)
	before := tg.conns.Load()
	if got, _ := f.roundTrip("denied"); got != "" {
		t.Fatalf("denied session got %q", got)
	}
	if tg.conns.Load() != before {
		t.Fatal("target must not be dialed when admission is denied")
	}
}

func TestRuleSwapKeepsExistingSessions(t *testing.T) {
	tg := newTarget(t)
	f := newEntry(t, []types.Target{tg.tgt(1, 0)}, types.ScheduleWeightedRoundRobin, nil)
	c, err := net.Dial("tcp", f.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 5)
	if _, err := c.Write([]byte("first")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(c, buf); err != nil {
		t.Fatal(err)
	}
	// 规则表切换到不含该规则的新版本：新会话拒绝，已有会话继续（ENG-032）
	f.tables.Swap(NewTable(2, nil))
	// 留出时间：若实现错误地让已有会话跟随新快照（例如周期性重查规则后关闭），会在这段时间内发生
	time.Sleep(100 * time.Millisecond)
	if got, _ := f.roundTrip("new"); got != "" {
		t.Fatalf("new session after rule removal got %q", got)
	}
	if _, err := c.Write([]byte("again")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(c, buf); err != nil || string(buf) != "again" {
		t.Fatalf("existing session broken: %q %v", buf, err)
	}
}

func TestDirectShutdownLeakFree(t *testing.T) {
	g0 := lifecycle.LiveGoroutines()
	tg := newTarget(t)
	f := newEntry(t, []types.Target{tg.tgt(1, 0)}, types.ScheduleWeightedRoundRobin, nil)
	// 留 20 个处于活动状态的会话
	var conns []net.Conn
	for i := 0; i < 20; i++ {
		c, err := net.Dial("tcp", f.addr)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = c.Write([]byte("hold"))
		conns = append(conns, c)
	}
	deadline := time.Now().Add(2 * time.Second)
	for f.entry.Active.Load() < 20 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if f.entry.Active.Load() != 20 {
		t.Fatalf("active %d", f.entry.Active.Load())
	}
	// 每个会话 2 个 goroutine + 接入循环 1 个（ENG-045）。会话的第二个 goroutine 在连上目标后才启动，
	// 因此等待计数到达 41，并在等待期间确认从未超出预算。
	want := int64(20*2 + 1)
	deadline = time.Now().Add(2 * time.Second)
	for {
		got := lifecycle.LiveGoroutines() - g0
		if got > want {
			t.Fatalf("goroutine budget exceeded: %d > %d", got, want)
		}
		if got == want {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutines %d, want %d", got, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
	_ = f.scope.Close()
	for _, c := range conns {
		_ = c.SetReadDeadline(time.Now().Add(time.Second))
		if _, err := io.ReadAll(c); err != nil {
			t.Fatalf("client not closed: %v", err)
		}
		c.Close()
	}
	if lifecycle.LiveGoroutines() != g0 || f.entry.Active.Load() != 0 {
		t.Fatalf("leak after shutdown: goroutines %d active %d", lifecycle.LiveGoroutines()-g0, f.entry.Active.Load())
	}
	// 端口已释放，可重新预留
	ap := netip.MustParseAddrPort(f.addr)
	r, err := ReserveTCP(ap)
	if err != nil {
		t.Fatalf("port not released: %v", err)
	}
	_ = r.Abort()
}
