package engine_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"runtime"
	"testing"
	"time"

	"vortex/engine"
	"vortex/engine/enginetest"
	"vortex/engine/route"
	"vortex/engine/types"
)

var (
	tenant = types.UUID{0x7e}
	ruleA  = types.UUID{0xaa}
	ruleB  = types.UUID{0xbb}
)

// tagServer 是测试目标：接入后先写一个标识字节，然后回显。
func tagServer(t *testing.T, tag byte) netip.AddrPort {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				if _, err := c.Write([]byte{tag}); err != nil {
					return
				}
				_, _ = io.Copy(c, c)
			}()
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return netip.MustParseAddrPort(ln.Addr().String())
}

func udpEcho(t *testing.T) netip.AddrPort {
	t.Helper()
	c, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		buf := make([]byte, 65535)
		for {
			n, a, err := c.ReadFromUDP(buf)
			if err != nil {
				return
			}
			_, _ = c.WriteToUDP(buf[:n], a)
		}
	}()
	t.Cleanup(func() { c.Close() })
	return netip.MustParseAddrPort(c.LocalAddr().String())
}

func rule(id types.UUID, nets []types.Network, targets ...netip.AddrPort) types.Rule {
	r := types.Rule{RuleID: id, TenantID: tenant, RuleVersion: 1, Role: types.RoleEntry, EnforcementPoint: true,
		Networks: nets, TargetSchedule: types.ScheduleWeightedRoundRobin}
	for _, ap := range targets {
		r.Targets = append(r.Targets, types.Target{Host: ap.Addr().String(), Port: ap.Port(), Weight: 1})
	}
	return r
}

var anyLocal = netip.MustParseAddrPort("127.0.0.1:0")

func tcpL(id types.UUID) types.Listener {
	return types.Listener{Bind: anyLocal, Network: types.NetworkTCP, RuleID: id}
}

func config(v uint64, ls []types.Listener, rs ...types.Rule) *types.RuntimeConfig {
	return &types.RuntimeConfig{Version: v, Listeners: ls, Rules: rs, TargetPolicy: types.TargetPolicy{SelfHostedNode: true}}
}

type fixture struct {
	rt *engine.Runtime
	ag *enginetest.Agent
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	before := runtime.NumGoroutine()
	rt := engine.New(engine.Options{})
	ag := enginetest.Start(rt, 50*time.Millisecond)
	t.Cleanup(func() {
		ag.Stop()
		_ = rt.Close()
		// Close 等待引擎全部 goroutine 退出；测试服务端与客户端连接的 goroutine 稍后由 t.Cleanup 收尾
		deadline := time.Now().Add(2 * time.Second)
		for runtime.NumGoroutine() > before+8 && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
	})
	return &fixture{rt: rt, ag: ag}
}

func (f *fixture) apply(t *testing.T, cfg *types.RuntimeConfig) *types.PrepareResult {
	t.Helper()
	pr, _, err := f.ag.Apply(context.Background(), cfg)
	if err != nil {
		t.Fatalf("apply v%d: %v", cfg.Version, err)
	}
	f.ag.GrantAll(cfg, enginetest.Grants{TTL: time.Hour})
	return pr
}

// dial 连接入口并读出目标标识字节。
func dial(t *testing.T, addr netip.AddrPort) (net.Conn, byte) {
	t.Helper()
	c, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	var b [1]byte
	if _, err := io.ReadFull(c, b[:]); err != nil {
		return c, 0 // 被拒绝：入口关闭了连接
	}
	_ = c.SetDeadline(time.Time{})
	return c, b[0]
}

func alive(c net.Conn) bool {
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	defer c.SetDeadline(time.Time{})
	if _, err := c.Write([]byte("ping")); err != nil {
		return false
	}
	var b [4]byte
	_, err := io.ReadFull(c, b[:])
	return err == nil && string(b[:]) == "ping"
}

func closedSoon(c net.Conn) bool {
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	defer c.SetReadDeadline(time.Time{})
	_, err := io.Copy(io.Discard, c)
	return err == nil // EOF 表示对端关闭；超时返回错误
}

func change(pr *types.PrepareResult, kind types.ObjectKind) []types.ObjectChange {
	var out []types.ObjectChange
	for _, c := range pr.Changes {
		if c.Object.Kind == kind {
			out = append(out, c)
		}
	}
	return out
}

func TestApplyForwardsAndMeters(t *testing.T) {
	f := newFixture(t)
	a := tagServer(t, 'A')
	cfg := config(1, []types.Listener{tcpL(ruleA)}, rule(ruleA, []types.Network{types.NetworkTCP}, a))
	pr := f.apply(t, cfg)
	if len(pr.Changes) != 2 || pr.Changes[0].Class != types.ChangeHot || pr.Changes[1].Class != types.ChangeHot {
		t.Fatalf("first apply should classify everything hot: %+v", pr.Changes)
	}
	addr := f.rt.ListenerAddr(cfg.Listeners[0])
	c, tag := dial(t, addr)
	if tag != 'A' {
		t.Fatalf("tag %q", tag)
	}
	payload, back := make([]byte, 1<<20), make([]byte, 1<<20)
	go func() { _, _ = c.Write(payload) }()
	if _, err := io.ReadFull(c, back); err != nil {
		t.Fatal(err)
	}
	c.Close()
	deadline := time.Now().Add(3 * time.Second)
	for f.ag.Usage(ruleA, types.DirUp) < 1<<20 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if up, down := f.ag.Usage(ruleA, types.DirUp), f.ag.Usage(ruleA, types.DirDown); up != 1<<20 || down != 1<<20+1 {
		t.Fatalf("metered up=%d down=%d, want %d / %d", up, down, 1<<20, 1<<20+1)
	}
	st := f.rt.State()
	if st.ActualVersion != 1 || len(st.Objects) != 2 {
		t.Fatalf("state %+v", st)
	}
	for _, o := range st.Objects {
		if o.Status != types.StatusActive || o.ActualVersion != 1 {
			t.Fatalf("object %+v", o)
		}
	}
}

func freePort(t *testing.T) netip.AddrPort {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ap := netip.MustParseAddrPort(ln.Addr().String())
	ln.Close()
	return ap
}

func TestPrepareBindsWithoutListeningAndAbortReleases(t *testing.T) {
	f := newFixture(t)
	a := tagServer(t, 'A')
	bind := freePort(t)
	cfg := config(1, []types.Listener{{Bind: bind, Network: types.NetworkTCP, RuleID: ruleA}}, rule(ruleA, []types.Network{types.NetworkTCP}, a))
	pr, err := f.rt.Prepare(context.Background(), cfg)
	if err != nil || len(pr.Errors) != 0 {
		t.Fatal(err, pr)
	}
	if c, err := net.DialTimeout("tcp", bind.String(), time.Second); err == nil {
		c.Close()
		t.Fatal("prepared listener accepted a connection before commit (ENG-031)")
	}
	if _, err := route.ReserveTCP(bind); !errors.Is(err, route.ErrPortInUse) {
		t.Fatalf("prepared port not held: %v", err)
	}
	if err := f.rt.Abort(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	r, err := route.ReserveTCP(bind)
	if err != nil {
		t.Fatalf("abort did not release port: %v", err)
	}
	_ = r.Abort()
	if st := f.rt.State(); st.ActualVersion != 0 || len(st.Objects) != 0 {
		t.Fatalf("abort changed active state: %+v", st)
	}
	// 中止后同一版本可以重新准备并提交
	f.apply(t, cfg)
	if _, tag := dial(t, bind); tag != 'A' {
		t.Fatal("re-prepared version not serving")
	}
}

func TestPortInUseReportedAndVersionRules(t *testing.T) {
	f := newFixture(t)
	a := tagServer(t, 'A')
	busy, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	bind := netip.MustParseAddrPort(busy.Addr().String())
	cfg := config(5, []types.Listener{{Bind: bind, Network: types.NetworkTCP, RuleID: ruleA}}, rule(ruleA, []types.Network{types.NetworkTCP}, a))
	_, _, err = f.ag.Apply(context.Background(), cfg)
	var pe *enginetest.PrepareError
	if !errors.As(err, &pe) || pe.Errors[0].Code != engine.CodePortInUse {
		t.Fatalf("want PORT_IN_USE, got %v", err)
	}

	ok := config(5, []types.Listener{tcpL(ruleA)}, rule(ruleA, []types.Network{types.NetworkTCP}, a))
	if _, err := f.rt.Prepare(context.Background(), ok); err != nil {
		t.Fatal(err)
	}
	if _, err := f.rt.Prepare(context.Background(), config(6, nil)); !errors.Is(err, engine.ErrPendingVersion) {
		t.Fatalf("second pending version: %v", err)
	}
	if _, err := f.rt.Commit(context.Background(), 4); !errors.Is(err, engine.ErrNoPending) {
		t.Fatalf("commit of other version: %v", err)
	}
	if _, err := f.rt.Commit(context.Background(), 5); err != nil {
		t.Fatal(err)
	}
	if _, err := f.rt.Prepare(context.Background(), config(5, nil)); !errors.Is(err, engine.ErrStaleVersion) {
		t.Fatalf("same version again: %v", err)
	}
	if _, err := f.rt.Prepare(context.Background(), config(3, nil)); !errors.Is(err, engine.ErrStaleVersion) {
		t.Fatalf("older version: %v", err)
	}
}

func TestInvalidConfigRejected(t *testing.T) {
	f := newFixture(t)
	a := tagServer(t, 'A')
	cases := map[string]*types.RuntimeConfig{
		"unknown rule":     config(1, []types.Listener{tcpL(ruleB)}, rule(ruleA, []types.Network{types.NetworkTCP}, a)),
		"network disabled": config(1, []types.Listener{{Bind: anyLocal, Network: types.NetworkUDP, RuleID: ruleA}}, rule(ruleA, []types.Network{types.NetworkTCP}, a)),
		"no targets":       config(1, nil, rule(ruleA, []types.Network{types.NetworkTCP})),
		"duplicate rule":   config(1, nil, rule(ruleA, nil, a), rule(ruleA, nil, a)),
	}
	for name, cfg := range cases {
		if _, err := f.rt.Prepare(context.Background(), cfg); !errors.Is(err, engine.ErrInvalidConfig) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestNextHopRuleReportedNotImplemented(t *testing.T) {
	f := newFixture(t)
	r := types.Rule{RuleID: ruleA, TenantID: tenant, Networks: []types.Network{types.NetworkTCP},
		NextHop: &types.NextHop{Members: []types.Member{{NodeID: types.UUID{9}, Weight: 1}}}}
	pr, err := f.rt.Prepare(context.Background(), config(1, nil, r))
	if err != nil {
		t.Fatal(err)
	}
	if len(pr.Errors) != 1 || pr.Errors[0].Code != engine.CodeNotImplemented {
		t.Fatalf("errors %+v", pr.Errors)
	}
	// 部分应用：调用方仍可提交，该规则报告为 failed（ENG-034）
	if _, err := f.rt.Commit(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	st := f.rt.State()
	if len(st.Objects) != 1 || st.Objects[0].Status != types.StatusFailed || st.Objects[0].ErrorCode != engine.CodeNotImplemented {
		t.Fatalf("state %+v", st)
	}
}

func TestHotChangeKeepsSessionsTighteningClosesThem(t *testing.T) {
	f := newFixture(t)
	a, b := tagServer(t, 'A'), tagServer(t, 'B')
	tcp := []types.Network{types.NetworkTCP}
	ls := []types.Listener{tcpL(ruleA)}
	f.apply(t, config(1, ls, rule(ruleA, tcp, a)))
	addr := f.rt.ListenerAddr(ls[0])
	old, _ := dial(t, addr)

	// v2：只改限速与新增目标 → hot，已有会话不受影响
	r2 := rule(ruleA, tcp, a, b)
	r2.Limits.RateBps = 800_000_000
	r2.TargetSchedule = types.SchedulePrimaryBackup
	r2.Targets[1].Priority = 1
	pr := f.apply(t, config(2, ls, r2))
	if rc := change(pr, types.ObjectRule); len(rc) != 1 || rc[0].Class != types.ChangeHot {
		t.Fatalf("rule change %+v", rc)
	}
	if lc := change(pr, types.ObjectListener); len(lc) != 1 || lc[0].Class != types.ChangeNone {
		t.Fatalf("listener change %+v", lc)
	}
	if !alive(old) {
		t.Fatal("hot change broke an existing session")
	}
	if st := f.rt.State(); st.Objects[0].ActualVersion != 1 || st.Objects[1].ActualVersion != 2 {
		t.Fatalf("per-object versions %+v", st.Objects)
	}

	// v3：移除目标 A → 权限收紧（stop），提交前报告受影响会话数，提交后关闭
	pr = f.apply(t, config(3, ls, rule(ruleA, tcp, b)))
	rc := change(pr, types.ObjectRule)
	if len(rc) != 1 || rc[0].Class != types.ChangeStop || rc[0].AffectedSessions != 1 {
		t.Fatalf("tightening change %+v", rc)
	}
	if !closedSoon(old) {
		t.Fatal("session on removed target survived commit (ENG-032)")
	}
	if _, tag := dial(t, addr); tag != 'B' {
		t.Fatalf("new session tag %q, want B", tag)
	}
}

func TestListenerRemovalAndRebind(t *testing.T) {
	f := newFixture(t)
	a, b := tagServer(t, 'A'), tagServer(t, 'B')
	tcp := []types.Network{types.NetworkTCP}
	l1 := tcpL(ruleA)
	l2 := types.Listener{Bind: netip.MustParseAddrPort("127.0.0.2:0"), Network: types.NetworkTCP, RuleID: ruleB}
	f.apply(t, config(1, []types.Listener{l1, l2}, rule(ruleA, tcp, a), rule(ruleB, tcp, b)))
	addr1, addr2 := f.rt.ListenerAddr(l1), f.rt.ListenerAddr(l2)
	onA, _ := dial(t, addr1)
	onB, _ := dial(t, addr2)

	// v2：l1 改绑到规则 B（rebuild：已有会话保持旧快照）；l2 删除（stop）
	l1b := l1
	l1b.RuleID = ruleB
	pr := f.apply(t, config(2, []types.Listener{l1b}, rule(ruleA, tcp, a), rule(ruleB, tcp, b)))
	got := map[types.ChangeClass]uint32{}
	for _, c := range change(pr, types.ObjectListener) {
		got[c.Class] = c.AffectedSessions
	}
	if got[types.ChangeRebuild] != 1 || got[types.ChangeStop] != 1 || len(got) != 2 {
		t.Fatalf("listener changes %+v", pr.Changes)
	}
	if !closedSoon(onB) {
		t.Fatal("session on removed listener survived")
	}
	if c, err := net.DialTimeout("tcp", addr2.String(), time.Second); err == nil {
		c.Close()
		t.Fatal("removed listener still accepting")
	}
	if !alive(onA) {
		t.Fatal("rebind closed an existing session")
	}
	if _, tag := dial(t, addr1); tag != 'B' {
		t.Fatalf("rebound listener routed to %q, want B", tag)
	}
	if _, err := route.ReserveTCP(addr2); err != nil {
		t.Fatalf("removed listener port not released: %v", err)
	}
}

func TestRuleRemovalClosesSessions(t *testing.T) {
	f := newFixture(t)
	a, b := tagServer(t, 'A'), tagServer(t, 'B')
	tcp := []types.Network{types.NetworkTCP}
	l1, l2 := tcpL(ruleA), tcpL(ruleB)
	l2.Bind = netip.MustParseAddrPort("127.0.0.3:0")
	f.apply(t, config(1, []types.Listener{l1, l2}, rule(ruleA, tcp, a), rule(ruleB, tcp, b)))
	c, _ := dial(t, f.rt.ListenerAddr(l1))
	other, _ := dial(t, f.rt.ListenerAddr(l2))
	// 规则 A 与它的监听一起删除；规则 B 不变
	pr := f.apply(t, config(2, []types.Listener{l2}, rule(ruleB, tcp, b)))
	stops := 0
	for _, ch := range pr.Changes {
		if ch.Class == types.ChangeStop {
			stops++
			if ch.AffectedSessions != 1 {
				t.Fatalf("affected %+v", ch)
			}
		}
	}
	if stops != 2 {
		t.Fatalf("changes %+v", pr.Changes)
	}
	if !closedSoon(c) {
		t.Fatal("session of removed rule survived")
	}
	if !alive(other) {
		t.Fatal("unrelated rule's session closed")
	}
}

func TestDrain(t *testing.T) {
	f := newFixture(t)
	a := tagServer(t, 'A')
	ls := []types.Listener{tcpL(ruleA)}
	f.apply(t, config(1, ls, rule(ruleA, []types.Network{types.NetworkTCP}, a)))
	addr := f.rt.ListenerAddr(ls[0])
	c, _ := dial(t, addr)

	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	go func() { done <- f.rt.Drain(ctx, engine.DrainScope{}) }()
	time.Sleep(50 * time.Millisecond)
	if _, tag := dial(t, addr); tag != 0 {
		t.Fatal("draining engine admitted a new session")
	}
	if !alive(c) {
		t.Fatal("drain closed an existing session before its deadline")
	}
	if err := <-done; !errors.Is(err, engine.ErrDrainAborted) {
		t.Fatalf("drain: %v", err)
	}
	if !closedSoon(c) {
		t.Fatal("drain deadline did not close the remaining session")
	}
	// 没有会话时立即完成
	if err := f.rt.Drain(context.Background(), engine.DrainScope{}); err != nil {
		t.Fatal(err)
	}
}

func TestDrainCompletesWhenSessionsEnd(t *testing.T) {
	f := newFixture(t)
	a := tagServer(t, 'A')
	ls := []types.Listener{tcpL(ruleA)}
	f.apply(t, config(1, ls, rule(ruleA, []types.Network{types.NetworkTCP}, a)))
	c, _ := dial(t, f.rt.ListenerAddr(ls[0]))
	go func() { time.Sleep(100 * time.Millisecond); c.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := f.rt.Drain(ctx, engine.DrainScope{RuleIDs: []types.UUID{ruleA}}); err != nil {
		t.Fatalf("drain should finish once the session ends: %v", err)
	}
}

func TestUDPApply(t *testing.T) {
	f := newFixture(t)
	echo := udpEcho(t)
	ls := []types.Listener{{Bind: anyLocal, Network: types.NetworkUDP, RuleID: ruleA}}
	f.apply(t, config(1, ls, rule(ruleA, []types.Network{types.NetworkUDP}, echo)))
	addr := f.rt.ListenerAddr(ls[0])
	c, err := net.Dial("udp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	buf := make([]byte, 1200)
	for i := 0; i < 10; i++ {
		if _, err := c.Write(buf); err != nil {
			t.Fatal(err)
		}
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		if n, err := c.Read(buf); err != nil || n != 1200 {
			t.Fatalf("echo %d: %d %v", i, n, err)
		}
	}
	f.ag.Pull()
	if up := f.ag.Usage(ruleA, types.DirUp); up != 12000 {
		t.Fatalf("udp metered %d, want 12000", up)
	}
	// 删除 UDP 规则：会话被关闭，之后数据报不再转发
	f.apply(t, config(2, nil))
	_, _ = c.Write(buf)
	_ = c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if _, err := c.Read(buf); err == nil {
		t.Fatal("datagram forwarded after rule removal")
	}
}

func TestGrantNeededEventReachesAgent(t *testing.T) {
	f := newFixture(t)
	a := tagServer(t, 'A')
	ls := []types.Listener{tcpL(ruleA)}
	cfg := config(1, ls, rule(ruleA, []types.Network{types.NetworkTCP}, a))
	if _, _, err := f.ag.Apply(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	set := f.ag.GrantAll(cfg, enginetest.Grants{TTL: time.Hour})
	set.Billing = nil // 有运行授权与份额、无计费授权：新会话等待 200 ms 后被拒绝，并请求授权
	f.rt.UpdateGrants(set)
	if _, tag := dial(t, f.rt.ListenerAddr(ls[0])); tag != 0 {
		t.Fatal("session admitted without billing grant")
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(f.ag.Events(types.EventGrantNeeded)) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if len(f.ag.Events(types.EventGrantNeeded)) == 0 {
		t.Fatal("grant_needed event not delivered to the agent")
	}
}

func TestCloseStopsEverything(t *testing.T) {
	before := runtime.NumGoroutine()
	a := tagServer(t, 'A')
	rt := engine.New(engine.Options{})
	ag := enginetest.Start(rt, 0)
	ls := []types.Listener{tcpL(ruleA), {Bind: anyLocal, Network: types.NetworkUDP, RuleID: ruleA}}
	cfg := config(1, ls, rule(ruleA, []types.Network{types.NetworkTCP, types.NetworkUDP}, a))
	if _, _, err := ag.Apply(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	ag.GrantAll(cfg, enginetest.Grants{})
	c, _ := dial(t, rt.ListenerAddr(ls[0]))
	ag.Stop()
	_ = rt.Close()
	if !closedSoon(c) {
		t.Fatal("Close left a session open")
	}
	c.Close()
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before+2 && time.Now().Before(deadline) { // tagServer 接入循环与其连接
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before+2 {
		t.Fatalf("goroutines leaked: before %d after %d", before, n)
	}
}
