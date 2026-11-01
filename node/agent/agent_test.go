package agent

import (
	"context"
	"io"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"vortex/engine"
	"vortex/engine/types"
	"vortex/node/agent/boot"
	"vortex/node/agent/grants"
	"vortex/node/agent/probe"
	"vortex/node/agent/reconcile"
	"vortex/node/agent/store"
)

var (
	nodeID = types.UUID{0x0d}
	hubID  = types.UUID{0x4b}
	tenant = types.UUID{0x7e}
	ruleA  = types.UUID{0xaa}
)

// fakeHub 是内存中的 Edge Hub：按 C2 应答，自动 Commit，立即确认计量。消息在自己的 goroutine 里处理
// （Agent 在持锁时调用 Send，不能同步回调）。
type fakeHub struct {
	t     *testing.T
	ch    chan Out
	a     *Agent
	cfg   *types.RuntimeConfig
	ctx   context.Context
	n     byte
	noAck bool

	mu      sync.Mutex
	hellos  []Hello
	held    []HeldGrants
	applies []ApplyReport
	batches map[types.UUID]map[uint64]UsageBatch
	boots   []types.UUID // 批次中出现的 boot，按首次出现顺序
	reqs    []grants.Request
	boot    types.UUID
	id      types.UUID
	probes  int
}

func newHub(t *testing.T, ctx context.Context, cfg *types.RuntimeConfig) *fakeHub {
	return &fakeHub{t: t, ch: make(chan Out, 4096), cfg: cfg, ctx: ctx, batches: map[types.UUID]map[uint64]UsageBatch{}, id: hubID}
}

func (h *fakeHub) Send(m Out) error {
	h.ch <- m
	return nil
}

func (h *fakeHub) run(a *Agent) {
	h.a = a
	go func() {
		for {
			select {
			case <-h.ctx.Done():
				return
			case m := <-h.ch:
				h.handle(m)
			}
		}
	}()
}

func (h *fakeHub) meta(ttl time.Duration) types.GrantMeta {
	h.n++
	return types.GrantMeta{GrantID: types.UUID{0xe0, h.n}, IssuerHub: h.id, BootID: h.boot, TTL: ttl, Generation: 1}
}

func (h *fakeHub) handle(m Out) {
	h.mu.Lock()
	var reply []In
	switch m := m.(type) {
	case Hello:
		h.hellos = append(h.hellos, m)
		h.boot = m.Boot
		reply = append(reply, HubHello{Hub: h.id, Mode: ModeOnline})
	case HeldGrants:
		h.held = append(h.held, m)
	case ProbeReport:
		h.probes++
	case PrepareReport:
		if m.Err == "" && len(m.Result.Errors) == 0 {
			reply = append(reply, ApplyCommand{Version: m.Version, Action: reconcile.ActionCommit})
		}
	case ApplyReport:
		h.applies = append(h.applies, m)
	case GrantRequest:
		h.reqs = append(h.reqs, m.Request)
		resp := grants.Response{RequestID: m.ID}
		switch m.Kind {
		case grants.KindRun:
			run := types.RunGrant{Meta: h.meta(5 * time.Minute)}
			for _, r := range h.cfg.Rules {
				run.RuleIDs = append(run.RuleIDs, r.RuleID)
			}
			resp.Set.Run = []types.RunGrant{run}
			resp.Set.Shares = []types.ShareLease{
				{Meta: h.meta(5 * time.Minute), TenantID: tenant, Kind: types.ShareRate, Amount: 10_000_000_000},
				{Meta: h.meta(5 * time.Minute), TenantID: tenant, Kind: types.ShareConn, Amount: 1000}}
		case grants.KindBilling:
			resp.Set.Billing = []types.BillingGrant{{Meta: h.meta(10 * time.Minute), RuleID: m.Rule, BillingVersion: 1, RawBytes: 1 << 30}}
		}
		reply = append(reply, GrantResponse{resp})
	case UsageBatch:
		bs := h.batches[m.Boot]
		if bs == nil {
			bs = map[uint64]UsageBatch{}
			h.batches[m.Boot] = bs
			h.boots = append(h.boots, m.Boot)
		}
		bs[m.Seq] = m // 幂等
		through := uint64(0)
		for bs[through+1].Seq != 0 {
			through++
		}
		if !h.noAck {
			reply = append(reply, UsageAck{Boot: m.Boot, Through: through})
		}
	}
	h.mu.Unlock()
	for _, r := range reply {
		h.a.HandleHub(h.ctx, r)
	}
}

func (h *fakeHub) bytes(boot types.UUID, dir types.Direction) (n uint64, seqs int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, b := range h.batches[boot] {
		seqs++
		for _, d := range b.Snapshot.Deltas {
			if d.RuleID == ruleA && d.Direction == dir {
				n += d.RawBytes
			}
		}
	}
	return
}

func tagServer(t *testing.T) netip.AddrPort {
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
				_, _ = c.Write([]byte{'T'})
				_, _ = io.Copy(c, c)
			}()
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return netip.MustParseAddrPort(ln.Addr().String())
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

func testConfig(t *testing.T) *types.RuntimeConfig {
	target := tagServer(t)
	return &types.RuntimeConfig{Version: 1, NodeID: nodeID,
		Listeners: []types.Listener{{Bind: freePort(t), Network: types.NetworkTCP, RuleID: ruleA}},
		Rules: []types.Rule{{RuleID: ruleA, TenantID: tenant, RuleVersion: 1, Role: types.RoleEntry, EnforcementPoint: true,
			Networks: []types.Network{types.NetworkTCP}, TargetSchedule: types.ScheduleWeightedRoundRobin, PathCoefMilli: 1000, BillingVersion: 1,
			Targets: []types.Target{{Host: target.Addr().String(), Port: target.Port(), Weight: 1}}}},
		TargetPolicy: types.TargetPolicy{SelfHostedNode: true}}
}

type logs struct{ boot, usage, config *store.Mem }

func newLogs() logs { return logs{&store.Mem{}, &store.Mem{}, &store.Mem{}} }

// node 是一次启动：新的 Engine 与 Agent，事件转发给 Agent。
type node struct {
	rt     *engine.Runtime
	a      *Agent
	cancel context.CancelFunc
	done   chan struct{}
}

func start(t *testing.T, l logs) *node {
	rt := engine.New(engine.Options{})
	a, err := New(context.Background(), Options{Node: nodeID, AgentVersion: "test", Engine: rt, BootLog: l.boot, UsageLog: l.usage, ConfigLog: l.config})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := &node{rt: rt, a: a, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(n.done)
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-rt.Events():
				a.HandleEvent(ev)
			}
		}
	}()
	return n
}

func (n *node) stop() {
	n.cancel()
	<-n.done
	_ = n.a.Shutdown(n.rt.Close)
}

// roundTrip 经入口发送 size 字节并收回；返回是否成功。
func roundTrip(addr netip.AddrPort, size int) bool {
	c, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		return false
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	tag := make([]byte, 1)
	if _, err := io.ReadFull(c, tag); err != nil || tag[0] != 'T' {
		return false
	}
	go func() { _, _ = c.Write(make([]byte, size)) }()
	_, err = io.ReadFull(c, make([]byte, size))
	return err == nil
}

func eventually(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// 首次启动 → 配置下发与应用 → 转发与计量上报 → 断线继续转发 → 重启：恢复配置、授权待确认、上次启动的计量先报、
// 取得授权后恢复转发（AGT-006、AGT-009、AGT-010、AGT-011、NFR-016）。
func TestAgentLifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := testConfig(t)
	addr := cfg.Listeners[0].Bind
	l := newLogs()

	n1 := start(t, l)
	h1 := newHub(t, ctx, cfg)
	h1.run(n1.a)
	n1.a.Connected(h1)
	n1.a.HandleHub(ctx, DesiredConfig{cfg})
	eventually(t, "config applied", func() bool { return n1.a.Status().Phase == reconcile.PhaseApplied })
	eventually(t, "run grant", func() bool { return n1.a.Status().RunRemaining[ruleA] > 0 })
	if !roundTrip(addr, 1<<20) {
		t.Fatal("no forwarding after apply")
	}
	n1.a.Tick(ctx)
	if b := n1.a.SealTick(); b == nil {
		t.Fatal("nothing sealed after traffic")
	}
	eventually(t, "batch acked", func() bool { return n1.a.Status().PendingBatches == 0 })
	if up, _ := h1.bytes(n1.a.boot.Current, types.DirUp); up < 1<<20 {
		t.Fatalf("hub got %d up bytes, want ≥ 1 MiB", up)
	}

	// 断线：不主动停止，授权有效期内继续转发
	n1.a.Disconnected()
	if !roundTrip(addr, 64<<10) {
		t.Fatal("forwarding stopped on control disconnect")
	}
	n1.a.Tick(ctx) // 落盘但不封批：重启时封为上次启动的最后一批
	oldBoot := n1.a.boot.Current
	n1.stop()

	// 重启
	n2 := start(t, l)
	defer n2.stop()
	if st := n2.a.Status(); st.Phase != reconcile.PhaseGrantPending || st.PreviousBoot != oldBoot || st.Boot == oldBoot {
		t.Fatalf("after restart %+v", st)
	}
	if roundTrip(addr, 1) {
		t.Fatal("forwarding before run grants (must stay grant-pending)")
	}
	h2 := newHub(t, ctx, cfg)
	h2.run(n2.a)
	n2.a.Connected(h2)
	eventually(t, "recovery committed", func() bool { return n2.a.Status().Phase == reconcile.PhaseApplied })
	if !roundTrip(addr, 1<<20) {
		t.Fatal("no forwarding after recovery")
	}
	h2.mu.Lock()
	hello := h2.hellos[0]
	firstBoot := h2.boots[0]
	h2.mu.Unlock()
	if hello.PreviousBoot != oldBoot || hello.AppliedConfigVersion != 1 {
		t.Fatalf("hello %+v", hello)
	}
	if firstBoot != oldBoot {
		t.Fatal("previous boot's batches not reported first")
	}
	if up, seqs := h2.bytes(oldBoot, types.DirUp); seqs != 1 || up != 64<<10 { // Shutdown 收齐关闭过程中计入的字节
		t.Fatalf("previous boot final batch: %d bytes in %d batches", up, seqs)
	}
	// 恢复时的 ApplyReport 由授权触发，不是后端的 Commit
	h2.mu.Lock()
	defer h2.mu.Unlock()
	if len(h2.applies) != 1 || h2.applies[0].Action != reconcile.ActionCommit || h2.applies[0].Err != "" {
		t.Fatalf("applies %+v", h2.applies)
	}
}

// 计量在确认前不清理：Edge Hub 不确认时批次累积，重连后按序重发，确认后清零（MTR-006）。
func TestUsageResentAfterReconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := testConfig(t)
	l := newLogs()
	n := start(t, l)
	defer n.stop()
	h := newHub(t, ctx, cfg)
	h.noAck = true
	h.run(n.a)
	n.a.Connected(h)
	n.a.HandleHub(ctx, DesiredConfig{cfg})
	eventually(t, "run grant", func() bool { return n.a.Status().RunRemaining[ruleA] > 0 })
	for i := 0; i < 3; i++ {
		if !roundTrip(cfg.Listeners[0].Bind, 32<<10) {
			t.Fatal("forwarding")
		}
		n.a.Tick(ctx)
		n.a.SealTick()
	}
	if p := n.a.Status().PendingBatches; p != 3 {
		t.Fatalf("pending %d", p)
	}
	n.a.Disconnected()
	h2 := newHub(t, ctx, cfg) // 同区域另一实例（HUB-014）
	h2.id = types.UUID{0x4c}
	h2.run(n.a)
	n.a.Connected(h2)
	eventually(t, "resent and acked", func() bool { return n.a.Status().PendingBatches == 0 })
	h2.mu.Lock()
	if len(h2.held) != 1 || len(h2.held[0].Grants) < 3 || h2.held[0].Grants[0].Issuer != hubID {
		t.Fatalf("held grants report after switching %+v", h2.held)
	}
	h2.mu.Unlock()
	if _, seqs := h2.bytes(n.a.boot.Current, types.DirUp); seqs != 3 {
		t.Fatalf("resent %d batches", seqs)
	}
}

// 计费授权：执行点规则在会话准入时由 Engine 发出 grant_needed，Agent 同步申请，额度依据为近 60 秒速率。
func TestBillingGrantRequestedOnDemand(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := testConfig(t)
	n := start(t, newLogs())
	defer n.stop()
	h := newHub(t, ctx, cfg)
	h.run(n.a)
	n.a.Connected(h)
	n.a.HandleHub(ctx, DesiredConfig{cfg})
	eventually(t, "run grant", func() bool { return n.a.Status().RunRemaining[ruleA] > 0 })
	if !roundTrip(cfg.Listeners[0].Bind, 1024) {
		t.Fatal("first session must succeed via a sync billing request")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	var billing int
	for _, r := range h.reqs {
		if r.Kind == grants.KindBilling && r.Rule == ruleA {
			billing++
		}
	}
	if billing == 0 {
		t.Fatalf("no billing request: %+v", h.reqs)
	}
}

func TestRateWindowCountsOnlyBilledTraffic(t *testing.T) {
	w := newRateWindow(60)
	for i := 0; i < 60; i++ {
		w.add(types.UsageSnapshot{Deltas: []types.UsageDelta{
			{RuleID: ruleA, RawBytes: 1000, BillingGrantID: types.UUID{1}},
			{RuleID: ruleA, RawBytes: 5000}, // 非执行点（中继、出口）不计
		}})
	}
	if r := w.rate(ruleA); r != 8000 {
		t.Fatalf("rate %d bit/s, want 8000", r)
	}
	for i := 0; i < 60; i++ {
		w.add(types.UsageSnapshot{})
	}
	if r := w.rate(ruleA); r != 0 {
		t.Fatalf("rate after the window %d", r)
	}
}

// 配置生效后通知（更新探测对象）；探测结果上报；DrainCommand 触发排空。
func TestAppliedHookProbesAndDrain(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := testConfig(t)
	n := start(t, newLogs())
	defer n.stop()
	applied := make(chan uint64, 4)
	n.a.opt.OnApplied = func(c *types.RuntimeConfig) { applied <- c.Version }
	h := newHub(t, ctx, cfg)
	h.run(n.a)
	n.a.Connected(h)
	n.a.HandleHub(ctx, DesiredConfig{cfg})
	select {
	case v := <-applied:
		if v != 1 {
			t.Fatalf("applied %d", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnApplied not called")
	}
	n.a.ReportProbes([]probe.Result{{Subject: probe.Subject{Target: "1.2.3.4:80"}, Reachable: true}})
	eventually(t, "probe report", func() bool { h.mu.Lock(); defer h.mu.Unlock(); return h.probes == 1 })
	eventually(t, "run grant", func() bool { return n.a.Status().RunRemaining[ruleA] > 0 })
	if !roundTrip(cfg.Listeners[0].Bind, 1024) {
		t.Fatal("forwarding before drain")
	}
	n.a.HandleHub(ctx, DrainCommand{Timeout: time.Second})
	eventually(t, "drained: new sessions refused", func() bool { return !roundTrip(cfg.Listeners[0].Bind, 1024) })
}

// closingEngine：关闭时才产出最后的计量（关闭过程中结束的会话）。
type closingEngine struct {
	engine.Engine
	closed bool
}

func (e *closingEngine) DrainUsage() types.UsageSnapshot {
	if !e.closed {
		return types.UsageSnapshot{}
	}
	e.closed = false
	return types.UsageSnapshot{Deltas: []types.UsageDelta{{RuleID: ruleA, Direction: types.DirUp, RawBytes: 4096}}}
}

func TestShutdownDrainsAfterClosingEngine(t *testing.T) {
	l := newLogs()
	e := &closingEngine{}
	a, err := New(context.Background(), Options{Node: nodeID, Engine: e, BootLog: l.boot, UsageLog: l.usage, ConfigLog: l.config})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Shutdown(func() error { e.closed = true; return nil }); err != nil {
		t.Fatal(err)
	}
	a2, err := New(context.Background(), Options{Node: nodeID, Engine: &closingEngine{}, BootLog: l.boot, UsageLog: l.usage, ConfigLog: l.config})
	if err != nil {
		t.Fatal(err)
	}
	p := a2.rep.Pending()
	if len(p) != 1 || p[0].Boot != a.boot.Current || p[0].Snapshot.Deltas[0].RawBytes != 4096 {
		t.Fatalf("usage of sessions ended by shutdown lost: %+v", p)
	}
}

// 调用方先开始启动（Engine 的链路需要 boot_id）时，Agent 使用同一个 boot_id，且不再生成新的。
func TestPreStartedBoot(t *testing.T) {
	l := newLogs()
	b, err := boot.Start(l.boot)
	if err != nil {
		t.Fatal(err)
	}
	a, err := New(context.Background(), Options{Node: nodeID, Engine: &closingEngine{}, Boot: &b, BootLog: l.boot, UsageLog: l.usage, ConfigLog: l.config})
	if err != nil {
		t.Fatal(err)
	}
	if a.boot.Current != b.Current || a.Hello().Boot != b.Current {
		t.Fatal("agent did not use the pre-started boot")
	}
	next, _ := boot.Start(l.boot)
	if next.Previous != b.Current {
		t.Fatalf("an extra boot was recorded: previous %x, want %x", next.Previous, b.Current)
	}
}
