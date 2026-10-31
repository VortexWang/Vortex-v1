package e2e

import (
	"context"
	"io"
	"net"
	"net/netip"
	"testing"
	"time"

	"vortex/edgehub/clock"
	"vortex/edgehub/issuer"
	"vortex/edgehub/leases"
	"vortex/edgehub/ledger"
	"vortex/edgehub/model"
	"vortex/edgehub/session"
	"vortex/engine"
	"vortex/engine/types"
	"vortex/node/agent"
	"vortex/node/agent/control"
	"vortex/node/agent/reconcile"
	"vortex/node/agent/store"
)

var (
	nodeID = types.UUID{0x0d}
	hubA   = types.UUID{0xa1}
	hubB   = types.UUID{0xb1}
	tenant = types.UUID{0x7e}
	ruleA  = types.UUID{0xaa}
)

func target(t *testing.T) netip.AddrPort {
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
			go func() { defer c.Close(); _, _ = c.Write([]byte{'T'}); _, _ = io.Copy(c, c) }()
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
	defer ln.Close()
	return netip.MustParseAddrPort(ln.Addr().String())
}

func config(t *testing.T) *types.RuntimeConfig {
	tg := target(t)
	return &types.RuntimeConfig{Version: 1, NodeID: nodeID,
		Listeners: []types.Listener{{Bind: freePort(t), Network: types.NetworkTCP, RuleID: ruleA}},
		Rules: []types.Rule{{RuleID: ruleA, TenantID: tenant, RuleVersion: 1, Role: types.RoleEntry, EnforcementPoint: true,
			Networks: []types.Network{types.NetworkTCP}, TargetSchedule: types.ScheduleWeightedRoundRobin, PathCoefMilli: 1500, BillingVersion: 1,
			Targets: []types.Target{{Host: tg.Addr().String(), Port: tg.Port(), Weight: 1}}}},
		TargetPolicy: types.TargetPolicy{SelfHostedNode: true}}
}

func newHub(t *testing.T, id types.UUID, clk *clock.Fake, cfg *types.RuntimeConfig) *Hub {
	sh, err := session.Open(session.Options{HubID: toModel(id), Clock: clk, Store: &ledger.MemStore{}, Rules: RulesOf(cfg)})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = sh.Quota.ApplyBudget(toModel(tenant), 1, 1<<30, 0) // 委托 1 GiB 计费字节
	_ = sh.Issuer.SetBillingVersions([]issuer.BillingVersion{{Version: 1, EffectiveAt: clk.Now().Add(-time.Hour)}})
	return &Hub{Hub: sh, Cfg: cfg, Shares: []leases.Lease{
		{Tenant: toModel(tenant), Kind: model.GrantShareRate, Amount: 10_000_000_000},
		{Tenant: toModel(tenant), Kind: model.GrantShareConn, Amount: 1000}}}
}

type logs struct{ boot, usage, config *store.Mem }

// nodeProc 是一次节点进程：Engine + Agent + Controller + 每 50 ms 一次的 Tick。
type nodeProc struct {
	rt     *engine.Runtime
	a      *agent.Agent
	c      *control.Controller
	cancel context.CancelFunc
	done   chan struct{}
}

func startNode(t *testing.T, l logs, d Dialer, hubs []control.Endpoint) *nodeProc {
	rt := engine.New(engine.Options{})
	a, err := agent.New(context.Background(), agent.Options{Node: nodeID, AgentVersion: "e2e", Engine: rt,
		BootLog: l.boot, UsageLog: l.usage, ConfigLog: l.config})
	if err != nil {
		t.Fatal(err)
	}
	c := &control.Controller{Agent: a, Hubs: hubs, Dialer: d, BackoffMin: 10 * time.Millisecond,
		BackoffMax: 100 * time.Millisecond, ProbeEvery: 50 * time.Millisecond, HelloTimeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	p := &nodeProc{rt: rt, a: a, c: c, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(p.done)
		cdone := make(chan struct{})
		go func() { c.Run(ctx); close(cdone) }()
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				<-cdone
				return
			case ev := <-rt.Events():
				a.HandleEvent(ev)
			case <-tick.C:
				a.Tick(ctx)
			}
		}
	}()
	return p
}

func (p *nodeProc) stop() {
	p.cancel()
	<-p.done
	_ = p.a.Shutdown(p.rt.Close)
}

func roundTrip(addr netip.AddrPort, size int) bool {
	c, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		return false
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	tag := make([]byte, 1)
	if _, err := io.ReadFull(c, tag); err != nil {
		return false
	}
	go func() { _, _ = c.Write(make([]byte, size)) }()
	_, err = io.ReadFull(c, make([]byte, size))
	return err == nil
}

func eventually(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func usageRecords(h *Hub) int {
	n := 0
	_ = h.Ledger.Close // 账本条目通过 Unacked 读出（尚未对后端确认）
	es, _ := h.Ledger.Unacked()
	for _, e := range es {
		if e.Kind == session.KindUsage {
			n++
		}
	}
	return n
}

// 端到端：接入在线实例 → 配置下发与应用 → 授权 → 转发与计量 → 实例进入自治，60 秒内（这里缩短为毫秒级探测）
// 切换到在线实例并报告持有授权 → 节点重启：恢复配置、授权待确认、新实例按 LIM-010 转移、恢复转发；
// 全程两个实例都不超发（NFR-016、NFR-017、NFR-018、HUB-014、LIM-010）。
func TestAgentEdgeHubEndToEnd(t *testing.T) {
	cfg := config(t)
	addr := cfg.Listeners[0].Bind
	clkA := clock.NewFake(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC))
	clkB := clock.NewFake(clkA.Now())
	a, b := newHub(t, hubA, clkA, cfg), newHub(t, hubB, clkB, cfg)
	d := Dialer{Node: nodeID, Hubs: map[types.UUID]*Hub{hubA: a, hubB: b}}
	eps := []control.Endpoint{{ID: hubA}, {ID: hubB}}
	l := logs{&store.Mem{}, &store.Mem{}, &store.Mem{}}

	// 1. 首次启动，连上 A
	n1 := startNode(t, l, d, eps)
	eventually(t, "applied", func() bool { return n1.a.Status().Phase == reconcile.PhaseApplied })
	eventually(t, "run grant", func() bool { return n1.a.Status().RunRemaining[ruleA] > 0 })
	if cur, _ := n1.c.Current(); cur != hubA {
		t.Fatalf("connected to %v", cur)
	}
	if !roundTrip(addr, 1<<20) {
		t.Fatal("no forwarding")
	}
	n1.a.Tick(context.Background())
	n1.a.SealTick()
	eventually(t, "usage acked by A", func() bool { return n1.a.Status().PendingBatches == 0 })
	if usageRecords(a) == 0 {
		t.Fatal("A has no usage records")
	}
	issuedA := a.Quota.View(toModel(tenant))
	if issuedA.Issued == 0 {
		t.Fatal("A issued no billing grant")
	}

	// 2. A 与后端失联 30 秒 → 自治 → Agent 切换到在线的 B，报告持有的 A 授权；转发不中断
	clkA.Advance(31 * time.Second)
	if err := a.Tick(); err != nil {
		t.Fatal(err)
	}
	eventually(t, "switched to B", func() bool { cur, m := n1.c.Current(); return cur == hubB && m == agent.ModeOnline })
	eventually(t, "B recorded A's grants", func() bool {
		for _, g := range b.Issuer.Held() {
			if g.IssuerHub == toModel(hubA) {
				return true
			}
		}
		return false
	})
	if !roundTrip(addr, 256<<10) {
		t.Fatal("forwarding interrupted by the switch")
	}
	if v := b.Quota.View(toModel(tenant)); v.Issued+v.SettledLocal == 0 && len(b.Issuer.Held()) == 0 {
		t.Fatal("B saw nothing")
	}
	n1.a.Tick(context.Background()) // 落盘不封：重启时封为上次启动的最后一批
	oldBoot := n1.a.Status().Boot
	n1.stop()

	// 3. 重启：恢复配置 → 授权待确认 → 连上（A 自治则切到 B）→ B 转移并签发 → 恢复转发
	a.Down = true // A 不可达：直接连 B
	n2 := startNode(t, l, d, eps)
	defer n2.stop()
	eventually(t, "recovered", func() bool { return n2.a.Status().Phase == reconcile.PhaseApplied })
	if !roundTrip(addr, 1<<20) {
		t.Fatal("no forwarding after restart")
	}
	if bt, _ := b.Issuer.Boot(toModel(nodeID)); bt != toModel(n2.a.Status().Boot) || bt == toModel(oldBoot) {
		t.Fatalf("B's view of the node boot %v", bt)
	}
	eventually(t, "old boot batches reported", func() bool { return n2.a.Status().PendingBatches == 0 })
	b.mu.Lock()
	applies := append([]agent.ApplyReport(nil), b.Applies...)
	b.mu.Unlock()
	if len(applies) == 0 || applies[len(applies)-1].Err != "" {
		t.Fatalf("recovery apply report %+v", applies)
	}

	// 不超发：两个实例各自的 issued + 本地已结算 ≤ 委托额度
	for _, h := range []*Hub{a, b} {
		v := h.Quota.View(toModel(tenant))
		if v.Issued+v.SettledLocal > 1<<30 {
			t.Fatalf("hub %v over-issued %+v", h.ID, v)
		}
	}
}
