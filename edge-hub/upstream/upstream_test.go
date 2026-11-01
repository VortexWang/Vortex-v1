package upstream

import (
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"vortex/edgehub/clock"
	"vortex/edgehub/issuer"
	"vortex/edgehub/leases"
	"vortex/edgehub/ledger"
	"vortex/edgehub/model"
	"vortex/edgehub/session"
)

var (
	hubID  = model.UUID{0x4b}
	node   = model.UUID{0x0d}
	boot1  = model.UUID{0xb1}
	tenant = model.UUID{0x7e}
	ruleA  = model.UUID{0xaa}
)

type box[T any] struct {
	mu sync.Mutex
	ms []T
}

func (b *box[T]) put(m T) { b.mu.Lock(); b.ms = append(b.ms, m); b.mu.Unlock() }
func (b *box[T]) take() []T {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.ms
	b.ms = nil
	return out
}

type fixture struct {
	clk   *clock.Fake
	st    *ledger.MemStore
	h     *session.Hub
	l     *Link
	up    *box[Up]
	agent *box[session.HubMsg]
	s     *session.Session
}

func setup(t *testing.T) *fixture {
	clk := clock.NewFake(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC))
	st := &ledger.MemStore{}
	h, err := session.Open(session.Options{HubID: hubID, Clock: clk, Store: st, Rules: session.RulesFunc(func(model.UUID) []issuer.RuleState {
		return []issuer.RuleState{{RuleID: ruleA, TenantID: tenant, EnforcementPoint: true, PathCoefMilli: 1000}}
	})})
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{clk: clk, st: st, h: h, l: New(h), up: &box[Up]{}, agent: &box[session.HubMsg]{}}
	_ = h.Issuer.SetBillingVersions([]issuer.BillingVersion{{Version: 1, EffectiveAt: clk.Now().Add(-time.Hour)}})
	if err := f.l.Connected(f.up.put); err != nil {
		t.Fatal(err)
	}
	_ = f.l.Handle(Budget{Tenant: tenant, Version: 1, Delegated: 1 << 30})
	f.s = h.NewSession(node, f.agent.put)
	if err := f.s.Handle(session.Hello{Node: node, Boot: boot1}); err != nil {
		t.Fatal(err)
	}
	f.agent.take()
	return f
}

func entries(ups []Up) []LedgerEntry {
	var out []LedgerEntry
	for _, u := range ups {
		if le, ok := u.(LedgerEntries); ok {
			out = append(out, le.Entries...)
		}
	}
	return out
}

// 在线：随写随报，只报签发 / 结算 / 计量 / 转移；计量批次在后端确认对应账本条目后才确认给 Agent。
func TestOnlineUploadAndUsageAck(t *testing.T) {
	f := setup(t)
	ups := f.up.take()
	if r, ok := ups[0].(Register); !ok || r.Mode != model.ModeOnline {
		t.Fatalf("first message %+v", ups[0])
	}
	_ = f.s.Handle(session.GrantRequest{ID: 1, Kind: model.GrantBilling, Rule: ruleA})
	_ = f.s.Handle(session.GrantRequest{ID: 2, Kind: model.GrantRun})
	f.agent.take()
	if err := f.l.Tick(); err != nil {
		t.Fatal(err)
	}
	es := entries(f.up.take())
	if len(es) != 2 || es[0].Issued == nil || es[0].Issued.Kind != model.GrantBilling || es[0].Issued.Node != node ||
		es[1].Issued == nil || es[1].Issued.Kind != model.GrantRun || es[0].Seq >= es[1].Seq {
		t.Fatalf("uploaded %+v (budget and billing-version entries must stay local)", es)
	}
	_ = f.s.Handle(session.UsageBatch{Boot: boot1, Seq: 1})
	us := entries(f.up.take())
	if len(us) != 1 || us[0].Usage == nil || us[0].Usage.Seq != 1 || us[0].Usage.Node != node {
		t.Fatalf("usage upload %+v", us)
	}
	if len(f.agent.take()) != 0 {
		t.Fatal("usage acked to the agent before the backend")
	}
	_ = f.l.Handle(LedgerAck{Through: us[0].Seq})
	if ms := f.agent.take(); len(ms) != 1 || ms[0] != (session.UsageAck{Boot: boot1, Through: 1}) {
		t.Fatalf("agent ack %+v", ms)
	}
	// 重发已确认的批次：立即确认，不再上报
	_ = f.s.Handle(session.UsageBatch{Boot: boot1, Seq: 1})
	if len(f.agent.take()) != 1 || len(entries(f.up.take())) != 0 {
		t.Fatal("resend of an acked batch")
	}
	// 断线重连：从已确认位置重新上报未确认的条目
	f.l.Disconnected()
	_ = f.s.Handle(session.UsageBatch{Boot: boot1, Seq: 2})
	if len(f.up.take()) != 0 {
		t.Fatal("sent while disconnected")
	}
	_ = f.l.Connected(f.up.put)
	es = entries(f.up.take())
	if len(es) != 1 || es[0].Usage == nil || es[0].Usage.Seq != 2 {
		t.Fatalf("after reconnect %+v (only the batch written while disconnected; earlier entries were acked cumulatively)", es)
	}
}

// HUB-013：自治 → 恢复通信 → 对账中不上报，直到后端先下发吊销与限制并 STARTED；HUB-017 停签；
// COMPLETED 后回到在线、解除停签、推送模式、放行积压的配置。
func TestReconciliation(t *testing.T) {
	f := setup(t)
	f.up.take()
	f.l.Disconnected()
	f.clk.Advance(31 * time.Second)
	if err := f.h.Tick(); err != nil {
		t.Fatal(err)
	}
	if ms := f.agent.take(); len(ms) != 1 || ms[0] != (session.ModeChange{Mode: model.ModeAutonomous}) {
		t.Fatalf("autonomy not announced %+v", ms)
	}
	// 自治中：计量落盘即确认
	_ = f.s.Handle(session.UsageBatch{Boot: boot1, Seq: 1})
	if ms := f.agent.take(); len(ms) != 1 {
		t.Fatal("autonomous ack")
	}
	_ = f.s.Handle(session.GrantRequest{ID: 1, Kind: model.GrantBilling, Rule: ruleA})
	f.agent.take()

	// 恢复通信
	if err := f.l.Connected(f.up.put); err != nil {
		t.Fatal(err)
	}
	ups := f.up.take()
	r, ok := ups[0].(Register)
	if !ok || r.Mode != model.ModeReconciling || r.AutonomyStartedAt.IsZero() || r.LedgerLastSeq == 0 || len(entries(ups)) != 0 {
		t.Fatalf("register %+v (no ledger before STARTED)", ups)
	}
	// 对账中收到的配置积压
	if ms := f.agent.take(); len(ms) != 0 {
		t.Fatalf("autonomous → reconciling must not be announced again %+v", ms)
	}
	f.h.SetDesired(node, 7, json.RawMessage(`{"v":7}`))
	if len(f.agent.take()) != 0 {
		t.Fatal("config delivered while reconciling")
	}
	_ = f.l.Handle(Restrictions{Version: 3, Tenants: []issuer.TenantRestriction{{Tenant: model.UUID{0x99}, Status: issuer.AccountBanned}}})
	if len(entries(f.up.take())) != 0 {
		t.Fatal("ledger uploaded before STARTED")
	}
	_ = f.l.Handle(Reconcile{Phase: ReconcileStarted, RevokedTenants: []model.UUID{tenant}})
	es := entries(f.up.take())
	var usage, issued int
	for _, e := range es {
		if e.Usage != nil {
			usage++
		}
		if e.Issued != nil {
			issued++
		}
	}
	if usage != 1 || issued != 1 {
		t.Fatalf("reconciliation upload %+v", es)
	}
	if _, err := f.h.Issuer.IssueBilling(issuer.BillingRequest{Node: node, Boot: boot1, Rule: issuer.RuleState{RuleID: ruleA, TenantID: tenant, EnforcementPoint: true, PathCoefMilli: 1000}}); !errors.Is(err, issuer.ErrBudgetRevoked) {
		t.Fatalf("HUB-017: %v", err)
	}
	_ = f.l.Handle(LedgerAck{Through: es[len(es)-1].Seq})
	if err := f.l.Handle(Reconcile{Phase: ReconcileCompleted}); err != nil {
		t.Fatal(err)
	}
	if f.h.Mode.Mode() != model.ModeOnline || f.h.Leases.Frozen() != nil {
		t.Fatal("not back online / shares still frozen")
	}
	ms := f.agent.take()
	var sawMode, sawConfig bool
	for _, m := range ms {
		switch m := m.(type) {
		case session.ModeChange:
			sawMode = m.Mode == model.ModeOnline
		case session.DesiredConfig:
			sawConfig = m.Version == 7
		}
	}
	if !sawMode || !sawConfig {
		t.Fatalf("after completion %+v", ms)
	}
	if es, _ := f.h.Ledger.Unacked(); len(es) != 0 {
		t.Fatalf("local-only tail entries left unacked after reconciliation: %d", len(es))
	}
	if _, err := f.h.Issuer.IssueBilling(issuer.BillingRequest{Node: node, Boot: boot1, Rule: issuer.RuleState{RuleID: ruleA, TenantID: tenant, EnforcementPoint: true, PathCoefMilli: 1000}}); err != nil {
		t.Fatalf("billing after completion: %v", err)
	}
	// 新签发的条目照常上报，未确认前保持未确认
	_ = f.l.Tick()
	if es := entries(f.up.take()); len(es) != 1 || es[0].Issued == nil {
		t.Fatalf("after completion %+v", es)
	}
	if es, _ := f.h.Ledger.Unacked(); len(es) == 0 {
		t.Fatal("unacknowledged reportable entry marked acked")
	}
}

// HUB-T04：期望配置下发；Commit 在节点报告该版本 Prepare 成功后才转发；过期指令忽略；节点重连后重新下发。
func TestConfigSync(t *testing.T) {
	f := setup(t)
	_ = f.l.Handle(Desired{Node: node, Version: 2, Config: json.RawMessage(`{"v":2}`)})
	if ms := f.agent.take(); len(ms) != 1 || ms[0].(session.DesiredConfig).Version != 2 {
		t.Fatalf("desired %+v", ms)
	}
	_ = f.l.Handle(Desired{Node: node, Version: 1, Config: json.RawMessage(`{"v":1}`)})
	if len(f.agent.take()) != 0 {
		t.Fatal("older desired forwarded")
	}
	_ = f.l.Handle(Command{Node: node, Version: 1, Commit: true})
	_ = f.l.Handle(Command{Node: node, Version: 2, Commit: true})
	if len(f.agent.take()) != 0 {
		t.Fatal("commit forwarded before the node prepared")
	}
	_ = f.s.Handle(session.PrepareReport{Version: 2, OK: true})
	if ms := f.agent.take(); len(ms) != 1 || ms[0] != (session.ApplyCommand{Version: 2, Commit: true}) {
		t.Fatalf("commit after prepare %+v", ms)
	}
	var progress int
	for _, u := range f.up.take() {
		if p, ok := u.(ApplyProgress); ok && p.Prepare != nil && p.Prepare.Version == 2 {
			progress++
		}
	}
	if progress != 1 {
		t.Fatal("prepare result not forwarded to the backend")
	}
	// 节点断线后以旧版本重连：重新下发期望配置
	f.s.Close()
	s2 := f.h.NewSession(node, f.agent.put)
	_ = s2.Handle(session.Hello{Node: node, Boot: boot1, AppliedConfigVersion: 1})
	var again bool
	for _, m := range f.agent.take() {
		if d, ok := m.(session.DesiredConfig); ok && d.Version == 2 {
			again = true
		}
	}
	if !again {
		t.Fatal("desired not re-delivered on reconnect")
	}
	_ = s2.Handle(session.ApplyReport{Version: 2, Committed: false}) // Abort 或失败：不算已生效
	s2.Close()
	s2b := f.h.NewSession(node, f.agent.put)
	_ = s2b.Handle(session.Hello{Node: node, Boot: boot1, AppliedConfigVersion: 1})
	again = false
	for _, m := range f.agent.take() {
		if d, ok := m.(session.DesiredConfig); ok && d.Version == 2 {
			again = true
		}
	}
	if !again {
		t.Fatal("a failed apply must not mark the version applied")
	}
	_ = s2b.Handle(session.ApplyReport{Version: 2, Committed: true})
	s2b.Close()
	s3 := f.h.NewSession(node, f.agent.put)
	_ = s3.Handle(session.Hello{Node: node, Boot: boot1, AppliedConfigVersion: 2})
	for _, m := range f.agent.take() {
		if _, ok := m.(session.DesiredConfig); ok {
			t.Fatal("applied version re-delivered")
		}
	}
}

// 份额只在在线时接受后端的新租约（HUB-007 不得增加）；排空指令转发；低于 30% 请求补充额度。
func TestLeasesDrainAndTopUp(t *testing.T) {
	f := setup(t)
	_ = f.l.Handle(Leases{Node: node, Leases: []leases.Lease{{Tenant: tenant, Kind: model.GrantShareRate, Amount: 1000}}})
	if ms := f.agent.take(); len(ms) != 1 || ms[0].(session.GrantResponse).Shares[0].Amount != 1000 {
		t.Fatalf("share push %+v", ms)
	}
	_ = f.l.Handle(Drain{Node: node, Payload: json.RawMessage(`{}`)})
	if ms := f.agent.take(); len(ms) != 1 {
		t.Fatal("drain not forwarded")
	}
	f.up.take()
	_ = f.l.Handle(Budget{Tenant: tenant, Version: 2, Delegated: 10 << 20})
	_ = f.s.Handle(session.GrantRequest{ID: 1, Kind: model.GrantBilling, Rule: ruleA}) // 8 MiB：剩余 < 30%
	_ = f.l.Tick()
	var req bool
	for _, u := range f.up.take() {
		if b, ok := u.(BudgetRequest); ok && b.Tenant == tenant {
			req = true
		}
	}
	if !req {
		t.Fatal("no top-up request below 30%")
	}
	f.l.Disconnected()
	f.clk.Advance(31 * time.Second)
	_ = f.h.Tick()
	f.agent.take()
	_ = f.l.Connected(f.up.put) // 对账中
	_ = f.l.Handle(Leases{Node: node, Leases: []leases.Lease{{Tenant: tenant, Kind: model.GrantShareRate, Amount: 99999}}})
	if f.h.Leases.Held(node, leases.Key{Tenant: tenant, Kind: model.GrantShareRate}) != 1000 {
		t.Fatal("share increased while not online")
	}
}

// HUB-004：节点状态缓存与批量上报。接入立即上报；状态与探测随下一次 Tick 上报；断开标记离线；
// 后端重连时上报全部节点。
func TestNodeStatus(t *testing.T) {
	f := setup(t)
	var first []session.NodeState
	for _, u := range f.up.take() {
		if b, ok := u.(NodeStatusBatch); ok {
			first = b.Nodes
		}
	}
	if len(first) != 1 || !first[0].Connected || first[0].Boot != boot1 {
		t.Fatalf("status on hello %+v", first)
	}
	_ = f.s.Handle(session.Passthrough{Kind: "probes", Payload: json.RawMessage(`{"p":1}`)})
	_ = f.s.Handle(session.ApplyReport{Version: 4, Committed: true})
	f.up.take()
	_ = f.l.Tick()
	var got []session.NodeState
	for _, u := range f.up.take() {
		if b, ok := u.(NodeStatusBatch); ok {
			got = b.Nodes
		}
	}
	if len(got) != 1 || string(got[0].Probes) != `{"p":1}` || got[0].AppliedConfigVersion != 4 {
		t.Fatalf("changed status %+v", got)
	}
	_ = f.l.Tick()
	for _, u := range f.up.take() {
		if _, ok := u.(NodeStatusBatch); ok {
			t.Fatal("unchanged status re-sent")
		}
	}
	f.s.Close()
	_ = f.l.Tick()
	got = nil
	for _, u := range f.up.take() {
		if b, ok := u.(NodeStatusBatch); ok {
			got = b.Nodes
		}
	}
	if len(got) != 1 || got[0].Connected {
		t.Fatalf("disconnect not reported %+v", got)
	}
	f.l.Disconnected()
	_ = f.l.Connected(f.up.put)
	got = nil
	for _, u := range f.up.take() {
		if b, ok := u.(NodeStatusBatch); ok {
			got = b.Nodes
		}
	}
	if len(got) != 1 {
		t.Fatal("full status not sent on backend reconnect")
	}
}
