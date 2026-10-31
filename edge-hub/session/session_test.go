package session

import (
	"errors"
	"sync"
	"testing"
	"time"

	"vortex/edgehub/clock"
	"vortex/edgehub/issuer"
	"vortex/edgehub/leases"
	"vortex/edgehub/ledger"
	"vortex/edgehub/model"
)

var (
	hubID  = model.UUID{0x4b}
	node   = model.UUID{0x0d}
	boot1  = model.UUID{0xb1}
	boot2  = model.UUID{0xb2}
	tenant = model.UUID{0x7e}
	ruleA  = model.UUID{0xaa}
)

type inbox struct {
	mu   sync.Mutex
	msgs []HubMsg
}

func (b *inbox) send(m HubMsg) { b.mu.Lock(); b.msgs = append(b.msgs, m); b.mu.Unlock() }

func (b *inbox) take() []HubMsg {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.msgs
	b.msgs = nil
	return out
}

type fixture struct {
	clk *clock.Fake
	st  *ledger.MemStore
	h   *Hub
	up  *upstream
}

type upstream struct {
	mu   sync.Mutex
	acks []func()
	got  []uint64
}

func (u *upstream) Forward(_ model.UUID, b UsageBatch, ack func()) {
	u.mu.Lock()
	u.got = append(u.got, b.Seq)
	u.acks = append(u.acks, ack)
	u.mu.Unlock()
}

func rules(model.UUID) []issuer.RuleState {
	return []issuer.RuleState{{RuleID: ruleA, TenantID: tenant, EnforcementPoint: true, PathCoefMilli: 1000}}
}

func open(t *testing.T, clk *clock.Fake, st *ledger.MemStore, up Upstream) *Hub {
	h, err := Open(Options{HubID: hubID, Clock: clk, Store: st, Rules: RulesFunc(rules), Upstream: up})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func setup(t *testing.T, withUpstream bool) *fixture {
	clk := clock.NewFake(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC))
	f := &fixture{clk: clk, st: &ledger.MemStore{}}
	var up Upstream
	if withUpstream {
		f.up = &upstream{}
		up = f.up
	}
	f.h = open(t, clk, f.st, up)
	_, _ = f.h.Quota.ApplyBudget(tenant, 1, 1<<30, 0)
	_ = f.h.Issuer.SetBillingVersions([]issuer.BillingVersion{{Version: 1, EffectiveAt: clk.Now().Add(-time.Hour)}})
	return f
}

func connect(t *testing.T, h *Hub, b model.UUID) (*Session, *inbox) {
	in := &inbox{}
	s := h.NewSession(node, in.send)
	if err := s.Handle(Hello{Node: node, Boot: b}); err != nil {
		t.Fatal(err)
	}
	return s, in
}

func TestHelloRules(t *testing.T) {
	f := setup(t, false)
	s := f.h.NewSession(node, func(HubMsg) {})
	if err := s.Handle(GrantRequest{Kind: model.GrantRun}); !errors.Is(err, ErrNoHello) {
		t.Fatalf("message before hello: %v", err)
	}
	if err := s.Handle(Hello{Node: model.UUID{0x99}, Boot: boot1}); !errors.Is(err, ErrNodeMismatch) {
		t.Fatalf("node mismatch: %v", err)
	}
	s, in := connect(t, f.h, boot1)
	if m := in.take(); len(m) != 1 || m[0] != (HubHello{Hub: hubID, Mode: model.ModeOnline}) {
		t.Fatalf("hub hello %+v", m)
	}
	if err := s.Handle(Hello{Node: node, Boot: boot1}); !errors.Is(err, ErrNoHello) {
		t.Fatal("second hello accepted")
	}
	// 同一节点的新流取代旧流
	s2, _ := connect(t, f.h, boot1)
	if err := s.Handle(GrantRequest{Kind: model.GrantRun}); !errors.Is(err, ErrReplaced) {
		t.Fatalf("old stream still served: %v", err)
	}
	s.Close() // 旧流关闭不影响新流的登记
	if f.h.Sessions() != 1 {
		t.Fatal("old stream close removed the new one")
	}
	s2.Close()
	if f.h.Sessions() != 0 {
		t.Fatal("session not removed")
	}
}

func TestGrants(t *testing.T) {
	f := setup(t, false)
	_, _ = f.h.Leases.ApplyUpdate(node, boot1, []leases.Lease{{Tenant: tenant, Kind: model.GrantShareRate, Amount: 1000}})
	s, in := connect(t, f.h, boot1)
	in.take()
	f.clk.Advance(time.Second)
	_ = s.Handle(GrantRequest{ID: 1, Kind: model.GrantRun})
	r := in.take()[0].(GrantResponse)
	if r.RequestID != 1 || len(r.Run) != 1 || r.Run[0].Rules[0] != ruleA || len(r.Shares) != 1 || r.Shares[0].Amount != 1000 || r.Error != "" {
		t.Fatalf("bootstrap %+v", r)
	}
	if r.Shares[0].TTL != leases.TTL-time.Second {
		t.Fatalf("share TTL must be relative: %v", r.Shares[0].TTL)
	}
	_ = s.Handle(GrantRequest{ID: 2, Kind: model.GrantRun, CurrentGrant: r.Run[0].ID})
	if r := in.take()[0].(GrantResponse); len(r.Run) != 1 || len(r.Shares) != 0 {
		t.Fatalf("renewal must not resend shares %+v", r)
	}
	_ = s.Handle(GrantRequest{ID: 3, Kind: model.GrantBilling, Rule: ruleA, RateBps60s: 1_000_000})
	if r := in.take()[0].(GrantResponse); len(r.Billing) != 1 || r.Billing[0].RawBytes != issuer.MinBillingRaw {
		t.Fatalf("billing %+v", r)
	}
	_ = s.Handle(GrantRequest{ID: 4, Kind: model.GrantBilling, Rule: model.UUID{0x55}})
	if r := in.take()[0].(GrantResponse); r.Error != issuer.ErrRuleStopped.Error() {
		t.Fatalf("unknown rule %+v", r)
	}
	_ = s.Handle(GrantRequest{ID: 5, Kind: model.GrantShareRate, Tenant: tenant})
	if r := in.take()[0].(GrantResponse); len(r.Shares) != 1 || r.Shares[0].Amount != 1000 {
		t.Fatalf("share renewal %+v", r)
	}
	_ = s.Handle(GrantRequest{ID: 6, Kind: model.GrantShareConn, Tenant: tenant})
	if r := in.take()[0].(GrantResponse); r.Error == "" {
		t.Fatal("renewal of a share the node does not hold")
	}
}

// LIM-010：新 boot_id 接入时推送作废与按原值转移的份额；旧启动的流被拒。
func TestRestartTransfer(t *testing.T) {
	f := setup(t, false)
	_, _ = f.h.Leases.ApplyUpdate(node, boot1, []leases.Lease{{Tenant: tenant, Kind: model.GrantShareRate, Amount: 1000}})
	s1, in1 := connect(t, f.h, boot1)
	_ = s1.Handle(GrantRequest{ID: 1, Kind: model.GrantRun})
	_ = s1.Handle(GrantRequest{ID: 2, Kind: model.GrantBilling, Rule: ruleA})
	ms := in1.take()
	run := ms[1].(GrantResponse).Run[0].ID
	bill := ms[2].(GrantResponse).Billing[0].ID

	_, in2 := connect(t, f.h, boot2)
	ms = in2.take()
	if len(ms) != 2 {
		t.Fatalf("after restart %+v", ms)
	}
	push := ms[1].(GrantResponse)
	revoked := map[model.UUID]bool{}
	for _, id := range push.Revoked {
		revoked[id] = true
	}
	if !revoked[run] || !revoked[bill] || len(push.Shares) != 1 || push.Shares[0].Boot != boot2 || push.Shares[0].Amount != 1000 {
		t.Fatalf("transfer push %+v", push)
	}
	if g, ok := f.h.Issuer.Grant(bill); !ok || !g.Settling {
		t.Fatal("old billing grant must stay as settling")
	}
	old := f.h.NewSession(node, func(HubMsg) {})
	if err := old.Handle(Hello{Node: node, Boot: boot1}); !errors.Is(err, issuer.ErrStaleBoot) {
		t.Fatalf("stale boot stream: %v", err)
	}
}

// 计量：按序落盘、结算 final 消耗、记录需求；重复只再确认；缺批不确认；Hub 重启后仍去重。
func TestUsage(t *testing.T) {
	f := setup(t, false)
	s, in := connect(t, f.h, boot1)
	_ = s.Handle(GrantRequest{ID: 1, Kind: model.GrantBilling, Rule: ruleA})
	in.take()
	before := f.h.Quota.View(tenant)
	g := func() model.UUID {
		for _, x := range f.h.Issuer.Outstanding(node, boot1) {
			if x.Kind == model.GrantBilling {
				return x.ID
			}
		}
		return model.UUID{}
	}()
	_ = s.Handle(UsageBatch{Boot: boot1, Seq: 1, Demands: []Demand{{Tenant: tenant, Bytes60s: 10}}})
	_ = s.Handle(UsageBatch{Boot: boot1, Seq: 3})
	_ = s.Handle(UsageBatch{Boot: boot1, Seq: 2, Consumptions: []Consumption{{Grant: g, Used: 1 << 20, Final: true}}})
	_ = s.Handle(UsageBatch{Boot: boot1, Seq: 2, Consumptions: []Consumption{{Grant: g, Used: 1 << 20, Final: true}}})
	ms := in.take()
	// 序号 3 先到（2 可能已由另一实例确认）：照收；2 的重复只再确认
	if len(ms) != 4 || ms[0] != (UsageAck{boot1, 1}) || ms[1] != (UsageAck{boot1, 3}) || ms[2] != (UsageAck{boot1, 2}) || ms[3] != (UsageAck{boot1, 2}) {
		t.Fatalf("acks %+v", ms)
	}
	if v := f.h.Quota.View(tenant); v.SettledLocal != 1<<20 || v.Issued != 0 || before.Issued == 0 {
		t.Fatalf("final consumption not settled once: %+v", v)
	}
	h2 := open(t, f.clk, f.st, nil)
	s2 := h2.NewSession(node, func(HubMsg) {})
	_ = s2.Handle(Hello{Node: node, Boot: boot1})
	n := 0
	_ = f.st.Load(func(e ledger.Entry) error {
		if e.Kind == KindUsage {
			n++
		}
		return nil
	})
	_ = s2.Handle(UsageBatch{Boot: boot1, Seq: 2})
	n2 := 0
	_ = f.st.Load(func(e ledger.Entry) error {
		if e.Kind == KindUsage {
			n2++
		}
		return nil
	})
	if n != 3 || n2 != 3 {
		t.Fatalf("usage records %d → %d (duplicate after hub restart)", n, n2)
	}
}

// HUB-003：在线且有上游时，上游确认后才确认；自治时落盘即确认。
func TestUsageAckFollowsUpstreamWhenOnline(t *testing.T) {
	f := setup(t, true)
	s, in := connect(t, f.h, boot1)
	in.take()
	_ = s.Handle(UsageBatch{Boot: boot1, Seq: 1})
	if len(in.take()) != 0 {
		t.Fatal("acked before the backend")
	}
	f.up.acks[0]()
	if ms := in.take(); len(ms) != 1 || ms[0] != (UsageAck{boot1, 1}) {
		t.Fatalf("ack after upstream %+v", ms)
	}
	_ = s.Handle(UsageBatch{Boot: boot1, Seq: 1}) // 重发：再次转发（上游幂等），等上游确认
	if len(f.up.got) != 2 || len(in.take()) != 0 {
		t.Fatal("resend must go upstream again")
	}
	// 失联 30 秒进入自治：落盘即确认，并推送模式变化
	f.clk.Advance(31 * time.Second)
	if err := f.h.Tick(); err != nil {
		t.Fatal(err)
	}
	if ms := in.take(); len(ms) != 1 || ms[0] != (ModeChange{model.ModeAutonomous}) {
		t.Fatalf("mode change %+v", ms)
	}
	if f.h.Leases.Frozen() == nil {
		t.Fatal("shares not frozen in autonomy")
	}
	_ = s.Handle(UsageBatch{Boot: boot1, Seq: 2})
	if ms := in.take(); len(ms) != 1 || ms[0] != (UsageAck{boot1, 2}) {
		t.Fatalf("autonomous ack %+v", ms)
	}
	if len(f.up.got) != 2 {
		t.Fatal("forwarded upstream while autonomous")
	}
}

// HUB-014：他实例签发的授权记录下来，不进入本实例额度。
func TestHeldGrants(t *testing.T) {
	f := setup(t, false)
	s, _ := connect(t, f.h, boot1)
	other := model.UUID{0x4c}
	err := s.Handle(HeldGrants{Grants: []HeldGrant{{HeldGrant: issuer.HeldGrant{ID: model.UUID{0x31}, IssuerHub: other, Kind: model.GrantBilling, Tenant: tenant, Amount: 1 << 20}, Remaining: time.Minute}}})
	if err != nil {
		t.Fatal(err)
	}
	if hs := f.h.Issuer.Held(); len(hs) != 1 || hs[0].IssuerHub != other || !hs[0].ExpiresAt.Equal(f.clk.Now().Add(time.Minute)) {
		t.Fatalf("held %+v", hs)
	}
}
