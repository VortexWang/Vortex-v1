package issuer

import (
	"errors"
	"testing"
	"time"

	"vortex/edgehub/clock"
	"vortex/edgehub/ledger"
	"vortex/edgehub/model"
	"vortex/edgehub/quota"
)

var (
	tenantA = model.UUID{0xa}
	tenantB = model.UUID{0xb}
	ruleX   = model.UUID{0x11}
	ruleY   = model.UUID{0x12}
	node    = model.UUID{0x99}
	boot    = model.UUID{0x98}
)

type hub struct {
	clk *clock.Fake
	st  *ledger.MemStore
	q   *quota.Accounts
	is  *Issuer
	run bool
}

// open 按账本重放组装 quota 与 issuer（与进程启动相同）。
func open(t *testing.T, clk *clock.Fake, st *ledger.MemStore) *hub {
	h := &hub{clk: clk, st: st, run: true}
	h.q = quota.New(nil)
	h.is = New(clk, h.q, func() bool { return h.run })
	l, err := ledger.Open(st, clk, func(e ledger.Entry) error {
		if _, err := h.q.Replay(e); err != nil {
			return err
		}
		_, err := h.is.Replay(e)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	h.q.SetLedger(l)
	h.is.Bind(l)
	return h
}

func newHub(t *testing.T) *hub {
	h := open(t, clock.NewFake(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)), &ledger.MemStore{})
	if err := h.is.SetBillingVersions([]BillingVersion{{Version: 1, EffectiveAt: h.clk.Now().Add(-time.Hour)}}); err != nil {
		t.Fatal(err)
	}
	return h
}

func TestRunGrantRestrictions(t *testing.T) {
	h := newHub(t)
	now := h.clk.Now()
	_ = h.is.SetRestrictions(1, []TenantRestriction{
		{Tenant: tenantA, Status: AccountActive, PlanExpiresAt: now.Add(2 * time.Minute), StoppedRules: []model.UUID{ruleY}},
		{Tenant: tenantB, Status: AccountSuspended},
	})
	rules := []RuleState{{RuleID: ruleX, TenantID: tenantA}, {RuleID: ruleY, TenantID: tenantA}, {RuleID: model.UUID{0x13}, TenantID: tenantB}}
	g, refused, err := h.is.IssueRun(node, boot, rules)
	if err != nil || g == nil || len(g.Rules) != 1 || g.Rules[0] != ruleX {
		t.Fatalf("grant %+v %v", g, err)
	}
	if !errors.Is(refused[ruleY], ErrRuleStopped) || !errors.Is(refused[model.UUID{0x13}], ErrAccountSuspended) {
		t.Fatalf("refused %v", refused)
	}
	if g.TTL != 2*time.Minute {
		t.Fatalf("ttl %v must not exceed plan expiry", g.TTL)
	}
	h.clk.Advance(3 * time.Minute)
	if g, refused, _ := h.is.IssueRun(node, boot, rules[:1]); g != nil || !errors.Is(refused[ruleX], ErrPlanExpired) {
		t.Fatal("expired plan must not be renewed")
	}
	// 旧版本的快照被忽略
	_ = h.is.SetRestrictions(1, nil)
	if g, _, _ := h.is.IssueRun(node, boot, rules[2:]); g != nil {
		t.Fatal("stale restriction snapshot applied")
	}
	// 自治到期：全部停止续期
	_ = h.is.SetRestrictions(2, nil)
	h.run = false
	if g, refused, _ := h.is.IssueRun(node, boot, rules[:1]); g != nil || !errors.Is(refused[ruleX], ErrAutonomyExpired) {
		t.Fatal("run grant issued after autonomy expired")
	}
}

func TestBillingGrantSizingAndQuota(t *testing.T) {
	h := newHub(t)
	ep := RuleState{RuleID: ruleX, TenantID: tenantA, EnforcementPoint: true, PathCoefMilli: 2000}
	if _, err := h.is.IssueBilling(BillingRequest{Node: node, Boot: boot, Rule: RuleState{RuleID: ruleX, TenantID: tenantA}}); !errors.Is(err, ErrNotEnforcement) {
		t.Fatalf("non enforcement point: %v", err)
	}
	if _, err := h.is.IssueBilling(BillingRequest{Rule: ep}); !errors.Is(err, ErrQuotaExhausted) {
		t.Fatalf("no regional quota: %v", err)
	}
	_, _ = h.q.ApplyBudget(tenantA, 1, 1<<30, 0) // 委托 1 GiB 计费字节
	g, err := h.is.IssueBilling(BillingRequest{Node: node, Boot: boot, Rule: ep, RateBps60s: 1_000_000})
	if err != nil || g.RawBytes != MinBillingRaw || g.Reserved != 2*MinBillingRaw || g.BillingVersion != 1 {
		t.Fatalf("low rate grant %+v %v (want the 8 MiB minimum, reserved ×2 for coefficient 2.0)", g, err)
	}
	// 速率 800 Mbit/s × 30 秒 = 3 GB 原始字节，超过剩余区域额度：按 available 截断
	// （1 GiB − 16 MiB 计费字节，系数 2.0 → 528,482,304 原始字节）
	g2, err := h.is.IssueBilling(BillingRequest{Node: node, Boot: boot, Rule: ep, RateBps60s: 800_000_000})
	if want := uint64(1<<30-2*MinBillingRaw) / 2; err != nil || g2.RawBytes != want {
		t.Fatalf("capped grant %d, want %d (%v)", g2.RawBytes, want, err)
	}
	if v := h.q.View(tenantA); v.Available != 0 {
		t.Fatalf("available after a capped grant: %+v", v)
	}
	if _, err := h.is.IssueBilling(BillingRequest{Rule: ep}); !errors.Is(err, ErrQuotaExhausted) {
		t.Fatalf("exhausted: %v", err)
	}
	_, _ = h.is.ReportConsumption(g2.ID, 0, true) // 释放第二份，下面只剩第一份的预留
	// 结算：非 final 不动额度，final 释放余量；重复 final 被忽略
	if s, _ := h.is.ReportConsumption(g.ID, 1<<20, false); s != 0 || h.q.View(tenantA).Issued == 0 {
		t.Fatal("non-final report must not settle")
	}
	s, err := h.is.ReportConsumption(g.ID, 3<<20, true)
	if err != nil || s != 6<<20 {
		t.Fatalf("settled %d %v", s, err)
	}
	if s, _ := h.is.ReportConsumption(g.ID, 3<<20, true); s != 0 {
		t.Fatal("duplicate final settled twice")
	}
	if v := h.q.View(tenantA); v.Available != uint64(1<<30)-6<<20 || v.Issued != 0 || v.SettledLocal != 6<<20 {
		t.Fatalf("after settlement %+v, want available = 1 GiB − 6 MiB used", v)
	}
}

func TestBillingVersionBoundaryAndMissing(t *testing.T) {
	h := open(t, clock.NewFake(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)), &ledger.MemStore{})
	ep := RuleState{RuleID: ruleX, TenantID: tenantA, EnforcementPoint: true, PathCoefMilli: 1000}
	_, _ = h.q.ApplyBudget(tenantA, 1, 1<<30, 0)
	if _, err := h.is.IssueBilling(BillingRequest{Rule: ep}); !errors.Is(err, ErrNoBillingVersion) {
		t.Fatalf("no version: %v", err)
	}
	now := h.clk.Now()
	_ = h.is.SetBillingVersions([]BillingVersion{{2, now.Add(3 * time.Minute)}, {1, now.Add(-time.Hour)}})
	g, err := h.is.IssueBilling(BillingRequest{Rule: ep})
	if err != nil || g.BillingVersion != 1 || g.TTL != 3*time.Minute {
		t.Fatalf("grant spanning a version change: %+v %v", g, err)
	}
	h.clk.Advance(3 * time.Minute)
	g2, _ := h.is.IssueBilling(BillingRequest{Rule: ep})
	if g2.BillingVersion != 2 || g2.TTL != BillingTTL {
		t.Fatalf("after effective time: %+v", g2)
	}
}

// 重启（例如自治期间）后按账本恢复：未结算的计费授权、限制快照、计费版本都在。
func TestIssuerStateSurvivesRestart(t *testing.T) {
	h := newHub(t)
	_, _ = h.q.ApplyBudget(tenantA, 1, 1<<30, 0)
	_ = h.is.SetRestrictions(5, []TenantRestriction{{Tenant: tenantB, Status: AccountBanned}})
	ep := RuleState{RuleID: ruleX, TenantID: tenantA, EnforcementPoint: true, PathCoefMilli: 1500}
	g, _ := h.is.IssueBilling(BillingRequest{Node: node, Boot: boot, Rule: ep})
	before := h.q.View(tenantA)

	h2 := open(t, h.clk, h.st)
	if v := h2.q.View(tenantA); v != before {
		t.Fatalf("quota after restart %+v, before %+v", v, before)
	}
	if got, ok := h2.is.Grant(g.ID); !ok || got.Tenant != tenantA || got.RawBytes != g.RawBytes {
		t.Fatal("outstanding billing grant lost on restart")
	}
	if g, _, _ := h2.is.IssueRun(node, boot, []RuleState{{RuleID: ruleY, TenantID: tenantB}}); g != nil {
		t.Fatal("banned tenant renewed after restart (restriction snapshot lost)")
	}
	if s, err := h2.is.ReportConsumption(g.ID, 1000, true); err != nil || s != 1500 {
		t.Fatalf("settle after restart: %d %v", s, err)
	}
	if out := h2.is.Outstanding(node, boot); len(out) != 0 {
		t.Fatalf("outstanding after settlement %+v", out)
	}
}
