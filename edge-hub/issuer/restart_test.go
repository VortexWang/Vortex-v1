package issuer

import (
	"errors"
	"testing"
	"time"

	"vortex/edgehub/model"
)

var (
	boot2 = model.UUID{0x97}
	node2 = model.UUID{0x96}
)

// LIM-010：同一节点以新 boot_id 接入，旧启动的运行授权立即作废，计费授权转入待结算（预留保留到最终上报），
// 新计费授权只能从 available 签发；旧启动不能再申请；其他节点不受影响。
func TestRestartTransfer(t *testing.T) {
	h := newHub(t)
	_, _ = h.q.ApplyBudget(tenantA, 1, 64<<20, 0) // 64 MiB
	ep := RuleState{RuleID: ruleX, TenantID: tenantA, EnforcementPoint: true, PathCoefMilli: 1000}
	if tr, err := h.is.Connect(node, boot); tr != nil || err != nil {
		t.Fatalf("first connect %+v %v", tr, err)
	}
	run, _, _ := h.is.IssueRun(node, boot, []RuleState{ep})
	bill, _ := h.is.IssueBilling(BillingRequest{Node: node, Boot: boot, Rule: ep})
	other, _, _ := h.is.IssueRun(node2, boot, []RuleState{ep}) // 另一节点，碰巧同一 boot 值
	before := h.q.View(tenantA)

	tr, err := h.is.Connect(node, boot2)
	if err != nil || tr == nil || tr.OldBoot != boot || len(tr.RevokedRun) != 1 || tr.RevokedRun[0] != run.ID ||
		len(tr.SettlingIDs) != 1 || tr.SettlingIDs[0] != bill.ID {
		t.Fatalf("transfer %+v %v", tr, err)
	}
	if _, ok := h.is.Grant(run.ID); ok {
		t.Fatal("old run grant not revoked")
	}
	if g, ok := h.is.Grant(bill.ID); !ok || !g.Settling {
		t.Fatal("old billing grant must stay outstanding as settling")
	}
	if _, ok := h.is.Grant(other.ID); !ok {
		t.Fatal("transfer touched another node")
	}
	if v := h.q.View(tenantA); v != before {
		t.Fatalf("transfer changed the regional quota %+v → %+v", before, v)
	}
	if _, _, err := h.is.IssueRun(node, boot, []RuleState{ep}); !errors.Is(err, ErrStaleBoot) {
		t.Fatalf("old boot renewed: %v", err)
	}
	if _, err := h.is.IssueBilling(BillingRequest{Node: node, Boot: boot, Rule: ep}); !errors.Is(err, ErrStaleBoot) {
		t.Fatalf("old boot got billing: %v", err)
	}
	if _, err := h.is.Connect(node, boot); !errors.Is(err, ErrStaleBoot) {
		t.Fatalf("old boot reconnected: %v", err)
	}
	// 新启动从 available 签发：64 − 8 = 56 MiB 可用
	g, err := h.is.IssueBilling(BillingRequest{Node: node, Boot: boot2, Rule: ep, RateBps60s: 100_000_000_000})
	if err != nil || g.RawBytes != 56<<20 {
		t.Fatalf("new boot billing %+v %v", g, err)
	}
	// 旧授权按旧账本补报结算（AGT-009），释放余量
	if s, err := h.is.ReportConsumption(bill.ID, 1<<20, true); err != nil || s != 1<<20 {
		t.Fatalf("settle old billing %d %v", s, err)
	}
	if v := h.q.View(tenantA); v.Available != 7<<20 {
		t.Fatalf("after old settlement %+v, want 7 MiB available", v)
	}

	// 账本重放后转移状态完整
	h2 := open(t, h.clk, h.st)
	if b, _ := h2.is.Boot(node); b != boot2 {
		t.Fatalf("boot after restart %v", b)
	}
	if _, _, err := h2.is.IssueRun(node, boot, []RuleState{ep}); !errors.Is(err, ErrStaleBoot) {
		t.Fatal("stale boot accepted after hub restart")
	}
	if _, ok := h2.is.Grant(run.ID); ok {
		t.Fatal("revoked run grant resurrected by replay")
	}
	if v, w := h2.q.View(tenantA), h.q.View(tenantA); v != w {
		t.Fatalf("quota after replay %+v, want %+v", v, w)
	}
}

// 自治中（运行授权续期仍允许）同样转移；自治到期后转移仍记录，但新启动拿不到运行授权。
func TestRestartTransferDuringAutonomy(t *testing.T) {
	h := newHub(t)
	ep := RuleState{RuleID: ruleX, TenantID: tenantA}
	_, _ = h.is.Connect(node, boot)
	_, _, _ = h.is.IssueRun(node, boot, []RuleState{ep})
	h.run = false
	tr, err := h.is.Connect(node, boot2)
	if err != nil || tr == nil || len(tr.RevokedRun) != 1 {
		t.Fatalf("transfer while autonomy expired %+v %v", tr, err)
	}
	if g, refused, _ := h.is.IssueRun(node, boot2, []RuleState{ep}); g != nil || !errors.Is(refused[ruleX], ErrAutonomyExpired) {
		t.Fatal("run grant issued after autonomy expiry")
	}
}

// HUB-014 / HUB-011：他实例签发的授权只记录、不占本实例额度；本实例的只更新已用量；一份报告替换该节点之前的报告。
func TestHeldGrantsReport(t *testing.T) {
	h := newHub(t)
	h.is.HubID = model.UUID{0x01}
	other := model.UUID{0x02}
	_, _ = h.q.ApplyBudget(tenantA, 1, 64<<20, 0)
	ep := RuleState{RuleID: ruleX, TenantID: tenantA, EnforcementPoint: true, PathCoefMilli: 1000}
	own, _ := h.is.IssueBilling(BillingRequest{Node: node, Boot: boot, Rule: ep})
	before := h.q.View(tenantA)
	foreign := HeldGrant{ID: model.UUID{0x31}, IssuerHub: other, Kind: model.GrantBilling, Tenant: tenantA, Rule: ruleX,
		Amount: 32 << 20, Used: 1 << 20}
	ttl := func(HeldGrant) time.Duration { return 4 * time.Minute }
	n, err := h.is.RecordHeld(node, boot, []HeldGrant{foreign, {ID: own.ID, IssuerHub: h.is.HubID, Kind: model.GrantBilling, Used: 5 << 20}}, ttl)
	if err != nil || n != 1 {
		t.Fatalf("recorded %d %v", n, err)
	}
	if v := h.q.View(tenantA); v != before {
		t.Fatalf("foreign grant entered own quota %+v → %+v", before, v)
	}
	if g, _ := h.is.Grant(own.ID); g.Used != 5<<20 {
		t.Fatalf("own grant used %d", g.Used)
	}
	if hs := h.is.Held(); len(hs) != 1 || hs[0].ID != foreign.ID || hs[0].Node != node || hs[0].IssuerHub != other {
		t.Fatalf("held %+v", hs)
	}
	// 不会续期或重复签发他实例授权：上报他实例 ID 的消耗被忽略
	if s, _ := h.is.ReportConsumption(foreign.ID, 1<<20, true); s != 0 {
		t.Fatal("foreign grant settled against own quota")
	}
	// 重放后仍在；到期后不再上报
	h2 := open(t, h.clk, h.st)
	h2.is.HubID = h.is.HubID
	if hs := h2.is.Held(); len(hs) != 1 {
		t.Fatalf("held after restart %+v", hs)
	}
	// 新报告替换该节点的旧报告；其他节点的报告不受影响
	_, _ = h2.is.RecordHeld(node2, boot, []HeldGrant{{ID: model.UUID{0x32}, IssuerHub: other}}, ttl)
	_, _ = h2.is.RecordHeld(node, boot, nil, ttl)
	if hs := h2.is.Held(); len(hs) != 1 || hs[0].Node != node2 {
		t.Fatalf("empty report must clear only that node's grants %+v", hs)
	}
	h2.clk.Advance(4 * time.Minute)
	if hs := h2.is.Held(); len(hs) != 0 {
		t.Fatalf("expired held grants still reported %+v", hs)
	}
}

// HUB-017：额度已被回收的租户在对账完成前不签发计费授权（运行授权不受影响）；重启后保持。
func TestBudgetRevoked(t *testing.T) {
	h := newHub(t)
	_, _ = h.q.ApplyBudget(tenantA, 1, 64<<20, 0)
	ep := RuleState{RuleID: ruleX, TenantID: tenantA, EnforcementPoint: true, PathCoefMilli: 1000}
	if err := h.is.SetBudgetRevoked([]model.UUID{tenantA}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.is.IssueBilling(BillingRequest{Node: node, Boot: boot, Rule: ep}); !errors.Is(err, ErrBudgetRevoked) {
		t.Fatalf("billing for a revoked tenant: %v", err)
	}
	if g, _, _ := h.is.IssueRun(node, boot, []RuleState{ep}); g == nil {
		t.Fatal("run grant must not be affected")
	}
	h2 := open(t, h.clk, h.st)
	if _, err := h2.is.IssueBilling(BillingRequest{Node: node, Boot: boot, Rule: ep}); !errors.Is(err, ErrBudgetRevoked) {
		t.Fatal("revocation lost on restart")
	}
	_ = h2.is.SetBudgetRevoked(nil)
	if _, err := h2.is.IssueBilling(BillingRequest{Node: node, Boot: boot, Rule: ep}); err != nil {
		t.Fatalf("after reconciliation: %v", err)
	}
}

// 首次接入后、尚未签发任何授权就重启 Edge Hub：仍能识别节点的下一次新启动并转移。
func TestFirstBootSurvivesHubRestart(t *testing.T) {
	h := newHub(t)
	_, _ = h.is.Connect(node, boot)
	h2 := open(t, h.clk, h.st)
	tr, err := h2.is.Connect(node, boot2)
	if err != nil || tr == nil || tr.OldBoot != boot {
		t.Fatalf("restart of a node not recognised after hub restart: %+v %v", tr, err)
	}
}
