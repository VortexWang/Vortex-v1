package leases

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	p "vortex/backend/internal/platform"
	edgehubv1 "vortex/proto/gen/go/vortex/edgehub/v1"
	runtimev1 "vortex/proto/gen/go/vortex/runtime/v1"
)

func (f *fixture) delegated(hub p.UUID) string {
	return f.query("SELECT delegated || (CASE WHEN frozen THEN '/frozen' ELSE '' END) FROM regional_budgets WHERE tenant_id = $1 AND hub_id = $2",
		f.tenant.PG(), hub.PG())
}

func (f *fixture) budgetRow(hub p.UUID, n int64) {
	f.exec("INSERT INTO regional_budgets (tenant_id, hub_id, delegated, budget_version) VALUES ($1, $2, $3, 1)", f.tenant.PG(), hub.PG(), n)
}

// HUB-015 ②：失联超过“自治上限 + 运行授权有效期 + 安全余量”后回收全部未结算部分；HUB-017：恢复联系时报告被回收的租户。
func TestReclaimAfterTimeout(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	f.subscribe(f.tenant, 1000, 10, 1<<30)
	old := f.hub("online", true)
	f.budgetRow(old, 5000)
	// 后端启动：上次连接的实例视为失联并冻结
	if err := f.s.Startup(ctx); err != nil {
		t.Fatal(err)
	}
	if f.delegated(old) != "5000/frozen" || f.query("SELECT mode FROM edge_hubs WHERE id = $1", old.PG()) != "offline" {
		t.Fatalf("after startup %s", f.delegated(old))
	}
	// 最近联系过：冻结、不回收
	f.exec("UPDATE edge_hubs SET last_contact_at = now() - interval '12 hours' WHERE id = $1", old.PG())
	if err := f.s.Reclaim(ctx); err != nil {
		t.Fatal(err)
	}
	if f.delegated(old) != "5000/frozen" {
		t.Fatalf("reclaimed too early: %s", f.delegated(old))
	}
	f.exec("UPDATE edge_hubs SET last_contact_at = now() - interval '12 hours 11 minutes' WHERE id = $1", old.PG())
	if err := f.s.Reclaim(ctx); err != nil {
		t.Fatal(err)
	}
	if f.delegated(old) != "0/frozen" {
		t.Fatalf("after the timeout %s", f.delegated(old))
	}
	if got := f.query("SELECT detail->>'reason' FROM audit_logs WHERE action = 'budget.reclaim' AND tenant_id = $1", f.tenant.PG()); got != "timeout" {
		t.Fatalf("audit %q", got)
	}
	// 恢复联系：报告被回收的租户并解冻；再次注册时不再报告
	revoked, err := f.s.HubRegistered(ctx, old, nil, false)
	if err != nil || len(revoked) != 1 || revoked[0] != f.tenant || f.delegated(old) != "0" {
		t.Fatalf("revoked %v %v, row %s", revoked, err, f.delegated(old))
	}
	if err := f.s.HubDisconnected(ctx, old); err != nil {
		t.Fatal(err)
	}
	if revoked, _ := f.s.HubRegistered(ctx, old, nil, false); len(revoked) != 0 {
		t.Fatalf("an earlier reclaim reported again: %v", revoked)
	}
	// 实例不处于离线（已在对账）：不回收
	f.exec("UPDATE regional_budgets SET delegated = 7, frozen = true, frozen_at = now()")
	f.exec("UPDATE edge_hubs SET mode = 'reconciling' WHERE id = $1", old.PG())
	if err := f.s.Reclaim(ctx); err != nil {
		t.Fatal(err)
	}
	if f.delegated(old) != "7/frozen" {
		t.Fatalf("reclaimed a reconnected hub: %s", f.delegated(old))
	}
}

// HUB-015 ①：全部执行点已接入其他在线实例并报告持有的授权后，委托下调到已报告授权的预留；HUB-016：释放的余额补给接管的实例。
func TestReclaimAfterReports(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	f.subscribe(f.tenant, 1000, 10, 100<<20)
	rule, _ := f.rules()
	old, cur := f.hub("offline", false), f.hub("online", true)
	f.exec("UPDATE edge_hubs SET last_contact_at = now() - interval '5 minutes' WHERE id = $1", old.PG())
	f.budgetRow(old, 80<<20)
	if err := f.s.HubDisconnected(ctx, old); err != nil {
		t.Fatal(err)
	}
	n, boot := f.node(old, true, f.tenant) // 执行点仍在失联实例上
	far, _ := f.node(cur, false, f.tenant) // 其他区域的执行点不参与判定
	f.exec("UPDATE nodes SET region = 'us' WHERE id = $1", far.PG())
	cfg, _ := proto.Marshal(&runtimev1.RuntimeConfig{Rules: []*runtimev1.Rule{{RuleId: msgUUID(rule), TenantId: msgUUID(f.tenant),
		EnforcementPoint: true, PathCoefficientMilli: 1500}}})
	f.exec("UPDATE node_configs SET config = $2 WHERE node_id = $1", n.PG(), cfg)
	known := p.NewUUIDv7()
	f.exec(`INSERT INTO billing_grants (id, tenant_id, rule_id, node_id, boot_id, hub_id, billing_version, path_coefficient_milli, raw_bytes,
		reserved_billing_bytes, status) VALUES ($1, $2, $3, $4, $5, $6, 0, 1500, 20, 30, 'active')`, known.PG(), f.tenant.PG(), rule.PG(), n.PG(), boot.PG(), old.PG())
	held := []*edgehubv1.HeldGrant{
		{GrantId: msgUUID(known), IssuerHubId: msgUUID(old), Kind: edgehubv1.GrantKind_GRANT_KIND_BILLING, TenantId: msgUUID(f.tenant), RuleId: msgUUID(rule), Amount: 20},
		// 签发条目尚未上报：按配置中的路径系数估算 ceil(1001 × 1.5) = 1502
		{GrantId: msgUUID(p.NewUUIDv7()), IssuerHubId: msgUUID(old), Kind: edgehubv1.GrantKind_GRANT_KIND_BILLING, TenantId: msgUUID(f.tenant), RuleId: msgUUID(rule), Amount: 1001},
		// 份额、其他实例签发、其他租户的不计
		{GrantId: msgUUID(p.NewUUIDv7()), IssuerHubId: msgUUID(old), Kind: edgehubv1.GrantKind_GRANT_KIND_SHARE_RATE, TenantId: msgUUID(f.tenant), RuleId: msgUUID(rule), Amount: 1 << 30},
		{GrantId: msgUUID(p.NewUUIDv7()), IssuerHubId: msgUUID(cur), Kind: edgehubv1.GrantKind_GRANT_KIND_BILLING, TenantId: msgUUID(f.tenant), RuleId: msgUUID(rule), Amount: 1 << 30},
	}
	reclaim := func() {
		t.Helper()
		if err := f.s.Reclaim(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// 节点在失联实例上（或已切换但尚未在新实例报告）：不回收
	f.s.ReportForeign(cur, n, held)
	reclaim()
	if f.delegated(old) != "83886080/frozen" {
		t.Fatalf("node still on the lost hub: %s", f.delegated(old))
	}
	f.exec("UPDATE nodes SET edge_hub_id = $2 WHERE id = $1", n.PG(), cur.PG())
	f.s.ReportForeign(old, n, held) // 报告来自其他实例，不算
	reclaim()
	if f.delegated(old) != "83886080/frozen" {
		t.Fatalf("report from another hub accepted: %s", f.delegated(old))
	}
	f.exec("UPDATE edge_hubs SET mode = 'autonomous' WHERE id = $1", cur.PG()) // 接管的实例自身不在线：不算接入在线实例
	f.s.ReportForeign(cur, n, held)
	reclaim()
	if f.delegated(old) != "83886080/frozen" {
		t.Fatalf("taking-over hub not online: %s", f.delegated(old))
	}
	f.exec("UPDATE edge_hubs SET mode = 'online' WHERE id = $1", cur.PG())
	reclaim()
	if f.delegated(old) != "1532/frozen" {
		t.Fatalf("after the reports %s", f.delegated(old))
	}
	if got := f.query("SELECT detail->>'reason' FROM audit_logs WHERE action = 'budget.reclaim'"); got != "reported" {
		t.Fatalf("audit %q", got)
	}
	// HUB-016：接管的在线实例分到释放的余额（余额 100 MiB − 冻结 1532）
	if got := f.delegated(cur); got != "67108864" {
		t.Fatalf("top-up of the taking-over hub %s", got)
	}
	// 已结算的授权不再保留
	f.exec("UPDATE billing_grants SET status = 'settled', settled_at = now() WHERE id = $1", known.PG())
	reclaim()
	if f.delegated(old) != "1502/frozen" {
		t.Fatalf("after a settlement %s", f.delegated(old))
	}
	// 没有可回收的部分：不再写审计
	reclaim()
	if got := f.query("SELECT count(*)::text FROM audit_logs WHERE action = 'budget.reclaim'"); got != "2" {
		t.Fatalf("reclaim audits %s", got)
	}
	// 判定之后实例恢复了联系（不再离线）：事务内复查，不回收
	f.exec("UPDATE edge_hubs SET mode = 'reconciling' WHERE id = $1", old.PG())
	if done, err := f.s.reclaim(ctx, f.tenant, old, 0, "test"); err != nil || done || f.delegated(old) != "1502/frozen" {
		t.Fatalf("reclaimed a reconnecting hub: %v %v %s", done, err, f.delegated(old))
	}
}
