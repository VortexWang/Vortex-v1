package metering

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	"vortex/backend/apptest"
	"vortex/backend/compiler"
	"vortex/backend/internal/db"
	"vortex/backend/internal/leases"
	p "vortex/backend/internal/platform"
	"vortex/backend/internal/rules"
	backendv1 "vortex/proto/gen/go/vortex/backend/v1"
	commonv1 "vortex/proto/gen/go/vortex/common/v1"
	edgehubv1 "vortex/proto/gen/go/vortex/edgehub/v1"
	runtimev1 "vortex/proto/gen/go/vortex/runtime/v1"
)

type noHubs struct{}

func (noHubs) Sender(p.UUID) func(*backendv1.BackendMessage) { return nil }
func (noHubs) Connected() []p.UUID                           { return nil }

type demandLog struct {
	leases.Service
	mu  sync.Mutex
	got []string
}

func (d *demandLog) ReportDemand(node, tenant p.UUID, b uint64, s uint32) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.got = append(d.got, tenant.String())
	d.Service.ReportDemand(node, tenant, b, s)
}

type fixture struct {
	t      *testing.T
	dsn    string
	pool   *pgxpool.Pool
	s      *Service
	b      *demandLog
	tenant p.UUID
	hub    p.UUID
	node   p.UUID
	rule   p.UUID
	seq    uint64
}

func m(u p.UUID) *commonv1.Uuid { return &commonv1.Uuid{Value: append([]byte(nil), u[:]...)} }

func setup(t *testing.T) *fixture {
	dsn := apptest.Database(t)
	pool, err := p.OpenDB(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	f := &fixture{t: t, dsn: dsn, pool: pool, tenant: apptest.Tenant(t, dsn), hub: p.NewUUIDv7()}
	f.b = &demandLog{Service: leases.Service{DB: pool, Hubs: noHubs{}}}
	f.s = &Service{Budgets: f.b}
	f.exec("INSERT INTO edge_hubs (id, region, address, mode) VALUES ($1, 'eu', 'h:1', 'online')", f.hub.PG())
	// 平台入口 → 平台出口的规则
	e, x := p.NewUUIDv7(), p.NewUUIDv7()
	f.node = e
	f.exec("INSERT INTO nodes (id, name, region, can_entry, public_addresses, lifecycle) VALUES ($1, 'e', 'eu', true, '{203.0.113.1}', 'online')", e.PG())
	f.exec("INSERT INTO nodes (id, name, region, can_exit, public_addresses, lifecycle) VALUES ($1, 'x', 'eu', true, '{203.0.113.2}', 'online')", x.PG())
	f.exec("INSERT INTO node_port_ranges (node_id, kind, port_min, port_max) VALUES ($1, 'link', 7443, 7443)", x.PG())
	ge, gx := p.NewUUIDv7(), p.NewUUIDv7()
	f.exec("INSERT INTO node_groups (id, name, purpose) VALUES ($1, 'e', 'entry'), ($2, 'x', 'exit')", ge.PG(), gx.PG())
	f.exec("INSERT INTO node_group_members (node_group_id, node_id) VALUES ($1, $2), ($3, $4)", ge.PG(), e.PG(), gx.PG(), x.PG())
	f.exec("INSERT INTO segment_permits (upstream_group_id, downstream_group_id, created_by) VALUES ($1, $2, $3)", ge.PG(), gx.PG(), apptest.Admin(t, dsn).PG())
	f.exec("INSERT INTO billing_versions (id, version, effective_at) VALUES ($1, 0, now())", p.NewUUIDv7().PG())
	f.exec(`INSERT INTO group_multipliers (billing_version_id, node_group_id, multiplier_milli)
		SELECT id, g, 750 FROM billing_versions, unnest(ARRAY[$1::uuid, $2::uuid]) g WHERE version = 0`, ge.PG(), gx.PG())
	saved, err := (&rules.Service{DB: pool}).Create(context.Background(), f.tenant, rules.Spec{Name: "r", Network: "tcp", Mode: "tunnel",
		EntryGroup: rules.CID(ge), ServicePort: 20001, Path: []compiler.Segment{{Group: rules.CID(gx)}},
		Targets: []compiler.Target{{Host: "198.51.100.7", Port: 443, Weight: 1}}, TargetSchedule: "weighted_round_robin", ProxyProtocol: "off", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	f.rule = saved.ID
	return f
}

func (f *fixture) exec(sql string, args ...any) { f.t.Helper(); apptest.Exec(f.t, f.dsn, sql, args...) }

func (f *fixture) query(sql string, args ...any) string {
	f.t.Helper()
	var s string
	apptest.QueryRow(f.t, f.dsn, sql, args, &s)
	return s
}

func (f *fixture) subscribe(base int64, expires string) {
	plan := p.NewUUIDv7()
	f.exec(`INSERT INTO plans (id, name, duration_days, base_traffic_bytes, traffic_reset, max_rules, rate_bps, max_conns)
		VALUES ($1, 'p', 30, $2, 'none', 10, 1000, 10)`, plan.PG(), base)
	f.exec(`INSERT INTO subscriptions (tenant_id, plan_id, started_at, expires_at, base_period_end, base_remaining_bytes)
		VALUES ($1, $2, now() - interval '30 days', now() + $3::interval, now() + $3::interval, $4)`, f.tenant.PG(), plan.PG(), expires, base)
}

func (f *fixture) delegate(n int64) {
	f.exec(`INSERT INTO regional_budgets (tenant_id, hub_id, delegated, budget_version) VALUES ($1, $2, $3, 1)
		ON CONFLICT (tenant_id, hub_id) DO UPDATE SET delegated = EXCLUDED.delegated`, f.tenant.PG(), f.hub.PG(), n)
}

// apply 在一个事务内入账这些条目。
func (f *fixture) apply(es ...*backendv1.LedgerEntry) *Effect {
	f.t.Helper()
	eff := &Effect{}
	err := p.InTx(context.Background(), f.pool, func(tx pgx.Tx) error {
		eff = &Effect{}
		for _, e := range es {
			if err := f.s.Apply(context.Background(), db.New(tx), f.hub, e, eff); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		f.t.Fatal(err)
	}
	eff.Commit(f.b)
	return eff
}

func (f *fixture) issued(raw, coef uint64) p.UUID {
	id := p.NewUUIDv7()
	f.apply(&backendv1.LedgerEntry{Body: &backendv1.LedgerEntry_GrantIssued{GrantIssued: &backendv1.GrantIssued{GrantId: m(id),
		Kind: edgehubv1.GrantKind_GRANT_KIND_BILLING, TenantId: m(f.tenant), NodeId: m(f.node), BootId: m(p.NewUUIDv7()), RuleId: m(f.rule),
		BillingVersion: 0, PathCoefficientMilli: coef, RawBytes: raw, ReservedBillingBytes: (raw*coef + 999) / 1000}}})
	return id
}

func settle(tenant, grant p.UUID, used, settled uint64, final bool) *backendv1.LedgerEntry {
	return &backendv1.LedgerEntry{Body: &backendv1.LedgerEntry_GrantSettled{GrantSettled: &backendv1.GrantSettled{GrantId: m(grant),
		TenantId: m(tenant), UsedRawBytes: used, SettledBillingBytes: settled, Final: final}}}
}

func (f *fixture) base() string {
	return f.query("SELECT base_remaining_bytes::text FROM subscriptions WHERE tenant_id = $1", f.tenant.PG())
}

func (f *fixture) budget() string {
	return f.query("SELECT delegated || '/' || settled_acked_total || '/v' || budget_version FROM regional_budgets WHERE tenant_id = $1 AND hub_id = $2",
		f.tenant.PG(), f.hub.PG())
}

func TestSettlement(t *testing.T) {
	f := setup(t)
	f.subscribe(10_000, "10 days")
	f.delegate(5_000)

	// 签发：只记计费授权；运行授权与规则未知的条目不入库
	g := f.issued(2000, 1500)
	f.apply(&backendv1.LedgerEntry{Body: &backendv1.LedgerEntry_GrantIssued{GrantIssued: &backendv1.GrantIssued{GrantId: m(p.NewUUIDv7()),
		Kind: edgehubv1.GrantKind_GRANT_KIND_RUN, TenantId: m(f.tenant), NodeId: m(f.node), RuleId: m(f.rule), PathCoefficientMilli: 1000,
		RawBytes: 10, ReservedBillingBytes: 10}}})
	f.apply(&backendv1.LedgerEntry{Body: &backendv1.LedgerEntry_GrantIssued{GrantIssued: &backendv1.GrantIssued{GrantId: m(p.NewUUIDv7()),
		Kind: edgehubv1.GrantKind_GRANT_KIND_BILLING, TenantId: m(f.tenant), NodeId: m(f.node), RuleId: m(p.NewUUIDv7()), PathCoefficientMilli: 1000,
		RawBytes: 10, ReservedBillingBytes: 10}}})
	f.apply(&backendv1.LedgerEntry{Body: &backendv1.LedgerEntry_GrantIssued{GrantIssued: &backendv1.GrantIssued{GrantId: m(p.NewUUIDv7()),
		Kind: edgehubv1.GrantKind_GRANT_KIND_BILLING, TenantId: m(f.tenant), NodeId: m(f.node), RuleId: m(f.rule), PathCoefficientMilli: 1000,
		RawBytes: 0, ReservedBillingBytes: 1}}}) // 空授权：忽略
	if got := f.query("SELECT string_agg(status || ':' || reserved_billing_bytes, ',') FROM billing_grants"); got != "active:3000" {
		t.Fatalf("grants %s", got)
	}
	// 非 final 只记录已用量
	f.apply(settle(f.tenant, g, 700, 0, false))
	if got := f.query("SELECT status || ':' || used_raw_bytes FROM billing_grants WHERE id = $1", g.PG()); got != "active:700" || f.base() != "10000" {
		t.Fatalf("partial %s base %s", got, f.base())
	}
	// final：1001 × 1.5 = 1501.5 字节 → 扣 1501，余量 500；delegated 按 ceil 值 1502 下调，确认累计 +1502
	eff := f.apply(settle(f.tenant, g, 1001, 1502, true))
	if f.base() != "8499" || f.budget() != "3498/1502/v2" || len(eff.Budgets) != 1 || !eff.Tenants[f.tenant] {
		t.Fatalf("after settlement base %s budget %s eff %+v", f.base(), f.budget(), eff)
	}
	if got := f.query("SELECT status || ':' || used_raw_bytes || ':' || settled_billing_bytes || ':' || written_off_billing FROM billing_grants WHERE id = $1",
		g.PG()); got != "settled:1001:1502:0.000" {
		t.Fatalf("grant %s", got)
	}
	if got := f.query("SELECT remainder_milli::text FROM tenant_billing_carry WHERE tenant_id = $1", f.tenant.PG()); got != "500" {
		t.Fatalf("carry %s", got)
	}
	// 已入账的授权再次出现在实例的结算条目中（例如已由节点在其他实例上报的消耗结算）：不重复扣减，只确认，
	// 使实例的待入账量归零
	eff = f.apply(settle(f.tenant, g, 1001, 1502, true))
	if f.base() != "8499" || f.budget() != "3498/3004/v3" || len(eff.Budgets) != 1 {
		t.Fatalf("settled twice: base %s budget %s", f.base(), f.budget())
	}
	// 余量累计：1 × 1.5 + 0.5 = 2 字节，余量归 0
	g2 := f.issued(10, 1500)
	f.apply(settle(f.tenant, g2, 1, 2, true))
	if f.base() != "8497" || f.query("SELECT remainder_milli::text FROM tenant_billing_carry WHERE tenant_id = $1", f.tenant.PG()) != "0" {
		t.Fatalf("carry rollover: base %s", f.base())
	}
	// 授权未知（例如签发条目缺失）：按结算值入账
	f.apply(settle(f.tenant, p.NewUUIDv7(), 0, 7, true))
	if f.base() != "8490" || f.budget() != "3489/3013/v5" {
		t.Fatalf("unknown grant: base %s budget %s", f.base(), f.budget())
	}
	// 实例报告的版本更大：新版本超过它
	_, _ = f.b.HubRegistered(context.Background(), f.hub, map[p.UUID]uint64{f.tenant: 40}, false)
	f.apply(settle(f.tenant, p.NewUUIDv7(), 0, 1, true))
	if f.budget() != "3488/3014/v41" {
		t.Fatalf("version after report: %s", f.budget())
	}
}

func TestSettlementSources(t *testing.T) {
	f := setup(t)
	f.subscribe(100, "10 days")
	f.delegate(1000)
	// 流量包：最早到期优先，同到期按发放时间
	p1, p2, p3 := p.NewUUIDv7(), p.NewUUIDv7(), p.NewUUIDv7()
	f.exec(`INSERT INTO traffic_packs (id, tenant_id, bytes_total, bytes_remaining, expires_at, granted_at, source) VALUES
		($1, $4, 500, 500, now() + interval '5 days', now() - interval '1 day', 't'),
		($2, $4, 500, 500, now() + interval '2 days', now(), 't'),
		($3, $4, 500, 500, now() + interval '5 days', now() - interval '2 days', 't')`, p1.PG(), p2.PG(), p3.PG(), f.tenant.PG())
	g := f.issued(1000, 1000)
	f.apply(settle(f.tenant, g, 800, 800, true)) // 基础 100 → 包 2 的 500 → 包 3 的 200
	if got := f.query("SELECT string_agg(bytes_remaining::text, ',' ORDER BY expires_at, granted_at) FROM traffic_packs WHERE tenant_id = $1", f.tenant.PG()); f.base() != "0" || got != "0,300,500" {
		t.Fatalf("base %s packs %s", f.base(), got)
	}
	// 超出该实例剩余委托的部分核销、不追缴（C2b H4）：委托只剩 200，结算 500
	g2 := f.issued(1000, 1000)
	f.apply(settle(f.tenant, g2, 500, 500, true))
	if got := f.query("SELECT written_off_billing::text FROM billing_grants WHERE id = $1", g2.PG()); got != "300.000" || f.budget() != "0/1300/v3" {
		t.Fatalf("written off %s budget %s", got, f.budget())
	}
	if got := f.query("SELECT sum(bytes_remaining)::text FROM traffic_packs WHERE tenant_id = $1", f.tenant.PG()); got != "600" {
		t.Fatalf("packs after the capped settlement %s", got)
	}
	// 套餐到期：流量包暂停（PLN-003），无法扣减的部分核销
	f.exec("UPDATE subscriptions SET expires_at = now() - interval '1 second', base_period_end = now() - interval '1 second' WHERE tenant_id = $1", f.tenant.PG())
	f.delegate(1000)
	g3 := f.issued(1000, 1000)
	f.apply(settle(f.tenant, g3, 100, 100, true))
	if got := f.query("SELECT sum(bytes_remaining)::text FROM traffic_packs WHERE tenant_id = $1", f.tenant.PG()); got != "600" {
		t.Fatalf("packs consumed while the plan expired: %s", got)
	}
	if got := f.query("SELECT written_off_billing::text FROM billing_grants WHERE id = $1", g3.PG()); got != "100.000" {
		t.Fatalf("written off %s", got)
	}
}

func TestUsageAndTransfer(t *testing.T) {
	f := setup(t)
	g := f.issued(1<<20, 1500)
	boot := p.NewUUIDv7()
	at := time.Date(2026, 11, 3, 10, 15, 42, 0, time.UTC)
	batch := func(seq uint64) *backendv1.LedgerEntry {
		return &backendv1.LedgerEntry{RecordedAt: timestamppb.New(at), Body: &backendv1.LedgerEntry_NodeUsage{NodeUsage: &backendv1.NodeUsage{NodeId: m(f.node),
			Batch: &edgehubv1.UsageBatch{BootId: m(boot), Seq: seq, Snapshot: &runtimev1.UsageSnapshot{
				Deltas: []*runtimev1.UsageDelta{
					{RuleId: m(f.rule), Direction: commonv1.Direction_DIRECTION_UP, RawBytes: 1000, BillingGrantId: m(g)},
					{RuleId: m(f.rule), Direction: commonv1.Direction_DIRECTION_DOWN, RawBytes: 3001, BillingGrantId: m(g)},
					{RuleId: m(f.rule), Direction: commonv1.Direction_DIRECTION_UP, RawBytes: 99999},                          // 非执行点
					{RuleId: m(p.NewUUIDv7()), Direction: commonv1.Direction_DIRECTION_UP, RawBytes: 5, BillingGrantId: m(g)}, // 规则未知
				},
				Demands: []*runtimev1.TenantDemand{{TenantId: m(f.tenant), Bytes_60S: 4001, StarvedPermille: 500}}}}}}}
	}
	f.apply(batch(1), batch(2))
	f.apply(batch(1)) // 重复上报（MTR-006）
	got := f.query(`SELECT extract(epoch FROM minute)::bigint || '|' || raw_bytes_up || '|' || raw_bytes_down || '|' || billed_bytes FROM usage_minutely WHERE rule_id = $1`, f.rule.PG())
	if got != "1793700900|2000|6002|12002" {
		t.Fatalf("minutely %s", got)
	}
	if got := f.query("SELECT count(*)::text FROM usage_minutely"); got != "1" {
		t.Fatalf("usage rows %s (unknown rules must be skipped)", got)
	}
	if got := f.query(`SELECT day || '|' || raw_bytes || '|' || billed_bytes FROM usage_daily WHERE rule_id = $1`, f.rule.PG()); got != "2026-11-03|8002|12002" {
		t.Fatalf("daily %s", got)
	}
	if len(f.b.got) != 2 {
		t.Fatalf("demand signals %v", f.b.got)
	}
	// 重启转移：旧启动的授权进入 settling
	f.apply(&backendv1.LedgerEntry{Body: &backendv1.LedgerEntry_GrantTransferred{GrantTransferred: &backendv1.GrantTransferred{NodeId: m(f.node),
		RevokedGrantIds: []*commonv1.Uuid{m(g)}}}})
	if got := f.query("SELECT status FROM billing_grants WHERE id = $1", g.PG()); got != "settling" {
		t.Fatalf("status %s", got)
	}
}

// 节点在其他实例上报的、失联实例签发的授权消耗（HUB-015 ①）：由后端结算，下调签发实例的委托，不确认、不下发。
func TestForeignSettlement(t *testing.T) {
	f := setup(t)
	f.subscribe(10_000, "10 days")
	f.delegate(5_000)
	g := f.issued(4000, 1500) // 由 f.hub 签发
	via := p.NewUUIDv7()
	f.exec("INSERT INTO edge_hubs (id, region, address, mode) VALUES ($1, 'eu', 'h:2', 'online')", via.PG())
	boot := p.NewUUIDv7()
	batch := func(seq, used uint64, final bool, grant p.UUID) *backendv1.LedgerEntry {
		return &backendv1.LedgerEntry{Body: &backendv1.LedgerEntry_NodeUsage{NodeUsage: &backendv1.NodeUsage{NodeId: m(f.node),
			Batch: &edgehubv1.UsageBatch{BootId: m(boot), Seq: seq, Snapshot: &runtimev1.UsageSnapshot{
				GrantConsumptions: []*runtimev1.GrantConsumption{{GrantId: m(grant), UsedRawBytes: used, Final: final}}}}}}}
	}
	apply := func(e *backendv1.LedgerEntry) *Effect {
		t.Helper()
		eff := &Effect{}
		err := p.InTx(context.Background(), f.pool, func(tx pgx.Tx) error {
			eff = &Effect{}
			return f.s.Apply(context.Background(), db.New(tx), via, e, eff)
		})
		if err != nil {
			t.Fatal(err)
		}
		return eff
	}
	apply(batch(1, 500, false, g))
	if got := f.query("SELECT status || ':' || used_raw_bytes FROM billing_grants WHERE id = $1", g.PG()); got != "active:500" || f.base() != "10000" {
		t.Fatalf("partial %s", got)
	}
	// final：1001 × 1.5 → 扣 1501（余量 500），签发实例委托下调 ceil 值 1502，确认累计不变
	eff := apply(batch(2, 1001, true, g))
	if f.base() != "8499" || f.budget() != "3498/0/v2" || len(eff.Budgets) != 0 || !eff.Tenants[f.tenant] {
		t.Fatalf("foreign settlement: base %s budget %s eff %+v", f.base(), f.budget(), eff)
	}
	// 签发实例恢复后在自己的账本中结算同一授权：只确认（使它的待入账量归零），不重复扣减
	f.apply(settle(f.tenant, g, 1001, 1502, true))
	if f.base() != "8499" || f.budget() != "3498/1502/v3" {
		t.Fatalf("issuer settles later: base %s budget %s", f.base(), f.budget())
	}
	// 经签发实例自己上报的消耗：由它的结算条目入账，这里跳过
	own := f.issued(4000, 1000)
	f.apply(&backendv1.LedgerEntry{Body: &backendv1.LedgerEntry_NodeUsage{NodeUsage: &backendv1.NodeUsage{NodeId: m(f.node),
		Batch: &edgehubv1.UsageBatch{BootId: m(boot), Seq: 3, Snapshot: &runtimev1.UsageSnapshot{
			GrantConsumptions: []*runtimev1.GrantConsumption{{GrantId: m(own), UsedRawBytes: 10, Final: true}}}}}}})
	if got := f.query("SELECT status FROM billing_grants WHERE id = $1", own.PG()); got != "active" || f.base() != "8499" {
		t.Fatalf("own grant settled from usage: %s", got)
	}
}

// HUB-013 ④：对账中出现的超额消费进入 disputed；72 小时后自动结算（MTR-009）。正常运行时的核销只写审计。
func TestDisputes(t *testing.T) {
	f := setup(t)
	f.subscribe(10_000, "10 days")
	f.delegate(100)
	g := f.issued(1000, 1000)
	f.apply(settle(f.tenant, g, 300, 300, true)) // 在线：核销 200，写审计
	if got := f.query("SELECT status || ':' || written_off_billing FROM billing_grants WHERE id = $1", g.PG()); got != "settled:200.000" {
		t.Fatalf("online overrun %s", got)
	}
	if got := f.query("SELECT count(*)::text FROM audit_logs WHERE action = 'billing.written_off' AND tenant_id = $1", f.tenant.PG()); got != "1" {
		t.Fatalf("audit %s", got)
	}
	f.exec("UPDATE edge_hubs SET mode = 'reconciling' WHERE id = $1", f.hub.PG())
	f.delegate(100)
	g0 := f.issued(1000, 1000) // 对账中、未超额：正常结算
	f.apply(settle(f.tenant, g0, 50, 50, true))
	if got := f.query("SELECT status FROM billing_grants WHERE id = $1", g0.PG()); got != "settled" {
		t.Fatalf("reconciling without overrun %s", got)
	}
	f.delegate(100)
	g2 := f.issued(1000, 1000)
	f.apply(settle(f.tenant, g2, 300, 300, true))
	if got := f.query("SELECT status || ':' || written_off_billing || ':' || (settled_at IS NULL) FROM billing_grants WHERE id = $1", g2.PG()); got != "disputed:200.000:true" {
		t.Fatalf("reconciling overrun %s", got)
	}
	if got := f.query("SELECT (auto_settle_at > now() + interval '71 hours')::text FROM disputes WHERE grant_id = $1", g2.PG()); got != "true" {
		t.Fatalf("dispute %s", got)
	}
	// 争议授权的再次结算：只确认
	f.apply(settle(f.tenant, g2, 300, 300, true))
	if f.base() != "9750" {
		t.Fatalf("base %s", f.base())
	}
	if n, err := ResolveDisputes(context.Background(), f.pool, nil); err != nil || n != 0 {
		t.Fatalf("resolved early: %d %v", n, err)
	}
	f.exec("UPDATE disputes SET auto_settle_at = now() - interval '1 second'")
	if n, err := ResolveDisputes(context.Background(), f.pool, nil); err != nil || n != 1 {
		t.Fatalf("resolve %d %v", n, err)
	}
	if got := f.query("SELECT status || ':' || (settled_at IS NOT NULL) || ':' || (SELECT resolution->>'auto' FROM disputes WHERE grant_id = $1) FROM billing_grants WHERE id = $1",
		g2.PG()); got != "settled:true:true" {
		t.Fatalf("after auto-settle %s", got)
	}
	if got := f.query("SELECT count(*)::text FROM audit_logs WHERE action IN ('billing.disputed', 'billing.dispute_auto_settled')"); got != "2" {
		t.Fatalf("audit %s", got)
	}
}
