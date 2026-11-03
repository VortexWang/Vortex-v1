package leases

import (
	"context"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"vortex/backend/apptest"
	"vortex/backend/compiler"
	p "vortex/backend/internal/platform"
	"vortex/backend/internal/rules"
	backendv1 "vortex/proto/gen/go/vortex/backend/v1"
	commonv1 "vortex/proto/gen/go/vortex/common/v1"
	runtimev1 "vortex/proto/gen/go/vortex/runtime/v1"
)

// ---------------------------------------------------------------- 切分（纯函数）

func TestSplitShare(t *testing.T) {
	cases := []struct {
		limit  uint64
		demand []uint64
		want   []uint64
	}{
		{1000, []uint64{0, 0}, []uint64{500, 500}},
		{1001, []uint64{0, 0, 0}, []uint64{334, 334, 333}},
		// 保底 50% 均分（各 250），其余 500 按需求 3:1
		{1000, []uint64{300, 100}, []uint64{625, 375}},
		// 需求全在一个执行点：另一个只剩保底
		{1000, []uint64{0, 7}, []uint64{250, 750}},
		{7, []uint64{0}, []uint64{7}},
		// 比例分配的取整余数给需求最大者
		{1000, []uint64{1, 2, 2}, []uint64{266, 368, 366}},
	}
	for _, c := range cases {
		got := SplitShare(c.limit, c.demand)
		for i := range got {
			if got[i] != c.want[i] {
				t.Fatalf("SplitShare(%d, %v) = %v, want %v", c.limit, c.demand, got, c.want)
			}
		}
	}
	r := rand.New(rand.NewPCG(1, 2))
	for range 5000 {
		n := 1 + r.IntN(8)
		limit := r.Uint64N(1 << 40)
		if r.IntN(10) == 0 {
			limit = ^uint64(0) - r.Uint64N(1000)
		}
		d := make([]uint64, n)
		for i := range d {
			if r.IntN(3) > 0 {
				d[i] = r.Uint64N(1 << 30)
			}
		}
		got := SplitShare(limit, d)
		var sum uint64
		floor := limit / 100 * FloorRatioPercent / uint64(n)
		for _, g := range got {
			sum += g
			if g < floor {
				t.Fatalf("SplitShare(%d, %v) = %v: below the guaranteed share %d", limit, d, got, floor)
			}
		}
		if sum != limit {
			t.Fatalf("SplitShare(%d, %v) = %v: sum %d", limit, d, got, sum)
		}
	}
}

func TestSplitBudget(t *testing.T) {
	const hour = 12 * 3600
	cases := []struct {
		avail uint64
		rates []uint64
		want  []uint64
	}{
		// 余额充足：近期无用量的实例得 64 MiB，有用量的得 速率 × 12 小时
		{1 << 40, []uint64{0, 80_000_000}, []uint64{64 << 20, 10_000_000 * hour}},
		// 余额不足 64 MiB × 实例数：均分
		{90 << 20, []uint64{0, 0}, []uint64{45 << 20, 45 << 20}},
		// 余额不足以满足上限：无用量的实例保底，其余按用量比例
		{200 << 20, []uint64{0, 8 << 20, 24 << 20}, []uint64{64 << 20, 34 << 20, 102 << 20}},
		{0, []uint64{1, 2}, []uint64{0, 0}},
		// 上限之和恰好等于余额：各得上限
		{432067108864, []uint64{8, 80_000_000}, []uint64{64 << 20, 432000000000}},
		// 按比例切分时不超过各自上限
		{200 << 20, []uint64{8, 29128}, []uint64{57582, 3641 * hour}},
	}
	for _, c := range cases {
		got := SplitBudget(c.avail, c.rates)
		for i := range got {
			if got[i] != c.want[i] {
				t.Fatalf("SplitBudget(%d, %v) = %v, want %v", c.avail, c.rates, got, c.want)
			}
		}
	}
	r := rand.New(rand.NewPCG(3, 4))
	for range 5000 {
		n := 1 + r.IntN(6)
		avail := r.Uint64N(1 << 36)
		rates := make([]uint64, n)
		for i := range rates {
			if r.IntN(3) > 0 {
				rates[i] = r.Uint64N(1 << 33)
			}
		}
		got := SplitBudget(avail, rates)
		var sum uint64
		for i, g := range got {
			sum += g
			if rates[i] == 0 && g != min(uint64(BudgetFloor), avail/uint64(n)) {
				t.Fatalf("SplitBudget(%d, %v) = %v: idle hub %d not at the floor", avail, rates, got, i)
			}
			if c := max(satMul(rates[i]/8, hour), min(uint64(BudgetFloor), avail/uint64(n))); g > c {
				t.Fatalf("SplitBudget(%d, %v) = %v: hub %d above its cap %d", avail, rates, got, i, c)
			}
		}
		if sum > avail {
			t.Fatalf("SplitBudget(%d, %v) = %v: sum %d exceeds the balance", avail, rates, got, sum)
		}
	}
}

// ---------------------------------------------------------------- 数据库

type fakeHubs struct {
	mu   sync.Mutex
	sent map[p.UUID][]*backendv1.BackendMessage
	conn []p.UUID
}

func (f *fakeHubs) Sender(hub p.UUID) func(*backendv1.BackendMessage) {
	for _, c := range f.conn {
		if c == hub {
			return func(m *backendv1.BackendMessage) {
				f.mu.Lock()
				defer f.mu.Unlock()
				if f.sent == nil {
					f.sent = map[p.UUID][]*backendv1.BackendMessage{}
				}
				f.sent[hub] = append(f.sent[hub], m)
			}
		}
	}
	return nil
}

func (f *fakeHubs) Connected() []p.UUID { return f.conn }

func (f *fakeHubs) take(hub p.UUID) []*backendv1.BackendMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.sent[hub]
	delete(f.sent, hub)
	return out
}

type fixture struct {
	t      *testing.T
	dsn    string
	s      *Service
	hubs   *fakeHubs
	tenant p.UUID
	plan   p.UUID
}

func setup(t *testing.T) *fixture {
	dsn := apptest.Database(t)
	pool, err := p.OpenDB(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	f := &fixture{t: t, dsn: dsn, hubs: &fakeHubs{}, tenant: apptest.Tenant(t, dsn)}
	f.s = &Service{DB: pool, Hubs: f.hubs}
	return f
}

func (f *fixture) exec(sql string, args ...any) { f.t.Helper(); apptest.Exec(f.t, f.dsn, sql, args...) }

func (f *fixture) query(sql string, args ...any) string {
	f.t.Helper()
	var s string
	apptest.QueryRow(f.t, f.dsn, sql, args, &s)
	return s
}

// subscribe 给租户一个生效中的套餐。
func (f *fixture) subscribe(tenant p.UUID, rate, conns, base int64) {
	f.plan = p.NewUUIDv7()
	f.exec(`INSERT INTO plans (id, name, duration_days, base_traffic_bytes, traffic_reset, max_rules, rate_bps, max_conns)
		VALUES ($1, 'p', 30, $2, 'none', 10, $3, $4)`, f.plan.PG(), base, rate, conns)
	f.exec(`INSERT INTO subscriptions (tenant_id, plan_id, started_at, expires_at, base_period_end, base_remaining_bytes)
		VALUES ($1, $2, now() - interval '1 day', now() + interval '29 days', now() + interval '29 days', $3)`, tenant.PG(), f.plan.PG(), base)
}

func (f *fixture) hub(mode string, connected bool) p.UUID {
	id := p.NewUUIDv7()
	f.exec("INSERT INTO edge_hubs (id, region, address, mode) VALUES ($1, 'eu', 'h:1', $2)", id.PG(), mode)
	if connected {
		f.hubs.conn = append(f.hubs.conn, id)
	}
	return id
}

// node 建一个节点：online 时接入 hub（当前启动 boot）；运行配置中它是 eps 这些租户的执行点。
func (f *fixture) node(hub p.UUID, online bool, eps ...p.UUID) (p.UUID, p.UUID) {
	id, boot := p.NewUUIDv7(), p.NewUUIDv7()
	life := "offline"
	if online {
		life = "online"
	}
	f.exec("INSERT INTO nodes (id, name, region, edge_hub_id, can_entry, lifecycle, current_boot_id) VALUES ($1, 'n', 'eu', $2, true, $3, $4)",
		id.PG(), hub.PG(), life, boot.PG())
	f.config(id, true, eps...)
	return id, boot
}

// config 写节点的最新运行配置（applied：已生效）。
func (f *fixture) config(node p.UUID, applied bool, eps ...p.UUID) {
	cfg := &runtimev1.RuntimeConfig{}
	for _, t := range eps {
		r := p.NewUUIDv7()
		cfg.Rules = append(cfg.Rules, &runtimev1.Rule{RuleId: msgUUID(r), TenantId: msgUUID(t), EnforcementPoint: true})
	}
	cfg.Rules = append(cfg.Rules, &runtimev1.Rule{RuleId: msgUUID(p.NewUUIDv7()), TenantId: msgUUID(p.NewUUIDv7())}) // 非执行点
	raw, _ := proto.Marshal(cfg)
	var av any
	if applied {
		av = 1
	}
	f.exec(`INSERT INTO node_configs (node_id, version, config, content_digest, prepared_version, applied_version) VALUES ($1, 1, $2, '\x00', $3, $3)
		ON CONFLICT (node_id) DO UPDATE SET version = node_configs.version + 1, config = EXCLUDED.config,
		  prepared_version = CASE WHEN $3::bigint IS NULL THEN node_configs.prepared_version ELSE node_configs.version + 1 END,
		  applied_version = CASE WHEN $3::bigint IS NULL THEN node_configs.applied_version ELSE node_configs.version + 1 END`,
		node.PG(), raw, av)
}

func (f *fixture) live(node p.UUID) string {
	return f.query(`SELECT coalesce(string_agg(kind || ':' || amount, ',' ORDER BY kind), '') FROM share_leases
		WHERE node_id = $1 AND tenant_id = $2 AND released_at IS NULL AND expires_at > now()`, node.PG(), f.tenant.PG())
}

func pushed(ms []*backendv1.BackendMessage) []*backendv1.ShareLeaseUpdate {
	var out []*backendv1.ShareLeaseUpdate
	for _, m := range ms {
		if l := m.GetLeases(); l != nil {
			out = append(out, l)
		}
	}
	return out
}

func TestShares(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	f.subscribe(f.tenant, 1000, 10, 1<<30)
	h := f.hub("online", true)
	a, _ := f.node(h, true, f.tenant)
	b, _ := f.node(h, true, f.tenant)
	idle, _ := f.node(h, true) // 不是该租户的执行点
	if err := f.s.ReallocateShares(ctx, f.tenant); err != nil {
		t.Fatal(err)
	}
	if f.live(a) != "conn:5,rate:500" || f.live(b) != "conn:5,rate:500" || f.live(idle) != "" {
		t.Fatalf("shares a=%s b=%s idle=%s", f.live(a), f.live(b), f.live(idle))
	}
	ps := pushed(f.hubs.take(h))
	if len(ps) != 4 {
		t.Fatalf("pushed %d leases", len(ps))
	}
	for _, u := range ps {
		l := u.Leases[0]
		if l.GetMeta().GetTtl().AsDuration() != TTL || p.UUID(l.GetTenantId().GetValue()) != f.tenant {
			t.Fatalf("lease %v", l)
		}
	}
	// 未变化：只顺延库中的到期时刻，不重复下发
	f.exec("UPDATE share_leases SET expires_at = now() + interval '1 minute'")
	if err := f.s.ReallocateShares(ctx, f.tenant); err != nil {
		t.Fatal(err)
	}
	if n := len(pushed(f.hubs.take(h))); n != 0 {
		t.Fatalf("unchanged shares pushed again: %d", n)
	}
	if got := f.query("SELECT (min(expires_at) > now() + interval '5 minutes')::text FROM share_leases WHERE released_at IS NULL"); got != "true" {
		t.Fatal("unchanged leases were not extended past one renewal period")
	}

	// 同一启动有多份未释放的租约：只保留最新一份
	f.exec(`INSERT INTO share_leases (id, tenant_id, node_id, boot_id, kind, amount, expires_at)
		SELECT $1, tenant_id, node_id, boot_id, kind, 900, now() + interval '2 minutes' FROM share_leases WHERE node_id = $2 AND kind = 'rate'`,
		p.NewUUIDv7().PG(), b.PG())
	if err := f.s.ReallocateShares(ctx, f.tenant); err != nil {
		t.Fatal(err)
	}
	if f.live(b) != "conn:5,rate:500" {
		t.Fatalf("duplicate lease kept: %s", f.live(b))
	}
	f.hubs.take(h)

	// LIM-010：节点以新启动接入 → 旧启动的份额作废，新启动按切分结果接替
	nb := p.NewUUIDv7()
	f.exec("UPDATE nodes SET current_boot_id = $2 WHERE id = $1", a.PG(), nb.PG())
	if err := f.s.ReallocateShares(ctx, f.tenant); err != nil {
		t.Fatal(err)
	}
	if got := f.query("SELECT count(*)::text FROM share_leases WHERE node_id = $1 AND released_at IS NULL AND boot_id <> $2", a.PG(), nb.PG()); got != "0" {
		t.Fatalf("old boot still holds %s leases", got)
	}
	ps = pushed(f.hubs.take(h))
	if len(ps) != 2 || p.UUID(ps[0].NodeId.Value) != a || p.UUID(ps[0].Leases[0].Meta.BootId.Value) != nb || f.live(a) != "conn:5,rate:500" {
		t.Fatalf("new boot leases %v, live %s", ps, f.live(a))
	}

	// 离线执行点仍持有未到期的份额：计入占用，在线执行点只切分剩余部分（LIM-004）
	c, cboot := f.node(h, false, f.tenant)
	f.exec(`INSERT INTO share_leases (id, tenant_id, node_id, boot_id, kind, amount, expires_at) VALUES ($1, $2, $3, $4, 'rate', 300, now() + interval '3 minutes')`,
		p.NewUUIDv7().PG(), f.tenant.PG(), c.PG(), cboot.PG())
	if err := f.s.ReallocateShares(ctx, f.tenant); err != nil {
		t.Fatal(err)
	}
	if f.live(a) != "conn:5,rate:350" || f.live(b) != "conn:5,rate:350" {
		t.Fatalf("with an offline holder a=%s b=%s", f.live(a), f.live(b))
	}
	// 离线节点的新配置尚未生效：仍计入；生效后不再是执行点：释放
	f.config(c, false)
	if err := f.s.ReallocateShares(ctx, f.tenant); err != nil {
		t.Fatal(err)
	}
	if f.live(c) != "rate:300" {
		t.Fatalf("pending config: %s", f.live(c))
	}
	f.config(c, true)
	f.hubs.take(h)
	if err := f.s.ReallocateShares(ctx, f.tenant); err != nil {
		t.Fatal(err)
	}
	if f.live(c) != "" || f.live(a) != "conn:5,rate:500" {
		t.Fatalf("after the config applied c=%s a=%s", f.live(c), f.live(a))
	}

	// LIM-006：调小先于调大下发
	f.exec("UPDATE share_leases SET amount = CASE WHEN node_id = $1 THEN 800 ELSE 200 END WHERE kind = 'rate' AND released_at IS NULL", b.PG())
	f.hubs.take(h)
	if err := f.s.ReallocateShares(ctx, f.tenant); err != nil {
		t.Fatal(err)
	}
	ps = pushed(f.hubs.take(h))
	if len(ps) != 2 || p.UUID(ps[0].NodeId.Value) != b || ps[0].Leases[0].Amount != 500 || p.UUID(ps[1].NodeId.Value) != a {
		t.Fatalf("order %v", ps)
	}
	if got := f.query("SELECT sum(amount)::text FROM share_leases WHERE tenant_id = $1 AND kind = 'rate' AND released_at IS NULL", f.tenant.PG()); got != "1000" {
		t.Fatalf("live rate total %s", got)
	}

	// 没有生效中的套餐：不发放
	other := apptest.Tenant(t, f.dsn)
	d, _ := f.node(h, true, other)
	if err := f.s.ReallocateShares(ctx, other); err != nil {
		t.Fatal(err)
	}
	if got := f.query("SELECT count(*)::text FROM share_leases WHERE node_id = $1", d.PG()); got != "0" {
		t.Fatalf("leases without a plan: %s", got)
	}
}

func budgets(ms []*backendv1.BackendMessage) []*backendv1.BudgetUpdate {
	var out []*backendv1.BudgetUpdate
	for _, m := range ms {
		if b := m.GetBudget(); b != nil {
			out = append(out, b)
		}
	}
	return out
}

func TestBudgets(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	f.subscribe(f.tenant, 1000, 10, 300<<20)
	h1, h2 := f.hub("online", true), f.hub("online", true)
	gone := f.hub("offline", false)
	f.node(h1, true, f.tenant)
	f.node(h2, true, f.tenant)
	// 离线实例持有 100 MiB：保持不变并从余额中扣除
	f.exec("INSERT INTO regional_budgets (tenant_id, hub_id, delegated, budget_version) VALUES ($1, $2, $3, 4)", f.tenant.PG(), gone.PG(), int64(100<<20))
	// 实例报告本地已应用版本 7：后端的新版本必须更大
	if err := f.s.HubRegistered(ctx, h1, map[p.UUID]uint64{f.tenant: 7}, true); err != nil {
		t.Fatal(err)
	}
	ms := f.hubs.take(h1)
	if len(ms) == 0 || ms[0].GetRestrictions() == nil {
		t.Fatalf("register: restrictions first, got %v", ms)
	}
	bs := budgets(ms)
	if len(bs) != 1 || bs[0].Delegated != 64<<20 || bs[0].BudgetVersion != 8 {
		t.Fatalf("budget on register %v", bs)
	}
	if b := budgets(f.hubs.take(h2)); len(b) != 1 || b[0].Delegated != 64<<20 || b[0].BudgetVersion != 1 {
		t.Fatalf("other hub %v", b)
	}
	// 实例重新注册（额度未变）：仍下发当前值，版本递增
	if err := f.s.HubRegistered(ctx, h2, nil, true); err != nil {
		t.Fatal(err)
	}
	if b := budgets(f.hubs.take(h2)); len(b) != 1 || b[0].Delegated != 64<<20 || b[0].BudgetVersion != 2 {
		t.Fatalf("re-register %v", b)
	}
	// 自治中的实例管理执行点：不参与切分
	auto := f.hub("autonomous", true)
	f.node(auto, true, f.tenant)
	if err := f.s.Rebudget(ctx, f.tenant, p.UUID{}); err != nil {
		t.Fatal(err)
	}
	if got := f.query("SELECT count(*)::text FROM regional_budgets WHERE hub_id = $1", auto.PG()); got != "0" || len(budgets(f.hubs.take(auto))) != 0 {
		t.Fatalf("autonomous hub got a budget")
	}
	// 两个实例都有用量，余额不足以满足上限：按用量比例切分（余额 300 MiB − 离线 100 MiB）
	f.s.ReportRate(f.tenant, h1, 8<<20)
	f.s.ReportRate(f.tenant, h2, 24<<20)
	if err := f.s.Rebudget(ctx, f.tenant, h1); err != nil {
		t.Fatal(err)
	}
	got := f.query(`SELECT string_agg(delegated::text, ',' ORDER BY delegated) FROM regional_budgets WHERE tenant_id = $1`, f.tenant.PG())
	if got != "52428800,104857600,157286400" {
		t.Fatalf("delegated %s", got)
	}
	if got := f.query(`SELECT (sum(delegated) <= (SELECT balance_bytes FROM tenant_balances WHERE tenant_id = $1))::text FROM regional_budgets WHERE tenant_id = $1`,
		f.tenant.PG()); got != "true" {
		t.Fatal("Σ delegated exceeds the balance")
	}
	// h2 不再管理该租户的执行点：收回到 0
	f.exec("UPDATE nodes SET lifecycle = 'offline' WHERE edge_hub_id = $1", h2.PG())
	f.hubs.take(h2)
	if err := f.s.Rebudget(ctx, f.tenant, p.UUID{}); err != nil {
		t.Fatal(err)
	}
	if b := budgets(f.hubs.take(h2)); len(b) != 1 || b[0].Delegated != 0 {
		t.Fatalf("withdrawn %v", b)
	}
	// 余额被动减少到离线实例持有量以下：在线实例为 0（离线实例的额度在 HUB-015 处理）
	f.exec("UPDATE subscriptions SET base_remaining_bytes = $2 WHERE tenant_id = $1", f.tenant.PG(), int64(50<<20))
	if err := f.s.Rebudget(ctx, f.tenant, p.UUID{}); err != nil {
		t.Fatal(err)
	}
	if got := f.query(`SELECT delegated::text FROM regional_budgets WHERE tenant_id = $1 AND hub_id = $2`, f.tenant.PG(), h1.PG()); got != "0" {
		t.Fatalf("online hub keeps %s after the balance dropped", got)
	}
	// 对账中注册：只下发限制，不下发额度
	f.hubs.take(h1)
	if err := f.s.HubRegistered(ctx, h1, nil, false); err != nil {
		t.Fatal(err)
	}
	if b := budgets(f.hubs.take(h1)); len(b) != 0 {
		t.Fatalf("budgets while reconciling %v", b)
	}
}

func TestRestrictions(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	f.subscribe(f.tenant, 1000, 10, 1<<30)
	bare := apptest.Tenant(t, f.dsn) // 没有套餐
	banned, susp, empty, flagged := apptest.Tenant(t, f.dsn), apptest.Tenant(t, f.dsn), apptest.Tenant(t, f.dsn), apptest.Tenant(t, f.dsn)
	for _, x := range []p.UUID{banned, susp, empty, flagged} {
		f.subscribe(x, 1000, 10, 1<<30)
	}
	f.exec("UPDATE subscriptions SET base_remaining_bytes = 0 WHERE tenant_id = $1", empty.PG())
	f.exec("INSERT INTO restrictions (id, tenant_id, kind) VALUES ($1, $2, 'account_banned'), ($3, $2, 'account_suspended'), ($4, $5, 'account_suspended')",
		p.NewUUIDv7().PG(), banned.PG(), p.NewUUIDv7().PG(), p.NewUUIDv7().PG(), susp.PG())
	f.exec("INSERT INTO restrictions (id, tenant_id, kind) VALUES ($1, $2, 'traffic_exhausted')", p.NewUUIDv7().PG(), flagged.PG())

	// 两条规则；一条单独停止，另一个租户级的停止覆盖该租户全部规则
	rule1, rule2 := f.rules()
	f.exec("INSERT INTO restrictions (id, tenant_id, rule_id, kind) VALUES ($1, $2, $3, 'manual_stop')", p.NewUUIDv7().PG(), f.tenant.PG(), rule1.PG())

	snap, err := f.s.Restrictions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	by := map[p.UUID]*backendv1.TenantRestriction{}
	for _, tr := range snap.Tenants {
		by[p.UUID(tr.TenantId.Value)] = tr
	}
	if len(by) != 6 || !by[flagged].TrafficExhausted {
		t.Fatalf("snapshot covers %d tenants", len(by))
	}
	if tr := by[bare]; tr.PlanExpiresAt == nil || !tr.PlanExpiresAt.AsTime().Before(time.Now()) || !tr.TrafficExhausted {
		t.Fatalf("no plan: %v", tr)
	}
	if by[banned].AccountStatus != backendv1.AccountStatus_ACCOUNT_STATUS_BANNED || by[susp].AccountStatus != backendv1.AccountStatus_ACCOUNT_STATUS_SUSPENDED {
		t.Fatalf("account status banned=%v suspended=%v", by[banned].AccountStatus, by[susp].AccountStatus)
	}
	if !by[empty].TrafficExhausted || by[f.tenant].TrafficExhausted || by[f.tenant].AccountStatus != backendv1.AccountStatus_ACCOUNT_STATUS_ACTIVE {
		t.Fatalf("exhausted %v / %v", by[empty], by[f.tenant])
	}
	if tr := by[f.tenant]; len(tr.StoppedRuleIds) != 1 || p.UUID(tr.StoppedRuleIds[0].Value) != rule1 || !tr.PlanExpiresAt.AsTime().After(time.Now()) {
		t.Fatalf("rule stop %v", tr)
	}

	// 内容不变：版本不变，广播只发一次；变化：版本递增
	f.hubs.conn = []p.UUID{f.hub("online", true)}
	if err := f.s.BroadcastRestrictions(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.s.BroadcastRestrictions(ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(f.hubs.take(f.hubs.conn[0])); n != 1 {
		t.Fatalf("broadcast %d times", n)
	}
	again, _ := f.s.Restrictions(ctx)
	if again.SnapshotVersion != snap.SnapshotVersion {
		t.Fatal("unchanged snapshot got a new version")
	}
	f.exec("INSERT INTO restrictions (id, tenant_id, kind) VALUES ($1, $2, 'over_limit')", p.NewUUIDv7().PG(), f.tenant.PG())
	f.exec("INSERT INTO restrictions (id, tenant_id, kind) VALUES ($1, $2, 'plan_expired')", p.NewUUIDv7().PG(), susp.PG())
	next, _ := f.s.Restrictions(ctx)
	if next.SnapshotVersion <= snap.SnapshotVersion {
		t.Fatal("changed snapshot kept its version")
	}
	for _, tr := range next.Tenants {
		switch p.UUID(tr.TenantId.Value) {
		case f.tenant:
			if len(tr.StoppedRuleIds) != 3 || !hasID(tr.StoppedRuleIds, rule2) {
				t.Fatalf("tenant-wide stop %v", tr.StoppedRuleIds)
			}
		case susp:
			if tr.PlanExpiresAt.AsTime().After(time.Now()) {
				t.Fatal("plan_expired restriction not applied")
			}
		}
	}
	if err := f.s.BroadcastRestrictions(ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(f.hubs.take(f.hubs.conn[0])); n != 1 {
		t.Fatalf("changed snapshot broadcast %d times", n)
	}
}

func hasID(ids []*commonv1.Uuid, id p.UUID) bool {
	for _, x := range ids {
		if p.UUID(x.Value) == id {
			return true
		}
	}
	return false
}

// rules 为租户保存两条规则（平台入口 → 平台出口）。
func (f *fixture) rules() (p.UUID, p.UUID) {
	e, x := p.NewUUIDv7(), p.NewUUIDv7()
	f.exec("INSERT INTO nodes (id, name, region, can_entry, public_addresses, lifecycle) VALUES ($1, 'e', 'eu', true, '{203.0.113.1}', 'online')", e.PG())
	f.exec("INSERT INTO nodes (id, name, region, can_exit, public_addresses, lifecycle) VALUES ($1, 'x', 'eu', true, '{203.0.113.2}', 'online')", x.PG())
	f.exec("INSERT INTO node_port_ranges (node_id, kind, port_min, port_max) VALUES ($1, 'link', 7443, 7443)", x.PG())
	ge, gx := p.NewUUIDv7(), p.NewUUIDv7()
	f.exec("INSERT INTO node_groups (id, name, purpose) VALUES ($1, 'e', 'entry'), ($2, 'x', 'exit')", ge.PG(), gx.PG())
	f.exec("INSERT INTO node_group_members (node_group_id, node_id) VALUES ($1, $2), ($3, $4)", ge.PG(), e.PG(), gx.PG(), x.PG())
	admin := apptest.Admin(f.t, f.dsn)
	f.exec("INSERT INTO segment_permits (upstream_group_id, downstream_group_id, created_by) VALUES ($1, $2, $3)", ge.PG(), gx.PG(), admin.PG())
	f.exec("INSERT INTO billing_versions (id, version, effective_at) VALUES ($1, 0, now()) ON CONFLICT (version) DO NOTHING", p.NewUUIDv7().PG())
	f.exec(`INSERT INTO group_multipliers (billing_version_id, node_group_id, multiplier_milli)
		SELECT id, g, 1000 FROM billing_versions, unnest(ARRAY[$1::uuid, $2::uuid]) g WHERE version = 0`, ge.PG(), gx.PG())
	svc := &rules.Service{DB: f.s.DB}
	var ids []p.UUID
	for _, port := range []uint16{20001, 20002} {
		s, err := svc.Create(context.Background(), f.tenant, rules.Spec{Name: "r", Network: "tcp", Mode: "tunnel", EntryGroup: rules.CID(ge), ServicePort: port,
			Path: []compiler.Segment{{Group: rules.CID(gx)}}, Targets: []compiler.Target{{Host: "198.51.100.7", Port: 443, Weight: 1}},
			TargetSchedule: "weighted_round_robin", ProxyProtocol: "off", Enabled: true})
		if err != nil {
			f.t.Fatal(err)
		}
		ids = append(ids, s.ID)
	}
	return ids[0], ids[1]
}

// LIM-009：需求信号 = 近 60 秒用量 × (1 + 令牌耗尽占比)；超过 DemandTTL 未更新视为 0。
func TestSharesFollowDemand(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	f.subscribe(f.tenant, 1000, 10, 1<<30)
	h := f.hub("online", true)
	a, _ := f.node(h, true, f.tenant)
	b, _ := f.node(h, true, f.tenant)
	f.s.ReportDemand(a, f.tenant, 3000, 0)
	f.s.ReportDemand(b, f.tenant, 1000, 0)
	if err := f.s.ReallocateShares(ctx, f.tenant); err != nil {
		t.Fatal(err)
	}
	if f.live(a) != "conn:7,rate:625" || f.live(b) != "conn:3,rate:375" {
		t.Fatalf("by demand a=%s b=%s", f.live(a), f.live(b))
	}
	// 被限速（令牌耗尽 100%）的执行点需求加倍
	f.s.ReportDemand(a, f.tenant, 1000, 1000)
	f.s.ReportDemand(b, f.tenant, 2000, 0)
	if err := f.s.ReallocateShares(ctx, f.tenant); err != nil {
		t.Fatal(err)
	}
	if f.live(a) != "conn:5,rate:500" || f.live(b) != "conn:5,rate:500" {
		t.Fatalf("starved a=%s b=%s", f.live(a), f.live(b))
	}
	// 过期的信号不再计入
	f.s.mu.Lock()
	d := f.s.demand[demandKey{b, f.tenant}]
	d.at = d.at.Add(-DemandTTL - time.Second)
	f.s.demand[demandKey{b, f.tenant}] = d
	f.s.mu.Unlock()
	if err := f.s.ReallocateShares(ctx, f.tenant); err != nil {
		t.Fatal(err)
	}
	if f.live(a) != "conn:8,rate:750" || f.live(b) != "conn:2,rate:250" {
		t.Fatalf("stale demand a=%s b=%s", f.live(a), f.live(b))
	}
}

// 重算不收回已签发、尚未结算的部分（MTR-013），切分只针对未签发的余量。
func TestBudgetKeepsIssued(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	f.subscribe(f.tenant, 1000, 10, 100<<20)
	rule, _ := f.rules()
	h := f.hub("online", true)
	n, boot := f.node(h, false, f.tenant) // 执行点离线：实例不参与切分
	f.exec("INSERT INTO regional_budgets (tenant_id, hub_id, delegated, budget_version) VALUES ($1, $2, $3, 1)", f.tenant.PG(), h.PG(), int64(64<<20))
	grant := func(reserved int64, status string) {
		settledAt := any(nil)
		if status == "settled" {
			settledAt = time.Now()
		}
		f.exec(`INSERT INTO billing_grants (id, tenant_id, rule_id, node_id, boot_id, hub_id, billing_version, path_coefficient_milli, raw_bytes,
			reserved_billing_bytes, status, settled_at) VALUES ($1, $2, $3, $4, $5, $6, 0, 1000, $7, $7, $8, $9)`,
			p.NewUUIDv7().PG(), f.tenant.PG(), rule.PG(), n.PG(), boot.PG(), h.PG(), reserved, status, settledAt)
	}
	grant(30<<20, "active")
	grant(10<<20, "settling")
	grant(5<<20, "settled")
	if err := f.s.Rebudget(ctx, f.tenant, p.UUID{}); err != nil {
		t.Fatal(err)
	}
	d := func() string {
		return f.query("SELECT delegated::text FROM regional_budgets WHERE tenant_id = $1 AND hub_id = $2", f.tenant.PG(), h.PG())
	}
	if d() != "41943040" { // 40 MiB
		t.Fatalf("withdrawn below the issued part: %s", d())
	}
	f.exec("UPDATE nodes SET lifecycle = 'online' WHERE id = $1", n.PG())
	if err := f.s.Rebudget(ctx, f.tenant, p.UUID{}); err != nil {
		t.Fatal(err)
	}
	if d() != "104857600" { // 40 MiB 已签发 + 余额中未签发的 60 MiB
		t.Fatalf("participant: %s", d())
	}
}
