package billing

import (
	"context"
	"sync"
	"testing"
	"time"

	"vortex/backend/apptest"
	"vortex/backend/internal/db"
	p "vortex/backend/internal/platform"
)

type fakeBudgets struct {
	mu        sync.Mutex
	in        []p.UUID
	sent      int
	broadcast int
	balance   map[p.UUID]int64 // 重算时（同一事务内）看到的余额
}

func (f *fakeBudgets) RebudgetIn(ctx context.Context, q *db.Queries, tenant p.UUID) (func(), error) {
	b, err := q.TenantBalance(ctx, tenant.PG())
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.in = append(f.in, tenant)
	f.balance[tenant] = b
	f.mu.Unlock()
	return func() { f.mu.Lock(); f.sent++; f.mu.Unlock() }, nil
}

func (f *fakeBudgets) BroadcastRestrictions(context.Context) error { f.broadcast++; return nil }

func TestNextPeriodEnd(t *testing.T) {
	start := time.Date(2026, 1, 31, 8, 0, 0, 0, time.UTC)
	exp := start.AddDate(1, 0, 0)
	now := time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC)
	// 自开通日起每满一个月：1/31 + 2 个月 = 3/31（Go 的 AddDate 规范化 2/31 → 3/3，3 个月 → 5/1）
	if got := nextPeriodEnd(start, exp, now); !got.Equal(start.AddDate(0, 2, 0)) {
		t.Fatalf("next %v", got)
	}
	if got := nextPeriodEnd(start, start.AddDate(0, 1, 10), now); !got.Equal(start.AddDate(0, 1, 10)) {
		t.Fatalf("capped at expiry: %v", got)
	}
}

func TestTick(t *testing.T) {
	dsn := apptest.Database(t)
	pool, err := p.OpenDB(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	fb := &fakeBudgets{balance: map[p.UUID]int64{}}
	s := &Service{DB: pool, Budgets: fb}
	ctx := context.Background()
	q := func(sql string, args ...any) string {
		var v string
		apptest.QueryRow(t, dsn, sql, args, &v)
		return v
	}
	plan := func(reset string) p.UUID {
		id := p.NewUUIDv7()
		apptest.Exec(t, dsn, `INSERT INTO plans (id, name, duration_days, base_traffic_bytes, traffic_reset, max_rules, rate_bps, max_conns)
			VALUES ($1, 'p', 90, 1000, $2, 10, 1000, 10)`, id.PG(), reset)
		return id
	}
	monthly, once := plan("monthly"), plan("none")
	sub := func(tenant, plan p.UUID, started, expires, periodEnd string, remaining int64) {
		apptest.Exec(t, dsn, `INSERT INTO subscriptions (tenant_id, plan_id, started_at, expires_at, base_period_end, base_remaining_bytes)
			VALUES ($1, $2, now() + $3::interval, now() + $4::interval, now() + $5::interval, $6)`, tenant.PG(), plan.PG(), started, expires, periodEnd, remaining)
	}
	due := apptest.Tenant(t, dsn) // 按月、当期已结束：重置为 1000，未用完部分不结转
	sub(due, monthly, "-40 days", "50 days", "-1 hour", 300)
	notDue := apptest.Tenant(t, dsn) // 按月、当期未结束
	sub(notDue, monthly, "-10 days", "80 days", "20 days", 300)
	oneShot := apptest.Tenant(t, dsn) // 一次性发放：不重置；余额为 0 → traffic_exhausted
	sub(oneShot, once, "-40 days", "50 days", "-1 hour", 0)
	expired := apptest.Tenant(t, dsn) // 已到期：plan_expired；流量包随之暂停
	sub(expired, monthly, "-100 days", "-1 hour", "-1 hour", 500)
	packs := apptest.Tenant(t, dsn) // 流量包过期作废
	sub(packs, once, "-1 day", "80 days", "80 days", 100)
	apptest.Exec(t, dsn, `INSERT INTO traffic_packs (id, tenant_id, bytes_total, bytes_remaining, expires_at, source) VALUES
		($1, $3, 500, 400, now() - interval '1 minute', 't'), ($2, $3, 500, 500, now() + interval '1 day', 't')`, p.NewUUIDv7().PG(), p.NewUUIDv7().PG(), packs.PG())

	never := apptest.Tenant(t, dsn) // 从未订阅：不记 plan_expired
	n, err := s.Tick(ctx)
	if err != nil || n != 3 {
		t.Fatalf("tick %d %v", n, err)
	}
	if got := q("SELECT base_remaining_bytes || ':' || (base_period_end > now() AND base_period_end <= started_at + interval '2 months')::text FROM subscriptions WHERE tenant_id = $1", due.PG()); got != "1000:true" {
		t.Fatalf("monthly reset %s", got)
	}
	if got := q("SELECT base_remaining_bytes::text FROM subscriptions WHERE tenant_id = $1", notDue.PG()); got != "300" {
		t.Fatalf("not due %s", got)
	}
	if got := q("SELECT base_remaining_bytes::text FROM subscriptions WHERE tenant_id = $1", oneShot.PG()); got != "0" {
		t.Fatalf("one-shot %s", got)
	}
	if got := q("SELECT sum(bytes_remaining)::text FROM traffic_packs WHERE tenant_id = $1", packs.PG()); got != "500" {
		t.Fatalf("packs %s", got)
	}
	active := func(tenant p.UUID, kind string) string {
		return q("SELECT count(*)::text FROM restrictions WHERE tenant_id = $1 AND kind = $2 AND cleared_at IS NULL", tenant.PG(), kind)
	}
	if active(never, "plan_expired") != "0" || active(expired, "plan_expired") != "1" || active(oneShot, "traffic_exhausted") != "1" || active(due, "traffic_exhausted") != "0" ||
		active(expired, "traffic_exhausted") != "0" {
		t.Fatal("restrictions")
	}
	// MTR-013：余额变化的租户在同一事务内重算（看到的是变化后的余额），提交后下发；限制变化后广播
	if len(fb.in) != 3 || fb.sent != 3 || fb.balance[due] != 1000 || fb.balance[packs] != 600 || fb.balance[expired] != 0 || fb.broadcast != 1 {
		t.Fatalf("rebudget %+v", fb)
	}
	// 再次执行：没有变化
	if n, err := s.Tick(ctx); err != nil || n != 0 || fb.broadcast != 1 {
		t.Fatalf("second tick %d %v %d", n, err, fb.broadcast)
	}
	// 续期与充值：解除限制
	apptest.Exec(t, dsn, "UPDATE subscriptions SET expires_at = now() + interval '30 days', base_period_end = now() + interval '30 days' WHERE tenant_id = $1", expired.PG())
	apptest.Exec(t, dsn, "UPDATE subscriptions SET base_remaining_bytes = 10, base_period_end = expires_at WHERE tenant_id = $1", oneShot.PG())
	if _, err := s.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if active(expired, "plan_expired") != "0" || active(oneShot, "traffic_exhausted") != "0" || fb.broadcast != 2 {
		t.Fatal("restrictions not cleared")
	}
	// 只有限制变化（流量用尽）：同样广播
	apptest.Exec(t, dsn, "UPDATE subscriptions SET base_remaining_bytes = 0 WHERE tenant_id = $1", notDue.PG())
	if n, err := s.Tick(ctx); err != nil || n != 0 || fb.broadcast != 3 || active(notDue, "traffic_exhausted") != "1" {
		t.Fatalf("exhaustion only: %d %v %d", n, err, fb.broadcast)
	}
}
