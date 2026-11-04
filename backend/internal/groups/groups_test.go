package groups

import (
	"context"
	"errors"
	"testing"
	"time"

	"vortex/backend/apptest"
	"vortex/backend/compiler"
	"vortex/backend/internal/db"
	p "vortex/backend/internal/platform"
	"vortex/backend/internal/rules"
)

type fixture struct {
	t        *testing.T
	dsn      string
	s        *Service
	q        *db.Queries
	admin    p.UUID
	changed  int
	billings int
}

func setup(t *testing.T) *fixture {
	dsn := apptest.Database(t)
	pool, err := p.OpenDB(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	f := &fixture{t: t, dsn: dsn, q: db.New(pool), admin: apptest.Admin(t, dsn)}
	f.s = &Service{DB: pool, Changed: func(context.Context) { f.changed++ }, BillingChanged: func(context.Context) { f.billings++ }}
	apptest.Exec(t, dsn, "INSERT INTO billing_versions (id, version, effective_at) VALUES ($1, 0, now() - interval '1 day')", p.NewUUIDv7().PG())
	return f
}

func (f *fixture) query(sql string, args ...any) string {
	f.t.Helper()
	var s string
	apptest.QueryRow(f.t, f.dsn, sql, args, &s)
	return s
}

func code(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	if err != nil {
		return "other: " + err.Error()
	}
	return ""
}

func (f *fixture) node(entry, exit bool, public string, tenant *p.UUID) p.UUID {
	id := p.NewUUIDv7()
	var tn any
	if tenant != nil {
		tn = tenant.PG()
	}
	addrs := "{}"
	if public != "" {
		addrs = "{" + public + "}"
	}
	apptest.Exec(f.t, f.dsn, `INSERT INTO nodes (id, tenant_id, name, region, can_entry, can_exit, public_addresses, lifecycle)
		VALUES ($1, $2, 'n', 'eu', $3, $4, $5::inet[], 'online')`, id.PG(), tn, entry, exit, addrs)
	return id
}

// 新建：平台组初始倍率写入当前版本与全部未来版本（NOD-014）；个人组倍率固定为 0；参数校验；审计。
func TestCreateGroups(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	// 版本 0（已被取代）、2（当前生效）、5（未来）
	apptest.Exec(t, f.dsn, `INSERT INTO billing_versions (id, version, effective_at, created_at) VALUES
		($1, 2, now() - interval '1 hour', now() - interval '2 days'), ($2, 5, now() + interval '2 days', now())`, p.NewUUIDv7().PG(), p.NewUUIDv7().PG())
	g, err := f.s.Create(ctx, f.admin, Spec{Name: " entry ", Purpose: "entry", MultiplierMilli: 1500})
	if err != nil {
		t.Fatal(err)
	}
	if got := f.query("SELECT string_agg(bv.version || ':' || gm.multiplier_milli, ',' ORDER BY bv.version) FROM group_multipliers gm JOIN billing_versions bv ON bv.id = gm.billing_version_id WHERE gm.node_group_id = $1", g.PG()); got != "2:1500,5:1500" {
		t.Fatalf("multipliers %s", got)
	}
	if got := f.query("SELECT name || ':' || member_schedule || ':' || (health_params->>'interval_s') FROM node_groups WHERE id = $1", g.PG()); got != "entry:weighted_round_robin:3" {
		t.Fatalf("group %s", got)
	}
	if f.billings != 1 || f.query("SELECT count(*)::text FROM audit_logs WHERE action = 'node_group.create'") != "1" {
		t.Fatal("billing sync or audit missing")
	}
	tenant := apptest.Tenant(t, f.dsn)
	personal, err := f.s.Create(ctx, f.admin, Spec{Name: "mine", Purpose: "exit", Tenant: &tenant, Schedule: "primary_backup"})
	if err != nil {
		t.Fatal(err)
	}
	if f.query("SELECT count(*)::text FROM group_multipliers WHERE node_group_id = $1", personal.PG()) != "0" || f.billings != 1 {
		t.Fatal("personal group got a multiplier")
	}
	for _, c := range []struct {
		want string
		sp   Spec
	}{
		{ErrMultiplier, Spec{Name: "x", Purpose: "exit"}},
		{ErrMultiplier, Spec{Name: "x", Purpose: "exit", Tenant: &tenant, MultiplierMilli: 1}},
		{ErrInvalid, Spec{Name: "x", Purpose: "both", MultiplierMilli: 1}},
		{ErrInvalid, Spec{Name: " ", Purpose: "exit", MultiplierMilli: 1}},
		{ErrInvalid, Spec{Name: "x", Purpose: "exit", Schedule: "random", MultiplierMilli: 1}},
		{ErrInvalid, Spec{Name: "x", Purpose: "exit", Health: Health{IntervalS: 3}, MultiplierMilli: 1}},
	} {
		if _, err := f.s.Create(ctx, f.admin, c.sp); code(err) != c.want {
			t.Errorf("%+v: %v", c.sp, err)
		}
	}
}

// 成员：触发器的规则映射为错误码；入口组成员登记 joining；移除；变化后重新部署。
func TestMembers(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	entry, _ := f.s.Create(ctx, f.admin, Spec{Name: "e", Purpose: "entry", MultiplierMilli: 1000})
	exit, _ := f.s.Create(ctx, f.admin, Spec{Name: "x", Purpose: "exit", MultiplierMilli: 1000})
	tenant := apptest.Tenant(t, f.dsn)
	e1 := f.node(true, false, "203.0.113.1", nil)
	private := f.node(true, false, "", nil)
	x1 := f.node(false, true, "203.0.113.2", nil)
	self := f.node(false, true, "", &tenant)
	if err := f.s.AddMember(ctx, f.admin, entry, e1, 2, 1); err != nil {
		t.Fatal(err)
	}
	if got := f.query("SELECT state FROM entry_members WHERE node_group_id = $1 AND node_id = $2", entry.PG(), e1.PG()); got != "joining" {
		t.Fatalf("entry member %s", got)
	}
	cases := []struct {
		group, node p.UUID
		want        string
	}{
		{entry, e1, ErrMemberExists},
		{entry, private, "NODE_GROUP_ENTRY_NEEDS_PUBLIC_ADDRESS"},
		{entry, x1, "NODE_GROUP_CAPABILITY_MISMATCH"},
		{exit, self, "NODE_GROUP_TENANT_MISMATCH"},
		{p.NewUUIDv7(), x1, ErrNotFound},
	}
	for _, c := range cases {
		if got := code(f.s.AddMember(ctx, f.admin, c.group, c.node, 1, 0)); got != c.want {
			t.Errorf("add %s: %s", c.want, got)
		}
	}
	if code(f.s.AddMember(ctx, f.admin, exit, x1, 1, -1)) != ErrInvalid {
		t.Error("negative priority accepted")
	}
	if err := f.s.AddMember(ctx, f.admin, exit, x1, 0, 0); err != nil {
		t.Fatal(err)
	}
	if got := f.query("SELECT weight::text FROM node_group_members WHERE node_id = $1", x1.PG()); got != "1" {
		t.Fatalf("default weight %s", got)
	}
	if err := f.s.RemoveMember(ctx, f.admin, entry, e1); err != nil {
		t.Fatal(err)
	}
	if f.query("SELECT count(*)::text FROM entry_members") != "0" || code(f.s.RemoveMember(ctx, f.admin, entry, e1)) != ErrMemberNotFound {
		t.Fatal("remove")
	}
	if f.changed != 3 || f.query("SELECT count(*)::text FROM audit_logs WHERE action LIKE 'node_group.member_%'") != "3" {
		t.Fatalf("changed %d", f.changed)
	}
}

// 组倍率修改：新计费版本（复制最新版本、改动这一组），提前 24 小时；同步到实例；个人组不可设。
func TestSetMultiplier(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	a, _ := f.s.Create(ctx, f.admin, Spec{Name: "a", Purpose: "exit", MultiplierMilli: 1000})
	b, _ := f.s.Create(ctx, f.admin, Spec{Name: "b", Purpose: "exit", MultiplierMilli: 2000})
	f.billings = 0
	if _, err := f.s.SetMultiplier(ctx, f.admin, a, 1200, time.Now().Add(23*time.Hour)); code(err) != ErrMultiplierLead {
		t.Fatalf("too soon: %v", err)
	}
	if _, err := f.s.SetMultiplier(ctx, f.admin, a, 0, time.Now().Add(25*time.Hour)); code(err) != ErrMultiplier {
		t.Fatalf("zero: %v", err)
	}
	v, err := f.s.SetMultiplier(ctx, f.admin, a, 1200, time.Now().Add(25*time.Hour))
	if err != nil || v != 1 {
		t.Fatalf("set %d %v", v, err)
	}
	ms := func(version int) string {
		return f.query(`SELECT string_agg(gm.multiplier_milli::text, ',' ORDER BY g.name) FROM group_multipliers gm JOIN billing_versions bv ON bv.id = gm.billing_version_id
			JOIN node_groups g ON g.id = gm.node_group_id WHERE bv.version = $1`, version)
	}
	if ms(0) != "1000,2000" || ms(1) != "1200,2000" || f.billings != 1 {
		t.Fatalf("versions %s / %s, billings %d", ms(0), ms(1), f.billings)
	}
	// 下一次修改基于最新（未来）版本，且不能早于它生效
	if _, err := f.s.SetMultiplier(ctx, f.admin, b, 2500, time.Now().Add(24*time.Hour+time.Minute)); code(err) != ErrMultiplierLead {
		t.Fatalf("before the latest version: %v", err)
	}
	if v, err := f.s.SetMultiplier(ctx, f.admin, b, 2500, time.Now().Add(48*time.Hour)); err != nil || v != 2 || ms(2) != "1200,2500" {
		t.Fatalf("chained %d %v %s", v, err, ms(2))
	}
	// 编译仍按当前生效的版本
	snap, err := rules.LoadSnapshot(ctx, f.q)
	if err != nil || snap.BillingVersion != 0 || snap.Groups[rules.CID(a)].MultiplierMilli != 1000 {
		t.Fatalf("snapshot %d %v", snap.BillingVersion, err)
	}
	tenant := apptest.Tenant(t, f.dsn)
	personal, _ := f.s.Create(ctx, f.admin, Spec{Name: "p", Purpose: "exit", Tenant: &tenant})
	if _, err := f.s.SetMultiplier(ctx, f.admin, personal, 1000, time.Now().Add(72*time.Hour)); code(err) != ErrMultiplier {
		t.Fatalf("personal: %v", err)
	}
	if f.query("SELECT count(*)::text FROM audit_logs WHERE action = 'billing.multiplier_change'") != "2" {
		t.Fatal("audit")
	}
}

// 段许可与删除：仍被规则使用时拒绝删除组与撤销许可。
func TestPermitsAndDelete(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	entry, _ := f.s.Create(ctx, f.admin, Spec{Name: "e", Purpose: "entry", MultiplierMilli: 1000})
	exit, _ := f.s.Create(ctx, f.admin, Spec{Name: "x", Purpose: "exit", MultiplierMilli: 1000})
	if code(f.s.Permit(ctx, f.admin, entry, entry)) != ErrPermitSameGroups || code(f.s.Permit(ctx, f.admin, entry, p.NewUUIDv7())) != ErrNotFound {
		t.Fatal("permit validation")
	}
	if err := f.s.Permit(ctx, f.admin, entry, exit); err != nil {
		t.Fatal(err)
	}
	if err := f.s.Permit(ctx, f.admin, entry, exit); err != nil { // 重复创建无害
		t.Fatal(err)
	}
	_ = f.s.AddMember(ctx, f.admin, entry, f.node(true, false, "203.0.113.1", nil), 1, 0)
	x := f.node(false, true, "203.0.113.2", nil)
	apptest.Exec(t, f.dsn, "INSERT INTO node_port_ranges (node_id, kind, port_min, port_max) VALUES ($1, 'link', 7443, 7443)", x.PG())
	_ = f.s.AddMember(ctx, f.admin, exit, x, 1, 0)
	tenant := apptest.Tenant(t, f.dsn)
	apptest.Entitle(t, f.dsn, tenant)
	saved, err := (&rules.Service{DB: f.s.DB}).Create(ctx, tenant, rules.Spec{Name: "r", Network: "tcp", Mode: "tunnel", EntryGroup: rules.CID(entry),
		ServicePort: 20000, Path: []compiler.Segment{{Group: rules.CID(exit)}}, Targets: []compiler.Target{{Host: "198.51.100.7", Port: 443, Weight: 1}},
		TargetSchedule: "weighted_round_robin", ProxyProtocol: "off", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if code(f.s.Revoke(ctx, f.admin, entry, exit)) != ErrPermitInUse || code(f.s.Delete(ctx, f.admin, exit)) != ErrInUse || code(f.s.Delete(ctx, f.admin, entry)) != ErrInUse {
		t.Fatal("in use")
	}
	_ = (&rules.Service{DB: f.s.DB}).Delete(ctx, tenant, saved.ID)
	if err := f.s.Revoke(ctx, f.admin, entry, exit); err != nil {
		t.Fatal(err)
	}
	if code(f.s.Revoke(ctx, f.admin, entry, exit)) != ErrPermitNotFound {
		t.Fatal("revoke twice")
	}
	if err := f.s.Delete(ctx, f.admin, exit); err != nil {
		t.Fatal(err)
	}
	if code(f.s.Delete(ctx, f.admin, exit)) != ErrNotFound || code(f.s.AddMember(ctx, f.admin, exit, x, 1, 0)) != ErrNotFound {
		t.Fatal("deleted group still usable")
	}
	if code(f.s.Update(ctx, f.admin, entry, 99, "e2", "", Health{})) != ErrConflict {
		t.Fatal("stale row version")
	}
	if err := f.s.Update(ctx, f.admin, entry, 1, "e2", "primary_backup", Health{IntervalS: 5, FailThreshold: 2, RecoverThreshold: 2}); err != nil {
		t.Fatal(err)
	}
	if got := f.query("SELECT name || ':' || member_schedule || ':' || (health_params->>'interval_s') || ':' || row_version FROM node_groups WHERE id = $1", entry.PG()); got != "e2:primary_backup:5:2" {
		t.Fatalf("update %s", got)
	}
}
