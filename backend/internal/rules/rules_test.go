package rules

import (
	"context"
	"errors"
	"testing"

	"vortex/backend/apptest"
	"vortex/backend/compiler"
	"vortex/backend/internal/db"
	p "vortex/backend/internal/platform"
)

type fixture struct {
	t                 *testing.T
	dsn               string
	svc               *Service
	q                 *db.Queries
	tenant, other     p.UUID
	admin             p.UUID
	entry, exit, priv p.UUID // 平台入口组、平台出口组、租户的个人出口组
	nodes             map[string]p.UUID
}

func setup(t *testing.T) *fixture {
	dsn := apptest.Database(t)
	pool, err := p.OpenDB(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	f := &fixture{t: t, dsn: dsn, svc: &Service{DB: pool}, q: db.New(pool), tenant: apptest.Tenant(t, dsn), other: apptest.Tenant(t, dsn),
		admin: apptest.Admin(t, dsn), nodes: map[string]p.UUID{}}
	node := func(name, pub string, tenant *p.UUID, entry, exit bool) p.UUID {
		id := p.NewUUIDv7()
		var tn any
		if tenant != nil {
			tn = tenant.PG()
		}
		addrs := "{}"
		if pub != "" {
			addrs = "{" + pub + "}"
		}
		apptest.Exec(t, dsn, `INSERT INTO nodes (id, tenant_id, name, region, can_entry, can_exit, public_addresses, lifecycle)
			VALUES ($1, $2, $3, 'eu', $4, $5, $6::inet[], 'online')`, id.PG(), tn, name, entry, exit, addrs)
		apptest.Exec(t, dsn, "INSERT INTO node_port_ranges (node_id, kind, port_min, port_max) VALUES ($1, 'link', 7443, 7443)", id.PG())
		f.nodes[name] = id
		return id
	}
	group := func(purpose string, tenant *p.UUID, members ...p.UUID) p.UUID {
		id := p.NewUUIDv7()
		var tn any
		if tenant != nil {
			tn = tenant.PG()
		}
		apptest.Exec(t, dsn, "INSERT INTO node_groups (id, tenant_id, name, purpose) VALUES ($1, $2, 'g', $3)", id.PG(), tn, purpose)
		for _, m := range members {
			apptest.Exec(t, dsn, "INSERT INTO node_group_members (node_group_id, node_id) VALUES ($1, $2)", id.PG(), m.PG())
		}
		return id
	}
	e1 := node("e1", "203.0.113.1", nil, true, false)
	x1 := node("x1", "203.0.113.2", nil, false, true)
	px := node("px", "", &f.tenant, false, true)
	f.entry, f.exit, f.priv = group("entry", nil, e1), group("exit", nil, x1), group("exit", &f.tenant, px)
	apptest.Exec(t, dsn, "INSERT INTO segment_permits (upstream_group_id, downstream_group_id, created_by) VALUES ($1, $2, $3), ($1, $4, $3)",
		f.entry.PG(), f.exit.PG(), f.admin.PG(), f.priv.PG())
	bv := p.NewUUIDv7()
	apptest.Exec(t, dsn, "INSERT INTO billing_versions (id, version, effective_at) VALUES ($1, 0, now() - interval '1 minute')", bv.PG())
	apptest.Exec(t, dsn, "INSERT INTO group_multipliers (billing_version_id, node_group_id, multiplier_milli) VALUES ($1, $2, 1000), ($1, $3, 800)",
		bv.PG(), f.entry.PG(), f.exit.PG())
	return f
}

func (f *fixture) spec(exit p.UUID) Spec {
	return Spec{Name: "web", Network: "tcp_udp", Mode: "tunnel", EntryGroup: CID(f.entry), ServicePort: 20000,
		Path: []compiler.Segment{{Group: CID(exit)}}, Targets: []compiler.Target{{Host: "198.51.100.7", Port: 443, Weight: 1}},
		TargetSchedule: "weighted_round_robin", ProxyProtocol: "off", Enabled: true}
}

func problems(err error) []string {
	var ve *ValidationError
	if !errors.As(err, &ve) {
		return nil
	}
	var out []string
	for _, p := range ve.Problems {
		out = append(out, p.Code)
	}
	return out
}

func TestCreateCompileUpdateDelete(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	saved, err := f.svc.Create(ctx, f.tenant, f.spec(f.exit))
	if err != nil {
		t.Fatal(err)
	}
	snap, err := LoadSnapshot(ctx, f.q)
	if err != nil {
		t.Fatal(err)
	}
	res := compiler.Compile(snap)
	if len(res.Problems) != 0 {
		t.Fatalf("problems %v", res.Problems)
	}
	entryCfg := res.Configs[CID(f.nodes["e1"])]
	if len(entryCfg.Rules) != 1 || len(entryCfg.Listeners) != 2 || entryCfg.Rules[0].PathCoefficientMilli != 1800 || entryCfg.Rules[0].RuleVersion != 1 {
		t.Fatalf("entry config %+v", entryCfg)
	}
	if exitCfg := res.Configs[CID(f.nodes["x1"])]; len(exitCfg.Rules) != 1 || exitCfg.Rules[0].HopIndex != 1 {
		t.Fatalf("exit config %+v", exitCfg)
	}
	// 读回、并发修改冲突、修改后期望版本递增并保留每个版本的快照
	got, sv, err := f.svc.Get(ctx, f.tenant, saved.ID)
	if err != nil || got.Name != "web" || sv.Version != 1 {
		t.Fatalf("get %+v %+v %v", got, sv, err)
	}
	sp := f.spec(f.exit)
	sp.ServicePort = 20001
	if _, err := f.svc.Update(ctx, f.tenant, saved.ID, saved.RowVersion+5, sp); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale row version: %v", err)
	}
	up, err := f.svc.Update(ctx, f.tenant, saved.ID, saved.RowVersion, sp)
	if err != nil || up.Version != 2 {
		t.Fatalf("update %+v %v", up, err)
	}
	var versions int
	apptest.QueryRow(t, f.dsn, "SELECT count(*) FROM rule_versions WHERE rule_id = $1", []any{saved.ID.PG()}, &versions)
	if versions != 2 {
		t.Fatalf("%d rule versions", versions)
	}
	snap, _ = LoadSnapshot(ctx, f.q)
	if res := compiler.Compile(snap); res.Configs[CID(f.nodes["e1"])].Listeners[0].Port != 20001 {
		t.Fatal("snapshot does not use the latest desired version")
	}
	// 租户隔离（TEN-004）
	if _, _, err := f.svc.Get(ctx, f.other, saved.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("other tenant get: %v", err)
	}
	if _, err := f.svc.Update(ctx, f.other, saved.ID, up.RowVersion, sp); !errors.Is(err, ErrNotFound) {
		t.Fatalf("other tenant update: %v", err)
	}
	if err := f.svc.Delete(ctx, f.other, saved.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("other tenant delete: %v", err)
	}
	// 已删除的节点进入吊销列表（SEC-002），编译进每个节点的配置
	apptest.Exec(t, f.dsn, "UPDATE nodes SET deleted_at = now() WHERE id = $1", f.nodes["px"].PG())
	snap, _ = LoadSnapshot(ctx, f.q)
	if len(snap.Revocations.NodeIDs) != 1 || snap.Revocations.NodeIDs[0] != CID(f.nodes["px"]) {
		t.Fatalf("revoked nodes %v", snap.Revocations.NodeIDs)
	}
	if _, ok := snap.Nodes[CID(f.nodes["px"])]; ok {
		t.Fatal("deleted node still a fleet member")
	}
	if err := f.svc.Delete(ctx, f.tenant, saved.ID); err != nil {
		t.Fatal(err)
	}
	snap, _ = LoadSnapshot(ctx, f.q)
	if res := compiler.Compile(snap); len(res.Configs[CID(f.nodes["e1"])].Rules) != 0 {
		t.Fatal("deleted rule still compiled")
	}
}

func TestValidationOnSave(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	// 个人出口（无公网地址）：自动反向建链，可访问本机
	ok := f.spec(f.priv)
	ok.Targets = []compiler.Target{{Host: "127.0.0.1", Port: 22, Weight: 1}}
	if _, err := f.svc.Create(ctx, f.tenant, ok); err != nil {
		t.Fatalf("self-hosted exit: %v", err)
	}
	noPermit := p.NewUUIDv7()
	apptest.Exec(t, f.dsn, "INSERT INTO node_groups (id, name, purpose) VALUES ($1, 'np', 'exit')", noPermit.PG())
	apptest.Exec(t, f.dsn, "INSERT INTO node_group_members (node_group_id, node_id) VALUES ($1, $2)", noPermit.PG(), f.nodes["x1"].PG())
	cases := []struct {
		name string
		mut  func(*Spec)
		want string
	}{
		{"private target on a platform exit", func(s *Spec) { s.Targets[0].Host = "10.0.0.5" }, compiler.ErrTargetForbidden},
		{"no permit", func(s *Spec) { s.Path[0].Group = CID(noPermit) }, compiler.ErrSegmentNotPermitted},
		{"entry group at the exit position", func(s *Spec) { s.EntryGroup = CID(f.exit) }, compiler.ErrGroupPurpose},
		{"unknown group", func(s *Spec) { s.Path[0].Group = CID(p.NewUUIDv7()) }, compiler.ErrGroupMissing},
		{"port taken by another rule", func(s *Spec) { s.ServicePort = 20000 }, ErrPortUnavailable},
		{"proxy v1 with udp", func(s *Spec) { s.ProxyProtocol = "v1" }, compiler.ErrProxyProtocol},
		{"reverse without entitlement", func(s *Spec) { s.Path[0].Dial = "reverse" }, compiler.ErrReverseNotEntitled},
		{"bad group id", func(s *Spec) { s.EntryGroup = "x" }, compiler.ErrGroupMissing},
	}
	for i, c := range cases {
		s := f.spec(f.exit)
		s.ServicePort = uint16(21000 + i)
		c.mut(&s)
		if _, err := f.svc.Create(ctx, f.tenant, s); len(problems(err)) == 0 || problems(err)[0] != c.want {
			t.Errorf("%s: %v", c.name, err)
		}
	}
	// 他租户使用本租户的个人组
	ok.ServicePort = 22000
	if _, err := f.svc.Create(ctx, f.other, ok); len(problems(err)) == 0 || problems(err)[0] != compiler.ErrGroupTenant {
		t.Fatalf("personal group of another tenant: %v", err)
	}
	// 拒绝的保存不留下任何行
	var n int
	apptest.QueryRow(t, f.dsn, "SELECT count(*) FROM rules", nil, &n)
	if n != 1 {
		t.Fatalf("%d rules stored", n)
	}
	// 组 ID 大小写规范化
	up := f.spec(f.exit)
	up.ServicePort = 0 // 自动分配
	up.EntryGroup = compiler.UUID(upper(string(CID(f.entry))))
	if _, err := f.svc.Create(ctx, f.tenant, up); err != nil {
		t.Fatalf("uppercase group id: %v", err)
	}
}

func upper(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'a' && c <= 'f' {
			b[i] = c - 32
		}
	}
	return string(b)
}
