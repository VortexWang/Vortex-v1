package apply

import (
	"context"
	"testing"

	"vortex/backend/apptest"
	"vortex/backend/compiler"
	"vortex/backend/internal/db"
	p "vortex/backend/internal/platform"
	"vortex/backend/internal/rules"
)

type fixture struct {
	t      *testing.T
	dsn    string
	svc    *Service
	rules  *rules.Service
	q      *db.Queries
	tenant p.UUID
	entry  p.UUID
	exit   p.UUID
	e1, x1 p.UUID
	woke   int
}

func setup(t *testing.T) *fixture {
	dsn := apptest.Database(t)
	pool, err := p.OpenDB(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	f := &fixture{t: t, dsn: dsn, rules: &rules.Service{DB: pool}, q: db.New(pool), tenant: apptest.Tenant(t, dsn)}
	f.svc = &Service{DB: pool, Notify: func() { f.woke++ }}
	admin := apptest.Admin(t, dsn)
	f.e1, f.x1, f.entry, f.exit = p.NewUUIDv7(), p.NewUUIDv7(), p.NewUUIDv7(), p.NewUUIDv7()
	apptest.Exec(t, dsn, `INSERT INTO nodes (id, name, region, can_entry, can_exit, public_addresses, lifecycle) VALUES
		($1, 'e1', 'eu', true, false, '{203.0.113.1}', 'online'), ($2, 'x1', 'eu', false, true, '{203.0.113.2}', 'online')`, f.e1.PG(), f.x1.PG())
	apptest.Exec(t, dsn, "INSERT INTO node_groups (id, name, purpose) VALUES ($1, 'e', 'entry'), ($2, 'x', 'exit')", f.entry.PG(), f.exit.PG())
	apptest.Exec(t, dsn, "INSERT INTO node_group_members (node_group_id, node_id) VALUES ($1, $2), ($3, $4)", f.entry.PG(), f.e1.PG(), f.exit.PG(), f.x1.PG())
	apptest.Exec(t, dsn, "INSERT INTO segment_permits (upstream_group_id, downstream_group_id, created_by) VALUES ($1, $2, $3)", f.entry.PG(), f.exit.PG(), admin.PG())
	return f
}

func (f *fixture) spec(port uint16) rules.Spec {
	return rules.Spec{Name: "r", Network: "tcp", Mode: "tunnel", EntryGroup: rules.CID(f.entry), ServicePort: port,
		Path: []compiler.Segment{{Group: rules.CID(f.exit)}}, Targets: []compiler.Target{{Host: "198.51.100.7", Port: 443, Weight: 1}},
		TargetSchedule: "weighted_round_robin", ProxyProtocol: "off", Enabled: true}
}

func (f *fixture) config(n p.UUID) db.NodeConfig {
	c, err := f.q.GetNodeConfig(context.Background(), n.PG())
	if err != nil {
		f.t.Fatal(err)
	}
	return c
}

func (f *fixture) sync(rule p.UUID) string {
	var s string
	apptest.QueryRow(f.t, f.dsn, "SELECT sync_status FROM rules WHERE id = $1", []any{rule.PG()}, &s)
	return s
}

func (f *fixture) ports(rule p.UUID) map[string]int {
	out := map[string]int{}
	as, _ := f.q.ListRuleAllocations(context.Background(), rule.PG())
	for _, a := range as {
		out[a.State]++
	}
	var released int
	apptest.QueryRow(f.t, f.dsn, "SELECT count(*) FROM port_allocations WHERE rule_id = $1 AND state = 'released'", []any{rule.PG()}, &released)
	out["released"] = released
	return out
}

func (f *fixture) advance() *AdvanceResult {
	r, err := f.svc.Advance(context.Background())
	if err != nil {
		f.t.Fatal(err)
	}
	return r
}

// 两阶段（RUL-010）：全部节点准备好才提交；全部生效后 applied；端口随生效转为 occupied。
// 内容不变不出新版本；删除后节点确认不再监听，端口 released。
func TestTwoPhase(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	saved, err := f.rules.Create(ctx, f.tenant, f.spec(20000))
	if err != nil {
		t.Fatal(err)
	}
	res, err := f.svc.Deploy(ctx)
	if err != nil || len(res.Changed) != 2 || res.Runs != 1 || f.woke == 0 {
		t.Fatalf("deploy %+v %v", res, err)
	}
	ve, vx := uint64(f.config(f.e1).Version), uint64(f.config(f.x1).Version)
	if f.sync(saved.ID) != "preparing" {
		t.Fatal(f.sync(saved.ID))
	}
	var outbox int
	apptest.QueryRow(t, f.dsn, "SELECT count(*) FROM outbox WHERE topic = $1", []any{TopicDesired}, &outbox)
	if outbox != 2 {
		t.Fatalf("outbox %d", outbox)
	}
	// 再次部署：内容不变，不出新版本、不建新编排
	if res, _ := f.svc.Deploy(ctx); len(res.Changed) != 0 || res.Runs != 0 {
		t.Fatalf("redeploy %+v", res)
	}
	// 只有入口准备好：不提交
	if err := f.svc.Prepared(ctx, f.e1, ve, nil); err != nil {
		t.Fatal(err)
	}
	if r := f.advance(); len(r.Committed) != 0 || f.sync(saved.ID) != "preparing" {
		t.Fatalf("committed before every node prepared: %+v", r)
	}
	_ = f.svc.Prepared(ctx, f.x1, vx, nil)
	r := f.advance()
	if len(r.Committed) != 2 || r.Committed[f.e1] != ve || f.sync(saved.ID) != "committing" {
		t.Fatalf("commit %+v %s", r, f.sync(saved.ID))
	}
	if r := f.advance(); len(r.Committed) != 0 {
		t.Fatal("commit sent twice")
	}
	_ = f.svc.Applied(ctx, f.e1, ve, true, "")
	if f.advance(); f.sync(saved.ID) != "committing" {
		t.Fatal("applied before every node applied")
	}
	_ = f.svc.Applied(ctx, f.x1, vx, true, "")
	if r := f.advance(); len(r.Applied) != 1 || f.sync(saved.ID) != "applied" {
		t.Fatalf("applied %+v %s", r, f.sync(saved.ID))
	}
	if st := f.ports(saved.ID); st["occupied"] != 1 {
		t.Fatalf("ports %v", st)
	}
	// 删除：新期望版本只涉及原来的节点；生效后端口释放（节点确认不再监听）
	if err := f.rules.Delete(ctx, f.tenant, saved.ID); err != nil {
		t.Fatal(err)
	}
	res, _ = f.svc.Deploy(ctx)
	if len(res.Changed) != 2 || res.Runs != 1 {
		t.Fatalf("delete deploy %+v", res)
	}
	f.advance() // 节点尚未生效删除：端口仍在释放中
	var delPhase string
	apptest.QueryRow(t, f.dsn, "SELECT phase FROM apply_runs WHERE rule_id = $1 ORDER BY version DESC LIMIT 1", []any{saved.ID.PG()}, &delPhase)
	if delPhase != "preparing" {
		t.Fatalf("deletion run %s: it must wait for the nodes that had the rule", delPhase)
	}
	if st := f.ports(saved.ID); st["releasing"] != 1 {
		t.Fatalf("ports after delete %v", st)
	}
	for _, n := range []p.UUID{f.e1, f.x1} {
		v := uint64(f.config(n).Version)
		_ = f.svc.Prepared(ctx, n, v, nil)
	}
	f.advance()
	for _, n := range []p.UUID{f.e1, f.x1} {
		_ = f.svc.Applied(ctx, n, uint64(f.config(n).Version), true, "")
	}
	f.advance()
	if st := f.ports(saved.ID); st["released"] != 1 || st["releasing"] != 0 {
		t.Fatalf("ports after the nodes applied the deletion %v", st)
	}
}

// 准备失败（RUL-011）：该规则版本失败，重新部署回到上一已生效版本；已有规则不受影响。
func TestPrepareFailureFallsBack(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	saved, _ := f.rules.Create(ctx, f.tenant, f.spec(20000))
	_, _ = f.svc.Deploy(ctx)
	for _, n := range []p.UUID{f.e1, f.x1} {
		_ = f.svc.Prepared(ctx, n, uint64(f.config(n).Version), nil)
	}
	f.advance()
	for _, n := range []p.UUID{f.e1, f.x1} {
		_ = f.svc.Applied(ctx, n, uint64(f.config(n).Version), true, "")
	}
	f.advance()
	good := f.config(f.e1)
	// 修改为另一端口；入口节点准备失败（例如端口被占用）
	up, err := f.rules.Update(ctx, f.tenant, saved.ID, saved.RowVersion, f.spec(20001))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.svc.Deploy(ctx)
	bad := f.config(f.e1)
	if bad.Version <= good.Version {
		t.Fatal("no new version")
	}
	_ = f.svc.Prepared(ctx, f.x1, uint64(f.config(f.x1).Version), nil)
	_ = f.svc.Prepared(ctx, f.e1, uint64(bad.Version), []string{"PORT_IN_USE: 20001"})
	r := f.advance()
	if len(r.Failed) != 1 || len(r.Committed) != 0 {
		t.Fatalf("failure %+v", r)
	}
	if s := f.sync(saved.ID); s != "failed" {
		t.Fatalf("sync %s", s)
	}
	// 重新部署（Advance 已触发）：内容回到上一已生效版本，版本号继续递增
	back := f.config(f.e1)
	if back.Version <= bad.Version || string(back.ContentDigest) != string(good.ContentDigest) {
		t.Fatalf("not rolled back to the last applied content: v%d", back.Version)
	}
	var phase string
	apptest.QueryRow(t, f.dsn, "SELECT phase FROM apply_runs WHERE rule_id = $1 AND version = $2", []any{saved.ID.PG(), int64(up.Version)}, &phase)
	if phase != "failed" {
		t.Fatalf("run phase %s", phase)
	}
	// 回退后的版本可以准备并提交
	_ = f.svc.Prepared(ctx, f.e1, uint64(back.Version), nil)
	if r := f.advance(); r.Committed[f.e1] != uint64(back.Version) {
		t.Fatalf("fallback not committed: %+v", r)
	}
	// 回退生效后：节点仍监听原端口，原端口不得释放；新端口没有监听，不得记为占用
	_ = f.svc.Applied(ctx, f.e1, uint64(back.Version), true, "")
	f.advance()
	var old, nw string
	apptest.QueryRow(t, f.dsn, "SELECT string_agg(state, ',') FROM port_allocations WHERE rule_id = $1 AND port = 20000", []any{saved.ID.PG()}, &old)
	apptest.QueryRow(t, f.dsn, "SELECT string_agg(state, ',') FROM port_allocations WHERE rule_id = $1 AND port = 20001", []any{saved.ID.PG()}, &nw)
	if old == "released" || nw == "occupied" {
		t.Fatalf("after the fallback: port 20000 %s, port 20001 %s", old, nw)
	}
}
