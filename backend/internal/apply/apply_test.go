package apply

import (
	"context"
	"testing"
	"time"

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
	apptest.Entitle(t, dsn, f.tenant)
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
	if f.advance(); f.sync(saved.ID) != "partially_applied" { // RUL-012：部分节点已生效
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
	if old != "occupied" || nw != "released" { // 恢复为上一已生效版本的端口；失败版本的端口在节点确认后释放
		t.Fatalf("after the fallback: port 20000 %s, port 20001 %s", old, nw)
	}
}

func (f *fixture) prepareAll(nodes ...p.UUID) {
	for _, n := range nodes {
		_ = f.svc.Prepared(context.Background(), n, uint64(f.config(n).Version), nil)
	}
}

func (f *fixture) applyAll(nodes ...p.UUID) {
	for _, n := range nodes {
		_ = f.svc.Applied(context.Background(), n, uint64(f.config(n).Version), true, "")
	}
}

func (f *fixture) expire(rule p.UUID) {
	apptest.Exec(f.t, f.dsn, "UPDATE apply_runs SET deadline_at = now() - interval '1 second' WHERE rule_id = $1 AND phase <> 'applied'", rule.PG())
}

func (f *fixture) count(sql string, args ...any) int {
	var n int
	apptest.QueryRow(f.t, f.dsn, sql, args, &n)
	return n
}

// 已生效的规则修改后提交只部分生效：partially_applied、重发提交；120 秒后以上一已生效版本的内容生成新版本回滚
// （rolling_back），回滚生效后 applied（RUL-012）。
func TestPartialApplyRollsBack(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	saved, _ := f.rules.Create(ctx, f.tenant, f.spec(20000))
	_, _ = f.svc.Deploy(ctx)
	f.prepareAll(f.e1, f.x1)
	f.advance()
	f.applyAll(f.e1, f.x1)
	f.advance()
	// 修改目标；两个节点都准备好、都下发提交，只有入口生效，出口提交失败
	spec := f.spec(0)
	spec.Targets = []compiler.Target{{Host: "198.51.100.8", Port: 443, Weight: 1}}
	up, err := f.rules.Update(ctx, f.tenant, saved.ID, saved.RowVersion, spec)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.svc.Deploy(ctx)
	f.expire(saved.ID) // 期限自进入提交起算：准备阶段用掉的时间不算
	f.prepareAll(f.e1, f.x1)
	if r := f.advance(); len(r.Committed) != 2 {
		t.Fatalf("commit %+v", r)
	}
	f.applyAll(f.e1)
	_ = f.svc.Applied(ctx, f.x1, uint64(f.config(f.x1).Version), false, "LISTEN_FAILED")
	old := CommitRetry
	CommitRetry = time.Hour
	defer func() { CommitRetry = old }()
	if r := f.advance(); f.sync(saved.ID) != "partially_applied" || len(r.Committed) != 0 {
		t.Fatalf("partial %s %+v", f.sync(saved.ID), r)
	}
	if f.count("SELECT count(*) FROM apply_node_states s JOIN apply_runs r ON r.id = s.run_id WHERE r.version = $1 AND s.status = 'commit_failed'",
		int64(up.Version)) != 1 {
		t.Fatal("commit failure not recorded")
	}
	// 重试收敛：间隔到了才重发同一提交
	CommitRetry = 0
	if r := f.advance(); r.Committed[f.x1] != uint64(f.config(f.x1).Version) || len(r.Committed) != 1 {
		t.Fatalf("retry %+v", r)
	}
	CommitRetry = time.Hour
	if r := f.advance(); len(r.Committed) != 0 || len(r.RolledBack) != 0 {
		t.Fatalf("retried within the interval, or rolled back before the deadline: %+v", r)
	}
	// 节点已有更新的期望配置：不再重发旧版本的提交
	apptest.Exec(t, f.dsn, "UPDATE node_configs SET version = version + 1 WHERE node_id = $1", f.x1.PG())
	CommitRetry = 0
	if r := f.advance(); len(r.Committed) != 0 {
		t.Fatalf("stale commit resent: %+v", r)
	}
	CommitRetry = time.Hour
	// 超时：回滚为新的递增版本，内容取上一已生效版本
	f.expire(saved.ID)
	r := f.advance()
	if len(r.RolledBack) != 1 || f.sync(saved.ID) != "rolling_back" {
		t.Fatalf("rollback %+v %s", r, f.sync(saved.ID))
	}
	cur, info, err := f.rules.Get(ctx, f.tenant, saved.ID)
	if err != nil || info.Version != up.Version+1 || cur.Targets[0].Host != "198.51.100.7" || cur.ServicePort != 20000 {
		t.Fatalf("rollback version %+v %+v %v", info, cur, err)
	}
	if f.count("SELECT count(*) FROM apply_runs WHERE rule_id = $1 AND version = $2 AND is_rollback", saved.ID.PG(), int64(info.Version)) != 1 {
		t.Fatal("rollback run not marked")
	}
	if f.count("SELECT count(*) FROM apply_runs WHERE rule_id = $1 AND version = $2 AND phase = 'failed'", saved.ID.PG(), int64(up.Version)) != 1 {
		t.Fatal("timed-out run not closed")
	}
	f.prepareAll(f.e1, f.x1)
	f.advance()
	if f.sync(saved.ID) != "rolling_back" {
		t.Fatalf("during the rollback commit %s", f.sync(saved.ID))
	}
	f.applyAll(f.e1, f.x1)
	if f.advance(); f.sync(saved.ID) != "applied" {
		t.Fatalf("after the rollback %s", f.sync(saved.ID))
	}
}

// 回滚自身超时：failed 并告警，不再自动回滚；新建的规则没有可回滚的版本：直接 failed 并告警（RUL-012）。
func TestRollbackFailureAndNothingToRollBackTo(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	fresh, _ := f.rules.Create(ctx, f.tenant, f.spec(20000))
	_, _ = f.svc.Deploy(ctx)
	f.prepareAll(f.e1, f.x1)
	f.advance()
	f.applyAll(f.e1)
	f.advance()
	f.expire(fresh.ID)
	if r := f.advance(); len(r.RolledBack) != 0 || f.sync(fresh.ID) != "failed" {
		t.Fatalf("new rule: %+v %s", r, f.sync(fresh.ID))
	}
	if f.count("SELECT count(*) FROM audit_logs WHERE action = 'rule.apply_failed' AND target_id = $1 AND detail->>'reason' LIKE '%no applied version%'",
		fresh.ID.String()) != 1 {
		t.Fatal("no alert")
	}
	// 另一条已生效的规则：修改后部分生效、回滚，回滚自身再次超时
	g := setup(t)
	f = g
	fresh, _ = f.rules.Create(ctx, f.tenant, f.spec(20000))
	_, _ = f.svc.Deploy(ctx)
	f.prepareAll(f.e1, f.x1)
	f.advance()
	f.applyAll(f.e1, f.x1)
	f.advance()
	_, info, _ := f.rules.Get(ctx, f.tenant, fresh.ID)
	if f.sync(fresh.ID) != "applied" {
		t.Fatalf("setup: %s", f.sync(fresh.ID))
	}
	spec := f.spec(0)
	spec.Targets = []compiler.Target{{Host: "198.51.100.9", Port: 443, Weight: 1}}
	if _, err := f.rules.Update(ctx, f.tenant, fresh.ID, f.rowVersion(fresh.ID), spec); err != nil {
		t.Fatal(err)
	}
	_, _ = f.svc.Deploy(ctx)
	f.prepareAll(f.e1, f.x1)
	f.advance()
	f.applyAll(f.e1)
	f.expire(fresh.ID)
	if r := f.advance(); len(r.RolledBack) != 1 {
		t.Fatalf("rollback %+v", r)
	}
	f.prepareAll(f.e1, f.x1)
	f.advance()
	f.applyAll(f.e1)
	f.expire(fresh.ID)
	if r := f.advance(); len(r.RolledBack) != 0 || f.sync(fresh.ID) != "failed" {
		t.Fatalf("rollback timed out: %+v %s", r, f.sync(fresh.ID))
	}
	if f.count("SELECT count(*) FROM audit_logs WHERE action = 'rule.apply_failed' AND target_id = $1", fresh.ID.String()) != 1 {
		t.Fatal("no alert for the failed rollback")
	}
	_, after, _ := f.rules.Get(ctx, f.tenant, fresh.ID)
	if after.Version != info.Version+2 {
		t.Fatalf("versions %d → %d: a failed rollback must not roll back again", info.Version, after.Version)
	}
}

func (f *fixture) rowVersion(rule p.UUID) int64 {
	var v int64
	apptest.QueryRow(f.t, f.dsn, "SELECT row_version FROM rules WHERE id = $1", []any{rule.PG()}, &v)
	return v
}

// 失联节点（RUL-013）：未生效的节点全部失联时 unknown，不回滚、不报成功；恢复并生效后 applied。
func TestUnknownWhileNodeLost(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	saved, _ := f.rules.Create(ctx, f.tenant, f.spec(20000))
	_, _ = f.svc.Deploy(ctx)
	f.prepareAll(f.e1, f.x1)
	f.advance()
	// 提交后两个节点都失联、都未生效：unknown，期限到了也不回滚
	apptest.Exec(t, f.dsn, "UPDATE nodes SET lifecycle = 'offline'")
	f.expire(saved.ID)
	if r := f.advance(); len(r.RolledBack) != 0 || f.sync(saved.ID) != "unknown" {
		t.Fatalf("all lost: %+v %s", r, f.sync(saved.ID))
	}
	apptest.Exec(t, f.dsn, "UPDATE nodes SET lifecycle = 'online' WHERE id = $1", f.e1.PG())
	f.applyAll(f.e1)
	apptest.Exec(t, f.dsn, "UPDATE nodes SET lifecycle = 'offline' WHERE id = $1", f.x1.PG())
	if f.advance(); f.sync(saved.ID) != "unknown" {
		t.Fatalf("lost node: %s", f.sync(saved.ID))
	}
	f.expire(saved.ID)
	if r := f.advance(); len(r.RolledBack) != 0 || f.sync(saved.ID) != "unknown" {
		t.Fatalf("after the deadline: %+v %s", r, f.sync(saved.ID))
	}
	if f.count("SELECT count(*) FROM apply_node_states WHERE node_id = $1 AND status = 'unknown'", f.x1.PG()) != 1 {
		t.Fatal("node state not unknown")
	}
	apptest.Exec(t, f.dsn, "UPDATE nodes SET lifecycle = 'online' WHERE id = $1", f.x1.PG())
	f.applyAll(f.x1)
	if f.advance(); f.sync(saved.ID) != "applied" {
		t.Fatalf("after recovery: %s", f.sync(saved.ID))
	}
}

// 入口组成员变化（PRT-007）：新成员补预留同一端口，退出成员的预留转 releasing。
func TestEntryMembersFollowPorts(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	saved, _ := f.rules.Create(ctx, f.tenant, f.spec(20000))
	e2 := p.NewUUIDv7()
	apptest.Exec(t, f.dsn, "INSERT INTO nodes (id, name, region, can_entry, public_addresses, lifecycle) VALUES ($1, 'e2', 'eu', true, '{203.0.113.3}', 'online')", e2.PG())
	apptest.Exec(t, f.dsn, "INSERT INTO node_group_members (node_group_id, node_id) VALUES ($1, $2)", f.entry.PG(), e2.PG())
	if _, err := f.svc.Deploy(ctx); err != nil {
		t.Fatal(err)
	}
	if f.count("SELECT count(*) FROM port_allocations WHERE rule_id = $1 AND node_id = $2 AND port = 20000 AND state = 'reserved'", saved.ID.PG(), e2.PG()) != 1 {
		t.Fatal("new member has no port")
	}
	apptest.Exec(t, f.dsn, "DELETE FROM node_group_members WHERE node_group_id = $1 AND node_id = $2", f.entry.PG(), f.e1.PG())
	if _, err := f.svc.Deploy(ctx); err != nil {
		t.Fatal(err)
	}
	if f.count("SELECT count(*) FROM port_allocations WHERE rule_id = $1 AND node_id = $2 AND state = 'releasing'", saved.ID.PG(), f.e1.PG()) != 1 {
		t.Fatal("departed member's port not releasing")
	}
	// 新成员上端口已被占用：不静默排除，告警（该成员的准备将失败）
	e3 := p.NewUUIDv7()
	apptest.Exec(t, f.dsn, "INSERT INTO nodes (id, name, region, can_entry, public_addresses, lifecycle) VALUES ($1, 'e3', 'eu', true, '{203.0.113.4}', 'online')", e3.PG())
	apptest.Exec(t, f.dsn, "INSERT INTO node_port_ranges (node_id, kind, port_min, port_max) VALUES ($1, 'service', 30000, 30010)", e3.PG())
	apptest.Exec(t, f.dsn, "INSERT INTO node_group_members (node_group_id, node_id) VALUES ($1, $2)", f.entry.PG(), e3.PG())
	if _, err := f.svc.Deploy(ctx); err != nil {
		t.Fatalf("deploy must go on: %v", err)
	}
	if f.count("SELECT count(*) FROM port_allocations WHERE rule_id = $1 AND node_id = $2", saved.ID.PG(), e3.PG()) != 0 {
		t.Fatal("port reserved outside the member's range")
	}
}

// 回滚按当前权限重新校验（RUL-016）：上一已生效版本已不合法时不回滚，failed 并告警；回滚的准备失败同样告警。
func TestRollbackRevalidates(t *testing.T) {
	for _, mode := range []string{"invalid", "prepare_failed"} {
		t.Run(mode, func(t *testing.T) {
			f := setup(t)
			ctx := context.Background()
			saved, _ := f.rules.Create(ctx, f.tenant, f.spec(20000))
			_, _ = f.svc.Deploy(ctx)
			f.prepareAll(f.e1, f.x1)
			f.advance()
			f.applyAll(f.e1, f.x1)
			f.advance()
			spec := f.spec(0)
			spec.Targets = []compiler.Target{{Host: "198.51.100.8", Port: 443, Weight: 1}}
			if _, err := f.rules.Update(ctx, f.tenant, saved.ID, saved.RowVersion, spec); err != nil {
				t.Fatal(err)
			}
			_, _ = f.svc.Deploy(ctx)
			f.prepareAll(f.e1, f.x1)
			f.advance()
			f.applyAll(f.e1)
			f.expire(saved.ID)
			if mode == "invalid" {
				apptest.Exec(t, f.dsn, "DELETE FROM segment_permits") // 段许可已撤销
				if r := f.advance(); len(r.RolledBack) != 0 || f.sync(saved.ID) != "failed" {
					t.Fatalf("rollback of an invalid version: %+v %s", r, f.sync(saved.ID))
				}
			} else {
				if r := f.advance(); len(r.RolledBack) != 1 {
					t.Fatalf("rollback %+v", r)
				}
				_ = f.svc.Prepared(ctx, f.x1, uint64(f.config(f.x1).Version), []string{"PORT_IN_USE"})
				f.prepareAll(f.e1)
				if f.advance(); f.sync(saved.ID) != "failed" {
					t.Fatalf("rollback prepare failure: %s", f.sync(saved.ID))
				}
			}
			if f.count("SELECT count(*) FROM audit_logs WHERE action = 'rule.apply_failed' AND target_id = $1", saved.ID.String()) != 1 {
				t.Fatal("no alert")
			}
		})
	}
}

// 自动分配端口的规则：版本快照记录实际端口，准备失败回到上一已生效版本时恢复该端口（而不是失败版本的端口）。
func TestFallbackRestoresAutoPort(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	saved, _ := f.rules.Create(ctx, f.tenant, f.spec(0))
	var auto int
	apptest.QueryRow(t, f.dsn, "SELECT service_port FROM rules WHERE id = $1", []any{saved.ID.PG()}, &auto)
	_, _ = f.svc.Deploy(ctx)
	f.prepareAll(f.e1, f.x1)
	f.advance()
	f.applyAll(f.e1, f.x1)
	f.advance()
	if _, err := f.rules.Update(ctx, f.tenant, saved.ID, saved.RowVersion, f.spec(20001)); err != nil {
		t.Fatal(err)
	}
	_, _ = f.svc.Deploy(ctx)
	_ = f.svc.Prepared(ctx, f.e1, uint64(f.config(f.e1).Version), []string{"PORT_IN_USE: 20001"})
	f.advance() // 失败 → 重新部署回到上一已生效版本
	if got := f.count("SELECT count(*) FROM port_allocations WHERE rule_id = $1 AND port = $2 AND state IN ('reserved', 'occupied')", saved.ID.PG(), int32(auto)); got != 1 {
		t.Fatalf("auto port %d not restored", auto)
	}
	if got := f.count("SELECT count(*) FROM port_allocations WHERE rule_id = $1 AND port = 20001 AND state = 'releasing'", saved.ID.PG()); got != 1 {
		t.Fatal("failed version's port not releasing")
	}
}
