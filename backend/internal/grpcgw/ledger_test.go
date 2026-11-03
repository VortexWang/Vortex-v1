package grpcgw

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"vortex/backend/apptest"
	"vortex/backend/internal/fleet"
	"vortex/backend/internal/leases"
	"vortex/backend/internal/metering"
	"vortex/backend/internal/platform"
	backendv1 "vortex/proto/gen/go/vortex/backend/v1"
	commonv1 "vortex/proto/gen/go/vortex/common/v1"
	runtimev1 "vortex/proto/gen/go/vortex/runtime/v1"
)

// 账本条目首次落库时在同一事务内入账（BE-T18）：结算后下发区域额度确认；重发的条目只确认、不重复入账；
// 余额耗尽时立即广播限制快照。
func TestLedgerSettlement(t *testing.T) {
	e := setup(t)
	ls := &leases.Service{DB: e.fl.DB, Hubs: e.gw}
	e.gw.Leases, e.gw.Metering = ls, &metering.Service{Budgets: ls}
	hub, cert := e.enrollHub("eu")
	tenant := apptest.Tenant(t, e.dsn)
	plan := platform.NewUUIDv7()
	apptest.Exec(t, e.dsn, `INSERT INTO plans (id, name, duration_days, base_traffic_bytes, traffic_reset, max_rules, rate_bps, max_conns)
		VALUES ($1, 'p', 30, 1000, 'none', 10, 1000, 10)`, plan.PG())
	apptest.Exec(t, e.dsn, `INSERT INTO subscriptions (tenant_id, plan_id, started_at, expires_at, base_period_end, base_remaining_bytes)
		VALUES ($1, $2, now() - interval '1 day', now() + interval '9 days', now() + interval '9 days', 1000)`, tenant.PG(), plan.PG())
	apptest.Exec(t, e.dsn, "INSERT INTO regional_budgets (tenant_id, hub_id, delegated, budget_version) VALUES ($1, $2, 1000, 3)", tenant.PG(), hub.PG())
	// 本实例管理该租户的一个在线执行点
	tok, _ := e.fl.CreateEnrollToken(context.Background(), platform.UUID{}, e.admin)
	_, csr := keyCSR(t)
	ne, err := e.fl.EnrollNode(context.Background(), hub, tok, csr, fleet.Caps{Entry: true})
	if err != nil {
		t.Fatal(err)
	}
	cfg, _ := proto.Marshal(&runtimev1.RuntimeConfig{Rules: []*runtimev1.Rule{{RuleId: uuidMsg(platform.NewUUIDv7()), TenantId: uuidMsg(tenant), EnforcementPoint: true}}})
	apptest.Exec(t, e.dsn, "UPDATE nodes SET lifecycle = 'online', current_boot_id = $2 WHERE id = $1", ne.NodeID.PG(), platform.NewUUIDv7().PG())
	apptest.Exec(t, e.dsn, `INSERT INTO node_configs (node_id, version, config, content_digest) VALUES ($1, 1, $2, '\x00')`, ne.NodeID.PG(), cfg)

	s := e.connect(cert)
	s.send(&backendv1.HubUpMessage{Body: &backendv1.HubUpMessage_Register{Register: &backendv1.HubRegister{Region: "eu", Mode: commonv1.HubMode_HUB_MODE_ONLINE}}})
	s.expect("restrictions", func(m *backendv1.BackendMessage) bool { return m.GetRestrictions() != nil })
	settled := func(seq, n uint64) *backendv1.HubUpMessage {
		g := platform.NewUUIDv7()
		return &backendv1.HubUpMessage{Body: &backendv1.HubUpMessage_Ledger{Ledger: &backendv1.LedgerEntries{Entries: []*backendv1.LedgerEntry{{Seq: seq,
			Body: &backendv1.LedgerEntry_GrantSettled{GrantSettled: &backendv1.GrantSettled{GrantId: uuidMsg(g), TenantId: uuidMsg(tenant),
				UsedRawBytes: n, SettledBillingBytes: n, Final: true}}}}}}}
	}
	first := settled(1, 400)
	s.send(first)
	b := s.expect("budget after settlement", func(m *backendv1.BackendMessage) bool { return m.GetBudget().GetSettledAckedTotal() == 400 })
	if b.GetBudget().Delegated != 600 || b.GetBudget().BudgetVersion != 5 { // 注册时下发 v4，结算 v5
		t.Fatalf("budget %v", b.GetBudget())
	}
	s.send(first) // 重发：只确认
	s.expect("ack", func(m *backendv1.BackendMessage) bool { return m.GetLedgerAck().GetAckedThroughSeq() == 1 })
	var base int64
	apptest.QueryRow(t, e.dsn, "SELECT base_remaining_bytes FROM subscriptions WHERE tenant_id = $1", []any{tenant.PG()}, &base)
	if base != 600 {
		t.Fatalf("base %d after a duplicate entry", base)
	}
	// 余额耗尽：限制快照立即广播
	s.send(settled(2, 600))
	s.expect("traffic exhausted", func(m *backendv1.BackendMessage) bool {
		for _, tr := range m.GetRestrictions().GetTenants() {
			if platform.UUID(tr.GetTenantId().GetValue()) == tenant && tr.TrafficExhausted {
				return true
			}
		}
		return false
	})
}

// HUB-015：控制流断开即冻结（被同一实例的新流取代时不算失联）；HUB-017：恢复联系的对账 STARTED 带上冻结期间被回收的租户。
func TestLostHubBudgets(t *testing.T) {
	e := setup(t)
	ls := &leases.Service{DB: e.fl.DB, Hubs: e.gw}
	e.gw.Leases, e.gw.Metering = ls, &metering.Service{Budgets: ls}
	ctx := context.Background()
	hub, cert := e.enrollHub("eu")
	tenant := apptest.Tenant(t, e.dsn)
	apptest.Exec(t, e.dsn, "INSERT INTO regional_budgets (tenant_id, hub_id, delegated, budget_version) VALUES ($1, $2, 500, 1)", tenant.PG(), hub.PG())
	row := func() string {
		var s string
		apptest.QueryRow(t, e.dsn, "SELECT h.mode || ':' || rb.frozen FROM regional_budgets rb JOIN edge_hubs h ON h.id = rb.hub_id WHERE rb.hub_id = $1",
			[]any{hub.PG()}, &s)
		return s
	}
	wait := func(want string) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for row() != want {
			if time.Now().After(deadline) {
				t.Fatalf("got %s, want %s", row(), want)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	register := func(s *stream, mode commonv1.HubMode) {
		s.send(&backendv1.HubUpMessage{Body: &backendv1.HubUpMessage_Register{Register: &backendv1.HubRegister{Region: "eu", Mode: mode}}})
		s.expect("restrictions", func(m *backendv1.BackendMessage) bool { return m.GetRestrictions() != nil })
	}
	s1 := e.connect(cert)
	register(s1, commonv1.HubMode_HUB_MODE_ONLINE)
	s2 := e.connect(cert) // 同一实例的新流取代旧流
	register(s2, commonv1.HubMode_HUB_MODE_ONLINE)
	_ = s1.s.CloseSend()
	time.Sleep(300 * time.Millisecond)
	if row() != "online:false" {
		t.Fatalf("superseded stream marked the hub lost: %s", row())
	}
	_ = s2.s.CloseSend()
	wait("offline:true")

	// 注册时没有执行点，额度已被收回；模拟失联时仍持有委托
	apptest.Exec(t, e.dsn, "UPDATE regional_budgets SET delegated = 500 WHERE hub_id = $1", hub.PG())
	apptest.Exec(t, e.dsn, "UPDATE edge_hubs SET last_contact_at = now() - interval '13 hours' WHERE id = $1", hub.PG())
	if err := ls.Reclaim(ctx); err != nil {
		t.Fatal(err)
	}
	s3 := e.connect(cert)
	register(s3, commonv1.HubMode_HUB_MODE_RECONCILING)
	m := s3.expect("STARTED", func(m *backendv1.BackendMessage) bool {
		return m.GetReconcile().GetPhase() == backendv1.ReconcilePhase_RECONCILE_PHASE_STARTED
	})
	ids := m.GetReconcile().GetBudgetRevokedTenantIds()
	if len(ids) != 1 || platform.UUID(ids[0].GetValue()) != tenant {
		t.Fatalf("revoked tenants %v", ids)
	}
	if row() != "reconciling:false" {
		t.Fatalf("after reconnect %s", row())
	}
}
