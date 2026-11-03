package backendgrpc

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"vortex/edgehub/issuer"
	"vortex/edgehub/model"
	"vortex/edgehub/session"
	"vortex/edgehub/upstream"
	"vortex/edgehub/wire"
	backendv1 "vortex/proto/gen/go/vortex/backend/v1"
	commonv1 "vortex/proto/gen/go/vortex/common/v1"
	edgehubv1 "vortex/proto/gen/go/vortex/edgehub/v1"
	runtimev1 "vortex/proto/gen/go/vortex/runtime/v1"
)

func TestDown(t *testing.T) {
	n := model.UUID{1}
	exp := time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		in   *backendv1.BackendMessage
		want func(upstream.Down) bool
	}{
		{&backendv1.BackendMessage{Body: &backendv1.BackendMessage_Desired{Desired: &backendv1.NodeDesiredConfig{NodeId: wire.UUID(n), Config: &runtimev1.RuntimeConfig{Version: 7}}}},
			func(d upstream.Down) bool {
				x := d.(upstream.Desired)
				var c runtimev1.RuntimeConfig
				return x.Version == 7 && x.Node == n && proto.Unmarshal(x.Config, &c) == nil && c.Version == 7
			}},
		{&backendv1.BackendMessage{Body: &backendv1.BackendMessage_Command{Command: &backendv1.NodeApplyCommand{NodeId: wire.UUID(n),
			Command: &edgehubv1.ApplyCommand{Version: 7, Action: edgehubv1.ApplyAction_APPLY_ACTION_ABORT}}}},
			func(d upstream.Down) bool {
				return d.(upstream.Command) == upstream.Command{Node: n, Version: 7, Commit: false}
			}},
		{&backendv1.BackendMessage{Body: &backendv1.BackendMessage_Restrictions{Restrictions: &backendv1.RestrictionSnapshot{SnapshotVersion: 3,
			Tenants: []*backendv1.TenantRestriction{{TenantId: wire.UUID(n), AccountStatus: backendv1.AccountStatus_ACCOUNT_STATUS_BANNED,
				PlanExpiresAt: timestamppb.New(exp), TrafficExhausted: true}}}}},
			func(d upstream.Down) bool {
				r := d.(upstream.Restrictions)
				return r.Version == 3 && r.Tenants[0].Status == issuer.AccountBanned && r.Tenants[0].PlanExpiresAt.Equal(exp) && r.Tenants[0].TrafficExhausted
			}},
		{&backendv1.BackendMessage{Body: &backendv1.BackendMessage_Budget{Budget: &backendv1.BudgetUpdate{TenantId: wire.UUID(n), BudgetVersion: 4, Delegated: 9, SettledAckedTotal: 2}}},
			func(d upstream.Down) bool {
				return d.(upstream.Budget) == upstream.Budget{Tenant: n, Version: 4, Delegated: 9, Ack: 2}
			}},
		{&backendv1.BackendMessage{Body: &backendv1.BackendMessage_Leases{Leases: &backendv1.ShareLeaseUpdate{NodeId: wire.UUID(n),
			Leases: []*runtimev1.ShareLease{{Meta: &runtimev1.GrantMeta{GrantId: wire.UUID(n), Ttl: durationpb.New(time.Minute)}, TenantId: wire.UUID(n),
				Kind: runtimev1.ShareKind_SHARE_KIND_CONN, Amount: 5}}}}},
			func(d upstream.Down) bool {
				l := d.(upstream.Leases).Leases[0]
				return l.Kind == model.GrantShareConn && l.Amount == 5 && l.TTL == time.Minute && l.ID == n
			}},
		{&backendv1.BackendMessage{Body: &backendv1.BackendMessage_Reconcile{Reconcile: &backendv1.ReconcileDirective{Phase: backendv1.ReconcilePhase_RECONCILE_PHASE_STARTED,
			NewGeneration: 3, BudgetRevokedTenantIds: []*commonv1.Uuid{wire.UUID(n)}}}},
			func(d upstream.Down) bool {
				r := d.(upstream.Reconcile)
				return r.Phase == upstream.ReconcileStarted && r.NewGeneration == 3 && r.RevokedTenants[0] == n
			}},
		{&backendv1.BackendMessage{Body: &backendv1.BackendMessage_NodeOperation{NodeOperation: &backendv1.NodeOperation{NodeId: wire.UUID(n),
			Op: &backendv1.NodeOperation_Upgrade{Upgrade: &edgehubv1.UpgradeCommand{Version: "1.2.0", Sha256: []byte{1}, IrreversibleMigration: true}}}}},
			func(d upstream.Down) bool {
				u := d.(upstream.Upgrade)
				var c edgehubv1.UpgradeCommand
				return u.Node == n && proto.Unmarshal(u.Payload, &c) == nil && c.Version == "1.2.0" && c.IrreversibleMigration
			}},
	}
	for i, c := range cases {
		d, err := Down(c.in)
		if err != nil || !c.want(d) {
			t.Fatalf("case %d: %+v %v", i, d, err)
		}
	}
	if _, err := Down(&backendv1.BackendMessage{Body: &backendv1.BackendMessage_Reconcile{Reconcile: &backendv1.ReconcileDirective{}}}); err == nil {
		t.Fatal("unspecified reconcile phase accepted")
	}
	if _, err := Down(&backendv1.BackendMessage{}); err == nil {
		t.Fatal("empty message accepted")
	}
}

func TestUpLedger(t *testing.T) {
	m, err := Up(upstream.LedgerEntries{Entries: []upstream.LedgerEntry{
		{Seq: 3, Issued: &issuer.Grant{ID: model.UUID{1}, Kind: model.GrantBilling, RawBytes: 8, Reserved: 12, CoefMilli: 1500}},
		{Seq: 5, Transferred: &issuer.TransferRecord{Node: model.UUID{2}, Revoked: []model.UUID{{3}}}},
	}}, "r", "v", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	es := m.GetLedger().Entries
	if es[0].Seq != 3 || es[0].GetGrantIssued().ReservedBillingBytes != 12 || es[1].GetGrantTransferred().RevokedGrantIds[0].Value[0] != 3 {
		t.Fatalf("%+v", es)
	}
	if _, err := Up(upstream.LedgerEntries{Entries: []upstream.LedgerEntry{{Seq: 1}}}, "", "", time.Now()); err == nil {
		t.Fatal("empty entry accepted")
	}
}

// 节点的升级进度原样转给后端（AGT-008，C2 变更 CC-01）。
func TestUpUpgradeStatus(t *testing.T) {
	raw, _ := proto.Marshal(&edgehubv1.UpgradeStatus{TargetVersion: "2.0.0", Phase: edgehubv1.UpgradePhase_UPGRADE_PHASE_ROLLED_BACK, Error: "x"})
	m, err := Up(upstream.NodeForward{Node: model.UUID{4}, Msg: session.Passthrough{Kind: "upgrade", Payload: raw}}, "", "", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	u := m.GetNodeUpgrade()
	if u.GetNodeId().GetValue()[0] != 4 || u.GetStatus().GetTargetVersion() != "2.0.0" || u.GetStatus().GetPhase() != edgehubv1.UpgradePhase_UPGRADE_PHASE_ROLLED_BACK {
		t.Fatalf("%+v", m)
	}
	if _, err := Up(upstream.NodeForward{Node: model.UUID{4}, Msg: session.Passthrough{Kind: "upgrade", Payload: []byte{0xff, 0xff}}}, "", "", time.Now()); err == nil {
		t.Fatal("corrupt upgrade status accepted")
	}
}

func TestUpBudgetRequest(t *testing.T) {
	m, err := Up(upstream.BudgetRequest{Tenant: model.UUID{7}, RateBps: 12345}, "", "", time.Now())
	if err != nil || m.GetBudgetRequest().GetRateBillingBps_1H() != 12345 || m.GetBudgetRequest().GetTenantId().GetValue()[0] != 7 {
		t.Fatalf("%v %v", m, err)
	}
}
