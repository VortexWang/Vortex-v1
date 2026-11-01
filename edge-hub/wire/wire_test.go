package wire

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"vortex/edgehub/issuer"
	"vortex/edgehub/leases"
	"vortex/edgehub/model"
	"vortex/edgehub/session"
	commonv1 "vortex/proto/gen/go/vortex/common/v1"
	edgehubv1 "vortex/proto/gen/go/vortex/edgehub/v1"
	runtimev1 "vortex/proto/gen/go/vortex/runtime/v1"
)

var id = model.UUID{7}

func am(b *edgehubv1.AgentMessage) session.AgentMsg {
	m, err := FromAgent(b)
	if err != nil {
		panic(err)
	}
	return m
}

func TestFromAgent(t *testing.T) {
	ok := am(&edgehubv1.AgentMessage{Body: &edgehubv1.AgentMessage_ApplyResult{ApplyResult: &edgehubv1.ApplyReport{
		Action: edgehubv1.ApplyAction_APPLY_ACTION_COMMIT, Result: &runtimev1.ApplyResult{Version: 3}}}}).(session.ApplyReport)
	if !ok.Committed || ok.Version != 3 || len(ok.Payload) == 0 {
		t.Fatalf("commit %+v", ok)
	}
	ab := am(&edgehubv1.AgentMessage{Body: &edgehubv1.AgentMessage_ApplyResult{ApplyResult: &edgehubv1.ApplyReport{
		Action: edgehubv1.ApplyAction_APPLY_ACTION_ABORT, Result: &runtimev1.ApplyResult{Version: 3}}}}).(session.ApplyReport)
	if ab.Committed {
		t.Fatal("abort counted as committed")
	}
	fail := am(&edgehubv1.AgentMessage{Body: &edgehubv1.AgentMessage_ApplyResult{ApplyResult: &edgehubv1.ApplyReport{
		Action: edgehubv1.ApplyAction_APPLY_ACTION_COMMIT, Result: &runtimev1.ApplyResult{Version: 3, Objects: []*runtimev1.ObjectState{
			{Object: &runtimev1.ObjectRef{}, Status: runtimev1.ObjectStatus_OBJECT_STATUS_FAILED, ErrorCode: "ENGINE_ERROR: x"}}}}}}).(session.ApplyReport)
	if fail.Committed {
		t.Fatal("engine error counted as committed")
	}
	pr := am(&edgehubv1.AgentMessage{Body: &edgehubv1.AgentMessage_PrepareResult{PrepareResult: &edgehubv1.PrepareReport{
		Result: &runtimev1.PrepareResult{Version: 4, Errors: []*runtimev1.ResourceError{{Code: "PORT_IN_USE"}}}}}}).(session.PrepareReport)
	if pr.OK || pr.Version != 4 {
		t.Fatalf("prepare %+v", pr)
	}
	u := am(&edgehubv1.AgentMessage{Body: &edgehubv1.AgentMessage_Usage{Usage: &edgehubv1.UsageBatch{BootId: UUID(id), Seq: 2,
		Snapshot: &runtimev1.UsageSnapshot{GrantConsumptions: []*runtimev1.GrantConsumption{{GrantId: UUID(id), UsedRawBytes: 5, Final: true}},
			Demands: []*runtimev1.TenantDemand{{TenantId: UUID(id), Bytes_60S: 9, StarvedPermille: 3}}}}}}).(session.UsageBatch)
	if u.Boot != id || u.Seq != 2 || u.Consumptions[0] != (session.Consumption{Grant: id, Used: 5, Final: true}) || u.Demands[0].Bytes60s != 9 {
		t.Fatalf("usage %+v", u)
	}
	var snap runtimev1.UsageSnapshot
	if err := proto.Unmarshal(u.Snapshot, &snap); err != nil || len(snap.GrantConsumptions) != 1 {
		t.Fatal("snapshot not forwarded intact")
	}
	for _, bad := range []*edgehubv1.AgentMessage{
		{},
		{Body: &edgehubv1.AgentMessage_Hello{Hello: &edgehubv1.AgentHello{NodeId: UUID(id)}}}, // 无 boot_id
		{Body: &edgehubv1.AgentMessage_Usage{Usage: &edgehubv1.UsageBatch{BootId: UUID(id)}}}, // seq 0
		{Body: &edgehubv1.AgentMessage_GrantRequest{GrantRequest: &edgehubv1.GrantRequest{RuleId: &commonv1.Uuid{Value: []byte{1}}}}},
	} {
		if _, err := FromAgent(bad); err == nil {
			t.Fatalf("accepted %v", bad)
		}
	}
}

func TestToAgent(t *testing.T) {
	now := time.Now()
	m, err := ToAgent(id, "r", session.GrantResponse{RequestID: 5,
		Run:     []issuer.Grant{{ID: id, Boot: id, Rules: []model.UUID{id}, TTL: 5 * time.Minute, IssuedAt: now}},
		Billing: []issuer.Grant{{ID: id, Boot: id, Rule: id, RawBytes: 8, BillingVersion: 2, TTL: time.Minute}},
		Shares:  []leases.Lease{{ID: id, Boot: id, Tenant: id, Kind: model.GrantShareConn, Amount: 3, TTL: time.Minute}}})
	if err != nil {
		t.Fatal(err)
	}
	g := m.GetGrants().GetGrants()
	if len(g.RunGrants) != 1 || g.RunGrants[0].Meta.IssuerHubId.Value[0] != 7 || g.RunGrants[0].Meta.Ttl.AsDuration() != 5*time.Minute ||
		g.ShareLeases[0].Kind != runtimev1.ShareKind_SHARE_KIND_CONN || g.BillingGrants[0].RawBytes != 8 {
		t.Fatalf("grants %+v", g)
	}
	if _, err := ToAgent(id, "r", session.DesiredConfig{Version: 1, Payload: []byte{0xff, 0xff}}); err == nil {
		t.Fatal("corrupt config payload accepted")
	}
	h, _ := ToAgent(id, "r", session.HubHello{Hub: id, Mode: model.ModeAutonomous})
	if h.GetHello().GetMode() != commonv1.HubMode_HUB_MODE_AUTONOMOUS || h.GetHello().GetRegion() != "r" {
		t.Fatal("hello")
	}
}

func TestRulesFromConfig(t *testing.T) {
	b, _ := proto.Marshal(&runtimev1.RuntimeConfig{Rules: []*runtimev1.Rule{{RuleId: UUID(id), TenantId: UUID(model.UUID{8}), EnforcementPoint: true, PathCoefficientMilli: 1500}}})
	rs, err := RulesFromConfig(b)
	if err != nil || len(rs) != 1 || rs[0] != (issuer.RuleState{RuleID: id, TenantID: model.UUID{8}, EnforcementPoint: true, PathCoefMilli: 1500}) {
		t.Fatalf("%+v %v", rs, err)
	}
	if _, err := RulesFromConfig([]byte{0xff}); err == nil {
		t.Fatal("corrupt payload")
	}
}
