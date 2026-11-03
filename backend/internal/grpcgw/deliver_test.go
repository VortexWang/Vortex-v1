package grpcgw

import (
	"context"
	"testing"
	"time"

	"vortex/backend/apptest"
	"vortex/backend/internal/apply"
	"vortex/backend/internal/fleet"
	p "vortex/backend/internal/platform"
	backendv1 "vortex/proto/gen/go/vortex/backend/v1"
	commonv1 "vortex/proto/gen/go/vortex/common/v1"
)

// 投递（BE-T11）：Outbox 中的期望配置与提交按节点所在实例投递；实例注册时补发所辖节点的最新配置与未生效的提交；
// 节点接入（状态报告）时把配置发给它当前所在的实例。
func TestDelivery(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	hub, cert := e.enrollHub("eu")
	tok, _ := e.fl.CreateEnrollToken(ctx, p.UUID{}, e.admin)
	_, csr := keyCSR(t)
	ne, err := e.fl.EnrollNode(ctx, hub, tok, csr, fleet.Caps{})
	if err != nil {
		t.Fatal(err)
	}
	// 让节点有一份配置：没有规则的空配置
	ap := &apply.Service{DB: e.fl.DB}
	gw := &Server{Fleet: e.fl, Apply: ap}
	ap.Notify = gw.Wake
	if res, err := ap.Deploy(ctx); err != nil || res.Changed[ne.NodeID] != 1 {
		t.Fatalf("deploy %+v %v", res, err)
	}
	isDesired := func(m *backendv1.BackendMessage) bool {
		d := m.GetDesired()
		return d != nil && string(d.NodeId.Value) == string(ne.NodeID[:]) && d.Config.GetVersion() == 1
	}
	isCommit := func(m *backendv1.BackendMessage) bool {
		c := m.GetCommand()
		return c != nil && string(c.NodeId.Value) == string(ne.NodeID[:]) && c.Command.GetVersion() == 1
	}
	// 实例在线：Outbox 投递
	got := make(chan *backendv1.BackendMessage, 64)
	conn := gw.attach(hub, func(m *backendv1.BackendMessage) { got <- m })
	if err := gw.deliver(ctx); err != nil {
		t.Fatal(err)
	}
	wait := func(what string, f func(*backendv1.BackendMessage) bool) {
		t.Helper()
		deadline := time.After(2 * time.Second)
		for {
			select {
			case m := <-got:
				if f(m) {
					return
				}
			case <-deadline:
				t.Fatalf("timeout: %s", what)
			}
		}
	}
	wait("desired config from the outbox", isDesired)
	var unpublished int
	apptest.QueryRow(t, e.dsn, "SELECT count(*) FROM outbox WHERE published_at IS NULL", nil, &unpublished)
	if unpublished != 0 {
		t.Fatal("outbox not marked published")
	}
	_ = ap.Prepared(ctx, ne.NodeID, 1, nil)
	if _, err := ap.Advance(ctx); err != nil {
		t.Fatal(err)
	}
	_ = gw.deliver(ctx)
	wait("commit from the outbox", isCommit)
	gw.detach(hub, conn)

	// 实例重连注册：补发最新配置与未生效的提交
	s := e.connect(cert)
	s.send(&backendv1.HubUpMessage{Body: &backendv1.HubUpMessage_Register{Register: &backendv1.HubRegister{Region: "eu", Mode: commonv1.HubMode_HUB_MODE_ONLINE}}})
	s.expect("desired config on register", isDesired)
	s.expect("pending commit on register", isCommit)
	// 节点状态报告（接入）：再发一次给当前实例
	s.send(&backendv1.HubUpMessage{Body: &backendv1.HubUpMessage_NodeStatus{NodeStatus: &backendv1.NodeStatusBatch{Nodes: []*backendv1.NodeStatus{
		{NodeId: uuidMsg(ne.NodeID), Connected: true}}}}})
	s.expect("desired config on node connect", isDesired)
}
