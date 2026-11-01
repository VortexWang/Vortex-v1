package wire

import (
	"math/rand"
	"net/netip"
	"reflect"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"vortex/engine/types"
	"vortex/node/agent"
	"vortex/node/agent/grants"
	"vortex/node/agent/probe"
	"vortex/node/agent/reconcile"
	commonv1 "vortex/proto/gen/go/vortex/common/v1"
	edgehubv1 "vortex/proto/gen/go/vortex/edgehub/v1"
	runtimev1 "vortex/proto/gen/go/vortex/runtime/v1"
)

func rid(r *rand.Rand) types.UUID {
	var u types.UUID
	r.Read(u[:])
	return u
}

func rids(r *rand.Rand) []types.UUID {
	n := r.Intn(3)
	if n == 0 {
		return nil
	}
	out := make([]types.UUID, n)
	for i := range out {
		out[i] = rid(r)
	}
	return out
}

func randConfig(r *rand.Rand) *types.RuntimeConfig {
	c := &types.RuntimeConfig{Version: r.Uint64(), NodeID: rid(r), Generation: r.Uint64(),
		Revocations:  types.RevocationList{Serial: r.Uint64(), NodeIDs: rids(r)},
		TargetPolicy: types.TargetPolicy{SelfHostedNode: r.Intn(2) == 0}}
	if r.Intn(2) == 0 {
		c.Revocations.CertSerials = [][]byte{{1, 2, 3}}
		c.TargetPolicy.DenyPrefixes = []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8"), netip.MustParsePrefix("fd00::/8")}
		c.TargetPolicy.DenyPorts = []uint16{25}
		c.TargetPolicy.AllowPrivatePrefix = []netip.Prefix{netip.MustParsePrefix("192.168.1.0/24")}
	}
	for i := 0; i < r.Intn(4); i++ {
		addr := netip.MustParseAddr("0.0.0.0")
		if r.Intn(2) == 0 {
			addr = netip.MustParseAddr("2001:db8::1")
		}
		c.Listeners = append(c.Listeners, types.Listener{Bind: netip.AddrPortFrom(addr, uint16(r.Intn(65536))),
			Network: types.Network(1 + r.Intn(2)), RuleID: rid(r)})
	}
	for i := 0; i < r.Intn(4); i++ {
		ru := types.Rule{RuleID: rid(r), TenantID: rid(r), RuleVersion: r.Uint64(), PathGeneration: r.Uint64(),
			Role: types.HopRole(r.Intn(4)), HopIndex: r.Uint32(), EnforcementPoint: r.Intn(2) == 0, AllowedUpstreams: rids(r),
			TargetSchedule: types.Schedule(r.Intn(3)), Limits: types.RuleLimits{RateBps: r.Uint64(), MaxConns: r.Uint32()},
			ProxyProtocol: types.ProxyProtocol(r.Intn(4)), TCPIdle: time.Duration(r.Intn(3)) * time.Minute,
			UDPIdle: time.Duration(r.Intn(3)) * time.Second, PathCoefMilli: r.Uint64(), BillingVersion: r.Uint64()}
		if r.Intn(2) == 0 {
			ru.Networks = []types.Network{types.NetworkTCP, types.NetworkUDP}
		}
		if r.Intn(2) == 0 {
			ru.NextHop = &types.NextHop{GroupID: rid(r), MemberSchedule: types.SchedulePrimaryBackup, Dial: types.DialReverse,
				Members: []types.Member{{NodeID: rid(r), Weight: 3, Priority: 1}}}
		} else {
			ru.Targets = []types.Target{{Host: "example.com", Port: 443, Weight: 1, Priority: 2}}
		}
		c.Rules = append(c.Rules, ru)
	}
	for i := 0; i < r.Intn(3); i++ {
		c.Peers = append(c.Peers, types.Peer{NodeID: rid(r), Address: "1.2.3.4", LinkPort: 7443, Outbound: r.Intn(2) == 0,
			LinkMode: types.LinkMode(r.Intn(4)), ServedRuleIDs: rids(r)})
	}
	return c
}

// 引擎配置 → 契约 → 线上字节 → 契约 → 引擎配置，与原值完全相同。
func TestConfigRoundTrip(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 2000; i++ {
		c := randConfig(r)
		b, err := proto.Marshal(Config(c))
		if err != nil {
			t.Fatal(err)
		}
		var p runtimev1.RuntimeConfig
		if err := proto.Unmarshal(b, &p); err != nil {
			t.Fatal(err)
		}
		got, err := FromConfig(&p)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, c) {
			t.Fatalf("round trip %d:\n got %+v\nwant %+v", i, got, c)
		}
	}
}

func TestConfigInvalid(t *testing.T) {
	for _, p := range []*runtimev1.RuntimeConfig{
		nil,
		{NodeId: &commonv1.Uuid{Value: []byte{1, 2}}},
		{Listeners: []*runtimev1.Listener{{BindAddress: "not-an-ip", Port: 1}}},
		{Listeners: []*runtimev1.Listener{{BindAddress: "0.0.0.0", Port: 70000}}},
		{Rules: []*runtimev1.Rule{{Targets: []*runtimev1.Target{{Host: "a", Port: 65536}}}}},
		{TargetPolicy: &runtimev1.TargetPolicy{DenyCidrs: []string{"10.0.0.0/99"}}},
		{Peers: []*runtimev1.Peer{{LinkPort: 1 << 20}}},
	} {
		if _, err := FromConfig(p); err == nil {
			t.Fatalf("accepted %v", p)
		}
	}
}

func TestGrantsAndUsageRoundTrip(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	for i := 0; i < 500; i++ {
		m := types.GrantMeta{GrantID: rid(r), IssuerHub: rid(r), BootID: rid(r), Generation: r.Uint64(), TTL: time.Duration(1+r.Intn(600)) * time.Second}
		s := types.GrantSet{
			Run:     []types.RunGrant{{Meta: m, RuleIDs: rids(r)}},
			Billing: []types.BillingGrant{{Meta: m, RuleID: rid(r), BillingVersion: 3, RawBytes: r.Uint64()}},
			Shares:  []types.ShareLease{{Meta: m, TenantID: rid(r), Kind: types.ShareConn, Amount: r.Uint64()}},
		}
		got, err := FromGrants(Grants(&s))
		if err != nil || !reflect.DeepEqual(got, s) {
			t.Fatalf("grants %+v %v", got, err)
		}
		u := types.UsageSnapshot{
			Deltas:       []types.UsageDelta{{RuleID: rid(r), Role: types.RoleRelay, Direction: types.DirDown, RawBytes: r.Uint64(), BillingGrantID: rid(r)}},
			Demands:      []types.TenantDemand{{TenantID: rid(r), Bytes60s: r.Uint64(), StarvedPermille: r.Uint32()}},
			Consumptions: []types.GrantConsumption{{GrantID: rid(r), UsedRawBytes: r.Uint64(), Final: true}},
		}
		gu, err := FromUsage(Usage(u))
		if err != nil || !reflect.DeepEqual(gu, u) {
			t.Fatalf("usage %+v %v", gu, err)
		}
	}
}

// Agent 出站消息全部能编码；引擎错误按约定放进结果。
func TestAgentMessages(t *testing.T) {
	id := types.UUID{9}
	outs := []agent.Out{
		agent.Hello{Node: id, Boot: id, AgentVersion: "v", AppliedConfigVersion: 3, UnackedUsageFromSeq: 2},
		agent.HeldGrants{Grants: []grants.Held{{ID: id, Issuer: id, Kind: grants.KindBilling, Amount: 5, Used: 1, RemainingTTL: time.Minute}}},
		agent.PrepareReport{Version: 4, Result: &types.PrepareResult{Version: 4}},
		agent.PrepareReport{Version: 5, Err: "engine: version not newer"},
		agent.ApplyReport{Version: 4, Action: reconcile.ActionCommit, Result: &types.ApplyResult{Version: 4}},
		agent.ApplyReport{Version: 6, Action: reconcile.ActionCommit, Err: "engine: version is not the prepared one"},
		agent.GrantRequest{Request: grants.Request{ID: 7, Sync: true, Kind: grants.KindBilling, Rule: id, RateBps60s: 9}},
		agent.UsageBatch{},
		agent.EngineEvent{Event: types.Event{Kind: types.EventGrantLow, GrantID: id, Detail: "1234"}},
		agent.StateReport{State: types.RuntimeState{ActualVersion: 3}},
		agent.ProbeReport{Results: []probe.Result{{Subject: probe.Subject{Target: "a:1"}, Reachable: true, RTT: time.Millisecond}, {Subject: probe.Subject{Node: id}}}},
	}
	for _, o := range outs {
		m, err := AgentMessage(o)
		if err != nil || m == nil {
			t.Fatalf("%T: %v", o, err)
		}
		if _, err := proto.Marshal(m); err != nil {
			t.Fatal(err)
		}
	}
	m, _ := AgentMessage(agent.PrepareReport{Version: 5, Err: "boom"})
	if e := m.GetPrepareResult().GetResult().GetErrors(); len(e) != 1 || e[0].Code != ErrCodeEngine || e[0].Detail != "boom" {
		t.Fatalf("prepare error %+v", e)
	}
	m, _ = AgentMessage(agent.ApplyReport{Version: 6, Err: "x"})
	if o := m.GetApplyResult().GetResult().GetObjects(); len(o) != 1 || o[0].Status != runtimev1.ObjectStatus_OBJECT_STATUS_FAILED {
		t.Fatalf("apply error %+v", o)
	}
	m, _ = AgentMessage(agent.EngineEvent{Event: types.Event{Kind: types.EventGrantLow, Detail: "1234"}})
	if m.GetEvent().GetGrantLow().GetRemainingRawBytes() != 1234 {
		t.Fatal("grant low remaining")
	}
	m, _ = AgentMessage(agent.ProbeReport{Results: []probe.Result{{Subject: probe.Subject{Target: "a:1"}}}})
	if m.GetProbes().GetResults()[0].Rtt != nil {
		t.Fatal("unmeasured RTT must be unset (FE-006)")
	}
}

func TestHubMessages(t *testing.T) {
	hub := types.UUID{4}
	cfg := randConfig(rand.New(rand.NewSource(3)))
	cases := []struct {
		in   *edgehubv1.HubMessage
		want agent.In
	}{
		{&edgehubv1.HubMessage{Body: &edgehubv1.HubMessage_Hello{Hello: &edgehubv1.HubHello{HubId: UUID(hub), Region: "r", Mode: commonv1.HubMode_HUB_MODE_RECONCILING}}},
			agent.HubHello{Hub: hub, Region: "r", Mode: agent.ModeAutonomous}},
		{&edgehubv1.HubMessage{Body: &edgehubv1.HubMessage_Mode{Mode: &edgehubv1.ModeChange{Mode: commonv1.HubMode_HUB_MODE_ONLINE}}},
			agent.ModeChange{Mode: agent.ModeOnline}},
		{&edgehubv1.HubMessage{Body: &edgehubv1.HubMessage_Desired{Desired: &edgehubv1.DesiredConfig{Config: Config(cfg)}}},
			agent.DesiredConfig{Config: cfg}},
		{&edgehubv1.HubMessage{Body: &edgehubv1.HubMessage_Command{Command: &edgehubv1.ApplyCommand{Version: 3, Action: edgehubv1.ApplyAction_APPLY_ACTION_ABORT}}},
			agent.ApplyCommand{Version: 3, Action: reconcile.ActionAbort}},
		{&edgehubv1.HubMessage{Body: &edgehubv1.HubMessage_UsageAck{UsageAck: &edgehubv1.UsageAck{BootId: UUID(hub), AckedThroughSeq: 8}}},
			agent.UsageAck{Boot: hub, Through: 8}},
		{&edgehubv1.HubMessage{Body: &edgehubv1.HubMessage_Drain{Drain: &edgehubv1.DrainCommand{RuleIds: uuids([]types.UUID{hub})}}},
			agent.DrainCommand{Rules: []types.UUID{hub}}},
		{&edgehubv1.HubMessage{Body: &edgehubv1.HubMessage_Grants{Grants: &edgehubv1.GrantResponse{RequestId: 2, ErrorCode: "QUOTA_EXHAUSTED",
			RevokedGrantIds: uuids([]types.UUID{hub})}}},
			agent.GrantResponse{Response: grants.Response{RequestID: 2, Error: "QUOTA_EXHAUSTED", Revoked: []types.UUID{hub}}}},
	}
	for _, c := range cases {
		b, _ := proto.Marshal(c.in)
		var p edgehubv1.HubMessage
		_ = proto.Unmarshal(b, &p)
		got, err := HubMessage(&p)
		if err != nil || !reflect.DeepEqual(got, c.want) {
			t.Fatalf("%T:\n got %+v (%v)\nwant %+v", c.want, got, err, c.want)
		}
	}
	if _, err := HubMessage(&edgehubv1.HubMessage{}); err == nil {
		t.Fatal("empty message accepted")
	}
	bad := &edgehubv1.HubMessage{Body: &edgehubv1.HubMessage_UsageAck{UsageAck: &edgehubv1.UsageAck{BootId: &commonv1.Uuid{Value: []byte{1}}}}}
	if _, err := HubMessage(bad); err == nil {
		t.Fatal("bad uuid accepted")
	}
}
