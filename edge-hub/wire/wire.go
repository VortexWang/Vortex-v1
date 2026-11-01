// Package wire 在契约 C2（edgehub.v1）与 Edge Hub 会话（session）的消息之间转换。
//
// Edge Hub 不解析运行配置与应用结果（不得生成或修改配置）：期望配置、Prepare / Apply 结果、计量快照、
// 引擎事件、探测与状态以 protobuf 编码的字节原样转发；会话只取出它需要的字段（版本、是否成功、
// 授权消耗与需求信号）。
package wire

import (
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"vortex/edgehub/issuer"
	"vortex/edgehub/model"
	"vortex/edgehub/session"
	commonv1 "vortex/proto/gen/go/vortex/common/v1"
	edgehubv1 "vortex/proto/gen/go/vortex/edgehub/v1"
	runtimev1 "vortex/proto/gen/go/vortex/runtime/v1"
)

var ErrInvalid = errors.New("wire: invalid message")

func UUID(u model.UUID) *commonv1.Uuid {
	if u.IsZero() {
		return nil
	}
	return &commonv1.Uuid{Value: append([]byte(nil), u[:]...)}
}

func FromUUID(u *commonv1.Uuid) (model.UUID, error) {
	var out model.UUID
	if u == nil || len(u.Value) == 0 {
		return out, nil
	}
	if len(u.Value) != 16 {
		return out, fmt.Errorf("%w: uuid length %d", ErrInvalid, len(u.Value))
	}
	copy(out[:], u.Value)
	return out, nil
}

func fromUUIDs(us []*commonv1.Uuid) ([]model.UUID, error) {
	var out []model.UUID
	for _, u := range us {
		v, err := FromUUID(u)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func uuids(us []model.UUID) []*commonv1.Uuid {
	var out []*commonv1.Uuid
	for _, u := range us {
		if p := UUID(u); p != nil {
			out = append(out, p)
		} else {
			out = append(out, &commonv1.Uuid{Value: make([]byte, 16)})
		}
	}
	return out
}

func marshal(m proto.Message) []byte {
	b, _ := proto.Marshal(m) // 已解析的消息必然可编码
	return b
}

// FromAgent 把 Agent 消息转为会话消息；会话不处理的消息（升级状态，尚未实现）返回 nil。
func FromAgent(p *edgehubv1.AgentMessage) (session.AgentMsg, error) {
	switch b := p.GetBody().(type) {
	case *edgehubv1.AgentMessage_Hello:
		h := b.Hello
		var out session.Hello
		var err error
		if out.Node, err = FromUUID(h.GetNodeId()); err != nil {
			return nil, err
		}
		if out.Boot, err = FromUUID(h.GetBootId()); err != nil {
			return nil, err
		}
		if out.PreviousBoot, err = FromUUID(h.GetPreviousBootId()); err != nil {
			return nil, err
		}
		if out.Boot.IsZero() {
			return nil, fmt.Errorf("%w: hello without boot_id", ErrInvalid)
		}
		out.AgentVersion, out.AppliedConfigVersion, out.UnackedUsageFromSeq = h.GetAgentVersion(), h.GetAppliedConfigVersion(), h.GetUnackedUsageFromSeq()
		return out, nil
	case *edgehubv1.AgentMessage_HeldGrants:
		var out session.HeldGrants
		for _, g := range b.HeldGrants.GetGrants() {
			var h issuer.HeldGrant
			var err error
			if h.ID, err = FromUUID(g.GetGrantId()); err != nil {
				return nil, err
			}
			if h.IssuerHub, err = FromUUID(g.GetIssuerHubId()); err != nil {
				return nil, err
			}
			if h.Tenant, err = FromUUID(g.GetTenantId()); err != nil {
				return nil, err
			}
			if h.Rule, err = FromUUID(g.GetRuleId()); err != nil {
				return nil, err
			}
			h.Kind, h.Amount, h.Used = model.GrantKind(g.GetKind()), g.GetAmount(), g.GetUsed()
			out.Grants = append(out.Grants, session.HeldGrant{HeldGrant: h, Remaining: g.GetRemainingTtl().AsDuration()})
		}
		return out, nil
	case *edgehubv1.AgentMessage_PrepareResult:
		r := b.PrepareResult.GetResult()
		return session.PrepareReport{Version: r.GetVersion(), OK: r != nil && len(r.GetErrors()) == 0, Payload: marshal(b.PrepareResult)}, nil
	case *edgehubv1.AgentMessage_ApplyResult:
		r := b.ApplyResult.GetResult()
		ok := b.ApplyResult.GetAction() == edgehubv1.ApplyAction_APPLY_ACTION_COMMIT && r != nil
		for _, o := range r.GetObjects() {
			if o.GetStatus() == runtimev1.ObjectStatus_OBJECT_STATUS_FAILED && o.GetObject().GetKind() == runtimev1.ObjectKind_OBJECT_KIND_UNSPECIFIED {
				ok = false // 引擎错误（契约补充 L4）
			}
		}
		return session.ApplyReport{Version: r.GetVersion(), Committed: ok, Payload: marshal(b.ApplyResult)}, nil
	case *edgehubv1.AgentMessage_GrantRequest:
		g := b.GrantRequest
		out := session.GrantRequest{ID: g.GetRequestId(), Sync: g.GetSync(), Kind: model.GrantKind(g.GetKind()), RateBps60s: g.GetRateBps_60S()}
		var err error
		if out.Tenant, err = FromUUID(g.GetTenantId()); err != nil {
			return nil, err
		}
		if out.Rule, err = FromUUID(g.GetRuleId()); err != nil {
			return nil, err
		}
		if out.CurrentGrant, err = FromUUID(g.GetCurrentGrantId()); err != nil {
			return nil, err
		}
		return out, nil
	case *edgehubv1.AgentMessage_Usage:
		u := b.Usage
		out := session.UsageBatch{Seq: u.GetSeq(), Snapshot: marshal(u.GetSnapshot())}
		var err error
		if out.Boot, err = FromUUID(u.GetBootId()); err != nil {
			return nil, err
		}
		if out.Seq == 0 {
			return nil, fmt.Errorf("%w: usage batch seq 0", ErrInvalid)
		}
		for _, c := range u.GetSnapshot().GetGrantConsumptions() {
			id, err := FromUUID(c.GetGrantId())
			if err != nil {
				return nil, err
			}
			out.Consumptions = append(out.Consumptions, session.Consumption{Grant: id, Used: c.GetUsedRawBytes(), Final: c.GetFinal()})
		}
		for _, d := range u.GetSnapshot().GetDemands() {
			id, err := FromUUID(d.GetTenantId())
			if err != nil {
				return nil, err
			}
			out.Demands = append(out.Demands, session.Demand{Tenant: id, Bytes60s: d.GetBytes_60S(), StarvedPermille: d.GetStarvedPermille()})
		}
		return out, nil
	case *edgehubv1.AgentMessage_Event:
		return session.Passthrough{Kind: "event", Payload: marshal(b.Event)}, nil
	case *edgehubv1.AgentMessage_State:
		return session.Passthrough{Kind: "state", Payload: marshal(b.State)}, nil
	case *edgehubv1.AgentMessage_Probes:
		return session.Passthrough{Kind: "probes", Payload: marshal(b.Probes)}, nil
	case *edgehubv1.AgentMessage_Upgrade:
		return session.Passthrough{Kind: "upgrade", Payload: marshal(b.Upgrade)}, nil
	}
	return nil, fmt.Errorf("%w: empty agent message", ErrInvalid)
}

func dur(d time.Duration) *durationpb.Duration {
	if d < 0 {
		d = 0
	}
	return durationpb.New(d)
}

func meta(hub, id, boot model.UUID, gen uint64, ttl time.Duration) *runtimev1.GrantMeta {
	return &runtimev1.GrantMeta{GrantId: UUID(id), IssuerHubId: UUID(hub), BootId: UUID(boot), Generation: gen, Ttl: dur(ttl)}
}

// ToAgent 把会话消息转为契约消息。
func ToAgent(hub model.UUID, region string, m session.HubMsg) (*edgehubv1.HubMessage, error) {
	switch m := m.(type) {
	case session.HubHello:
		return &edgehubv1.HubMessage{Body: &edgehubv1.HubMessage_Hello{Hello: &edgehubv1.HubHello{HubId: UUID(m.Hub), Region: region,
			Mode: commonv1.HubMode(m.Mode)}}}, nil
	case session.ModeChange:
		return &edgehubv1.HubMessage{Body: &edgehubv1.HubMessage_Mode{Mode: &edgehubv1.ModeChange{Mode: commonv1.HubMode(m.Mode)}}}, nil
	case session.DesiredConfig:
		var cfg runtimev1.RuntimeConfig
		if err := proto.Unmarshal(m.Payload, &cfg); err != nil {
			return nil, fmt.Errorf("%w: desired config: %v", ErrInvalid, err)
		}
		return &edgehubv1.HubMessage{Body: &edgehubv1.HubMessage_Desired{Desired: &edgehubv1.DesiredConfig{Config: &cfg}}}, nil
	case session.ApplyCommand:
		a := edgehubv1.ApplyAction_APPLY_ACTION_ABORT
		if m.Commit {
			a = edgehubv1.ApplyAction_APPLY_ACTION_COMMIT
		}
		return &edgehubv1.HubMessage{Body: &edgehubv1.HubMessage_Command{Command: &edgehubv1.ApplyCommand{Version: m.Version, Action: a}}}, nil
	case session.GrantResponse:
		set := &runtimev1.GrantSet{}
		for _, g := range m.Run {
			set.RunGrants = append(set.RunGrants, &runtimev1.RunGrant{Meta: meta(hub, g.ID, g.Boot, g.Generation, g.TTL), RuleIds: uuids(g.Rules)})
		}
		for _, g := range m.Billing {
			set.BillingGrants = append(set.BillingGrants, &runtimev1.BillingGrant{Meta: meta(hub, g.ID, g.Boot, g.Generation, g.TTL),
				RuleId: UUID(g.Rule), BillingVersion: g.BillingVersion, RawBytes: g.RawBytes})
		}
		for _, l := range m.Shares {
			set.ShareLeases = append(set.ShareLeases, &runtimev1.ShareLease{Meta: meta(hub, l.ID, l.Boot, 0, l.TTL),
				TenantId: UUID(l.Tenant), Kind: shareKind(l.Kind), Amount: l.Amount})
		}
		return &edgehubv1.HubMessage{Body: &edgehubv1.HubMessage_Grants{Grants: &edgehubv1.GrantResponse{RequestId: m.RequestID,
			Grants: set, RevokedGrantIds: uuids(m.Revoked), ErrorCode: m.Error}}}, nil
	case session.UsageAck:
		return &edgehubv1.HubMessage{Body: &edgehubv1.HubMessage_UsageAck{UsageAck: &edgehubv1.UsageAck{BootId: UUID(m.Boot), AckedThroughSeq: m.Through}}}, nil
	case session.DrainCommand:
		var d edgehubv1.DrainCommand
		if err := proto.Unmarshal(m.Payload, &d); err != nil {
			return nil, fmt.Errorf("%w: drain: %v", ErrInvalid, err)
		}
		return &edgehubv1.HubMessage{Body: &edgehubv1.HubMessage_Drain{Drain: &d}}, nil
	case session.UpgradeCommand:
		var u edgehubv1.UpgradeCommand
		if err := proto.Unmarshal(m.Payload, &u); err != nil {
			return nil, fmt.Errorf("%w: upgrade: %v", ErrInvalid, err)
		}
		return &edgehubv1.HubMessage{Body: &edgehubv1.HubMessage_Upgrade{Upgrade: &u}}, nil
	}
	return nil, fmt.Errorf("%w: unknown hub message %T", ErrInvalid, m)
}

func shareKind(k model.GrantKind) runtimev1.ShareKind {
	if k == model.GrantShareConn {
		return runtimev1.ShareKind_SHARE_KIND_CONN
	}
	return runtimev1.ShareKind_SHARE_KIND_RATE
}

// RulesFromConfig 从期望配置取出签发所需的规则信息（session.Options.ParseRules）。只读，不改动配置。
func RulesFromConfig(payload []byte) ([]issuer.RuleState, error) {
	var cfg runtimev1.RuntimeConfig
	if err := proto.Unmarshal(payload, &cfg); err != nil {
		return nil, fmt.Errorf("%w: desired config: %v", ErrInvalid, err)
	}
	var out []issuer.RuleState
	for _, r := range cfg.GetRules() {
		id, err := FromUUID(r.GetRuleId())
		if err != nil {
			return nil, err
		}
		t, err := FromUUID(r.GetTenantId())
		if err != nil {
			return nil, err
		}
		out = append(out, issuer.RuleState{RuleID: id, TenantID: t, EnforcementPoint: r.GetEnforcementPoint(), PathCoefMilli: r.GetPathCoefficientMilli()})
	}
	return out, nil
}
