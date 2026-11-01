package wire

import (
	"fmt"

	"google.golang.org/protobuf/types/known/durationpb"

	"vortex/node/agent"
	"vortex/node/agent/grants"
	"vortex/node/agent/reconcile"
	"vortex/node/agent/upgrade"
	commonv1 "vortex/proto/gen/go/vortex/common/v1"
	edgehubv1 "vortex/proto/gen/go/vortex/edgehub/v1"
	runtimev1 "vortex/proto/gen/go/vortex/runtime/v1"
)

// 引擎错误（配置无效、版本过旧、未决版本不符）在契约中没有单独字段，按以下约定表示（契约补充 L4）：
//   - PrepareReport：PrepareResult.errors 中一条对象为空、code = ErrCodeEngine、detail 为错误文本；
//   - ApplyReport：ApplyResult.objects 中一条对象为空、status = FAILED、error_code = ErrCodeEngine + ": " + 错误文本。
const ErrCodeEngine = "ENGINE_ERROR"

// AgentMessage 把 Agent 的出站消息转为契约消息。
func AgentMessage(m agent.Out) (*edgehubv1.AgentMessage, error) {
	switch m := m.(type) {
	case agent.Hello:
		return &edgehubv1.AgentMessage{Body: &edgehubv1.AgentMessage_Hello{Hello: &edgehubv1.AgentHello{
			NodeId: UUID(m.Node), BootId: UUID(m.Boot), PreviousBootId: UUID(m.PreviousBoot), AgentVersion: m.AgentVersion,
			AppliedConfigVersion: m.AppliedConfigVersion, UnackedUsageFromSeq: m.UnackedUsageFromSeq}}}, nil
	case agent.HeldGrants:
		r := &edgehubv1.HeldGrantsReport{}
		for _, g := range m.Grants {
			r.Grants = append(r.Grants, &edgehubv1.HeldGrant{GrantId: UUID(g.ID), IssuerHubId: UUID(g.Issuer),
				Kind: edgehubv1.GrantKind(g.Kind), TenantId: UUID(g.Tenant), RuleId: UUID(g.Rule), Amount: g.Amount, Used: g.Used,
				RemainingTtl: durationpb.New(g.RemainingTTL)})
		}
		return &edgehubv1.AgentMessage{Body: &edgehubv1.AgentMessage_HeldGrants{HeldGrants: r}}, nil
	case agent.PrepareReport:
		var res *runtimev1.PrepareResult
		if m.Result != nil {
			res = Prepare(m.Result)
		} else {
			res = &runtimev1.PrepareResult{Version: m.Version}
		}
		if m.Err != "" {
			res.Errors = append(res.Errors, &runtimev1.ResourceError{Object: &runtimev1.ObjectRef{}, Code: ErrCodeEngine, Detail: m.Err})
		}
		return &edgehubv1.AgentMessage{Body: &edgehubv1.AgentMessage_PrepareResult{PrepareResult: &edgehubv1.PrepareReport{Result: res}}}, nil
	case agent.ApplyReport:
		var res *runtimev1.ApplyResult
		if m.Result != nil {
			res = Apply(m.Result)
		} else {
			res = &runtimev1.ApplyResult{Version: m.Version}
		}
		if m.Err != "" {
			res.Objects = append(res.Objects, &runtimev1.ObjectState{Object: &runtimev1.ObjectRef{},
				Status: runtimev1.ObjectStatus_OBJECT_STATUS_FAILED, ErrorCode: ErrCodeEngine + ": " + m.Err})
		}
		return &edgehubv1.AgentMessage{Body: &edgehubv1.AgentMessage_ApplyResult{ApplyResult: &edgehubv1.ApplyReport{
			Action: edgehubv1.ApplyAction(m.Action), Result: res}}}, nil
	case agent.GrantRequest:
		return &edgehubv1.AgentMessage{Body: &edgehubv1.AgentMessage_GrantRequest{GrantRequest: &edgehubv1.GrantRequest{
			RequestId: m.ID, Sync: m.Sync, Kind: edgehubv1.GrantKind(m.Kind), TenantId: UUID(m.Tenant), RuleId: UUID(m.Rule),
			CurrentGrantId: UUID(m.CurrentGrant), RateBps_60S: m.RateBps60s}}}, nil
	case agent.UsageBatch:
		return &edgehubv1.AgentMessage{Body: &edgehubv1.AgentMessage_Usage{Usage: &edgehubv1.UsageBatch{
			BootId: UUID(m.Boot), Seq: m.Seq, Snapshot: Usage(m.Snapshot)}}}, nil
	case agent.EngineEvent:
		ev := Event(m.Event)
		if ev == nil {
			return nil, nil
		}
		return &edgehubv1.AgentMessage{Body: &edgehubv1.AgentMessage_Event{Event: ev}}, nil
	case agent.StateReport:
		return &edgehubv1.AgentMessage{Body: &edgehubv1.AgentMessage_State{State: &edgehubv1.StateReport{State: State(m.State)}}}, nil
	case agent.ProbeReport:
		r := &edgehubv1.ProbeReport{}
		for _, p := range m.Results {
			pr := &edgehubv1.ProbeResult{Reachable: p.Reachable}
			if p.RTT > 0 {
				pr.Rtt = durationpb.New(p.RTT) // 未测得时不设置（FE-006）
			}
			if p.Target != "" {
				pr.Subject = &edgehubv1.ProbeResult_Target{Target: p.Target}
			} else {
				pr.Subject = &edgehubv1.ProbeResult_NodeId{NodeId: UUID(p.Node)}
			}
			r.Results = append(r.Results, pr)
		}
		return &edgehubv1.AgentMessage{Body: &edgehubv1.AgentMessage_Probes{Probes: r}}, nil
	case agent.UpgradeStatus:
		return &edgehubv1.AgentMessage{Body: &edgehubv1.AgentMessage_Upgrade{Upgrade: &edgehubv1.UpgradeStatus{
			TargetVersion: m.Target, Phase: edgehubv1.UpgradePhase(m.Phase), Error: m.Error}}}, nil
	}
	return nil, fmt.Errorf("%w: unknown agent message %T", ErrInvalid, m)
}

func mode(m commonv1.HubMode) agent.Mode {
	switch m {
	case commonv1.HubMode_HUB_MODE_ONLINE:
		return agent.ModeOnline
	case commonv1.HubMode_HUB_MODE_AUTONOMOUS, commonv1.HubMode_HUB_MODE_RECONCILING, commonv1.HubMode_HUB_MODE_AUTONOMY_EXPIRED:
		return agent.ModeAutonomous
	}
	return agent.ModeUnspecified
}

// HubMessage 把契约消息转为 Agent 的入站消息；Agent 不处理的消息（如升级指令，尚未实现）返回 nil。
func HubMessage(p *edgehubv1.HubMessage) (agent.In, error) {
	switch b := p.GetBody().(type) {
	case *edgehubv1.HubMessage_Hello:
		id, err := FromUUID(b.Hello.GetHubId())
		if err != nil {
			return nil, err
		}
		return agent.HubHello{Hub: id, Region: b.Hello.GetRegion(), Mode: mode(b.Hello.GetMode())}, nil
	case *edgehubv1.HubMessage_Mode:
		return agent.ModeChange{Mode: mode(b.Mode.GetMode())}, nil
	case *edgehubv1.HubMessage_Desired:
		cfg, err := FromConfig(b.Desired.GetConfig())
		if err != nil {
			return nil, err
		}
		return agent.DesiredConfig{Config: cfg}, nil
	case *edgehubv1.HubMessage_Command:
		a := reconcile.ActionNone
		switch b.Command.GetAction() {
		case edgehubv1.ApplyAction_APPLY_ACTION_COMMIT:
			a = reconcile.ActionCommit
		case edgehubv1.ApplyAction_APPLY_ACTION_ABORT:
			a = reconcile.ActionAbort
		}
		return agent.ApplyCommand{Version: b.Command.GetVersion(), Action: a}, nil
	case *edgehubv1.HubMessage_Grants:
		set, err := FromGrants(b.Grants.GetGrants())
		if err != nil {
			return nil, err
		}
		revoked, err := fromUUIDs(b.Grants.GetRevokedGrantIds())
		if err != nil {
			return nil, err
		}
		return agent.GrantResponse{Response: grants.Response{RequestID: b.Grants.GetRequestId(), Set: set, Revoked: revoked,
			Error: b.Grants.GetErrorCode()}}, nil
	case *edgehubv1.HubMessage_UsageAck:
		boot, err := FromUUID(b.UsageAck.GetBootId())
		if err != nil {
			return nil, err
		}
		return agent.UsageAck{Boot: boot, Through: b.UsageAck.GetAckedThroughSeq()}, nil
	case *edgehubv1.HubMessage_Drain:
		rules, err := fromUUIDs(b.Drain.GetRuleIds())
		if err != nil {
			return nil, err
		}
		return agent.DrainCommand{Timeout: fromDur(b.Drain.GetTimeout()), Rules: rules}, nil
	case *edgehubv1.HubMessage_Upgrade:
		u := b.Upgrade
		if u.GetVersion() == "" {
			return nil, fmt.Errorf("%w: upgrade without version", ErrInvalid)
		}
		return agent.UpgradeCommand{Command: upgrade.Command{Version: u.GetVersion(), URL: u.GetDownloadUrl(), SHA256: u.GetSha256(),
			Signature: u.GetSignature(), Irreversible: u.GetIrreversibleMigration()}}, nil
	}
	return nil, fmt.Errorf("%w: empty hub message", ErrInvalid)
}
