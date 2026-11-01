// Package backendgrpc 是 Edge Hub 到后端的 HubControl gRPC 客户端（HUB-T01）：在契约 backend.v1 与
// upstream 包的消息之间转换，维持一条 Connect 双向流（mTLS，断线退避重连），把流交给 upstream.Link。
package backendgrpc

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"vortex/edgehub/issuer"
	"vortex/edgehub/leases"
	"vortex/edgehub/model"
	"vortex/edgehub/upstream"
	"vortex/edgehub/wire"
	backendv1 "vortex/proto/gen/go/vortex/backend/v1"
	commonv1 "vortex/proto/gen/go/vortex/common/v1"
	edgehubv1 "vortex/proto/gen/go/vortex/edgehub/v1"
	runtimev1 "vortex/proto/gen/go/vortex/runtime/v1"
)

func ts(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func uuids(us []model.UUID) []*commonv1.Uuid {
	var out []*commonv1.Uuid
	for _, u := range us {
		out = append(out, &commonv1.Uuid{Value: append([]byte(nil), u[:]...)})
	}
	return out
}

func fromUUIDs(us []*commonv1.Uuid) ([]model.UUID, error) {
	var out []model.UUID
	for _, u := range us {
		v, err := wire.FromUUID(u)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// Up 把 Edge Hub 的上行消息转为契约消息；now 用于持有授权的剩余有效期。
func Up(m upstream.Up, region, version string, now time.Time) (*backendv1.HubUpMessage, error) {
	switch m := m.(type) {
	case upstream.Register:
		r := &backendv1.HubRegister{Region: region, HubVersion: version, Mode: commonv1.HubMode(m.Mode), LedgerLastSeq: m.LedgerLastSeq,
			AutonomyStartedAt: ts(m.AutonomyStartedAt)}
		for t, v := range m.BudgetVersions {
			r.BudgetVersions = append(r.BudgetVersions, &backendv1.TenantBudgetVersion{TenantId: wire.UUID(t), BudgetVersion: v})
		}
		return &backendv1.HubUpMessage{Body: &backendv1.HubUpMessage_Register{Register: r}}, nil
	case upstream.NodeStatusBatch:
		b := &backendv1.NodeStatusBatch{}
		for _, n := range m.Nodes {
			ns := &backendv1.NodeStatus{NodeId: wire.UUID(n.Node), BootId: wire.UUID(n.Boot), Connected: n.Connected,
				AppliedConfigVersion: n.AppliedConfigVersion, AgentVersion: n.AgentVersion, LastSeenAt: ts(n.LastSeen)}
			if len(n.State) > 0 {
				var sr edgehubv1.StateReport
				if err := proto.Unmarshal(n.State, &sr); err == nil {
					ns.State = sr.GetState()
				}
			}
			b.Nodes = append(b.Nodes, ns)
		}
		return &backendv1.HubUpMessage{Body: &backendv1.HubUpMessage_NodeStatus{NodeStatus: b}}, nil
	case upstream.ApplyProgress:
		p := &backendv1.NodeApplyProgress{NodeId: wire.UUID(m.Node)}
		switch {
		case m.Prepare != nil:
			var r edgehubv1.PrepareReport
			if err := proto.Unmarshal(m.Prepare.Payload, &r); err != nil {
				return nil, err
			}
			p.Report = &backendv1.NodeApplyProgress_Prepare{Prepare: &r}
		case m.Apply != nil:
			var r edgehubv1.ApplyReport
			if err := proto.Unmarshal(m.Apply.Payload, &r); err != nil {
				return nil, err
			}
			p.Report = &backendv1.NodeApplyProgress_Apply{Apply: &r}
		}
		return &backendv1.HubUpMessage{Body: &backendv1.HubUpMessage_ApplyProgress{ApplyProgress: p}}, nil
	case upstream.LedgerEntries:
		out := &backendv1.LedgerEntries{}
		for _, e := range m.Entries {
			le, err := ledgerEntry(e)
			if err != nil {
				return nil, err
			}
			out.Entries = append(out.Entries, le)
		}
		return &backendv1.HubUpMessage{Body: &backendv1.HubUpMessage_Ledger{Ledger: out}}, nil
	case upstream.BudgetRequest:
		return &backendv1.HubUpMessage{Body: &backendv1.HubUpMessage_BudgetRequest{BudgetRequest: &backendv1.BudgetRequest{TenantId: wire.UUID(m.Tenant)}}}, nil
	case upstream.ForeignGrants:
		f := &backendv1.ForeignGrants{NodeId: wire.UUID(m.Node)}
		for _, g := range m.Grants {
			rem := g.ExpiresAt.Sub(now)
			if rem < 0 {
				rem = 0
			}
			f.Grants = append(f.Grants, &edgehubv1.HeldGrant{GrantId: wire.UUID(g.ID), IssuerHubId: wire.UUID(g.IssuerHub),
				Kind: edgehubv1.GrantKind(g.Kind), TenantId: wire.UUID(g.Tenant), RuleId: wire.UUID(g.Rule), Amount: g.Amount, Used: g.Used,
				RemainingTtl: durationpb.New(rem)})
		}
		return &backendv1.HubUpMessage{Body: &backendv1.HubUpMessage_ForeignGrants{ForeignGrants: f}}, nil
	case upstream.NodeForward:
		switch m.Msg.Kind {
		case "event":
			var ev runtimev1.EngineEvent
			if err := proto.Unmarshal(m.Msg.Payload, &ev); err != nil {
				return nil, err
			}
			return &backendv1.HubUpMessage{Body: &backendv1.HubUpMessage_NodeEvent{NodeEvent: &backendv1.NodeEventForward{NodeId: wire.UUID(m.Node), Event: &ev}}}, nil
		case "probes":
			var pr edgehubv1.ProbeReport
			if err := proto.Unmarshal(m.Msg.Payload, &pr); err != nil {
				return nil, err
			}
			return &backendv1.HubUpMessage{Body: &backendv1.HubUpMessage_Probes{Probes: &backendv1.ProbeSummary{NodeId: wire.UUID(m.Node), Report: &pr}}}, nil
		}
		// 实际状态随 NodeStatusBatch 上报；升级进度（UpgradeStatus）在 HubUpMessage 中没有对应字段（契约缺口），
		// 后端以 NodeStatus 的 agent_version 与 boot_id 判断升级结果
		return nil, nil
	}
	return nil, fmt.Errorf("%w: unknown up message %T", wire.ErrInvalid, m)
}

func ledgerEntry(e upstream.LedgerEntry) (*backendv1.LedgerEntry, error) {
	le := &backendv1.LedgerEntry{Seq: e.Seq, RecordedAt: ts(e.At)}
	switch {
	case e.Issued != nil:
		g := e.Issued
		le.Body = &backendv1.LedgerEntry_GrantIssued{GrantIssued: &backendv1.GrantIssued{GrantId: wire.UUID(g.ID), Kind: edgehubv1.GrantKind(g.Kind),
			TenantId: wire.UUID(g.Tenant), NodeId: wire.UUID(g.Node), BootId: wire.UUID(g.Boot), RuleId: wire.UUID(g.Rule),
			BillingVersion: g.BillingVersion, PathCoefficientMilli: g.CoefMilli, RawBytes: g.RawBytes, ReservedBillingBytes: g.Reserved}}
	case e.Settled != nil:
		s := e.Settled
		le.Body = &backendv1.LedgerEntry_GrantSettled{GrantSettled: &backendv1.GrantSettled{GrantId: wire.UUID(s.Grant), TenantId: wire.UUID(s.Tenant),
			UsedRawBytes: s.UsedRaw, SettledBillingBytes: s.Settled, Final: true}} // 本实例只在 final 时结算
	case e.Usage != nil:
		u := e.Usage
		var snap runtimev1.UsageSnapshot
		if err := proto.Unmarshal(u.Snapshot, &snap); err != nil {
			return nil, err
		}
		le.Body = &backendv1.LedgerEntry_NodeUsage{NodeUsage: &backendv1.NodeUsage{NodeId: wire.UUID(u.Node),
			Batch: &edgehubv1.UsageBatch{BootId: wire.UUID(u.Boot), Seq: u.Seq, Snapshot: &snap}}}
	case e.Transferred != nil:
		t := e.Transferred
		le.Body = &backendv1.LedgerEntry_GrantTransferred{GrantTransferred: &backendv1.GrantTransferred{NodeId: wire.UUID(t.Node),
			OldBootId: wire.UUID(t.OldBoot), NewBootId: wire.UUID(t.NewBoot), RevokedGrantIds: uuids(t.Revoked)}}
	default:
		return nil, fmt.Errorf("%w: empty ledger entry %d", wire.ErrInvalid, e.Seq)
	}
	return le, nil
}

// Down 把后端消息转为 Edge Hub 的下行消息；Edge Hub 不处理的（升级指令）返回 nil。
func Down(p *backendv1.BackendMessage) (upstream.Down, error) {
	switch b := p.GetBody().(type) {
	case *backendv1.BackendMessage_Desired:
		n, err := wire.FromUUID(b.Desired.GetNodeId())
		if err != nil {
			return nil, err
		}
		raw, err := proto.Marshal(b.Desired.GetConfig())
		if err != nil {
			return nil, err
		}
		return upstream.Desired{Node: n, Version: b.Desired.GetConfig().GetVersion(), Config: raw}, nil
	case *backendv1.BackendMessage_Command:
		n, err := wire.FromUUID(b.Command.GetNodeId())
		if err != nil {
			return nil, err
		}
		c := b.Command.GetCommand()
		return upstream.Command{Node: n, Version: c.GetVersion(), Commit: c.GetAction() == edgehubv1.ApplyAction_APPLY_ACTION_COMMIT}, nil
	case *backendv1.BackendMessage_Restrictions:
		r := upstream.Restrictions{Version: b.Restrictions.GetSnapshotVersion()}
		for _, t := range b.Restrictions.GetTenants() {
			id, err := wire.FromUUID(t.GetTenantId())
			if err != nil {
				return nil, err
			}
			stopped, err := fromUUIDs(t.GetStoppedRuleIds())
			if err != nil {
				return nil, err
			}
			tr := issuer.TenantRestriction{Tenant: id, Status: issuer.AccountStatus(t.GetAccountStatus()),
				TrafficExhausted: t.GetTrafficExhausted(), StoppedRules: stopped}
			if t.PlanExpiresAt != nil {
				tr.PlanExpiresAt = t.PlanExpiresAt.AsTime()
			}
			r.Tenants = append(r.Tenants, tr)
		}
		return r, nil
	case *backendv1.BackendMessage_Revocations:
		raw, err := proto.Marshal(b.Revocations)
		if err != nil {
			return nil, err
		}
		return upstream.Revocations{Serial: b.Revocations.GetSerial(), Payload: raw}, nil
	case *backendv1.BackendMessage_Budget:
		t, err := wire.FromUUID(b.Budget.GetTenantId())
		if err != nil {
			return nil, err
		}
		return upstream.Budget{Tenant: t, Version: b.Budget.GetBudgetVersion(), Delegated: b.Budget.GetDelegated(), Ack: b.Budget.GetSettledAckedTotal()}, nil
	case *backendv1.BackendMessage_Leases:
		n, err := wire.FromUUID(b.Leases.GetNodeId())
		if err != nil {
			return nil, err
		}
		out := upstream.Leases{Node: n}
		for _, l := range b.Leases.GetLeases() {
			id, err := wire.FromUUID(l.GetMeta().GetGrantId())
			if err != nil {
				return nil, err
			}
			t, err := wire.FromUUID(l.GetTenantId())
			if err != nil {
				return nil, err
			}
			k := model.GrantShareRate
			if l.GetKind() == runtimev1.ShareKind_SHARE_KIND_CONN {
				k = model.GrantShareConn
			}
			out.Leases = append(out.Leases, leases.Lease{ID: id, Tenant: t, Kind: k, Amount: l.GetAmount(), TTL: l.GetMeta().GetTtl().AsDuration()})
		}
		return out, nil
	case *backendv1.BackendMessage_LedgerAck:
		return upstream.LedgerAck{Through: b.LedgerAck.GetAckedThroughSeq()}, nil
	case *backendv1.BackendMessage_BillingVersions:
		var out upstream.BillingVersions
		for _, v := range b.BillingVersions.GetVersions() {
			out.Versions = append(out.Versions, issuer.BillingVersion{Version: v.GetVersion(), EffectiveAt: v.GetEffectiveAt().AsTime()})
		}
		return out, nil
	case *backendv1.BackendMessage_Reconcile:
		revoked, err := fromUUIDs(b.Reconcile.GetBudgetRevokedTenantIds())
		if err != nil {
			return nil, err
		}
		var ph upstream.ReconcilePhase
		switch b.Reconcile.GetPhase() {
		case backendv1.ReconcilePhase_RECONCILE_PHASE_STARTED:
			ph = upstream.ReconcileStarted
		case backendv1.ReconcilePhase_RECONCILE_PHASE_COMPLETED:
			ph = upstream.ReconcileCompleted
		default:
			return nil, fmt.Errorf("%w: reconcile phase %v", wire.ErrInvalid, b.Reconcile.GetPhase())
		}
		return upstream.Reconcile{Phase: ph, NewGeneration: b.Reconcile.GetNewGeneration(), RevokedTenants: revoked}, nil
	case *backendv1.BackendMessage_NodeOperation:
		n, err := wire.FromUUID(b.NodeOperation.GetNodeId())
		if err != nil {
			return nil, err
		}
		if d := b.NodeOperation.GetDrain(); d != nil {
			raw, err := proto.Marshal(d)
			if err != nil {
				return nil, err
			}
			return upstream.Drain{Node: n, Payload: raw}, nil
		}
		if u := b.NodeOperation.GetUpgrade(); u != nil {
			raw, err := proto.Marshal(u)
			if err != nil {
				return nil, err
			}
			return upstream.Upgrade{Node: n, Payload: raw}, nil
		}
		return nil, nil
	}
	return nil, fmt.Errorf("%w: empty backend message", wire.ErrInvalid)
}
