// Package wire 在契约 C2 的生成代码（runtime.v1、edgehub.v1）与 Agent / Engine 的 Go 类型之间转换。
//
// 枚举的数值在两侧一致（types 包按契约编号定义），直接转换。引擎事件只有通用的 Detail 文本，转换为
// EngineEvent 时放进对应消息的说明字段（如 OpenRejected.check、PeerDown.reason），不丢信息。
package wire

import (
	"errors"
	"fmt"
	"net/netip"
	"strconv"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	"vortex/engine/types"
	commonv1 "vortex/proto/gen/go/vortex/common/v1"
	runtimev1 "vortex/proto/gen/go/vortex/runtime/v1"
)

var ErrInvalid = errors.New("wire: invalid message")

// ---------------------------------------------------------------- 基本类型

func UUID(u types.UUID) *commonv1.Uuid {
	if u == (types.UUID{}) {
		return nil
	}
	return &commonv1.Uuid{Value: append([]byte(nil), u[:]...)}
}

func FromUUID(u *commonv1.Uuid) (types.UUID, error) {
	var out types.UUID
	if u == nil || len(u.Value) == 0 {
		return out, nil
	}
	if len(u.Value) != 16 {
		return out, fmt.Errorf("%w: uuid length %d", ErrInvalid, len(u.Value))
	}
	copy(out[:], u.Value)
	return out, nil
}

func uuids(us []types.UUID) []*commonv1.Uuid {
	if len(us) == 0 {
		return nil
	}
	out := make([]*commonv1.Uuid, len(us))
	for i, u := range us {
		out[i] = UUID(u)
		if out[i] == nil {
			out[i] = &commonv1.Uuid{Value: make([]byte, 16)}
		}
	}
	return out
}

func fromUUIDs(us []*commonv1.Uuid) ([]types.UUID, error) {
	if len(us) == 0 {
		return nil, nil
	}
	out := make([]types.UUID, len(us))
	for i, u := range us {
		v, err := FromUUID(u)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

func dur(d time.Duration) *durationpb.Duration {
	if d == 0 {
		return nil
	}
	return durationpb.New(d)
}

func fromDur(d *durationpb.Duration) time.Duration {
	if d == nil {
		return 0
	}
	return d.AsDuration()
}

// ---------------------------------------------------------------- RuntimeConfig

// Config 把引擎配置转为契约消息。
func Config(c *types.RuntimeConfig) *runtimev1.RuntimeConfig {
	out := &runtimev1.RuntimeConfig{Version: c.Version, NodeId: UUID(c.NodeID), Generation: c.Generation,
		Revocations: &runtimev1.RevocationList{Serial: c.Revocations.Serial, CertSerials: c.Revocations.CertSerials,
			NodeIds: uuids(c.Revocations.NodeIDs)},
		TargetPolicy: &runtimev1.TargetPolicy{SelfHostedNode: c.TargetPolicy.SelfHostedNode}}
	for _, l := range c.Listeners {
		out.Listeners = append(out.Listeners, &runtimev1.Listener{BindAddress: l.Bind.Addr().String(), Port: uint32(l.Bind.Port()),
			Network: commonv1.Network(l.Network), RuleId: UUID(l.RuleID)})
	}
	for _, r := range c.Rules {
		pr := &runtimev1.Rule{RuleId: UUID(r.RuleID), TenantId: UUID(r.TenantID), RuleVersion: r.RuleVersion,
			PathGeneration: r.PathGeneration, Role: commonv1.HopRole(r.Role), HopIndex: r.HopIndex,
			EnforcementPoint: r.EnforcementPoint, AllowedUpstreams: uuids(r.AllowedUpstreams),
			TargetSchedule:       commonv1.Schedule(r.TargetSchedule),
			Limits:               &runtimev1.RuleLimits{RateBps: r.Limits.RateBps, MaxConns: r.Limits.MaxConns},
			ProxyProtocol:        runtimev1.ProxyProtocol(r.ProxyProtocol),
			Timeouts:             &runtimev1.SessionTimeouts{TcpIdle: dur(r.TCPIdle), UdpIdle: dur(r.UDPIdle)},
			PathCoefficientMilli: r.PathCoefMilli, BillingVersion: r.BillingVersion}
		for _, n := range r.Networks {
			pr.Networks = append(pr.Networks, commonv1.Network(n))
		}
		for _, t := range r.Targets {
			pr.Targets = append(pr.Targets, &runtimev1.Target{Host: t.Host, Port: uint32(t.Port), Weight: t.Weight, Priority: t.Priority})
		}
		if nh := r.NextHop; nh != nil {
			p := &runtimev1.NextHop{GroupId: UUID(nh.GroupID), MemberSchedule: commonv1.Schedule(nh.MemberSchedule),
				DialDirection: runtimev1.DialDirection(nh.Dial)}
			for _, m := range nh.Members {
				p.Members = append(p.Members, &runtimev1.Member{NodeId: UUID(m.NodeID), Weight: m.Weight, Priority: m.Priority})
			}
			pr.NextHop = p
		}
		out.Rules = append(out.Rules, pr)
	}
	for _, p := range c.Peers {
		dial := runtimev1.PeerDial_PEER_DIAL_INBOUND
		if p.Outbound {
			dial = runtimev1.PeerDial_PEER_DIAL_OUTBOUND
		}
		out.Peers = append(out.Peers, &runtimev1.Peer{NodeId: UUID(p.NodeID), Address: p.Address, LinkPort: uint32(p.LinkPort),
			Dial: dial, LinkMode: runtimev1.LinkMode(p.LinkMode), ServedRuleIds: uuids(p.ServedRuleIDs)})
	}
	for _, p := range c.TargetPolicy.DenyPrefixes {
		out.TargetPolicy.DenyCidrs = append(out.TargetPolicy.DenyCidrs, p.String())
	}
	for _, p := range c.TargetPolicy.DenyPorts {
		out.TargetPolicy.DenyPorts = append(out.TargetPolicy.DenyPorts, uint32(p))
	}
	for _, p := range c.TargetPolicy.AllowPrivatePrefix {
		out.TargetPolicy.AllowPrivateCidrs = append(out.TargetPolicy.AllowPrivateCidrs, p.String())
	}
	return out
}

func port(p uint32) (uint16, error) {
	if p > 65535 {
		return 0, fmt.Errorf("%w: port %d", ErrInvalid, p)
	}
	return uint16(p), nil
}

// FromConfig 把契约消息转为引擎配置；字段不合法（UUID 长度、地址、端口、网段）时返回 ErrInvalid。
// 配置语义的校验（规则引用、互斥字段等）由 Engine.Prepare 完成。
func FromConfig(p *runtimev1.RuntimeConfig) (*types.RuntimeConfig, error) {
	if p == nil {
		return nil, fmt.Errorf("%w: nil config", ErrInvalid)
	}
	var err error
	c := &types.RuntimeConfig{Version: p.Version, Generation: p.Generation}
	if c.NodeID, err = FromUUID(p.NodeId); err != nil {
		return nil, err
	}
	for _, l := range p.Listeners {
		a, err := netip.ParseAddr(l.BindAddress)
		if err != nil {
			return nil, fmt.Errorf("%w: listener address %q", ErrInvalid, l.BindAddress)
		}
		pt, err := port(l.Port)
		if err != nil {
			return nil, err
		}
		rid, err := FromUUID(l.RuleId)
		if err != nil {
			return nil, err
		}
		c.Listeners = append(c.Listeners, types.Listener{Bind: netip.AddrPortFrom(a, pt), Network: types.Network(l.Network), RuleID: rid})
	}
	for _, pr := range p.Rules {
		r := types.Rule{RuleVersion: pr.RuleVersion, PathGeneration: pr.PathGeneration, Role: types.HopRole(pr.Role),
			HopIndex: pr.HopIndex, EnforcementPoint: pr.EnforcementPoint, TargetSchedule: types.Schedule(pr.TargetSchedule),
			ProxyProtocol: types.ProxyProtocol(pr.ProxyProtocol), PathCoefMilli: pr.PathCoefficientMilli, BillingVersion: pr.BillingVersion}
		if r.RuleID, err = FromUUID(pr.RuleId); err != nil {
			return nil, err
		}
		if r.TenantID, err = FromUUID(pr.TenantId); err != nil {
			return nil, err
		}
		if r.AllowedUpstreams, err = fromUUIDs(pr.AllowedUpstreams); err != nil {
			return nil, err
		}
		if pr.Limits != nil {
			r.Limits = types.RuleLimits{RateBps: pr.Limits.RateBps, MaxConns: pr.Limits.MaxConns}
		}
		if pr.Timeouts != nil {
			r.TCPIdle, r.UDPIdle = fromDur(pr.Timeouts.TcpIdle), fromDur(pr.Timeouts.UdpIdle)
		}
		for _, n := range pr.Networks {
			r.Networks = append(r.Networks, types.Network(n))
		}
		for _, t := range pr.Targets {
			pt, err := port(t.Port)
			if err != nil {
				return nil, err
			}
			r.Targets = append(r.Targets, types.Target{Host: t.Host, Port: pt, Weight: t.Weight, Priority: t.Priority})
		}
		if nh := pr.NextHop; nh != nil {
			n := &types.NextHop{MemberSchedule: types.Schedule(nh.MemberSchedule), Dial: types.DialDirection(nh.DialDirection)}
			if n.GroupID, err = FromUUID(nh.GroupId); err != nil {
				return nil, err
			}
			for _, m := range nh.Members {
				id, err := FromUUID(m.NodeId)
				if err != nil {
					return nil, err
				}
				n.Members = append(n.Members, types.Member{NodeID: id, Weight: m.Weight, Priority: m.Priority})
			}
			r.NextHop = n
		}
		c.Rules = append(c.Rules, r)
	}
	for _, pp := range p.Peers {
		id, err := FromUUID(pp.NodeId)
		if err != nil {
			return nil, err
		}
		lp, err := port(pp.LinkPort)
		if err != nil {
			return nil, err
		}
		served, err := fromUUIDs(pp.ServedRuleIds)
		if err != nil {
			return nil, err
		}
		c.Peers = append(c.Peers, types.Peer{NodeID: id, Address: pp.Address, LinkPort: lp,
			Outbound: pp.Dial == runtimev1.PeerDial_PEER_DIAL_OUTBOUND, LinkMode: types.LinkMode(pp.LinkMode), ServedRuleIDs: served})
	}
	if rv := p.Revocations; rv != nil {
		c.Revocations = types.RevocationList{Serial: rv.Serial, CertSerials: rv.CertSerials}
		if c.Revocations.NodeIDs, err = fromUUIDs(rv.NodeIds); err != nil {
			return nil, err
		}
	}
	if tp := p.TargetPolicy; tp != nil {
		c.TargetPolicy.SelfHostedNode = tp.SelfHostedNode
		for _, s := range tp.DenyCidrs {
			pf, err := netip.ParsePrefix(s)
			if err != nil {
				return nil, fmt.Errorf("%w: deny cidr %q", ErrInvalid, s)
			}
			c.TargetPolicy.DenyPrefixes = append(c.TargetPolicy.DenyPrefixes, pf)
		}
		for _, x := range tp.DenyPorts {
			pt, err := port(x)
			if err != nil {
				return nil, err
			}
			c.TargetPolicy.DenyPorts = append(c.TargetPolicy.DenyPorts, pt)
		}
		for _, s := range tp.AllowPrivateCidrs {
			pf, err := netip.ParsePrefix(s)
			if err != nil {
				return nil, fmt.Errorf("%w: private cidr %q", ErrInvalid, s)
			}
			c.TargetPolicy.AllowPrivatePrefix = append(c.TargetPolicy.AllowPrivatePrefix, pf)
		}
	}
	return c, nil
}

// ---------------------------------------------------------------- 授权

func meta(m types.GrantMeta) *runtimev1.GrantMeta {
	return &runtimev1.GrantMeta{GrantId: UUID(m.GrantID), IssuerHubId: UUID(m.IssuerHub), BootId: UUID(m.BootID),
		Generation: m.Generation, Ttl: dur(m.TTL)}
}

func fromMeta(p *runtimev1.GrantMeta) (types.GrantMeta, error) {
	var m types.GrantMeta
	if p == nil {
		return m, fmt.Errorf("%w: grant without meta", ErrInvalid)
	}
	var err error
	if m.GrantID, err = FromUUID(p.GrantId); err != nil {
		return m, err
	}
	if m.IssuerHub, err = FromUUID(p.IssuerHubId); err != nil {
		return m, err
	}
	if m.BootID, err = FromUUID(p.BootId); err != nil {
		return m, err
	}
	m.Generation, m.TTL = p.Generation, fromDur(p.Ttl)
	return m, nil
}

func Grants(s *types.GrantSet) *runtimev1.GrantSet {
	out := &runtimev1.GrantSet{}
	for _, g := range s.Run {
		out.RunGrants = append(out.RunGrants, &runtimev1.RunGrant{Meta: meta(g.Meta), RuleIds: uuids(g.RuleIDs)})
	}
	for _, g := range s.Billing {
		out.BillingGrants = append(out.BillingGrants, &runtimev1.BillingGrant{Meta: meta(g.Meta), RuleId: UUID(g.RuleID),
			BillingVersion: g.BillingVersion, RawBytes: g.RawBytes})
	}
	for _, g := range s.Shares {
		out.ShareLeases = append(out.ShareLeases, &runtimev1.ShareLease{Meta: meta(g.Meta), TenantId: UUID(g.TenantID),
			Kind: runtimev1.ShareKind(g.Kind), Amount: g.Amount})
	}
	return out
}

func FromGrants(p *runtimev1.GrantSet) (types.GrantSet, error) {
	var s types.GrantSet
	if p == nil {
		return s, nil
	}
	for _, g := range p.RunGrants {
		m, err := fromMeta(g.Meta)
		if err != nil {
			return s, err
		}
		rules, err := fromUUIDs(g.RuleIds)
		if err != nil {
			return s, err
		}
		s.Run = append(s.Run, types.RunGrant{Meta: m, RuleIDs: rules})
	}
	for _, g := range p.BillingGrants {
		m, err := fromMeta(g.Meta)
		if err != nil {
			return s, err
		}
		rule, err := FromUUID(g.RuleId)
		if err != nil {
			return s, err
		}
		s.Billing = append(s.Billing, types.BillingGrant{Meta: m, RuleID: rule, BillingVersion: g.BillingVersion, RawBytes: g.RawBytes})
	}
	for _, g := range p.ShareLeases {
		m, err := fromMeta(g.Meta)
		if err != nil {
			return s, err
		}
		t, err := FromUUID(g.TenantId)
		if err != nil {
			return s, err
		}
		s.Shares = append(s.Shares, types.ShareLease{Meta: m, TenantID: t, Kind: types.ShareKind(g.Kind), Amount: g.Amount})
	}
	return s, nil
}

// ---------------------------------------------------------------- 用量

func Usage(s types.UsageSnapshot) *runtimev1.UsageSnapshot {
	out := &runtimev1.UsageSnapshot{}
	for _, d := range s.Deltas {
		out.Deltas = append(out.Deltas, &runtimev1.UsageDelta{RuleId: UUID(d.RuleID), Role: commonv1.HopRole(d.Role),
			Direction: commonv1.Direction(d.Direction), RawBytes: d.RawBytes, BillingGrantId: UUID(d.BillingGrantID)})
	}
	for _, d := range s.Demands {
		out.Demands = append(out.Demands, &runtimev1.TenantDemand{TenantId: UUID(d.TenantID), Bytes_60S: d.Bytes60s, StarvedPermille: d.StarvedPermille})
	}
	for _, c := range s.Consumptions {
		out.GrantConsumptions = append(out.GrantConsumptions, &runtimev1.GrantConsumption{GrantId: UUID(c.GrantID),
			UsedRawBytes: c.UsedRawBytes, Final: c.Final})
	}
	return out
}

func FromUsage(p *runtimev1.UsageSnapshot) (types.UsageSnapshot, error) {
	var s types.UsageSnapshot
	if p == nil {
		return s, nil
	}
	for _, d := range p.Deltas {
		r, err := FromUUID(d.RuleId)
		if err != nil {
			return s, err
		}
		g, err := FromUUID(d.BillingGrantId)
		if err != nil {
			return s, err
		}
		s.Deltas = append(s.Deltas, types.UsageDelta{RuleID: r, Role: types.HopRole(d.Role), Direction: types.Direction(d.Direction),
			RawBytes: d.RawBytes, BillingGrantID: g})
	}
	for _, d := range p.Demands {
		t, err := FromUUID(d.TenantId)
		if err != nil {
			return s, err
		}
		s.Demands = append(s.Demands, types.TenantDemand{TenantID: t, Bytes60s: d.Bytes_60S, StarvedPermille: d.StarvedPermille})
	}
	for _, c := range p.GrantConsumptions {
		g, err := FromUUID(c.GrantId)
		if err != nil {
			return s, err
		}
		s.Consumptions = append(s.Consumptions, types.GrantConsumption{GrantID: g, UsedRawBytes: c.UsedRawBytes, Final: c.Final})
	}
	return s, nil
}

// ---------------------------------------------------------------- 应用结果与状态

func ref(o types.ObjectRef) *runtimev1.ObjectRef {
	return &runtimev1.ObjectRef{Kind: runtimev1.ObjectKind(o.Kind), Id: o.ID}
}

func Prepare(r *types.PrepareResult) *runtimev1.PrepareResult {
	out := &runtimev1.PrepareResult{Version: r.Version}
	for _, c := range r.Changes {
		out.Changes = append(out.Changes, &runtimev1.ObjectChange{Object: ref(c.Object), ChangeClass: runtimev1.ChangeClass(c.Class),
			AffectedSessions: c.AffectedSessions})
	}
	for _, e := range r.Errors {
		out.Errors = append(out.Errors, &runtimev1.ResourceError{Object: ref(e.Object), Code: e.Code, Detail: e.Detail})
	}
	return out
}

func objects(os []types.ObjectState) []*runtimev1.ObjectState {
	var out []*runtimev1.ObjectState
	for _, o := range os {
		out = append(out, &runtimev1.ObjectState{Object: ref(o.Object), ActualVersion: o.ActualVersion,
			Status: runtimev1.ObjectStatus(o.Status), ErrorCode: o.ErrorCode})
	}
	return out
}

func Apply(r *types.ApplyResult) *runtimev1.ApplyResult {
	return &runtimev1.ApplyResult{Version: r.Version, Objects: objects(r.Objects)}
}

func State(s types.RuntimeState) *runtimev1.RuntimeState {
	return &runtimev1.RuntimeState{ActualVersion: s.ActualVersion, Objects: objects(s.Objects)}
}

// ---------------------------------------------------------------- 事件

// Event 把引擎事件转为 EngineEvent；未知类型返回 nil。
func Event(e types.Event) *runtimev1.EngineEvent {
	out := &runtimev1.EngineEvent{MonotonicAt: durationpb.New(e.At)}
	var rules []*commonv1.Uuid
	if e.RuleID != (types.UUID{}) {
		rules = []*commonv1.Uuid{UUID(e.RuleID)}
	}
	switch e.Kind {
	case types.EventRestrictionStarted:
		out.Body = &runtimev1.EngineEvent_RestrictionStarted{RestrictionStarted: &runtimev1.RestrictionStarted{TenantId: UUID(e.TenantID), RuleIds: rules}}
	case types.EventRestrictionCompleted:
		out.Body = &runtimev1.EngineEvent_RestrictionCompleted{RestrictionCompleted: &runtimev1.RestrictionCompleted{TenantId: UUID(e.TenantID), RuleIds: rules}}
	case types.EventOpenRejected:
		out.Body = &runtimev1.EngineEvent_OpenRejected{OpenRejected: &runtimev1.OpenRejected{PeerNodeId: UUID(e.PeerID), RuleId: UUID(e.RuleID), Check: e.Detail}}
	case types.EventGrantLow:
		remaining, _ := strconv.ParseUint(e.Detail, 10, 64)
		out.Body = &runtimev1.EngineEvent_GrantLow{GrantLow: &runtimev1.GrantLow{GrantId: UUID(e.GrantID), RuleId: UUID(e.RuleID), RemainingRawBytes: remaining}}
	case types.EventGrantNeeded:
		out.Body = &runtimev1.EngineEvent_GrantNeeded{GrantNeeded: &runtimev1.GrantNeeded{RuleId: UUID(e.RuleID), TenantId: UUID(e.TenantID)}}
	case types.EventPeerDown:
		out.Body = &runtimev1.EngineEvent_PeerDown{PeerDown: &runtimev1.PeerDown{PeerNodeId: UUID(e.PeerID), Reason: e.Detail}}
	case types.EventTargetRejected:
		out.Body = &runtimev1.EngineEvent_TargetRejected{TargetRejected: &runtimev1.TargetRejected{RuleId: UUID(e.RuleID), ResolvedAddress: e.Detail}}
	default:
		return nil
	}
	return out
}
