package rules

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"vortex/backend/compiler"
	"vortex/backend/internal/db"
	p "vortex/backend/internal/platform"
)

// LoadSnapshot 从数据库装载编译快照（不含 NodeVersions：由下发编排填写）。规则内容取自各规则当前期望版本的
// 快照，路径代次取期望版本（BE-T15 引入成员变化时的代次递增）。权益暂按“无反向建链权益”（BE-T16 接入套餐）。
func LoadSnapshot(ctx context.Context, q *db.Queries) (*compiler.Snapshot, error) {
	s := &compiler.Snapshot{Groups: map[compiler.UUID]compiler.Group{}, Nodes: map[compiler.UUID]compiler.Node{},
		NodeVersions: map[compiler.UUID]uint64{}, Entitlements: map[compiler.UUID]compiler.Entitled{}}
	gen, err := q.CurrentGeneration(ctx)
	if err != nil {
		return nil, err
	}
	s.Generation = uint64(gen)
	var bvID p.UUID
	if bv, err := q.CurrentBillingVersion(ctx); err == nil {
		s.BillingVersion, bvID = uint64(bv.Version), p.FromPG(bv.ID)
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	mult := map[p.UUID]uint64{}
	if bvID != (p.UUID{}) {
		ms, err := q.ListGroupMultipliers(ctx, bvID.PG())
		if err != nil {
			return nil, err
		}
		for _, m := range ms {
			mult[p.FromPG(m.NodeGroupID)] = uint64(m.MultiplierMilli)
		}
	}
	links, err := q.ListLinkPorts(ctx)
	if err != nil {
		return nil, err
	}
	linkPort := map[p.UUID]uint16{}
	for _, l := range links {
		if _, ok := linkPort[p.FromPG(l.NodeID)]; !ok {
			linkPort[p.FromPG(l.NodeID)] = uint16(l.PortMin)
		}
	}
	nodes, err := q.ListAllNodes(ctx)
	if err != nil {
		return nil, err
	}
	for _, n := range nodes {
		id := p.FromPG(n.ID)
		if n.DeletedAt.Valid {
			s.Revocations.NodeIDs = append(s.Revocations.NodeIDs, CID(id)) // 已删除的节点：身份吊销（SEC-002）
			continue
		}
		cn := compiler.Node{LinkPort: linkPort[id], Disabled: n.Lifecycle == "disabled", PublicAddresses: []string{}}
		if n.TenantID.Valid {
			cn.Tenant = CID(p.FromPG(n.TenantID))
		}
		for _, a := range n.PublicAddresses {
			cn.PublicAddresses = append(cn.PublicAddresses, a.String())
		}
		s.Nodes[CID(id)] = cn
	}
	groups, err := q.ListAllGroups(ctx)
	if err != nil {
		return nil, err
	}
	for _, g := range groups {
		id := p.FromPG(g.ID)
		var hp struct {
			IntervalS     uint32 `json:"interval_s"`
			FailThreshold uint32 `json:"fail_threshold"`
			RecoverThresh uint32 `json:"recover_threshold"`
		}
		_ = json.Unmarshal(g.HealthParams, &hp)
		cg := compiler.Group{Purpose: g.Purpose, Schedule: g.MemberSchedule, MultiplierMilli: mult[id], ProbeIntervalS: hp.IntervalS,
			FailThreshold: hp.FailThreshold, RecoverThresh: hp.RecoverThresh}
		if g.TenantID.Valid {
			cg.Tenant = CID(p.FromPG(g.TenantID)) // 个人组倍率固定为 0（NOD-014）：数据库不允许为个人组登记倍率
		}
		s.Groups[CID(id)] = cg
	}
	members, err := q.ListAllGroupMembers(ctx)
	if err != nil {
		return nil, err
	}
	for _, m := range members {
		gid := CID(p.FromPG(m.NodeGroupID))
		g, ok := s.Groups[gid]
		if !ok {
			continue
		}
		g.Members = append(g.Members, compiler.Member{Node: CID(p.FromPG(m.NodeID)), Weight: uint32(m.Weight), Priority: uint32(m.Priority)})
		s.Groups[gid] = g
	}
	permits, err := q.ListPermits(ctx)
	if err != nil {
		return nil, err
	}
	for _, pm := range permits {
		s.Permits = append(s.Permits, compiler.Permit{Upstream: CID(p.FromPG(pm.UpstreamGroupID)), Downstream: CID(p.FromPG(pm.DownstreamGroupID))})
	}
	ver, serials, err := revocations(ctx, q)
	if err != nil {
		return nil, err
	}
	s.Revocations.Serial = ver
	for _, b := range serials {
		s.Revocations.CertSerials = append(s.Revocations.CertSerials, hex.EncodeToString(b))
	}
	rules, err := q.ListAllActiveRules(ctx)
	if err != nil {
		return nil, err
	}
	for _, r := range rules {
		v, err := q.GetRuleVersion(ctx, db.GetRuleVersionParams{RuleID: r.ID, Version: r.DesiredVersion})
		if err != nil {
			return nil, err
		}
		var spec Spec
		if err := json.Unmarshal(v.Snapshot, &spec); err != nil {
			return nil, err
		}
		s.Rules = append(s.Rules, ToCompiler(p.FromPG(r.ID), p.FromPG(r.TenantID), uint64(r.DesiredVersion), 0, spec))
	}
	return s, nil
}

func revocations(ctx context.Context, q *db.Queries) (uint64, [][]byte, error) {
	v, err := q.CurrentRevocationSerial(ctx)
	if err != nil {
		return 0, nil, err
	}
	ss, err := q.ListRevokedSerials(ctx)
	return uint64(v), ss, err
}
