// Package apply 是下发编排（BE-T15；RUL-010～RUL-013、7.5 节）：
//
//	Deploy：装载快照 → 编译 → 各节点配置内容变化时版本 +1 并写入 node_configs，经 Outbox 通知 grpcgw 下发；
//	        为每条规则的新期望版本建立一次编排（apply_runs），记录涉及的节点及其须达到的配置版本。
//	Advance：按节点报告推进——规则的全部节点都准备好后才允许提交（RUL-010 两阶段）；任一节点准备失败则该规则
//	        版本失败、只撤销本次准备（RUL-011：重新部署时该规则回到上一已生效版本，节点上的未决版本被新版本取代）；
//	        全部节点生效后规则 applied。端口预留随节点生效转为 occupied，释放中的端口在节点确认不再监听后 released（PRT-009）。
//
// 本批未包含：部分应用 120 秒回滚与失联节点的 unknown（RUL-012、RUL-013），在下一批实现。
package apply

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	"vortex/backend/compiler"
	"vortex/backend/internal/db"
	p "vortex/backend/internal/platform"
	"vortex/backend/internal/rules"
	runtimev1 "vortex/proto/gen/go/vortex/runtime/v1"
)

// Outbox 主题（grpcgw 消费）。
const (
	TopicDesired = "node.desired_config" // 负载：node_id（16 字节）
	TopicCommit  = "node.apply_command"  // 负载：node_id（16 字节）+ version（8 字节，大端）
)

// PartialTimeout 是部分应用的回滚期限（RUL-012，附录 A 默认 120 秒）；记录在 apply_runs.deadline_at。
const PartialTimeout = 120 * time.Second

type Service struct {
	DB     *pgxpool.Pool
	Notify func() // Outbox 有新内容时唤醒 grpcgw 的投递
}

func (s *Service) notify() {
	if s.Notify != nil {
		s.Notify()
	}
}

// DeployResult 是一次部署的摘要。
type DeployResult struct {
	Changed  map[p.UUID]uint64 // 配置变化的节点 → 新版本
	Problems map[compiler.UUID][]compiler.Problem
	Runs     int // 新建的编排数
}

func digest(c *runtimev1.RuntimeConfig) []byte {
	cc := proto.Clone(c).(*runtimev1.RuntimeConfig)
	cc.Version = 0
	b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(cc)
	h := sha256.Sum256(b)
	return h[:]
}

func nodeID(c *runtimev1.RuntimeConfig) p.UUID {
	var u p.UUID
	copy(u[:], c.GetNodeId().GetValue())
	return u
}

// rulesIn 返回配置中出现的规则（含监听引用的规则）。
func rulesIn(c *runtimev1.RuntimeConfig) map[p.UUID]bool {
	out := map[p.UUID]bool{}
	for _, r := range c.GetRules() {
		var u p.UUID
		copy(u[:], r.GetRuleId().GetValue())
		out[u] = true
	}
	return out
}

// Deploy 编译当前期望状态并写入各节点配置与编排记录。并发的部署由事务级咨询锁串行化。
func (s *Service) Deploy(ctx context.Context) (*DeployResult, error) {
	res := &DeployResult{Changed: map[p.UUID]uint64{}}
	err := p.InTx(ctx, s.DB, func(tx pgx.Tx) error {
		q := db.New(tx)
		if err := q.LockDeploy(ctx); err != nil {
			return err
		}
		snap, err := rules.LoadSnapshot(ctx, q)
		if err != nil {
			return err
		}
		if err := fallBackFailed(ctx, q, snap); err != nil {
			return err
		}
		existing := map[p.UUID]db.NodeConfig{}
		ncs, err := q.ListNodeConfigs(ctx)
		if err != nil {
			return err
		}
		for _, nc := range ncs {
			existing[p.FromPG(nc.NodeID)] = nc
		}
		for id := range snap.Nodes {
			u, _ := rules.ParseUUID(string(id))
			snap.NodeVersions[id] = uint64(existing[u].Version) + 1
		}
		out := compiler.Compile(snap)
		res.Problems = out.Problems
		cur := map[p.UUID]uint64{}
		now := map[p.UUID]map[p.UUID]bool{} // 节点 → 本次配置中的规则
		ids := make([]compiler.UUID, 0, len(out.Configs))
		for id := range out.Configs {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		for _, id := range ids {
			c := out.Configs[id]
			n := nodeID(c)
			now[n] = rulesIn(c)
			d := digest(c)
			if old, ok := existing[n]; ok && string(old.ContentDigest) == string(d) {
				cur[n] = uint64(old.Version)
				continue
			}
			raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(c)
			if err != nil {
				return err
			}
			if err := q.UpsertNodeConfig(ctx, db.UpsertNodeConfigParams{NodeID: n.PG(), Version: int64(c.Version), Config: raw, ContentDigest: d}); err != nil {
				return err
			}
			if err := q.InsertOutbox(ctx, db.InsertOutboxParams{ID: p.NewUUIDv7().PG(), Topic: TopicDesired, Key: n.String(), Payload: n[:]}); err != nil {
				return err
			}
			cur[n] = c.Version
			res.Changed[n] = c.Version
		}
		// 上一份配置中出现过的规则（用于规则移出节点：删除、停用、成员变化）
		before := map[p.UUID]map[p.UUID]bool{}
		for n, nc := range existing {
			var c runtimev1.RuntimeConfig
			if proto.Unmarshal(nc.Config, &c) == nil {
				before[n] = rulesIn(&c)
			}
		}
		all, err := q.ListRulesForDeploy(ctx)
		if err != nil {
			return err
		}
		for _, r := range all {
			rid := p.FromPG(r.ID)
			if _, err := q.GetApplyRun(ctx, db.GetApplyRunParams{RuleID: r.ID, Version: r.DesiredVersion}); err == nil {
				continue // 该期望版本已有编排
			} else if !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
			var nodes []p.UUID
			for n := range cur {
				if now[n][rid] || before[n][rid] {
					nodes = append(nodes, n)
				}
			}
			sort.Slice(nodes, func(i, j int) bool { return string(nodes[i][:]) < string(nodes[j][:]) })
			phase := "preparing"
			var finished = p.TS(time.Time{})
			finished.Valid = false
			if probs := out.Problems[rules.CID(rid)]; len(probs) > 0 {
				phase, finished = "failed", p.TS(time.Now())
			} else if len(nodes) == 0 {
				phase, finished = "applied", p.TS(time.Now())
			}
			run := p.NewUUIDv7()
			if err := q.InsertApplyRun(ctx, db.InsertApplyRunParams{ID: run.PG(), TenantID: r.TenantID, RuleID: r.ID, Version: r.DesiredVersion,
				Phase: phase, DeadlineAt: p.TS(time.Now().Add(PartialTimeout)), FinishedAt: finished}); err != nil {
				return err
			}
			if phase == "preparing" || phase == "failed" { // 失败的版本同样不得提交（随后重新部署取代它）
				for _, n := range nodes {
					v := int64(cur[n])
					if err := q.InsertApplyNodeState(ctx, db.InsertApplyNodeStateParams{RunID: run.PG(), NodeID: n.PG(), RequiredVersion: &v}); err != nil {
						return err
					}
				}
			}
			if !r.DeletedAt.Valid {
				if err := q.SetRuleSync(ctx, db.SetRuleSyncParams{ID: r.ID, SyncStatus: phase, DesiredVersion: r.DesiredVersion}); err != nil {
					return err
				}
			}
			res.Runs++
		}
		return nil
	})
	if err == nil && len(res.Changed) > 0 {
		s.notify()
	}
	return res, err
}

// fallBackFailed：期望版本的编排已失败的规则，改为编译上一已生效版本（没有则不编译）——只撤销本次准备（RUL-011）。
func fallBackFailed(ctx context.Context, q *db.Queries, snap *compiler.Snapshot) error {
	kept := snap.Rules[:0]
	for _, r := range snap.Rules {
		id, _ := rules.ParseUUID(string(r.ID))
		last, err := q.LatestApplyRun(ctx, id.PG())
		if errors.Is(err, pgx.ErrNoRows) || (err == nil && (uint64(last.Version) != r.Version || last.Phase != "failed")) {
			kept = append(kept, r)
			continue
		} else if err != nil {
			return err
		}
		good, err := q.LastAppliedVersion(ctx, id.PG())
		if err != nil {
			return err
		}
		if good == 0 {
			continue
		}
		v, err := q.GetRuleVersion(ctx, db.GetRuleVersionParams{RuleID: id.PG(), Version: good})
		if err != nil {
			return err
		}
		var spec rules.Spec
		if err := json.Unmarshal(v.Snapshot, &spec); err != nil {
			return err
		}
		old := rules.ToCompiler(id, mustParse(string(r.Tenant)), uint64(good), 0, spec)
		if old.Port == 0 {
			old.Port = r.Port // 自动分配：修改时保持原端口
		}
		kept = append(kept, old)
	}
	snap.Rules = kept
	return nil
}

func mustParse(s string) p.UUID { u, _ := rules.ParseUUID(s); return u }

// AdvanceResult 是一次推进的摘要。
type AdvanceResult struct {
	Committed map[p.UUID]uint64 // 本次下发 Commit 的节点 → 版本
	Applied   []p.UUID          // 本次完成的规则
	Failed    []p.UUID          // 本次失败的规则
}

// Advance 按节点报告推进编排。任一规则失败时随后重新部署（回到上一已生效版本）。
func (s *Service) Advance(ctx context.Context) (*AdvanceResult, error) {
	res := &AdvanceResult{Committed: map[p.UUID]uint64{}}
	err := p.InTx(ctx, s.DB, func(tx pgx.Tx) error {
		q := db.New(tx)
		if err := q.LockDeploy(ctx); err != nil {
			return err
		}
		runs, err := q.ListOpenRuns(ctx)
		if err != nil {
			return err
		}
		blocked := map[p.UUID]int64{} // 节点 → 仍在准备中的编排要求的最低版本（不得提交到 ≥ 该版本之前的内容）
		for _, run := range runs {
			nodes, err := q.ListRunNodes(ctx, run.ID)
			if err != nil {
				return err
			}
			prepared, applied, failed := true, true, false
			for _, n := range nodes {
				req := deref(n.RequiredVersion)
				pv, av := deref(n.PreparedVersion), deref(n.AppliedVersion)
				if pv < req && av < req {
					prepared = false
					if n.LastError != nil && deref(n.Version) == req {
						failed = true
						_ = q.SetRunNodeStatus(ctx, db.SetRunNodeStatusParams{RunID: run.ID, NodeID: n.NodeID, Status: "prepare_failed", ErrorCode: n.LastError})
					}
				}
				if av < req {
					applied = false
				}
			}
			phase := run.Phase
			switch {
			case phase == "preparing" && failed:
				phase = "failed"
				res.Failed = append(res.Failed, p.FromPG(run.RuleID))
			case phase == "preparing" && prepared:
				phase = "committing"
			}
			if phase == "committing" && applied {
				phase = "applied"
				res.Applied = append(res.Applied, p.FromPG(run.RuleID))
				for _, n := range nodes {
					av := n.AppliedVersion
					_ = q.SetRunNodeStatus(ctx, db.SetRunNodeStatusParams{RunID: run.ID, NodeID: n.NodeID, Status: "active", ActualVersion: av})
				}
			}
			if phase == "preparing" || phase == "failed" { // 失败的版本同样不得提交（随后重新部署取代它）
				for _, n := range nodes {
					nid := p.FromPG(n.NodeID)
					if req := deref(n.RequiredVersion); blocked[nid] == 0 || req < blocked[nid] {
						blocked[nid] = req
					}
				}
			}
			if phase != run.Phase {
				if err := q.SetRunPhase(ctx, db.SetRunPhaseParams{ID: run.ID, Phase: phase}); err != nil {
					return err
				}
				if err := q.SetRuleSync(ctx, db.SetRuleSyncParams{ID: run.RuleID, SyncStatus: phase, DesiredVersion: run.Version}); err != nil {
					return err
				}
			}
		}
		// 提交：节点已准备好最新版本、尚未提交，且没有仍在准备中的编排要求不低于该版本的内容
		ncs, err := q.ListNodeConfigs(ctx)
		if err != nil {
			return err
		}
		for _, nc := range ncs {
			n := p.FromPG(nc.NodeID)
			pv := deref(nc.PreparedVersion)
			if pv != nc.Version || deref(nc.CommitSent) >= pv {
				continue
			}
			if b, ok := blocked[n]; ok && b <= pv {
				continue
			}
			if err := q.MarkCommitSent(ctx, db.MarkCommitSentParams{NodeID: nc.NodeID, CommitSent: &pv}); err != nil {
				return err
			}
			payload := make([]byte, 24)
			copy(payload, n[:])
			binary.BigEndian.PutUint64(payload[16:], uint64(pv))
			if err := q.InsertOutbox(ctx, db.InsertOutboxParams{ID: p.NewUUIDv7().PG(), Topic: TopicCommit, Key: n.String(), Payload: payload}); err != nil {
				return err
			}
			res.Committed[n] = uint64(pv)
		}
		return settlePorts(ctx, q, ncs)
	})
	if err != nil {
		return nil, err
	}
	if len(res.Committed) > 0 {
		s.notify()
	}
	if len(res.Failed) > 0 {
		if _, err := s.Deploy(ctx); err != nil {
			return res, err
		}
	}
	return res, nil
}

// settlePorts：节点已生效的配置中有该监听 → reserved 转 occupied；没有 → releasing 转 released（PRT-009 节点确认）。
// 只依据“已生效版本 = 最新编译版本”的节点（此时已生效的内容即 node_configs.config）。
func settlePorts(ctx context.Context, q *db.Queries, ncs []db.NodeConfig) error {
	active := map[p.UUID]map[[3]any]bool{}
	for _, nc := range ncs {
		if deref(nc.AppliedVersion) != nc.Version {
			continue
		}
		var c runtimev1.RuntimeConfig
		if proto.Unmarshal(nc.Config, &c) != nil {
			continue
		}
		set := map[[3]any]bool{}
		for _, l := range c.GetListeners() {
			var r p.UUID
			copy(r[:], l.GetRuleId().GetValue())
			net := "tcp"
			if l.GetNetwork() == 2 {
				net = "udp"
			}
			set[[3]any{r, net, int32(l.GetPort())}] = true
		}
		active[p.FromPG(nc.NodeID)] = set
	}
	allocs, err := q.ListAllocationsInFlux(ctx)
	if err != nil {
		return err
	}
	for _, a := range allocs {
		set, ok := active[p.FromPG(a.NodeID)]
		if !ok {
			continue
		}
		listening := set[[3]any{p.FromPG(a.RuleID), a.Network, a.Port}]
		switch {
		case a.State == "reserved" && listening:
			err = q.SetAllocationState(ctx, db.SetAllocationStateParams{ID: a.ID, State: "occupied"})
		case a.State == "releasing" && !listening:
			err = q.SetAllocationState(ctx, db.SetAllocationStateParams{ID: a.ID, State: "released"})
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func deref(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

// ---------------------------------------------------------------- 节点报告（grpcgw 调用）

// Prepared 记录节点 Prepare 的结果；errs 为空表示成功。
func (s *Service) Prepared(ctx context.Context, node p.UUID, version uint64, errs []string) error {
	q := db.New(s.DB)
	if len(errs) > 0 {
		msg := ""
		for i, e := range errs {
			if i > 0 {
				msg += "; "
			}
			msg += e
		}
		return q.NodeApplyFailed(ctx, db.NodeApplyFailedParams{NodeID: node.PG(), LastError: &msg})
	}
	return q.NodePrepared(ctx, db.NodePreparedParams{NodeID: node.PG(), Version: int64(version)})
}

// Applied 记录节点 Commit 的结果。
func (s *Service) Applied(ctx context.Context, node p.UUID, version uint64, ok bool, detail string) error {
	q := db.New(s.DB)
	if !ok {
		return q.NodeApplyFailed(ctx, db.NodeApplyFailedParams{NodeID: node.PG(), LastError: &detail})
	}
	return q.NodeApplied(ctx, db.NodeAppliedParams{NodeID: node.PG(), Version: int64(version)})
}
