// Package apply 是下发编排（BE-T15；RUL-010～RUL-013、7.5 节）：
//
//	Deploy：装载快照 → 编译 → 各节点配置内容变化时版本 +1 并写入 node_configs，经 Outbox 通知 grpcgw 下发；
//	        为每条规则的新期望版本建立一次编排（apply_runs），记录涉及的节点及其须达到的配置版本。
//	Advance：按节点报告推进——规则的全部节点都准备好后才允许提交（RUL-010 两阶段）；任一节点准备失败则该规则
//	        版本失败、只撤销本次准备（RUL-011：重新部署时该规则回到上一已生效版本，节点上的未决版本被新版本取代）；
//	        全部节点生效后规则 applied。端口预留随节点生效转为 occupied，释放中的端口在节点确认不再监听后 released（PRT-009）。
//	提交后（RUL-012、RUL-013）：部分节点已生效时 partially_applied，仍未生效的在线节点按 10 秒间隔重发提交；
//	        自进入提交起 PartialTimeout（120 秒）仍有在线节点未生效则回滚——以上一已生效版本的内容生成新的递增版本
//	        （rules.Rollback，RUL-016 重新校验），走同一两阶段流程，期间 rolling_back；回滚自身失败（校验不过、准备
//	        失败或再次超时）则 failed 并告警，不再自动回滚。没有上一已生效版本、或规则已删除时直接 failed 并告警。
//	        未生效的节点全部失联时 unknown（不回滚、不报成功），节点恢复并生效后转为 applied。
//	端口（PRT-007、PRT-009）：入口组成员变化后补齐新成员的预留、退出成员的预留转 releasing；准备失败回到上一已生效
//	        版本时，端口预留随之恢复为该版本的端口，失败版本新预留的端口转 releasing，节点确认不再监听后释放。
package apply

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	"vortex/backend/compiler"
	"vortex/backend/internal/db"
	p "vortex/backend/internal/platform"
	"vortex/backend/internal/ports"
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

// CommitRetry 是提交未生效的在线节点重发提交的最小间隔（RUL-012“重试收敛”；测试可调）。
var CommitRetry = 10 * time.Second

type Service struct {
	DB     *pgxpool.Pool
	Notify func() // Outbox 有新内容时唤醒 grpcgw 的投递
	Log    *slog.Logger

	mu        sync.Mutex
	rollbacks map[rollbackKey]bool // 回滚生成的（规则, 版本），部署时标记 is_rollback
	retried   map[p.UUID]time.Time // 节点 → 上次重发提交的时刻
}

type rollbackKey struct {
	rule    p.UUID
	version int64
}

func (s *Service) log() *slog.Logger {
	if s.Log == nil {
		return slog.New(slog.DiscardHandler)
	}
	return s.Log
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
		if err := s.syncEntryPorts(ctx, q); err != nil {
			return err
		}
		snap, err := rules.LoadSnapshot(ctx, q)
		if err != nil {
			return err
		}
		if err := s.fallBackFailed(ctx, q, snap); err != nil {
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
			s.mu.Lock()
			rb := s.rollbacks[rollbackKey{rid, r.DesiredVersion}]
			delete(s.rollbacks, rollbackKey{rid, r.DesiredVersion})
			s.mu.Unlock()
			if err := q.InsertApplyRun(ctx, db.InsertApplyRunParams{ID: run.PG(), TenantID: r.TenantID, RuleID: r.ID, Version: r.DesiredVersion,
				Phase: phase, DeadlineAt: p.TS(time.Now().Add(PartialTimeout)), FinishedAt: finished, IsRollback: rb}); err != nil {
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
				if err := q.SetRuleSync(ctx, db.SetRuleSyncParams{ID: r.ID, SyncStatus: ruleSync(phase, rb), DesiredVersion: r.DesiredVersion}); err != nil {
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
// 端口预留随之恢复为该版本的端口（失败版本新预留的转 releasing）。
func (s *Service) fallBackFailed(ctx context.Context, q *db.Queries, snap *compiler.Snapshot) error {
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
			old.Port = r.Port // 早期快照未记录端口：保持当前端口
		}
		if entry, err := rules.ParseUUID(string(spec.EntryGroup)); err == nil && old.Port != 0 {
			tenant := mustParse(string(r.Tenant))
			if _, err := ports.Reserve(ctx, q, tenant, id, entry, spec.Network, old.Port); err != nil {
				s.log().Warn("restore ports of the last applied version", "rule", id.String(), "port", old.Port, "err", err)
			}
		}
		kept = append(kept, old)
	}
	snap.Rules = kept
	return nil
}

func mustParse(s string) p.UUID { u, _ := rules.ParseUUID(s); return u }

// ruleSync 是编排阶段对应的规则同步状态：回滚编排进行中显示 rolling_back（7.5 节）。
func ruleSync(phase string, rollback bool) string {
	if rollback && (phase == "preparing" || phase == "committing" || phase == "partially_applied") {
		return "rolling_back"
	}
	return phase
}

// syncEntryPorts：入口组成员变化后补齐新成员的端口预留、退出成员的预留转 releasing（PRT-007）。新成员上端口不可用时
// 不静默排除（PRT-005）：记录告警，该成员的准备会失败，规则停留在上一已生效版本。
func (s *Service) syncEntryPorts(ctx context.Context, q *db.Queries) error {
	rs, err := q.RulesNeedingPortSync(ctx)
	if err != nil {
		return err
	}
	for _, r := range rs {
		if _, err := ports.Reserve(ctx, q, p.FromPG(r.TenantID), p.FromPG(r.ID), p.FromPG(r.EntryGroupID), r.Network, uint16(deref32(r.ServicePort))); err != nil {
			var ue *ports.UnavailableError
			if errors.As(err, &ue) || errors.Is(err, ports.ErrExhausted) {
				s.log().Warn("entry group changed: port unavailable on a member", "rule", p.FromPG(r.ID).String(), "err", err)
				continue
			}
			return err
		}
	}
	return nil
}

func deref32(v *int32) int32 {
	if v == nil {
		return 0
	}
	return *v
}

// AdvanceResult 是一次推进的摘要。
type AdvanceResult struct {
	Committed  map[p.UUID]uint64 // 本次下发 Commit 的节点 → 版本
	Applied    []p.UUID          // 本次完成的规则
	Failed     []p.UUID          // 本次失败的规则
	RolledBack []p.UUID          // 本次触发回滚的规则
}

type rollbackPlan struct {
	tenant, rule p.UUID
	from         int64 // 超时的版本
	to           int64 // 上一已生效版本
}

// Advance 按节点报告推进编排。任一规则失败时随后重新部署（回到上一已生效版本）；部分应用超时则回滚（RUL-012）。
func (s *Service) Advance(ctx context.Context) (*AdvanceResult, error) {
	res := &AdvanceResult{Committed: map[p.UUID]uint64{}}
	var plans []rollbackPlan
	var alerts []rollbackPlan
	err := p.InTx(ctx, s.DB, func(tx pgx.Tx) error {
		q := db.New(tx)
		plans, alerts = nil, nil
		res.Committed, res.Applied, res.Failed = map[p.UUID]uint64{}, nil, nil
		if err := q.LockDeploy(ctx); err != nil {
			return err
		}
		runs, err := q.ListOpenRuns(ctx)
		if err != nil {
			return err
		}
		now := time.Now()
		blocked := map[p.UUID]int64{} // 节点 → 仍在准备中的编排要求的最低版本（不得提交到 ≥ 该版本之前的内容）
		retry := map[p.UUID]int64{}   // 提交后仍未生效的在线节点 → 重发的版本
		for _, run := range runs {
			nodes, err := q.ListRunNodes(ctx, run.ID)
			if err != nil {
				return err
			}
			prepared, applied, failed := true, true, false
			anyApplied, missingOnline := false, 0
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
				if av >= req {
					anyApplied = true
					continue
				}
				applied = false
				if run.Phase == "preparing" {
					continue
				}
				// 已提交、尚未生效
				switch {
				case n.Lifecycle != "online":
					_ = q.SetRunNodeStatus(ctx, db.SetRunNodeStatusParams{RunID: run.ID, NodeID: n.NodeID, Status: "unknown"}) // RUL-013
				default:
					missingOnline++
					if n.LastError != nil && deref(n.CommitSent) >= req {
						_ = q.SetRunNodeStatus(ctx, db.SetRunNodeStatusParams{RunID: run.ID, NodeID: n.NodeID, Status: "commit_failed", ErrorCode: n.LastError})
					}
					if deref(n.CommitSent) >= req && deref(n.CommitSent) == deref(n.Version) {
						retry[p.FromPG(n.NodeID)] = deref(n.CommitSent)
					}
				}
			}
			phase := run.Phase
			switch {
			case phase == "preparing" && failed:
				phase = "failed"
				res.Failed = append(res.Failed, p.FromPG(run.RuleID))
			case phase == "preparing" && prepared:
				phase = "committing"
				if err := q.SetRunDeadline(ctx, db.SetRunDeadlineParams{ID: run.ID, DeadlineAt: p.TS(now.Add(PartialTimeout))}); err != nil {
					return err
				}
				run.DeadlineAt = p.TS(now.Add(PartialTimeout))
			}
			if phase != "preparing" && phase != "failed" && run.Phase != "preparing" { // 刚进入提交：本轮先下发 Commit
				switch {
				case applied:
					phase = "applied"
					res.Applied = append(res.Applied, p.FromPG(run.RuleID))
					for _, n := range nodes {
						av := n.AppliedVersion
						_ = q.SetRunNodeStatus(ctx, db.SetRunNodeStatusParams{RunID: run.ID, NodeID: n.NodeID, Status: "active", ActualVersion: av})
					}
				case missingOnline == 0:
					phase = "unknown" // 未生效的节点全部失联（RUL-013）：不回滚、不报成功
				case run.DeadlineAt.Valid && now.After(run.DeadlineAt.Time):
					phase = "failed" // 超时：回滚（RUL-012）
					plan := rollbackPlan{tenant: p.FromPG(run.TenantID), rule: p.FromPG(run.RuleID), from: run.Version}
					if run.IsRollback {
						alerts = append(alerts, plan) // 回滚自身失败：不再自动回滚
					} else {
						plans = append(plans, plan)
					}
				case anyApplied:
					phase = "partially_applied"
				default:
					phase = "committing"
				}
			}
			if phase == "failed" && run.Phase == "preparing" && run.IsRollback {
				alerts = append(alerts, rollbackPlan{tenant: p.FromPG(run.TenantID), rule: p.FromPG(run.RuleID), from: run.Version})
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
				if err := q.SetRuleSync(ctx, db.SetRuleSyncParams{ID: run.RuleID, SyncStatus: ruleSync(phase, run.IsRollback), DesiredVersion: run.Version}); err != nil {
					return err
				}
			}
		}
		for i := range plans { // 回滚的目标：上一已生效版本
			good, err := q.LastAppliedVersion(ctx, plans[i].rule.PG())
			if err != nil {
				return err
			}
			plans[i].to = good
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
			if err := commit(ctx, q, n, pv); err != nil {
				return err
			}
			res.Committed[n] = uint64(pv)
		}
		// 重试收敛：已提交仍未生效的在线节点，按间隔重发同一提交
		s.mu.Lock()
		if s.retried == nil {
			s.retried = map[p.UUID]time.Time{}
		}
		var resend []p.UUID
		for n := range retry {
			if last, ok := s.retried[n]; !ok || now.Sub(last) >= CommitRetry {
				if ok { // 第一次下发不算重试
					resend = append(resend, n)
				}
				s.retried[n] = now
			}
		}
		s.mu.Unlock()
		for _, n := range resend {
			if err := commit(ctx, q, n, retry[n]); err != nil {
				return err
			}
			res.Committed[n] = uint64(retry[n])
		}
		return settlePorts(ctx, q, ncs)
	})
	if err != nil {
		return nil, err
	}
	if len(res.Committed) > 0 {
		s.notify()
	}
	for _, a := range alerts {
		s.alert(ctx, a, "rollback failed; an administrator must resolve it")
	}
	rs := &rules.Service{DB: s.DB}
	for _, pl := range plans {
		if pl.to == 0 {
			s.alert(ctx, pl, "partial apply timed out and there is no applied version to roll back to")
			continue
		}
		s.mu.Lock()
		if s.rollbacks == nil {
			s.rollbacks = map[rollbackKey]bool{}
		}
		s.mu.Unlock()
		saved, err := rs.Rollback(ctx, pl.tenant, pl.rule, uint64(pl.to))
		if err != nil {
			s.alert(ctx, pl, "rollback rejected: "+err.Error()) // RUL-016 校验不过，或规则已删除
			continue
		}
		s.mu.Lock()
		s.rollbacks[rollbackKey{pl.rule, int64(saved.Version)}] = true
		s.mu.Unlock()
		res.RolledBack = append(res.RolledBack, pl.rule)
		s.log().Warn("partial apply timed out; rolling back", "rule", pl.rule.String(), "from", pl.from, "to", pl.to, "version", saved.Version)
	}
	if len(res.Failed) > 0 || len(res.RolledBack) > 0 {
		if _, err := s.Deploy(ctx); err != nil {
			return res, err
		}
	}
	return res, nil
}

func commit(ctx context.Context, q *db.Queries, n p.UUID, v int64) error {
	payload := make([]byte, 24)
	copy(payload, n[:])
	binary.BigEndian.PutUint64(payload[16:], uint64(v))
	return q.InsertOutbox(ctx, db.InsertOutboxParams{ID: p.NewUUIDv7().PG(), Topic: TopicCommit, Key: n.String(), Payload: payload})
}

// alert：回滚无法进行或回滚自身失败——规则置为 failed 并写审计，由管理员处理（RUL-012）。
func (s *Service) alert(ctx context.Context, pl rollbackPlan, msg string) {
	s.log().Error("rule apply needs an administrator", "rule", pl.rule.String(), "version", pl.from, "reason", msg)
	q := db.New(s.DB) // 规则的同步状态已随编排置为 failed
	detail, _ := json.Marshal(map[string]any{"version": pl.from, "reason": msg})
	kind, target := "rule", pl.rule.String()
	if err := q.InsertAudit(ctx, db.InsertAuditParams{ActorKind: "system", TenantID: pl.tenant.PG(), Action: "rule.apply_failed",
		TargetKind: &kind, TargetID: &target, Detail: detail}); err != nil {
		s.log().Warn("audit", "err", err)
	}
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
