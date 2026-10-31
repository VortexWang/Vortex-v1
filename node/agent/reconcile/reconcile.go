// Package reconcile 是 Agent 的配置协调（AGT-004、AGT-010，02 方案 3.4）。
//
// 两阶段的编排在后端（RUL-010～013），Agent 只执行并如实上报：
//
//	DesiredConfig(v) → Engine.Prepare → 上报 PrepareResult（含资源错误）
//	Commit(v) / Abort(v) → Engine.Commit / Abort → Commit 成功后持久化为“最后生效配置” → 上报 ApplyResult
//
// 新的期望版本到达时若已有未决版本，先 Abort 旧的（它已被后端取代）。
//
// 启动恢复（AGT-010）：从本地恢复最后生效配置 → Prepare（占端口）→ 状态“授权待确认”，不转发；
// 取得运行授权（配置中的全部规则都有，或签发方已对启动时的申请作答——被拒的规则由 Engine 自行限制）
// 后才 Commit。恢复期间收到新的期望版本时放弃恢复，按正常流程处理。
package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"vortex/engine"
	"vortex/engine/types"
	"vortex/node/agent/store"
)

const kindApplied = "config.applied"

// Phase 是协调状态（status 子命令）。
type Phase uint8

const (
	PhaseIdle         Phase = iota // 没有任何配置
	PhaseGrantPending              // 启动恢复：已 Prepare，等待运行授权（“授权待确认”）
	PhasePrepared                  // 已 Prepare，等待 Commit / Abort
	PhaseApplied                   // 已生效，无未决版本
)

func (p Phase) String() string {
	return [...]string{"idle", "grant_pending", "prepared", "applied"}[p]
}

// Report 是要上报给 Edge Hub 的结果。
type Report struct {
	Prepare *types.PrepareResult
	Apply   *types.ApplyResult
	Action  Action
	Version uint64
	Err     string // Engine 返回错误（配置无效、版本过旧等）
}

type Action uint8

const (
	ActionNone Action = iota
	ActionCommit
	ActionAbort
)

// Reconciler 驱动 Engine 的配置应用。方法不可并发调用（由 Agent 的事件循环串行调用）。
type Reconciler struct {
	mu       sync.Mutex
	eng      engine.Engine
	log      store.Log
	applied  *types.RuntimeConfig
	pending  *types.RuntimeConfig
	recovery bool // pending 来自启动恢复
}

func New(eng engine.Engine, log store.Log) *Reconciler { return &Reconciler{eng: eng, log: log} }

// Recover 在启动时调用：恢复最后生效配置并 Prepare。返回 Prepare 结果（无配置时为 nil）。
func (r *Reconciler) Recover(ctx context.Context) (*types.PrepareResult, error) {
	var last *types.RuntimeConfig
	err := r.log.Load(func(rec store.Record) error {
		if rec.Kind != kindApplied {
			return nil
		}
		var c types.RuntimeConfig
		if err := json.Unmarshal(rec.Data, &c); err != nil {
			return err
		}
		last = &c
		return nil
	})
	if err != nil || last == nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.applied = last
	pr, err := r.eng.Prepare(ctx, last)
	if err != nil {
		return nil, err
	}
	r.pending, r.recovery = last, true
	return pr, nil
}

// Desired 处理 DesiredConfig。
func (r *Reconciler) Desired(ctx context.Context, cfg *types.RuntimeConfig) Report {
	r.mu.Lock()
	defer r.mu.Unlock()
	rep := Report{Version: cfg.Version}
	if r.pending != nil {
		if r.pending.Version == cfg.Version && !r.recovery {
			return Report{} // 重复下发（例如重连后重发）：结果已上报过，由后端按版本去重
		}
		if err := r.eng.Abort(ctx, r.pending.Version); err != nil && !errors.Is(err, engine.ErrNoPending) {
			rep.Err = err.Error()
			return rep
		}
		r.pending, r.recovery = nil, false
	}
	pr, err := r.eng.Prepare(ctx, cfg)
	if err != nil {
		rep.Err = err.Error()
		return rep
	}
	r.pending = cfg
	rep.Prepare = pr
	return rep
}

func (r *Reconciler) commitLocked(ctx context.Context, version uint64) Report {
	rep := Report{Version: version, Action: ActionCommit}
	if r.pending == nil || r.pending.Version != version {
		rep.Err = engine.ErrNoPending.Error()
		return rep
	}
	ar, err := r.eng.Commit(ctx, version)
	if err != nil {
		rep.Err = err.Error()
		return rep
	}
	cfg := r.pending
	r.pending, r.recovery = nil, false
	r.applied = cfg
	rec, err := store.Make(kindApplied, cfg)
	if err == nil {
		err = r.log.Rewrite([]store.Record{rec}) // 只保留最后生效配置
	}
	if err != nil {
		rep.Err = "persist applied config: " + err.Error() // 已生效，但重启后会恢复旧版本：上报给后端
	}
	rep.Apply = ar
	return rep
}

// Command 处理 ApplyCommand。恢复中的版本不接受外部 Commit（由授权到达触发）；Abort 视为放弃恢复。
func (r *Reconciler) Command(ctx context.Context, version uint64, a Action) Report {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.recovery && a == ActionCommit {
		return Report{Version: version, Action: a, Err: "recovering: commit follows run grants"}
	}
	switch a {
	case ActionCommit:
		return r.commitLocked(ctx, version)
	case ActionAbort:
		rep := Report{Version: version, Action: a}
		if r.pending == nil || r.pending.Version != version {
			rep.Err = engine.ErrNoPending.Error()
			return rep
		}
		if err := r.eng.Abort(ctx, version); err != nil {
			rep.Err = err.Error()
			return rep
		}
		r.pending, r.recovery = nil, false
		return rep
	}
	return Report{Version: version, Err: "unknown action"}
}

// GrantsUpdated 在授权变化时调用。恢复中且全部规则都有运行授权（covered），或签发方已对启动时的申请作答
// （answered），则 Commit 恢复的配置并返回结果；否则返回 nil。
func (r *Reconciler) GrantsUpdated(ctx context.Context, covered func(rule types.UUID) bool, answered bool) *Report {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.recovery || r.pending == nil {
		return nil
	}
	if !answered {
		for _, rule := range r.pending.Rules {
			if !covered(rule.RuleID) {
				return nil
			}
		}
	}
	rep := r.commitLocked(ctx, r.pending.Version)
	return &rep
}

// Phase 返回当前状态。
func (r *Reconciler) Phase() Phase {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch {
	case r.recovery:
		return PhaseGrantPending
	case r.pending != nil:
		return PhasePrepared
	case r.applied != nil:
		return PhaseApplied
	}
	return PhaseIdle
}

// AppliedVersion 返回最后生效的配置版本（AgentHello.applied_config_version；恢复中为恢复的版本）。
func (r *Reconciler) AppliedVersion() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.applied == nil {
		return 0
	}
	return r.applied.Version
}

// Applied 返回最后生效的配置。
func (r *Reconciler) Applied() *types.RuntimeConfig {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.applied
}
