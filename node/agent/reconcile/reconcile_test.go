package reconcile

import (
	"context"
	"errors"
	"testing"

	"vortex/engine"
	"vortex/engine/types"
	"vortex/node/agent/store"
)

// fakeEngine 记录调用，并按 engine.Runtime 的版本规则返回错误。
type fakeEngine struct {
	engine.Engine
	calls   []string
	active  uint64
	pending uint64
	resErr  bool
}

func (f *fakeEngine) Prepare(_ context.Context, c *types.RuntimeConfig) (*types.PrepareResult, error) {
	f.calls = append(f.calls, "prepare")
	if f.pending != 0 {
		return nil, engine.ErrPendingVersion
	}
	if c.Version <= f.active {
		return nil, engine.ErrStaleVersion
	}
	f.pending = c.Version
	pr := &types.PrepareResult{Version: c.Version}
	if f.resErr {
		pr.Errors = []types.ResourceError{{Code: "PORT_IN_USE"}}
	}
	return pr, nil
}

func (f *fakeEngine) Commit(_ context.Context, v uint64) (*types.ApplyResult, error) {
	f.calls = append(f.calls, "commit")
	if v != f.pending {
		return nil, engine.ErrNoPending
	}
	f.active, f.pending = v, 0
	return &types.ApplyResult{Version: v}, nil
}

func (f *fakeEngine) Abort(_ context.Context, v uint64) error {
	f.calls = append(f.calls, "abort")
	if v != f.pending {
		return engine.ErrNoPending
	}
	f.pending = 0
	return nil
}

func cfg(v uint64, rules ...types.UUID) *types.RuntimeConfig {
	c := &types.RuntimeConfig{Version: v}
	for _, r := range rules {
		c.Rules = append(c.Rules, types.Rule{RuleID: r})
	}
	return c
}

var rA, rB = types.UUID{0xa}, types.UUID{0xb}

func TestTwoPhase(t *testing.T) {
	e, log := &fakeEngine{}, &store.Mem{}
	r := New(e, log)
	ctx := context.Background()
	if rep := r.Desired(ctx, cfg(1, rA)); rep.Prepare == nil || rep.Err != "" || r.Phase() != PhasePrepared {
		t.Fatalf("prepare %+v", rep)
	}
	if rep := r.Desired(ctx, cfg(1, rA)); rep.Prepare != nil || rep.Err != "" {
		t.Fatal("duplicate desired re-prepared")
	}
	if rep := r.Command(ctx, 2, ActionCommit); rep.Err == "" || rep.Apply != nil {
		t.Fatal("commit of a version that is not prepared")
	}
	// 新的期望版本取代未决版本：先 Abort
	if rep := r.Desired(ctx, cfg(2, rA)); rep.Prepare == nil || e.pending != 2 {
		t.Fatalf("superseding desired %+v calls %v", rep, e.calls)
	}
	if rep := r.Command(ctx, 2, ActionCommit); rep.Apply == nil || rep.Err != "" || r.Phase() != PhaseApplied || r.AppliedVersion() != 2 {
		t.Fatalf("commit %+v", rep)
	}
	if rep := r.Desired(ctx, cfg(1, rA)); rep.Err == "" {
		t.Fatal("stale version must report the engine error")
	}
	_ = r.Desired(ctx, cfg(3, rA))
	if rep := r.Command(ctx, 3, ActionAbort); rep.Err != "" || r.Phase() != PhaseApplied || r.AppliedVersion() != 2 {
		t.Fatalf("abort %+v", rep)
	}
	// 只保留最后生效配置
	if log.Len() != 1 {
		t.Fatalf("config log has %d records", log.Len())
	}
}

// AGT-010：恢复最后生效配置并 Prepare，授权到达前不 Commit，外部 Commit 被拒；
// 全部规则有运行授权或签发方已作答后 Commit。
func TestRecovery(t *testing.T) {
	log := &store.Mem{}
	ctx := context.Background()
	e1 := &fakeEngine{}
	r1 := New(e1, log)
	_ = r1.Desired(ctx, cfg(5, rA, rB))
	_ = r1.Command(ctx, 5, ActionCommit)

	e := &fakeEngine{}
	r := New(e, log)
	if r.Phase() != PhaseIdle {
		t.Fatal("phase before recover")
	}
	pr, err := r.Recover(ctx)
	if err != nil || pr == nil || e.pending != 5 || r.Phase() != PhaseGrantPending || r.AppliedVersion() != 5 {
		t.Fatalf("recover %+v %v phase %v", pr, err, r.Phase())
	}
	if rep := r.Command(ctx, 5, ActionCommit); rep.Err == "" || e.active != 0 {
		t.Fatal("external commit during recovery")
	}
	covered := map[types.UUID]bool{rA: true}
	if rep := r.GrantsUpdated(ctx, func(id types.UUID) bool { return covered[id] }, false); rep != nil {
		t.Fatal("committed with a rule still uncovered")
	}
	covered[rB] = true
	rep := r.GrantsUpdated(ctx, func(id types.UUID) bool { return covered[id] }, false)
	if rep == nil || rep.Err != "" || e.active != 5 || r.Phase() != PhaseApplied {
		t.Fatalf("recovery commit %+v", rep)
	}
	if rep := r.GrantsUpdated(ctx, func(types.UUID) bool { return true }, true); rep != nil {
		t.Fatal("second recovery commit")
	}

	// 签发方已作答（部分规则被拒）也 Commit
	e3 := &fakeEngine{}
	r3 := New(e3, log)
	_, _ = r3.Recover(ctx)
	if rep := r3.GrantsUpdated(ctx, func(types.UUID) bool { return false }, true); rep == nil || e3.active != 5 {
		t.Fatal("answered bootstrap must commit")
	}

	// 恢复中收到新的期望版本：放弃恢复，按正常流程
	e4 := &fakeEngine{}
	r4 := New(e4, log)
	_, _ = r4.Recover(ctx)
	if rep := r4.Desired(ctx, cfg(6, rA)); rep.Prepare == nil || e4.pending != 6 || r4.Phase() != PhasePrepared {
		t.Fatalf("desired during recovery %+v calls %v", rep, e4.calls)
	}
	if rep := r4.GrantsUpdated(ctx, func(types.UUID) bool { return true }, true); rep != nil {
		t.Fatal("abandoned recovery committed")
	}
	if rep := r4.Command(ctx, 6, ActionCommit); rep.Err != "" || e4.active != 6 {
		t.Fatalf("commit after abandoning recovery %+v", rep)
	}

	// 没有任何配置
	if pr, err := New(&fakeEngine{}, &store.Mem{}).Recover(ctx); pr != nil || err != nil {
		t.Fatal("recover without config")
	}
}

type failRewrite struct{ store.Mem }

func (f *failRewrite) Rewrite([]store.Record) error { return errors.New("disk full") }

// 持久化失败如实上报（已生效，但重启后会恢复旧版本）。
func TestPersistFailureReported(t *testing.T) {
	e := &fakeEngine{}
	r := New(e, &failRewrite{})
	ctx := context.Background()
	_ = r.Desired(ctx, cfg(1, rA))
	rep := r.Command(ctx, 1, ActionCommit)
	if rep.Apply == nil || rep.Err == "" || e.active != 1 {
		t.Fatalf("%+v", rep)
	}
}

func TestPrepareResourceErrorsPassedThrough(t *testing.T) {
	e := &fakeEngine{resErr: true}
	r := New(e, &store.Mem{})
	rep := r.Desired(context.Background(), cfg(1, rA))
	if rep.Prepare == nil || len(rep.Prepare.Errors) != 1 {
		t.Fatalf("%+v", rep)
	}
}
