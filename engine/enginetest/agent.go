// Package enginetest 提供驱动引擎的模拟 Agent（ENG-002、ENG-T02），用于集成测试、压测与故障注入。
// 它只通过 engine.Engine 接口与引擎交互，与 M3 的真实 Agent 处于同一位置。
package enginetest

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"vortex/engine"
	"vortex/engine/types"
)

// Agent 是模拟 Agent：应用配置、下发授权、按周期拉取用量、收集事件。
type Agent struct {
	E engine.Engine

	mu       sync.Mutex
	usage    map[UsageKey]uint64                   // 累计原始字节
	consumed map[types.UUID]types.GrantConsumption // 每个计费授权最新的已用量
	events   []types.Event
	pulls    int

	grantSeq atomic.Uint64
	stop     context.CancelFunc
	done     chan struct{}
}

// UsageKey 是用量累计键（ENG-039）。
type UsageKey struct {
	Rule  types.UUID
	Role  types.HopRole
	Dir   types.Direction
	Grant types.UUID
}

// Start 启动事件收集与用量拉取（interval 为 0 时取 1 秒，与 Agent 的实际拉取周期一致）。
func Start(e engine.Engine, interval time.Duration) *Agent {
	if interval <= 0 {
		interval = time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	a := &Agent{E: e, usage: map[UsageKey]uint64{}, consumed: map[types.UUID]types.GrantConsumption{},
		stop: cancel, done: make(chan struct{})}
	go func() {
		defer close(a.done)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				a.Pull()
				return
			case ev := <-e.Events():
				a.mu.Lock()
				a.events = append(a.events, ev)
				a.mu.Unlock()
			case <-t.C:
				a.Pull()
			}
		}
	}()
	return a
}

// Stop 停止收集，并做最后一次用量拉取。
func (a *Agent) Stop() {
	a.stop()
	<-a.done
}

// Pull 立即拉取一次用量并累加。
func (a *Agent) Pull() {
	snap := a.E.DrainUsage()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.pulls++
	for _, d := range snap.Deltas {
		a.usage[UsageKey{d.RuleID, d.Role, d.Direction, d.BillingGrantID}] += d.RawBytes
	}
	for _, c := range snap.Consumptions {
		a.consumed[c.GrantID] = c
	}
}

// Usage 返回某规则某方向的累计原始字节（所有授权之和）。
func (a *Agent) Usage(rule types.UUID, dir types.Direction) uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	var n uint64
	for k, v := range a.usage {
		if k.Rule == rule && k.Dir == dir {
			n += v
		}
	}
	return n
}

// Consumption 返回某计费授权最近一次报告的消耗。
func (a *Agent) Consumption(grant types.UUID) (types.GrantConsumption, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	c, ok := a.consumed[grant]
	return c, ok
}

// Events 返回已收集的某类事件（kind 为 0 时返回全部）。
func (a *Agent) Events(kind types.EventKind) []types.Event {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []types.Event
	for _, ev := range a.events {
		if kind == 0 || ev.Kind == kind {
			out = append(out, ev)
		}
	}
	return out
}

// PrepareError 表示 Prepare 返回了资源错误，配置已被中止。
type PrepareError struct{ Errors []types.ResourceError }

func (e *PrepareError) Error() string {
	return fmt.Sprintf("enginetest: prepare reported %d resource error(s), first: %s %s", len(e.Errors), e.Errors[0].Code, e.Errors[0].Object.ID)
}

// Apply 执行 Prepare → Commit；有资源错误时 Abort 并返回 *PrepareError（全有或全无）。
func (a *Agent) Apply(ctx context.Context, cfg *types.RuntimeConfig) (*types.PrepareResult, *types.ApplyResult, error) {
	pr, err := a.E.Prepare(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}
	if len(pr.Errors) > 0 {
		if err := a.E.Abort(ctx, cfg.Version); err != nil {
			return pr, nil, err
		}
		return pr, nil, &PrepareError{pr.Errors}
	}
	ar, err := a.E.Commit(ctx, cfg.Version)
	return pr, ar, err
}

// Grants 描述 GrantAll 下发的授权。
type Grants struct {
	TTL     time.Duration // 默认 1 分钟
	RateBps uint64        // 每租户速率份额（bit/s）；0 = 100 Gbps，等于不限速
	Conns   uint64        // 每租户连接份额；0 = 10,000
	Billing uint64        // 每条执行点规则的计费授权原始字节；0 = 1 TiB
}

// NewID 生成本 Agent 内唯一的授权 ID。
func (a *Agent) NewID() types.UUID {
	var id types.UUID
	id[0] = 0xa9
	n := a.grantSeq.Add(1)
	for i := 0; i < 8; i++ {
		id[15-i] = byte(n >> (8 * i))
	}
	return id
}

// GrantAll 为配置中的全部规则下发运行授权，为执行点规则的租户下发份额与计费授权，返回下发的集合。
// 授权集合是全集语义：再次调用会替换之前的全部授权。
func (a *Agent) GrantAll(cfg *types.RuntimeConfig, g Grants) *types.GrantSet {
	if g.TTL == 0 {
		g.TTL = time.Minute
	}
	if g.RateBps == 0 {
		g.RateBps = 100_000_000_000
	}
	if g.Conns == 0 {
		g.Conns = 10_000
	}
	if g.Billing == 0 {
		g.Billing = 1 << 40
	}
	meta := func() types.GrantMeta { return types.GrantMeta{GrantID: a.NewID(), TTL: g.TTL, Generation: 1} }
	set := &types.GrantSet{}
	run := types.RunGrant{Meta: meta()}
	tenants := map[types.UUID]bool{}
	for _, r := range cfg.Rules {
		run.RuleIDs = append(run.RuleIDs, r.RuleID)
		if r.EnforcementPoint {
			tenants[r.TenantID] = true
			set.Billing = append(set.Billing, types.BillingGrant{Meta: meta(), RuleID: r.RuleID,
				BillingVersion: r.BillingVersion, RawBytes: g.Billing})
		}
	}
	set.Run = []types.RunGrant{run}
	for t := range tenants {
		set.Shares = append(set.Shares,
			types.ShareLease{Meta: meta(), TenantID: t, Kind: types.ShareRate, Amount: g.RateBps},
			types.ShareLease{Meta: meta(), TenantID: t, Kind: types.ShareConn, Amount: g.Conns})
	}
	a.E.UpdateGrants(set)
	return set
}
