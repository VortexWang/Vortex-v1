// Package engine 是 VORTEX 自研转发引擎（需求第 8 节）。
//
// 引擎只通过 Engine 接口与 Agent 交互（ENG-001），不访问数据库、不主动请求控制面（GEN-004）。
// 分层：pipe（拷贝）、carrier（承载）、vlink（链路）、route（路由）、gate（门控）、meter（计量）、apply（配置应用）。
package engine

import (
	"context"
	"errors"

	"vortex/engine/types"
)

// Engine 是需求 8.2 节定义的进程内接口。
//
// 与需求 8.2 节代码块的一处差异：DrainUsage 返回 types.UsageSnapshot 而非 []UsageDelta，
// 因为契约 C2a（决定 D8）规定引擎输出还须携带 LIM-009 的需求信号与逐授权消费量。
// 已提交需求变更（见 docs/spec-change-requests.md CR-01），在变更评审通过前以契约为准。
type Engine interface {
	Prepare(ctx context.Context, cfg *types.RuntimeConfig) (*types.PrepareResult, error)
	Commit(ctx context.Context, version uint64) (*types.ApplyResult, error)
	Abort(ctx context.Context, version uint64) error
	UpdateGrants(set *types.GrantSet)
	Drain(ctx context.Context, scope DrainScope) error
	DrainUsage() types.UsageSnapshot // Agent 每秒拉取
	Events() <-chan types.Event
	State() types.RuntimeState
}

// DrainScope 指定排空范围；RuleIDs 为空表示整个节点（NOD-003）。
type DrainScope struct {
	RuleIDs []types.UUID
}

// ErrNotImplemented 由尚未完成的里程碑功能返回，随 ENG-T33 等任务逐步消除。
var ErrNotImplemented = errors.New("engine: not implemented yet")
