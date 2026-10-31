package agent

import (
	"vortex/engine/types"
	"vortex/node/agent/grants"
	"vortex/node/agent/reconcile"
	"vortex/node/agent/report"
)

// 控制流消息（契约 C2 NodeControl.Connect 的 AgentMessage / HubMessage）。gRPC 接入后由适配层与生成代码互转。

// Mode 是 Edge Hub 声明的模式（common.v1.HubMode）。
type Mode uint8

const (
	ModeUnspecified Mode = iota
	ModeOnline
	ModeAutonomous
)

// Out 是 Agent → Edge Hub 的消息。
type Out interface{ out() }

type Hello struct {
	Node, Boot, PreviousBoot types.UUID
	AgentVersion             string
	AppliedConfigVersion     uint64
	UnackedUsageFromSeq      uint64
}

type HeldGrants struct{ Grants []grants.Held }

type PrepareReport struct {
	Version uint64
	Result  *types.PrepareResult
	Err     string
}

type ApplyReport struct {
	Version uint64
	Action  reconcile.Action
	Result  *types.ApplyResult
	Err     string
}

type GrantRequest struct{ grants.Request }

type UsageBatch struct{ report.Batch }

type EngineEvent struct{ types.Event }

type StateReport struct{ State types.RuntimeState }

func (Hello) out()         {}
func (HeldGrants) out()    {}
func (PrepareReport) out() {}
func (ApplyReport) out()   {}
func (GrantRequest) out()  {}
func (UsageBatch) out()    {}
func (EngineEvent) out()   {}
func (StateReport) out()   {}

// In 是 Edge Hub → Agent 的消息。
type In interface{ in() }

type HubHello struct {
	Hub    types.UUID
	Region string
	Mode   Mode
}

type ModeChange struct{ Mode Mode }

type DesiredConfig struct{ Config *types.RuntimeConfig }

type ApplyCommand struct {
	Version uint64
	Action  reconcile.Action
}

type GrantResponse struct{ grants.Response }

type UsageAck struct {
	Boot    types.UUID
	Through uint64
}

func (HubHello) in()      {}
func (ModeChange) in()    {}
func (DesiredConfig) in() {}
func (ApplyCommand) in()  {}
func (GrantResponse) in() {}
func (UsageAck) in()      {}

// Session 是一条控制连接的发送端。Send 不得长时间阻塞（实现应带缓冲或超时，失败即视为连接断开）。
type Session interface {
	Send(Out) error
}
