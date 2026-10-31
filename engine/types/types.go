// Package types 是引擎内部使用的数据类型，与契约 C2 的 vortex.runtime.v1（runtime.proto）一一对应。
//
// 引擎不直接依赖 protobuf 生成代码（GEN-003：可脱离 Agent 单独测试与压测）；Agent 侧的适配层
// 负责 runtime.v1 消息与本包类型的双向转换。字段含义以 proto 文件注释为准，这里只标注对应关系。
package types

import (
	"net/netip"
	"time"
)

type UUID [16]byte

type HopRole uint8

const (
	RoleUnspecified HopRole = iota
	RoleEntry
	RoleRelay
	RoleExit
)

type Network uint8

const (
	NetworkUnspecified Network = iota
	NetworkTCP
	NetworkUDP
)

type Direction uint8

const (
	DirUnspecified Direction = iota
	DirUp                    // 客户端 → 目标
	DirDown                  // 目标 → 客户端
)

func (d Direction) String() string {
	switch d {
	case DirUp:
		return "up"
	case DirDown:
		return "down"
	}
	return "unspecified"
}

type Schedule uint8

const (
	ScheduleUnspecified Schedule = iota
	ScheduleWeightedRoundRobin
	SchedulePrimaryBackup
)

type DialDirection uint8

const (
	DialUnspecified DialDirection = iota
	DialForward
	DialReverse
)

type ProxyProtocol uint8

const (
	ProxyUnspecified ProxyProtocol = iota
	ProxyOff
	ProxyV1
	ProxyV2
)

// RuntimeConfig ↔ runtime.v1.RuntimeConfig
type RuntimeConfig struct {
	Version      uint64
	NodeID       UUID
	Generation   uint64
	Listeners    []Listener
	Rules        []Rule
	Peers        []Peer
	Revocations  RevocationList
	TargetPolicy TargetPolicy
}

type Listener struct {
	Bind    netip.AddrPort
	Network Network
	RuleID  UUID
}

type Rule struct {
	RuleID           UUID
	TenantID         UUID
	RuleVersion      uint64
	PathGeneration   uint64
	Role             HopRole
	HopIndex         uint32
	EnforcementPoint bool
	AllowedUpstreams []UUID
	NextHop          *NextHop // 与 Targets 互斥
	Targets          []Target
	TargetSchedule   Schedule
	Limits           RuleLimits
	ProxyProtocol    ProxyProtocol
	TCPIdle          time.Duration // 0 = 不超时
	UDPIdle          time.Duration
	PathCoefMilli    uint64
	BillingVersion   uint64
	Networks         []Network
}

type NextHop struct {
	GroupID        UUID
	Members        []Member
	MemberSchedule Schedule
	Dial           DialDirection
}

type Member struct {
	NodeID   UUID
	Weight   uint32
	Priority uint32
}

type Target struct {
	Host     string
	Port     uint16
	Weight   uint32
	Priority uint32
}

type RuleLimits struct {
	RateBps  uint64
	MaxConns uint32
}

type Peer struct {
	NodeID        UUID
	Address       string
	LinkPort      uint16
	Outbound      bool // PeerDial：true = OUTBOUND（本节点主动连接）
	LinkMode      LinkMode
	ServedRuleIDs []UUID
}

// LinkMode ↔ runtime.v1.LinkMode（ENG-017）；零值按 AUTO 处理。
type LinkMode uint8

const (
	LinkModeUnspecified LinkMode = iota
	LinkModeAuto
	LinkModeMux
	LinkModePool
)

type RevocationList struct {
	Serial      uint64
	CertSerials [][]byte
	NodeIDs     []UUID
}

type TargetPolicy struct {
	DenyPrefixes       []netip.Prefix
	DenyPorts          []uint16
	AllowPrivatePrefix []netip.Prefix
	SelfHostedNode     bool
}

// ---- 授权（runtime.v1.GrantSet） ----

type GrantMeta struct {
	GrantID    UUID
	IssuerHub  UUID
	BootID     UUID
	Generation uint64
	TTL        time.Duration // 相对有效期（ENG-025）
}

type GrantSet struct {
	Run     []RunGrant
	Billing []BillingGrant
	Shares  []ShareLease
}

type RunGrant struct {
	Meta    GrantMeta
	RuleIDs []UUID
}

type BillingGrant struct {
	Meta           GrantMeta
	RuleID         UUID
	BillingVersion uint64
	RawBytes       uint64
}

type ShareKind uint8

const (
	ShareUnspecified ShareKind = iota
	ShareRate
	ShareConn
)

type ShareLease struct {
	Meta     GrantMeta
	TenantID UUID
	Kind     ShareKind
	Amount   uint64
}

// ---- 用量（runtime.v1.UsageSnapshot） ----

type UsageDelta struct {
	RuleID         UUID
	Role           HopRole
	Direction      Direction
	RawBytes       uint64
	BillingGrantID UUID // 仅执行点
}

type TenantDemand struct {
	TenantID        UUID
	Bytes60s        uint64
	StarvedPermille uint32
}

type GrantConsumption struct {
	GrantID      UUID
	UsedRawBytes uint64
	Final        bool
}

type UsageSnapshot struct {
	Deltas       []UsageDelta
	Demands      []TenantDemand
	Consumptions []GrantConsumption
}

// ---- 配置应用结果（runtime.v1.PrepareResult / ApplyResult / RuntimeState） ----

type ChangeClass uint8

const (
	ChangeUnspecified ChangeClass = iota
	ChangeNone
	ChangeHot
	ChangeRebuild
	ChangeStop
)

type ObjectKind uint8

const (
	ObjectUnspecified ObjectKind = iota
	ObjectListener
	ObjectRule
	ObjectPeer
)

type ObjectRef struct {
	Kind ObjectKind
	ID   string
}

type ObjectChange struct {
	Object           ObjectRef
	Class            ChangeClass
	AffectedSessions uint32
}

type ResourceError struct {
	Object ObjectRef
	Code   string
	Detail string
}

type PrepareResult struct {
	Version uint64
	Changes []ObjectChange
	Errors  []ResourceError
}

type ObjectStatus uint8

const (
	StatusUnspecified ObjectStatus = iota
	StatusPrepared
	StatusActive
	StatusFailed
	StatusStopped
	StatusRestricted
)

type ObjectState struct {
	Object        ObjectRef
	ActualVersion uint64
	Status        ObjectStatus
	ErrorCode     string
}

type ApplyResult struct {
	Version uint64
	Objects []ObjectState
}

type RuntimeState struct {
	ActualVersion uint64
	Objects       []ObjectState
}

// ---- 事件（runtime.v1.EngineEvent） ----

type EventKind uint8

const (
	EventUnspecified EventKind = iota
	EventRestrictionStarted
	EventRestrictionCompleted
	EventOpenRejected
	EventGrantLow
	EventGrantNeeded
	EventPeerDown
	EventTargetRejected
)

type Event struct {
	Kind     EventKind
	At       time.Duration // 引擎启动后的单调时间
	TenantID UUID
	RuleID   UUID
	PeerID   UUID
	GrantID  UUID
	Detail   string
}
