// Package upstream 是 Edge Hub 到后端的链路（HubControl.Connect 双向流的非 gRPC 部分；HUB-T01、HUB-T06、
// HUB-T16，03 方案 3.8、3.9）。
//
// 连接建立：任何后端消息都算一次成功通信（mode.Contact），自治中由此转入对账；先发 HubRegister（模式、
// 账本最大序号、自治起算时刻、已应用的额度版本）。
//
// 账本上报：在线时随写随报；对账中只有在后端下发 ReconcileDirective(STARTED) 之后才上报（后端先下发
// 吊销与限制，HUB-010、HUB-013 ①②）。只上报后端需要的条目（签发、结算、计量批次、重启转移），
// 按序号递增发送；后端按 (hub_id, seq) 幂等、累计确认（LedgerAck）。本地专用条目（模式、限制快照、
// 租约、确认记录等）不上报，其序号在上报序列中留空。断线后从已确认位置重新上报。
//
// 计量确认（HUB-003）：在线时，计量批次所在账本条目被后端确认后，才向 Agent 确认。
//
// 对账完成（COMPLETED）：回到在线，解除 HUB-017 停签，解冻份额，放行积压的配置（HUB-013 ⑤）。
package upstream

import (
	"encoding/json"
	"sort"
	"sync"
	"time"

	"vortex/edgehub/issuer"
	"vortex/edgehub/leases"
	"vortex/edgehub/ledger"
	"vortex/edgehub/model"
	"vortex/edgehub/quota"
	"vortex/edgehub/session"
)

// MaxBatch 是一条 LedgerEntries 消息的最多条目数。
const MaxBatch = 512

// ---------------------------------------------------------------- Edge Hub → 后端

type Up interface{ up() }

type Register struct {
	Mode              model.Mode
	LedgerLastSeq     uint64
	AutonomyStartedAt time.Time
	BudgetVersions    map[model.UUID]uint64
}

// LedgerEntry 是一条上报的账本条目；四个字段恰有一个非空。
type LedgerEntry struct {
	Seq         uint64
	At          time.Time
	Issued      *issuer.Grant
	Settled     *quota.Settlement
	Usage       *session.UsageEntry
	Transferred *issuer.TransferRecord
}

type LedgerEntries struct{ Entries []LedgerEntry }

type ApplyProgress struct {
	Node    model.UUID
	Prepare *session.PrepareReport
	Apply   *session.ApplyReport
}

type ForeignGrants struct {
	Node   model.UUID
	Grants []issuer.HeldGrant
}

type NodeForward struct {
	Node model.UUID
	Msg  session.Passthrough
}

type BudgetRequest struct{ Tenant model.UUID }

// NodeStatus 是节点在线状态（NodeStatusBatch 的一项，HUB-004）。
type NodeStatus struct {
	Node, Boot           model.UUID
	Connected            bool
	AppliedConfigVersion uint64
	At                   time.Time
}

func (Register) up()      {}
func (LedgerEntries) up() {}
func (ApplyProgress) up() {}
func (ForeignGrants) up() {}
func (NodeForward) up()   {}
func (BudgetRequest) up() {}
func (NodeStatus) up()    {}

// ---------------------------------------------------------------- 后端 → Edge Hub

type Down interface{ down() }

type Desired struct {
	Node    model.UUID
	Version uint64
	Config  json.RawMessage
}

type Command struct {
	Node    model.UUID
	Version uint64
	Commit  bool
}

type Restrictions struct {
	Version uint64
	Tenants []issuer.TenantRestriction
}

type Revocations struct {
	Serial  uint64
	Payload json.RawMessage
}

type Budget struct {
	Tenant                  model.UUID
	Version, Delegated, Ack uint64
}

type Leases struct {
	Node   model.UUID
	Leases []leases.Lease
}

type LedgerAck struct{ Through uint64 }

type BillingVersions struct{ Versions []issuer.BillingVersion }

type ReconcilePhase uint8

const (
	ReconcileStarted ReconcilePhase = iota + 1
	ReconcileCompleted
)

type Reconcile struct {
	Phase          ReconcilePhase
	RevokedTenants []model.UUID
}

type Drain struct {
	Node    model.UUID
	Payload json.RawMessage
}

func (Desired) down()         {}
func (Command) down()         {}
func (Restrictions) down()    {}
func (Revocations) down()     {}
func (Budget) down()          {}
func (Leases) down()          {}
func (LedgerAck) down()       {}
func (BillingVersions) down() {}
func (Reconcile) down()       {}
func (Drain) down()           {}

// ---------------------------------------------------------------- Link

// Link 管理与后端的一条连接（同一时刻至多一条）。
type Link struct {
	hub *session.Hub

	mu          sync.Mutex
	send        func(Up) // nil：未连接
	sentThrough uint64   // 本连接已处理（发送或判定为本地条目）到的账本序号
	lastSent    uint64   // 本连接最后发送的上报条目序号
	started     bool     // 对账已开始（可以上报账本）
	waiters     map[uint64][]func()
	revSerial   uint64
	revocations json.RawMessage
}

// New 创建链路并登记为 Hub 的上游。
func New(h *session.Hub) *Link {
	l := &Link{hub: h, waiters: map[uint64][]func(){}}
	h.SetUpstream(l)
	return l
}

func (l *Link) sendUp(m Up) {
	l.mu.Lock()
	send := l.send
	l.mu.Unlock()
	if send != nil {
		send(m)
	}
}

// Connected：到后端的流建立。发送 HubRegister，在线时开始上报。
func (l *Link) Connected(send func(Up)) error {
	h := l.hub
	l.mu.Lock()
	l.send, l.started = send, false
	l.sentThrough = h.Ledger.Acked()
	l.lastSent = l.sentThrough
	l.mu.Unlock()
	if err := h.Mode.Contact(); err != nil {
		return err
	}
	send(Register{Mode: h.Mode.Mode(), LedgerLastSeq: h.Ledger.LastSeq(), AutonomyStartedAt: h.Mode.AutonomyStartedAt(),
		BudgetVersions: h.Quota.Versions()})
	return l.Flush()
}

// Disconnected：流断开。失联计时由 mode.Tick 判定（30 秒后自治）。
func (l *Link) Disconnected() {
	l.mu.Lock()
	l.send, l.started = nil, false
	l.mu.Unlock()
}

func (l *Link) canUpload() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.send == nil {
		return false
	}
	switch l.hub.Mode.Mode() {
	case model.ModeOnline:
		return true
	case model.ModeReconciling:
		return l.started
	}
	return false
}

// Flush 上报尚未发送的账本条目。
func (l *Link) Flush() error {
	if !l.canUpload() {
		return nil
	}
	es, err := l.hub.Ledger.Unacked()
	if err != nil {
		return err
	}
	l.mu.Lock()
	from := l.sentThrough
	l.mu.Unlock()
	var batch []LedgerEntry
	last, lastSent := from, uint64(0)
	for _, e := range es {
		if e.Seq <= from {
			continue
		}
		le, ok, err := convert(e)
		if err != nil {
			return err
		}
		last = e.Seq
		if !ok {
			continue
		}
		batch = append(batch, le)
		lastSent = e.Seq
		if len(batch) == MaxBatch {
			l.sendUp(LedgerEntries{batch})
			batch = nil
		}
	}
	if len(batch) > 0 {
		l.sendUp(LedgerEntries{batch})
	}
	l.mu.Lock()
	l.sentThrough = max(l.sentThrough, last)
	l.lastSent = max(l.lastSent, lastSent)
	l.mu.Unlock()
	return l.coverLocal()
}

// coverLocal：已发送的上报条目全部被确认时，其后只剩本地专用条目，本地把确认位置推进到 sentThrough，
// 否则这些条目永远显示为“未确认”，账本也无法按保留期清理（HUB-012）。
func (l *Link) coverLocal() error {
	l.mu.Lock()
	sent, last := l.sentThrough, l.lastSent
	l.mu.Unlock()
	if acked := l.hub.Ledger.Acked(); acked >= last && sent > acked {
		return l.hub.Ledger.Ack(sent)
	}
	return nil
}

// convert 把账本条目转为上报形式；本地专用条目返回 ok = false。
func convert(e ledger.Entry) (LedgerEntry, bool, error) {
	le := LedgerEntry{Seq: e.Seq, At: e.At}
	switch e.Kind {
	case issuer.KindRun:
		var g issuer.Grant
		if err := json.Unmarshal(e.Data, &g); err != nil {
			return le, false, err
		}
		le.Issued = &g
	case quota.KindIssue:
		tenant, grant, raw, coef, reserved, meta, err := quota.IssueMeta(e)
		if err != nil {
			return le, false, err
		}
		g := issuer.Grant{ID: grant, Kind: model.GrantBilling, Tenant: tenant, RawBytes: raw, CoefMilli: coef, Reserved: reserved}
		if len(meta) > 0 {
			if err := json.Unmarshal(meta, &g); err != nil {
				return le, false, err
			}
		}
		le.Issued = &g
	case quota.KindSettle:
		st, err := quota.DecodeSettle(e)
		if err != nil {
			return le, false, err
		}
		le.Settled = &st
	case session.KindUsage:
		u, err := session.DecodeUsage(e.Data)
		if err != nil {
			return le, false, err
		}
		le.Usage = &u
	case issuer.KindTransfer:
		t, err := issuer.DecodeTransfer(e.Data)
		if err != nil {
			return le, false, err
		}
		le.Transferred = &t
	default:
		return le, false, nil
	}
	return le, true, nil
}

// Usage 实现 session.Upstream。
func (l *Link) Usage(seq uint64, ack func()) {
	if l.hub.Ledger.Acked() >= seq {
		ack()
		return
	}
	l.mu.Lock()
	l.waiters[seq] = append(l.waiters[seq], ack)
	l.mu.Unlock()
	_ = l.Flush()
}

// Node 实现 session.Upstream：转发节点消息。
func (l *Link) Node(node model.UUID, m session.AgentMsg) {
	switch m := m.(type) {
	case session.PrepareReport:
		l.sendUp(ApplyProgress{Node: node, Prepare: &m})
	case session.ApplyReport:
		l.sendUp(ApplyProgress{Node: node, Apply: &m})
	case session.HeldGrants:
		var gs []issuer.HeldGrant
		for _, g := range l.hub.Issuer.Held() {
			if g.Node == node {
				gs = append(gs, g)
			}
		}
		l.sendUp(ForeignGrants{Node: node, Grants: gs})
	case session.Passthrough:
		l.sendUp(NodeForward{Node: node, Msg: m})
	case session.Hello:
		l.sendUp(NodeStatus{Node: node, Boot: m.Boot, Connected: true, AppliedConfigVersion: m.AppliedConfigVersion, At: l.hub.Clk.Now()})
	}
}

// Handle 处理一条后端消息。
func (l *Link) Handle(m Down) error {
	h := l.hub
	if err := h.Mode.Contact(); err != nil {
		return err
	}
	switch m := m.(type) {
	case Desired:
		h.SetDesired(m.Node, m.Version, m.Config)
	case Command:
		_ = h.SetCommand(m.Node, m.Version, m.Commit) // 过期指令忽略
	case Restrictions:
		return h.Issuer.SetRestrictions(m.Version, m.Tenants)
	case Revocations:
		l.mu.Lock()
		if m.Serial > l.revSerial {
			l.revSerial, l.revocations = m.Serial, m.Payload
		}
		l.mu.Unlock()
	case Budget:
		_, err := h.Quota.ApplyBudget(m.Tenant, m.Version, m.Delegated, m.Ack)
		return err
	case Leases:
		_, err := h.PushShares(m.Node, m.Leases)
		if err != nil && err != session.ErrNotOnline && err != session.ErrUnknownNode {
			return err
		}
	case LedgerAck:
		if err := h.Ledger.Ack(m.Through); err != nil {
			return err
		}
		l.fireWaiters(m.Through)
		return l.coverLocal()
	case BillingVersions:
		vs := append([]issuer.BillingVersion(nil), m.Versions...)
		sort.Slice(vs, func(i, j int) bool { return vs[i].EffectiveAt.Before(vs[j].EffectiveAt) })
		return h.Issuer.SetBillingVersions(vs)
	case Reconcile:
		switch m.Phase {
		case ReconcileStarted:
			if err := h.Issuer.SetBudgetRevoked(m.RevokedTenants); err != nil {
				return err
			}
			l.mu.Lock()
			l.started = true
			l.mu.Unlock()
			return l.Flush()
		case ReconcileCompleted:
			if err := h.Mode.ReconcileCompleted(); err != nil {
				return err
			}
			if err := h.Issuer.SetBudgetRevoked(nil); err != nil {
				return err
			}
			if err := h.Tick(); err != nil { // 解冻份额、推送模式变化
				return err
			}
			h.ReleaseConfig()
			return l.Flush()
		}
	case Drain:
		h.SendDrain(m.Node, m.Payload)
	}
	return nil
}

func (l *Link) fireWaiters(through uint64) {
	l.mu.Lock()
	var fire []func()
	for seq, fs := range l.waiters {
		if seq <= through {
			fire = append(fire, fs...)
			delete(l.waiters, seq)
		}
	}
	l.mu.Unlock()
	for _, f := range fire {
		f()
	}
}

// Tick 每秒调用：在线时补报（例如断线期间写入的条目），并为低于 30% 的租户请求补充额度（HUB-006）。
func (l *Link) Tick() error {
	if err := l.Flush(); err != nil {
		return err
	}
	if l.hub.Mode.Mode() != model.ModeOnline {
		return nil
	}
	for t := range l.hub.Quota.Versions() {
		if l.hub.Quota.NeedsTopUp(t) {
			l.sendUp(BudgetRequest{Tenant: t})
		}
	}
	return nil
}

// Revocations 返回最新的吊销列表（供 mTLS 终结检查，SEC-002）。
func (l *Link) Revocations() (uint64, json.RawMessage) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.revSerial, l.revocations
}
