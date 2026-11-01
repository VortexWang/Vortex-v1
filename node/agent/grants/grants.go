// Package grants 是 Agent 的授权与份额管理（AGT-005、AGT-009、AGT-011、HUB-014、ENG-050、LIM-010，02 方案 3.5）。
//
//   - 运行授权与份额租约每 60 秒续期，续期请求带当前持有的授权 ID；
//   - 计费授权在 Engine 发出 grant_low（异步）或 grant_needed（同步，200 ms）事件时申请，同一规则不重复申请；
//   - 授权以相对有效期下发，原样交给 Engine（Engine 按授权 ID 在首次见到时定截止点，不做墙钟换算）；
//     Agent 自己按单调时钟计算剩余时间，只用于清理与 HeldGrantsReport；
//   - 只接受当前所连实例签发、属于本次启动的新授权；切换实例后已持有的授权继续有效直至用完或到期，
//     并在切换后第一时间报告（HeldGrantsReport）；
//   - 控制连接中断不做任何主动停止（AGT-011）：授权留在集合里，到期由 Engine 自行停止。
package grants

import (
	"errors"
	"sort"
	"sync"
	"time"

	"vortex/engine/types"
	"vortex/node/internal/clock"
)

const (
	RenewEvery   = 60 * time.Second
	SyncTimeout  = 200 * time.Millisecond // ENG-050
	AsyncTimeout = 10 * time.Second       // 异步申请未获应答时重发
)

// Kind 与契约 C2 的 GrantKind 取值相同。
type Kind uint8

const (
	KindUnspecified Kind = iota
	KindRun
	KindBilling
	KindShareRate
	KindShareConn
)

var (
	ErrForeignIssuer = errors.New("grants: response from a hub other than the connected one")
	ErrNoHub         = errors.New("grants: not connected")
)

// Request 对应 GrantRequest。
type Request struct {
	ID           uint64
	Sync         bool
	Kind         Kind
	Tenant       types.UUID
	Rule         types.UUID
	CurrentGrant types.UUID
	RateBps60s   uint64
	sentAt       time.Time
}

// Response 对应 GrantResponse。
type Response struct {
	RequestID uint64 // 0 = 主动推送（转移、再平衡）
	Set       types.GrantSet
	Revoked   []types.UUID
	Error     string
}

// Held 对应 HeldGrant。
type Held struct {
	ID           types.UUID
	Issuer       types.UUID
	Kind         Kind
	Tenant       types.UUID
	Rule         types.UUID
	Amount       uint64
	Used         uint64
	RemainingTTL time.Duration
}

type grant struct {
	kind     Kind
	meta     types.GrantMeta
	received time.Time
	run      types.RunGrant
	billing  types.BillingGrant
	share    types.ShareLease
	used     uint64
	final    bool
}

func (g *grant) remaining(now time.Time) time.Duration { return g.meta.TTL - now.Sub(g.received) }

// Manager 管理本节点持有的授权。
type Manager struct {
	mu        sync.Mutex
	clk       clock.Clock
	boot      types.UUID
	hub       types.UUID // 当前所连实例；零值表示未连接
	grants    map[types.UUID]*grant
	nextID    uint64
	pending   map[uint64]*Request
	lastError map[types.UUID]string // 按规则（计费）或租户（份额）记录最近的拒绝原因
	answered  bool                  // 签发方已对启动申请（Bootstrap）作答

	RenewAfter time.Duration // 续期周期；0 = RenewEvery（测试用较短的值）
}

func New(clk clock.Clock, boot types.UUID) *Manager {
	return &Manager{clk: clk, boot: boot, grants: map[types.UUID]*grant{}, pending: map[uint64]*Request{},
		lastError: map[types.UUID]string{}}
}

// Connected 记录新的控制连接所连实例。实例变化时（切换或重连到另一实例）返回要立即发送的 HeldGrantsReport
// 内容（可能为空列表，但 switched 为 true）；未完成的申请全部作废（应答只会来自旧连接）。
func (m *Manager) Connected(hub types.UUID) (report []Held, switched bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pending = map[uint64]*Request{}
	if hub == m.hub {
		return nil, false
	}
	m.hub = hub
	m.expireLocked(m.clk.Now())
	return m.heldLocked(), true
}

// Disconnected：控制连接中断。授权保留（AGT-011）；未完成的申请作废。
func (m *Manager) Disconnected() {
	m.mu.Lock()
	m.pending = map[uint64]*Request{}
	m.mu.Unlock()
}

// Apply 处理来自 from 实例的 GrantResponse。返回 Engine 的授权集合是否变化。
// 只接受当前所连实例的应答，并只收下由该实例签发、属于本次启动的授权。
func (m *Manager) Apply(from types.UUID, r Response) (bool, error) {
	now := m.clk.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if (m.hub == types.UUID{}) || from != m.hub {
		return false, ErrForeignIssuer
	}
	req := m.pending[r.RequestID]
	delete(m.pending, r.RequestID)
	if req != nil && req.Kind == KindRun && req.CurrentGrant == (types.UUID{}) {
		m.answered = true
	}
	if r.Error != "" && req != nil {
		key := req.Rule
		if req.Kind != KindBilling {
			key = req.Tenant
		}
		m.lastError[key] = r.Error
	}
	changed := false
	for _, id := range r.Revoked {
		if _, ok := m.grants[id]; ok {
			delete(m.grants, id)
			changed = true
		}
	}
	accept := func(meta types.GrantMeta) bool {
		_, dup := m.grants[meta.GrantID]
		return !dup && meta.IssuerHub == from && meta.BootID == m.boot && meta.TTL > 0
	}
	for _, g := range r.Set.Run {
		if accept(g.Meta) {
			m.grants[g.Meta.GrantID] = &grant{kind: KindRun, meta: g.Meta, received: now, run: g}
			changed = true
		}
	}
	for _, g := range r.Set.Billing {
		if accept(g.Meta) {
			m.grants[g.Meta.GrantID] = &grant{kind: KindBilling, meta: g.Meta, received: now, billing: g}
			delete(m.lastError, g.RuleID)
			changed = true
		}
	}
	for _, g := range r.Set.Shares {
		if accept(g.Meta) {
			k := KindShareRate
			if g.Kind == types.ShareConn {
				k = KindShareConn
			}
			m.grants[g.Meta.GrantID] = &grant{kind: k, meta: g.Meta, received: now, share: g}
			changed = true
		}
	}
	return changed, nil
}

func (m *Manager) expireLocked(now time.Time) bool {
	changed := false
	for id, g := range m.grants {
		if g.remaining(now) <= 0 || (g.kind == KindBilling && g.final) {
			delete(m.grants, id)
			changed = true
		}
	}
	return changed
}

// EngineSet 返回交给 Engine.UpdateGrants 的完整集合（到期与已用完的除外，TTL 为下发时的原值）。
func (m *Manager) EngineSet() *types.GrantSet {
	now := m.clk.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expireLocked(now)
	set := &types.GrantSet{}
	for _, id := range m.sortedIDsLocked() {
		g := m.grants[id]
		switch g.kind {
		case KindRun:
			set.Run = append(set.Run, g.run)
		case KindBilling:
			set.Billing = append(set.Billing, g.billing)
		default:
			set.Shares = append(set.Shares, g.share)
		}
	}
	return set
}

func (m *Manager) sortedIDsLocked() []types.UUID {
	ids := make([]types.UUID, 0, len(m.grants))
	for id := range m.grants {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		gi, gj := m.grants[ids[i]], m.grants[ids[j]]
		if !gi.received.Equal(gj.received) {
			return gi.received.Before(gj.received)
		}
		return string(ids[i][:]) < string(ids[j][:])
	})
	return ids
}

// Observe 记录 Engine 上报的计费授权消耗；final 的授权从集合中移除。返回集合是否变化。
func (m *Manager) Observe(cs []types.GrantConsumption) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	changed := false
	for _, c := range cs {
		if g := m.grants[c.GrantID]; g != nil && g.kind == KindBilling {
			g.used = max(g.used, c.UsedRawBytes)
			if c.Final && !g.final {
				g.final, changed = true, true
			}
		}
	}
	if changed {
		m.expireLocked(m.clk.Now())
	}
	return changed
}

func (m *Manager) sendLocked(r Request, now time.Time) Request {
	m.nextID++
	r.ID, r.sentAt = m.nextID, now
	m.pending[r.ID] = &r
	return r
}

func (m *Manager) inFlightLocked(match func(*Request) bool, now time.Time, needSync bool) bool {
	for id, p := range m.pending {
		timeout := AsyncTimeout
		if p.Sync {
			timeout = SyncTimeout
		}
		if now.Sub(p.sentAt) >= timeout {
			if now.Sub(p.sentAt) >= RenewEvery { // 迟到的应答仍可匹配一段时间（记录拒绝原因）
				delete(m.pending, id)
			}
			continue
		}
		if match(p) && (p.Sync || !needSync) {
			return true
		}
	}
	return false
}

// Due 返回到期需要续期的运行授权与份额租约申请（每秒调用）。每个（种类, 规则集合或租户）只续最新的一份，
// 已在申请中的不重复。
func (m *Manager) Due() []Request {
	now := m.clk.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if (m.hub == types.UUID{}) {
		return nil
	}
	m.expireLocked(now)
	type key struct {
		kind   Kind
		tenant types.UUID
		rules  string
	}
	latest := map[key]*grant{}
	for _, id := range m.sortedIDsLocked() {
		g := m.grants[id]
		var k key
		switch g.kind {
		case KindRun:
			b := make([]byte, 0, 16*len(g.run.RuleIDs))
			for _, r := range g.run.RuleIDs {
				b = append(b, r[:]...)
			}
			k = key{kind: KindRun, rules: string(b)}
		case KindShareRate, KindShareConn:
			k = key{kind: g.kind, tenant: g.share.TenantID}
		default:
			continue
		}
		latest[k] = g // 按接收次序遍历，最后的为最新
	}
	var out []Request
	for _, g := range latest {
		renew := m.RenewAfter
		if renew == 0 {
			renew = RenewEvery
		}
		if now.Sub(g.received) < renew {
			continue
		}
		id := g.meta.GrantID
		if m.inFlightLocked(func(p *Request) bool { return p.CurrentGrant == id }, now, false) {
			continue
		}
		r := Request{Kind: g.kind, CurrentGrant: id}
		if g.kind != KindRun {
			r.Tenant = g.share.TenantID
		}
		out = append(out, m.sendLocked(r, now))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// OnEvent 处理 Engine 事件：grant_low → 异步申请计费授权；grant_needed → 同步申请（已有异步申请在途时
// 仍发同步申请，因为后者要求 200 ms 内应答）。返回要发送的申请；不需要时为 nil。
func (m *Manager) OnEvent(ev types.Event, rateBps60s uint64) (*Request, error) {
	if ev.Kind != types.EventGrantLow && ev.Kind != types.EventGrantNeeded {
		return nil, nil
	}
	now := m.clk.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if (m.hub == types.UUID{}) {
		return nil, ErrNoHub
	}
	sync := ev.Kind == types.EventGrantNeeded
	rule := ev.RuleID
	if m.inFlightLocked(func(p *Request) bool { return p.Kind == KindBilling && p.Rule == rule }, now, sync) {
		return nil, nil
	}
	r := m.sendLocked(Request{Sync: sync, Kind: KindBilling, Tenant: ev.TenantID, Rule: rule, RateBps60s: rateBps60s}, now)
	return &r, nil
}

func (m *Manager) heldLocked() []Held {
	now := m.clk.Now()
	out := []Held{}
	for _, id := range m.sortedIDsLocked() {
		g := m.grants[id]
		h := Held{ID: id, Issuer: g.meta.IssuerHub, Kind: g.kind, RemainingTTL: g.remaining(now)}
		switch g.kind {
		case KindBilling:
			h.Rule, h.Amount, h.Used = g.billing.RuleID, g.billing.RawBytes, g.used
		case KindShareRate, KindShareConn:
			h.Tenant, h.Amount = g.share.TenantID, g.share.Amount
		}
		out = append(out, h)
	}
	return out
}

// Held 返回当前持有的全部授权（HeldGrantsReport；status 子命令）。
func (m *Manager) Held() []Held {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expireLocked(m.clk.Now())
	return m.heldLocked()
}

// LastError 返回规则（计费）或租户（份额）最近一次被拒的原因。
func (m *Manager) LastError(id types.UUID) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastError[id]
}

// RunRemaining 返回各规则运行授权的最长剩余时间（status；启动时判断“授权待确认”，AGT-010）。
func (m *Manager) RunRemaining() map[types.UUID]time.Duration {
	now := m.clk.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[types.UUID]time.Duration{}
	for _, g := range m.grants {
		if g.kind != KindRun {
			continue
		}
		rem := g.remaining(now)
		if rem <= 0 {
			continue
		}
		for _, r := range g.run.RuleIDs {
			out[r] = max(out[r], rem)
		}
	}
	return out
}

// Bootstrap 返回启动（或运行授权全部失效）时的首次申请：kind = RUN、不带当前授权 ID，签发方为本节点全部
// 规则签发运行授权并一并下发份额（AGT-010）。已有有效运行授权或申请在途时返回 nil。
func (m *Manager) Bootstrap() *Request {
	now := m.clk.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.hub == (types.UUID{}) {
		return nil
	}
	m.expireLocked(now)
	for _, g := range m.grants {
		if g.kind == KindRun {
			return nil
		}
	}
	if m.inFlightLocked(func(p *Request) bool { return p.Kind == KindRun && p.CurrentGrant == (types.UUID{}) }, now, false) {
		return nil
	}
	r := m.sendLocked(Request{Kind: KindRun}, now)
	return &r
}

// Answered 报告签发方是否已对启动申请作答（无论是否签发了全部规则）。
func (m *Manager) Answered() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.answered
}
