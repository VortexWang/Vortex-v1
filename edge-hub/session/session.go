// Package session 组装 Edge Hub 面向 Agent 的服务（HUB-T02 的非 gRPC 部分，03 方案 3.1～3.6）：
// 账本、模式、区域额度、签发、份额租约，以及每条 Agent 控制流的会话处理。
//
// 每条流：第一条消息为 Hello（节点身份由连接层从证书取得并核对）→ 处理重启转移（LIM-010）→ 回 HubHello
// 声明模式（HUB-014）。之后处理授权申请（运行、计费、份额续期）、计量批次（落盘、结算消耗、记录需求信号、
// 确认）与持有授权报告。同一节点的新流取代旧流。模式变化推送给全部会话。
//
// 计量确认（HUB-003）：在线且配置了上游时，批次转发给后端，后端确认后才确认给 Agent；自治或没有上游时
// 本地落盘即确认。批次按 (node_id, boot_id, seq) 幂等。
package session

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	"vortex/edgehub/clock"
	"vortex/edgehub/issuer"
	"vortex/edgehub/leases"
	"vortex/edgehub/ledger"
	"vortex/edgehub/mode"
	"vortex/edgehub/model"
	"vortex/edgehub/quota"
)

const KindUsage = "usage.batch"

var (
	ErrNoHello      = errors.New("session: first message must be Hello")
	ErrNodeMismatch = errors.New("session: hello node_id differs from the certificate")
	ErrReplaced     = errors.New("session: replaced by a newer stream of the same node")
)

// ---------------------------------------------------------------- 消息（C2 的 Edge Hub 侧形式）

type AgentMsg interface{ agentMsg() }

type Hello struct {
	Node, Boot, PreviousBoot model.UUID
	AppliedConfigVersion     uint64
	UnackedUsageFromSeq      uint64
}

type GrantRequest struct {
	ID           uint64
	Sync         bool
	Kind         model.GrantKind
	Tenant, Rule model.UUID
	CurrentGrant model.UUID
	RateBps60s   uint64
}

type Consumption struct {
	Grant model.UUID
	Used  uint64
	Final bool
}

type Demand struct {
	Tenant          model.UUID
	Bytes60s        uint64
	StarvedPermille uint32
}

type UsageBatch struct {
	Boot         model.UUID
	Seq          uint64
	Consumptions []Consumption
	Demands      []Demand
	Snapshot     json.RawMessage // 原样转发给后端
}

// HeldGrant 是 Agent 报告的一份持有授权；Remaining 为 remaining_ttl。
type HeldGrant struct {
	issuer.HeldGrant
	Remaining time.Duration
}

type HeldGrants struct{ Grants []HeldGrant }

func (Hello) agentMsg()        {}
func (GrantRequest) agentMsg() {}
func (UsageBatch) agentMsg()   {}
func (HeldGrants) agentMsg()   {}

type HubMsg interface{ hubMsg() }

type HubHello struct {
	Hub  model.UUID
	Mode model.Mode
}

type ModeChange struct{ Mode model.Mode }

// GrantResponse：TTL 为相对有效期（ENG-025），由签发时刻与此刻换算。
type GrantResponse struct {
	RequestID uint64
	Run       []issuer.Grant
	Billing   []issuer.Grant
	Shares    []leases.Lease
	Revoked   []model.UUID
	Error     string
}

type UsageAck struct {
	Boot    model.UUID
	Through uint64
}

func (HubHello) hubMsg()      {}
func (ModeChange) hubMsg()    {}
func (GrantResponse) hubMsg() {}
func (UsageAck) hubMsg()      {}

// ---------------------------------------------------------------- 依赖

// Rules 返回节点当前生效配置中的规则状态（由配置分发 HUB-T04 维护）。
type Rules interface {
	Rules(node model.UUID) []issuer.RuleState
}

// RulesFunc 适配函数。
type RulesFunc func(node model.UUID) []issuer.RuleState

func (f RulesFunc) Rules(node model.UUID) []issuer.RuleState { return f(node) }

// Upstream 把计量批次转发给后端；ack 在后端确认后调用（可异步）。
type Upstream interface {
	Forward(node model.UUID, b UsageBatch, ack func())
}

// ---------------------------------------------------------------- Hub

type Options struct {
	HubID    model.UUID
	Clock    clock.Clock
	Store    ledger.Store
	Rules    Rules
	Upstream Upstream // nil：本地落盘即确认
}

// Hub 是一个 Edge Hub 实例。
type Hub struct {
	ID     model.UUID
	Clk    clock.Clock
	Ledger *ledger.Ledger
	Mode   *mode.Machine
	Quota  *quota.Accounts
	Issuer *issuer.Issuer
	Leases *leases.Manager

	opt      Options
	mu       sync.Mutex
	sessions map[model.UUID]*Session // 按节点
	usage    map[usageKey]bool       // 已收到的批次（幂等键）
}

type usageKey struct {
	node, boot model.UUID
	seq        uint64
}

type usageRecord struct {
	Node model.UUID      `json:"node"`
	Boot model.UUID      `json:"boot"`
	Seq  uint64          `json:"seq"`
	Snap json.RawMessage `json:"snap,omitempty"`
}

// Open 重放账本，组装各模块。
func Open(opt Options) (*Hub, error) {
	if opt.Clock == nil {
		opt.Clock = clock.System{}
	}
	h := &Hub{ID: opt.HubID, Clk: opt.Clock, opt: opt, sessions: map[model.UUID]*Session{},
		usage: map[usageKey]bool{}}
	h.Mode = mode.New(opt.Clock)
	h.Quota = quota.New(nil)
	h.Issuer = issuer.New(opt.Clock, h.Quota, h.Mode.CanRenewRun)
	h.Issuer.HubID = opt.HubID
	h.Leases = leases.New(opt.Clock)
	l, err := ledger.Open(opt.Store, opt.Clock, func(e ledger.Entry) error {
		if _, err := h.Mode.Replay(e); err != nil {
			return err
		}
		if _, err := h.Quota.Replay(e); err != nil {
			return err
		}
		if _, err := h.Issuer.Replay(e); err != nil {
			return err
		}
		if _, err := h.Leases.Replay(e); err != nil {
			return err
		}
		if e.Kind == KindUsage {
			var r usageRecord
			if err := json.Unmarshal(e.Data, &r); err != nil {
				return err
			}
			h.noteUsageLocked(r.Node, r.Boot, r.Seq)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	h.Ledger = l
	h.Mode.Bind(l)
	h.Quota.SetLedger(l)
	h.Issuer.Bind(l)
	h.Leases.Bind(l)
	h.Mode.OnChange(h.broadcastMode)
	return h, nil
}

// noteUsageLocked 记录收到的批次，返回是否为新批次。
//
// 不要求序号连续：同一启动的批次可能分别由不同实例确认（切换实例前已被原实例确认的批次不会重发）。
// Agent 在一条流上总是按序号递增发送全部未确认批次，所以确认某个序号即表示这条流上更早的未确认批次
// 都已落盘，累计确认成立。
func (h *Hub) noteUsageLocked(node, boot model.UUID, seq uint64) bool {
	k := usageKey{node, boot, seq}
	if h.usage[k] {
		return false
	}
	h.usage[k] = true
	return true
}

func (h *Hub) declared() model.Mode {
	m := h.Mode.Mode()
	if m == model.ModeReconciling || m == model.ModeAutonomyExpired {
		return model.ModeAutonomous // 对 Agent 只声明在线 / 自治（HUB-014）
	}
	return m
}

func (h *Hub) broadcastMode(model.Mode) {
	d := h.declared()
	h.mu.Lock()
	ss := make([]*Session, 0, len(h.sessions))
	for _, s := range h.sessions {
		ss = append(ss, s)
	}
	h.mu.Unlock()
	for _, s := range ss {
		s.send(ModeChange{d})
	}
}

// Tick 每秒调用：检查失联与自治到期，并随模式冻结或解冻份额总量（HUB-007）。
func (h *Hub) Tick() error {
	if err := h.Mode.Tick(); err != nil {
		return err
	}
	if h.declared() == model.ModeAutonomous {
		return h.Leases.Freeze()
	}
	return h.Leases.Unfreeze()
}

// Rebalance 在自治中再平衡份额并推送给各节点（每 60 秒）。
func (h *Hub) Rebalance() error {
	out, err := h.Leases.Rebalance()
	if err != nil {
		return err
	}
	for node, ls := range out {
		h.mu.Lock()
		s := h.sessions[node]
		h.mu.Unlock()
		if s != nil {
			rel := make([]leases.Lease, len(ls))
			for i, l := range ls {
				rel[i] = h.relLease(l)
			}
			s.send(GrantResponse{Shares: rel})
		}
	}
	return nil
}

// Session 是一条 Agent 控制流。
type Session struct {
	hub   *Hub
	cert  model.UUID // 证书中的节点 ID
	send  func(HubMsg)
	mu    sync.Mutex
	hello *Hello
	gone  bool
}

// NewSession 为证书身份为 node 的新流创建会话；send 不得阻塞。
func (h *Hub) NewSession(node model.UUID, send func(HubMsg)) *Session {
	return &Session{hub: h, cert: node, send: send}
}

// Close 结束会话。
func (s *Session) Close() {
	s.mu.Lock()
	s.gone = true
	s.mu.Unlock()
	h := s.hub
	h.mu.Lock()
	if h.sessions[s.cert] == s {
		delete(h.sessions, s.cert)
	}
	h.mu.Unlock()
}

func (h *Hub) relative(g issuer.Grant) issuer.Grant {
	g.TTL -= h.Clk.Now().Sub(g.IssuedAt)
	return g
}

func (h *Hub) relLease(l leases.Lease) leases.Lease {
	l.TTL -= h.Clk.Now().Sub(l.IssuedAt)
	return l
}

// Handle 处理一条 Agent 消息。返回错误时连接层应关闭流。
func (s *Session) Handle(m AgentMsg) error {
	h := s.hub
	s.mu.Lock()
	if s.gone {
		s.mu.Unlock()
		return ErrReplaced
	}
	hello := s.hello
	s.mu.Unlock()
	if hello == nil {
		hm, ok := m.(Hello)
		if !ok {
			return ErrNoHello
		}
		return s.onHello(hm)
	}
	switch m := m.(type) {
	case Hello:
		return ErrNoHello // 每条流只有一个 Hello
	case GrantRequest:
		s.send(s.onGrant(*hello, m))
	case UsageBatch:
		return s.onUsage(*hello, m)
	case HeldGrants:
		rem := map[model.UUID]time.Duration{}
		gs := make([]issuer.HeldGrant, len(m.Grants))
		for i, g := range m.Grants {
			gs[i], rem[g.ID] = g.HeldGrant, g.Remaining
		}
		_, err := h.Issuer.RecordHeld(hello.Node, hello.Boot, gs, func(g issuer.HeldGrant) time.Duration { return rem[g.ID] })
		return err
	}
	return nil
}

func (s *Session) onHello(m Hello) error {
	h := s.hub
	if m.Node != s.cert {
		return ErrNodeMismatch
	}
	tr, err := h.Issuer.Connect(m.Node, m.Boot)
	if err != nil {
		return err // STALE_BOOT：被取代的启动
	}
	var push *GrantResponse
	if tr != nil { // LIM-010：立即作废旧启动的授权与份额，按原值转给新启动
		revoked, fresh, err := h.Leases.Transfer(m.Node, m.Boot)
		if err != nil {
			return err
		}
		push = &GrantResponse{Revoked: append(append(tr.RevokedRun, tr.SettlingIDs...), revoked...)}
		for _, l := range fresh {
			push.Shares = append(push.Shares, h.relLease(l))
		}
	}
	s.mu.Lock()
	s.hello = &m
	s.mu.Unlock()
	h.mu.Lock()
	old := h.sessions[m.Node]
	h.sessions[m.Node] = s
	h.mu.Unlock()
	if old != nil && old != s {
		old.mu.Lock()
		old.gone = true
		old.mu.Unlock()
	}
	s.send(HubHello{Hub: h.ID, Mode: h.declared()})
	if push != nil {
		s.send(*push)
	}
	return nil
}

func (s *Session) onGrant(hello Hello, r GrantRequest) GrantResponse {
	h := s.hub
	resp := GrantResponse{RequestID: r.ID}
	rules := h.opt.Rules.Rules(hello.Node)
	switch r.Kind {
	case model.GrantRun:
		g, refused, err := h.Issuer.IssueRun(hello.Node, hello.Boot, rules)
		if err != nil {
			resp.Error = err.Error()
			return resp
		}
		if g != nil {
			resp.Run = []issuer.Grant{h.relative(*g)}
		} else {
			for _, e := range refused {
				resp.Error = e.Error()
				break
			}
		}
		if r.CurrentGrant.IsZero() { // 启动申请：一并下发当前份额
			for _, l := range h.Leases.Current(hello.Node) {
				resp.Shares = append(resp.Shares, h.relLease(l))
			}
		}
	case model.GrantBilling:
		var rule *issuer.RuleState
		for i := range rules {
			if rules[i].RuleID == r.Rule {
				rule = &rules[i]
			}
		}
		if rule == nil {
			resp.Error = issuer.ErrRuleStopped.Error()
			return resp
		}
		g, err := h.Issuer.IssueBilling(issuer.BillingRequest{Node: hello.Node, Boot: hello.Boot, Rule: *rule, RateBps60s: r.RateBps60s})
		if err != nil {
			resp.Error = err.Error()
			return resp
		}
		resp.Billing = []issuer.Grant{h.relative(*g)}
	case model.GrantShareRate, model.GrantShareConn:
		l, ok, err := h.Leases.Renew(hello.Node, hello.Boot, leases.Key{Tenant: r.Tenant, Kind: r.Kind})
		switch {
		case err != nil:
			resp.Error = err.Error()
		case !ok:
			resp.Error = issuer.ErrUnknownGrant.Error()
		default:
			resp.Shares = []leases.Lease{h.relLease(l)}
		}
	default:
		resp.Error = issuer.ErrUnknownGrant.Error()
	}
	return resp
}

func (s *Session) onUsage(hello Hello, b UsageBatch) error {
	h := s.hub
	h.mu.Lock()
	seen := h.usage[usageKey{hello.Node, b.Boot, b.Seq}]
	h.mu.Unlock()
	if seen { // 重发：只需再确认
		s.ackUsage(hello.Node, b)
		return nil
	}
	if _, err := h.Ledger.Append(KindUsage, usageRecord{Node: hello.Node, Boot: b.Boot, Seq: b.Seq, Snap: b.Snapshot}); err != nil {
		return err
	}
	h.mu.Lock()
	h.noteUsageLocked(hello.Node, b.Boot, b.Seq)
	h.mu.Unlock()
	for _, c := range b.Consumptions {
		if _, err := h.Issuer.ReportConsumption(c.Grant, c.Used, c.Final); err != nil {
			return err
		}
	}
	for _, d := range b.Demands {
		h.Leases.ReportDemand(hello.Node, d.Tenant, leases.Demand{Bytes60s: d.Bytes60s, StarvedPermille: d.StarvedPermille})
	}
	s.ackUsage(hello.Node, b)
	return nil
}

// ackUsage：在线且有上游时转发（包括重发的批次，上游按幂等键去重），上游确认后再确认；否则落盘即确认。
func (s *Session) ackUsage(node model.UUID, b UsageBatch) {
	h := s.hub
	ack := func() { s.send(UsageAck{Boot: b.Boot, Through: b.Seq}) }
	if h.opt.Upstream != nil && h.declared() == model.ModeOnline {
		h.opt.Upstream.Forward(node, b, ack)
		return
	}
	ack()
}

// Sessions 返回当前会话数。
func (h *Hub) Sessions() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.sessions)
}
