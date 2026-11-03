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
	AgentVersion             string
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
	Snapshot     []byte // 原样转发给后端
}

// HeldGrant 是 Agent 报告的一份持有授权；Remaining 为 remaining_ttl。
type HeldGrant struct {
	issuer.HeldGrant
	Remaining time.Duration
}

type HeldGrants struct{ Grants []HeldGrant }

// PrepareReport / ApplyReport 是配置两阶段的结果（RUL-010），原样转发给后端。
type PrepareReport struct {
	Version uint64
	OK      bool // 没有资源错误与引擎错误
	Payload []byte
}

type ApplyReport struct {
	Version   uint64
	Committed bool // Commit 成功
	Payload   []byte
}

// Passthrough 是只需转发给后端的节点消息（引擎事件、探测结果、状态）。
type Passthrough struct {
	Kind    string // "event" / "probes" / "state" / "upgrade"
	Payload []byte
}

func (PrepareReport) agentMsg() {}
func (ApplyReport) agentMsg()   {}
func (Passthrough) agentMsg()   {}
func (Hello) agentMsg()         {}
func (GrantRequest) agentMsg()  {}
func (UsageBatch) agentMsg()    {}
func (HeldGrants) agentMsg()    {}

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

// DesiredConfig / ApplyCommand / DrainCommand 是转发给节点的后端指令（载荷不解析）。
type DesiredConfig struct {
	Version uint64
	Payload []byte
}

type ApplyCommand struct {
	Version uint64
	Commit  bool // false = Abort
}

type DrainCommand struct{ Payload []byte }

// UpgradeCommand 是转发给节点的升级指令（NodeOperation.upgrade，AGT-008；载荷不解析）。
type UpgradeCommand struct{ Payload []byte }

func (DesiredConfig) hubMsg()  {}
func (ApplyCommand) hubMsg()   {}
func (DrainCommand) hubMsg()   {}
func (UpgradeCommand) hubMsg() {}
func (HubHello) hubMsg()       {}
func (ModeChange) hubMsg()     {}
func (GrantResponse) hubMsg()  {}
func (UsageAck) hubMsg()       {}

// ---------------------------------------------------------------- 依赖

// Rules 返回节点当前生效配置中的规则状态（由配置分发 HUB-T04 维护）。
type Rules interface {
	Rules(node model.UUID) []issuer.RuleState
}

// RulesFunc 适配函数。
type RulesFunc func(node model.UUID) []issuer.RuleState

func (f RulesFunc) Rules(node model.UUID) []issuer.RuleState { return f(node) }

// Upstream 是到后端的链路（upstream.Link 实现）。
type Upstream interface {
	// Usage：计量批次已写入账本第 seq 条；后端确认该条目后调用 ack（可异步、可立即）。
	Usage(seq uint64, ack func())
	// Node 转发节点消息（Prepare / Apply 结果、引擎事件、探测、持有授权报告）。
	Node(node model.UUID, m AgentMsg)
}

// ---------------------------------------------------------------- Hub

type Options struct {
	HubID    model.UUID
	Clock    clock.Clock
	Store    ledger.Store
	Rules    Rules
	Upstream Upstream      // nil：本地落盘即确认；也可在 Open 之后用 SetUpstream 设置
	RunTTL   time.Duration // 运行授权有效期；0 = 5 分钟（测试用）
	// ParseRules 从期望配置（protobuf 字节）取出签发所需的规则信息；Rules 为 nil 时据此得到各节点的规则
	// （写账本，重启后恢复）。生产部署使用 wire.RulesFromConfig。
	ParseRules func(payload []byte) ([]issuer.RuleState, error)
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

	opt          Options
	mu           sync.Mutex
	sessions     map[model.UUID]*Session // 按节点
	usage        map[usageKey]usageSeen  // 已收到的批次（幂等键）→ 账本序号与时刻
	up           Upstream
	configs      map[model.UUID]*nodeConfig // 配置分发（HUB-T04）
	lastDeclared model.Mode
	states       map[model.UUID]*nodeState // 节点状态缓存（HUB-004）
	rules        map[model.UUID]*nodeRules // 期望配置中的规则信息
}

type usageKey struct {
	node, boot model.UUID
	seq        uint64
}

type usageSeen struct {
	seq uint64 // 账本序号
	at  time.Time
}

type usageRecord struct {
	Node model.UUID `json:"node"`
	Boot model.UUID `json:"boot"`
	Seq  uint64     `json:"seq"`
	Snap []byte     `json:"snap,omitempty"`
}

// Open 重放账本，组装各模块。
func Open(opt Options) (*Hub, error) {
	if opt.Clock == nil {
		opt.Clock = clock.System{}
	}
	h := &Hub{ID: opt.HubID, Clk: opt.Clock, opt: opt, sessions: map[model.UUID]*Session{},
		usage: map[usageKey]usageSeen{}, up: opt.Upstream, configs: map[model.UUID]*nodeConfig{}, states: map[model.UUID]*nodeState{}, rules: map[model.UUID]*nodeRules{}}
	h.Mode = mode.New(opt.Clock)
	h.Quota = quota.New(nil)
	h.Quota.Now = opt.Clock.Now
	h.Issuer = issuer.New(opt.Clock, h.Quota, h.Mode.CanRenewRun)
	h.Issuer.HubID = opt.HubID
	h.Issuer.RunGrantTTL = opt.RunTTL
	h.Leases = leases.New(opt.Clock)
	l, err := ledger.Open(opt.Store, opt.Clock, func(e ledger.Entry) error {
		if e.Kind == KindCheckpoint {
			return h.restore(e.Data)
		}
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
		if e.Kind == KindRules {
			var r rulesRecord
			if err := json.Unmarshal(e.Data, &r); err != nil {
				return err
			}
			h.applyRulesLocked(r)
		}
		if e.Kind == KindUsage {
			var r usageRecord
			if err := json.Unmarshal(e.Data, &r); err != nil {
				return err
			}
			h.usage[usageKey{r.Node, r.Boot, r.Seq}] = usageSeen{e.Seq, e.At}
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
	h.lastDeclared = h.declared()
	h.Mode.OnChange(h.broadcastMode)
	return h, nil
}

// 计量批次的幂等：
//
// 不要求序号连续：同一启动的批次可能分别由不同实例确认（切换实例前已被原实例确认的批次不会重发）。
// Agent 在一条流上总是按序号递增发送全部未确认批次，所以确认某个序号即表示这条流上更早的未确认批次
// 都已落盘，累计确认成立。

// SetUpstream 设置到后端的链路。
func (h *Hub) SetUpstream(u Upstream) {
	h.mu.Lock()
	h.up = u
	h.mu.Unlock()
}

func (h *Hub) upstream() Upstream {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.up
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
	if d == h.lastDeclared {
		h.mu.Unlock()
		return // 例如自治 → 对账：对 Agent 声明的模式不变
	}
	h.lastDeclared = d
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
	current := h.sessions[s.cert] == s
	if current {
		delete(h.sessions, s.cert)
	}
	h.mu.Unlock()
	if current {
		h.touch(s.cert, func(st *NodeState) { st.Connected = false })
	}
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
	h.touch(hello.Node, nil)
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
		if _, err := h.Issuer.RecordHeld(hello.Node, hello.Boot, gs, func(g issuer.HeldGrant) time.Duration { return rem[g.ID] }); err != nil {
			return err
		}
		if up := h.upstream(); up != nil {
			up.Node(hello.Node, m) // HUB-015 回收判定
		}
	case PrepareReport:
		h.onPrepared(hello.Node, m)
		if up := h.upstream(); up != nil {
			up.Node(hello.Node, m)
		}
	case ApplyReport:
		h.onApplied(hello.Node, m)
		if m.Committed {
			h.touch(hello.Node, func(st *NodeState) { st.AppliedConfigVersion = max(st.AppliedConfigVersion, m.Version) })
		}
		if up := h.upstream(); up != nil {
			up.Node(hello.Node, m)
		}
	case Passthrough:
		switch m.Kind {
		case "state":
			h.touch(hello.Node, func(st *NodeState) { st.State = m.Payload })
		case "probes":
			h.touch(hello.Node, func(st *NodeState) { st.Probes = m.Payload })
		}
		if up := h.upstream(); up != nil {
			up.Node(hello.Node, m)
		}
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
	h.deliverConfig(m.Node, m.AppliedConfigVersion)
	h.touch(m.Node, func(st *NodeState) {
		st.Boot, st.Connected, st.AppliedConfigVersion, st.AgentVersion = m.Boot, true, m.AppliedConfigVersion, m.AgentVersion
	})
	if up := h.upstream(); up != nil {
		up.Node(m.Node, m) // 节点接入（HUB-004 状态；后端据此为新启动生成份额）
	}
	return nil
}

func (s *Session) onGrant(hello Hello, r GrantRequest) GrantResponse {
	h := s.hub
	resp := GrantResponse{RequestID: r.ID}
	var rules []issuer.RuleState
	if h.opt.Rules != nil {
		rules = h.opt.Rules.Rules(hello.Node)
	} else {
		rules = h.configRules(hello.Node)
	}
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
	k := usageKey{hello.Node, b.Boot, b.Seq}
	h.mu.Lock()
	u, seen := h.usage[k]
	seq := u.seq
	h.mu.Unlock()
	if seen { // 重发：只需再确认
		s.ackUsage(seq, b)
		return nil
	}
	h.mu.Lock() // 写账本与记录幂等键在同一把锁内（检查点一致性）
	seq, err := h.Ledger.Append(KindUsage, usageRecord{Node: hello.Node, Boot: b.Boot, Seq: b.Seq, Snap: b.Snapshot})
	if err == nil {
		h.usage[k] = usageSeen{seq, h.Clk.Now().UTC()}
	}
	h.mu.Unlock()
	if err != nil {
		return err
	}
	for _, c := range b.Consumptions {
		if _, err := h.Issuer.ReportConsumption(c.Grant, c.Used, c.Final); err != nil {
			return err
		}
	}
	for _, d := range b.Demands {
		h.Leases.ReportDemand(hello.Node, d.Tenant, leases.Demand{Bytes60s: d.Bytes60s, StarvedPermille: d.StarvedPermille})
	}
	s.ackUsage(seq, b)
	return nil
}

// ackUsage：在线且有上游时，账本条目被后端确认后才确认（HUB-003）；否则落盘即确认。
// 重发的批次不重复写账本，按原条目的确认情况确认。
func (s *Session) ackUsage(ledgerSeq uint64, b UsageBatch) {
	h := s.hub
	ack := func() { s.send(UsageAck{Boot: b.Boot, Through: b.Seq}) }
	if up := h.upstream(); up != nil && h.declared() == model.ModeOnline {
		up.Usage(ledgerSeq, ack)
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

// UsageEntry 是 usage.batch 条目的内容（上报后端的 NodeUsage）。
type UsageEntry struct {
	Node, Boot model.UUID
	Seq        uint64
	Snapshot   []byte
}

// DecodeUsage 解析 usage.batch 条目。
func DecodeUsage(data []byte) (UsageEntry, error) {
	var r usageRecord
	err := json.Unmarshal(data, &r)
	return UsageEntry{r.Node, r.Boot, r.Seq, r.Snap}, err
}
