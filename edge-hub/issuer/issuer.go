// Package issuer 是 Edge Hub 的授权签发（HUB-002、HUB-009、LIM-001、ENG-037、ENG-050、MTR-003，03 方案 3.3、3.4）。
//
// 运行授权：依据后端同步的限制快照签发，有效期 5 分钟（节点每 60 秒续期，每次新授权 ID）；账号暂停或封禁、
// 套餐到期、流量耗尽、规则被停止时拒绝；授权有效期不超过套餐到期时刻；自治到期后全部停止续期。
// 计费授权：只为执行点签发，额度 max(8 MiB, 近 1 分钟速率 × 30 秒)（原始字节），从区域额度按路径系数预留，
// 不超过 available；使用生效中的计费版本，有效期不跨越下一个版本的生效时刻。
//
// 签发记录、限制快照与计费版本都写入账本，重启（包括自治期间）后按账本恢复。
package issuer

import (
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"

	"vortex/edgehub/clock"
	"vortex/edgehub/ledger"
	"vortex/edgehub/model"
	"vortex/edgehub/quota"
)

const (
	RunTTL         = 5 * time.Minute
	BillingTTL     = 10 * time.Minute
	MinBillingRaw  = 8 << 20 // 8 MiB（D-04 起步额度）
	BillingWindow  = 30 * time.Second
	KindRun        = "grant.run"
	KindRestrict   = "restrictions"
	KindBillingVer = "billing.versions"
)

// 拒绝原因（GrantResponse.error_code）。
var (
	ErrAutonomyExpired  = errors.New("AUTONOMY_EXPIRED")
	ErrAccountSuspended = errors.New("ACCOUNT_SUSPENDED")
	ErrPlanExpired      = errors.New("PLAN_EXPIRED")
	ErrQuotaExhausted   = errors.New("QUOTA_EXHAUSTED")
	ErrRuleStopped      = errors.New("RULE_STOPPED")
	ErrNotEnforcement   = errors.New("NOT_ENFORCEMENT_POINT")
	ErrNoBillingVersion = errors.New("NO_BILLING_VERSION")
	ErrUnknownGrant     = errors.New("UNKNOWN_GRANT")
	ErrStaleBoot        = errors.New("STALE_BOOT")     // 该启动已被同一节点的新启动取代（LIM-010）
	ErrBudgetRevoked    = errors.New("BUDGET_REVOKED") // 后端已按 HUB-015 回收该租户在本实例的额度，对账完成前不签发（HUB-017）
)

type AccountStatus uint8

const (
	AccountActive AccountStatus = iota + 1
	AccountSuspended
	AccountBanned
)

// TenantRestriction ↔ backend.v1.TenantRestriction。
type TenantRestriction struct {
	Tenant           model.UUID    `json:"tenant"`
	Status           AccountStatus `json:"status"`
	PlanExpiresAt    time.Time     `json:"plan_expires_at"` // 零值表示不限
	TrafficExhausted bool          `json:"traffic_exhausted"`
	StoppedRules     []model.UUID  `json:"stopped_rules,omitempty"`
}

type restrictions struct {
	Version uint64              `json:"version"`
	Tenants []TenantRestriction `json:"tenants"`
}

// BillingVersion ↔ backend.v1.BillingVersion（组倍率由后端编译进路径系数，这里只需版本与生效时刻）。
type BillingVersion struct {
	Version     uint64    `json:"version"`
	EffectiveAt time.Time `json:"effective_at"`
}

// RuleState 是签发所需的规则信息（来自本实例转发的运行配置）。
type RuleState struct {
	RuleID           model.UUID
	TenantID         model.UUID
	EnforcementPoint bool
	PathCoefMilli    uint64
}

// Grant 是一份已签发的授权（运行授权或计费授权）。
type Grant struct {
	ID             model.UUID      `json:"id"`
	Kind           model.GrantKind `json:"kind"`
	Tenant         model.UUID      `json:"tenant,omitempty"`
	Node           model.UUID      `json:"node"`
	Boot           model.UUID      `json:"boot"`
	Rules          []model.UUID    `json:"rules,omitempty"` // 运行授权
	Rule           model.UUID      `json:"rule,omitempty"`  // 计费授权
	BillingVersion uint64          `json:"billing_version,omitempty"`
	CoefMilli      uint64          `json:"coef,omitempty"`
	RawBytes       uint64          `json:"raw,omitempty"`
	Reserved       uint64          `json:"reserved,omitempty"`
	Generation     uint64          `json:"generation"`
	IssuedAt       time.Time       `json:"issued_at"`
	TTL            time.Duration   `json:"ttl"`
	Used           uint64          `json:"-"` // 最近一次上报的已用量（非 final）
	Settling       bool            `json:"-"` // 所属启动已被新启动取代：只接受补报与结算（AGT-009、LIM-010）
}

// Issuer 签发运行授权与计费授权。
type Issuer struct {
	HubID       model.UUID
	Generation  uint64
	RunGrantTTL time.Duration // 0 = RunTTL；测试用较短的值验证到期行为

	mu       sync.Mutex
	clk      clock.Clock
	l        *ledger.Ledger
	q        *quota.Accounts
	canRun   func() bool // 模式：自治到期后为 false
	restrict map[model.UUID]TenantRestriction
	rver     uint64
	versions []BillingVersion
	grants   map[model.UUID]*Grant              // 未结算的计费授权与有效期内的运行授权
	boots    map[model.UUID]model.UUID          // 节点当前的启动
	dead     map[model.UUID]map[model.UUID]bool // 节点已被取代的启动
	held     map[model.UUID]HeldGrant           // 节点报告持有的、其他实例签发的授权（HUB-014）
	revoked  map[model.UUID]bool                // HUB-017：额度已被回收的租户
}

// New 创建签发器；canRenewRun 通常为 mode.Machine.CanRenewRun。
func New(clk clock.Clock, q *quota.Accounts, canRenewRun func() bool) *Issuer {
	return &Issuer{clk: clk, q: q, canRun: canRenewRun, restrict: map[model.UUID]TenantRestriction{}, grants: map[model.UUID]*Grant{},
		boots: map[model.UUID]model.UUID{}, dead: map[model.UUID]map[model.UUID]bool{}, held: map[model.UUID]HeldGrant{}, revoked: map[model.UUID]bool{}}
}

// Bind 在账本重放完成后绑定账本。
func (is *Issuer) Bind(l *ledger.Ledger) { is.l = l }

// Replay 恢复签发记录、限制快照与计费版本（quota.issue 条目中的授权信息也在这里恢复）。
func (is *Issuer) Replay(e ledger.Entry) (bool, error) {
	is.mu.Lock()
	defer is.mu.Unlock()
	switch e.Kind {
	case KindRun:
		var g Grant
		if err := json.Unmarshal(e.Data, &g); err != nil {
			return true, err
		}
		is.grants[g.ID] = &g
		is.noteBootLocked(g.Node, g.Boot)
	case KindBoot:
		var d bootData
		if err := json.Unmarshal(e.Data, &d); err != nil {
			return true, err
		}
		if !is.dead[d.Node][d.Boot] {
			is.boots[d.Node] = d.Boot
		}
	case KindTransfer:
		var d transferData
		if err := json.Unmarshal(e.Data, &d); err != nil {
			return true, err
		}
		is.applyTransferLocked(d)
	case KindRevokedBudget:
		var ids []model.UUID
		if err := json.Unmarshal(e.Data, &ids); err != nil {
			return true, err
		}
		is.applyRevokedLocked(ids)
	case KindHeld:
		var d heldData
		if err := json.Unmarshal(e.Data, &d); err != nil {
			return true, err
		}
		is.applyHeldLocked(d)
	case quota.KindIssue:
		_, _, _, _, _, meta, err := quota.IssueMeta(e)
		if err != nil {
			return false, err
		}
		if len(meta) > 0 {
			var g Grant
			if err := json.Unmarshal(meta, &g); err != nil {
				return false, err
			}
			is.grants[g.ID] = &g
			is.noteBootLocked(g.Node, g.Boot)
		}
		return false, nil // quota 也要处理这条
	case quota.KindSettle:
		var d struct {
			Grant model.UUID `json:"grant"`
		}
		if err := json.Unmarshal(e.Data, &d); err != nil {
			return false, err
		}
		delete(is.grants, d.Grant)
		return false, nil
	case KindRestrict:
		var r restrictions
		if err := json.Unmarshal(e.Data, &r); err != nil {
			return true, err
		}
		is.applyRestrictions(r)
	case KindBillingVer:
		var vs []BillingVersion
		if err := json.Unmarshal(e.Data, &vs); err != nil {
			return true, err
		}
		is.versions = vs
	default:
		return false, nil
	}
	return true, nil
}

func (is *Issuer) applyRestrictions(r restrictions) {
	is.rver = r.Version
	is.restrict = map[model.UUID]TenantRestriction{}
	for _, t := range r.Tenants {
		is.restrict[t.Tenant] = t
	}
}

// SetRestrictions 应用后端的限制快照（只接受版本更高的快照），写账本。
func (is *Issuer) SetRestrictions(version uint64, ts []TenantRestriction) error {
	is.mu.Lock()
	defer is.mu.Unlock()
	if version <= is.rver {
		return nil
	}
	r := restrictions{Version: version, Tenants: ts}
	if _, err := is.l.Append(KindRestrict, r); err != nil {
		return err
	}
	is.applyRestrictions(r)
	return nil
}

// SetBillingVersions 应用后端提前同步的计费版本（HUB-018），写账本。
func (is *Issuer) SetBillingVersions(vs []BillingVersion) error {
	vs = append([]BillingVersion(nil), vs...)
	sort.Slice(vs, func(i, j int) bool { return vs[i].EffectiveAt.Before(vs[j].EffectiveAt) })
	is.mu.Lock()
	defer is.mu.Unlock()
	if _, err := is.l.Append(KindBillingVer, vs); err != nil {
		return err
	}
	is.versions = vs
	return nil
}

// check 返回租户或规则当前的限制（nil 表示可签发）。
func (is *Issuer) checkLocked(tenant, rule model.UUID, now time.Time) error {
	r, ok := is.restrict[tenant]
	if !ok {
		return nil // 没有限制记录：后端未下发限制
	}
	switch {
	case r.Status == AccountSuspended || r.Status == AccountBanned:
		return ErrAccountSuspended
	case !r.PlanExpiresAt.IsZero() && !now.Before(r.PlanExpiresAt):
		return ErrPlanExpired
	case r.TrafficExhausted:
		return ErrQuotaExhausted
	}
	for _, s := range r.StoppedRules {
		if s == rule {
			return ErrRuleStopped
		}
	}
	return nil
}

// IssueRun 为节点的一组规则签发运行授权。返回授权（可能为 nil：全部被拒）与被拒规则及原因。
func (is *Issuer) IssueRun(node, boot model.UUID, rules []RuleState) (*Grant, map[model.UUID]error, error) {
	now := is.clk.Now()
	refused := map[model.UUID]error{}
	if is.canRun != nil && !is.canRun() {
		for _, r := range rules {
			refused[r.RuleID] = ErrAutonomyExpired
		}
		return nil, refused, nil
	}
	is.mu.Lock()
	defer is.mu.Unlock()
	if is.dead[node][boot] {
		return nil, nil, ErrStaleBoot
	}
	is.pruneRunLocked(now)
	g := &Grant{ID: model.NewUUID(), Kind: model.GrantRun, Node: node, Boot: boot, Generation: is.Generation, IssuedAt: now, TTL: is.runTTL()}
	for _, r := range rules {
		if err := is.checkLocked(r.TenantID, r.RuleID, now); err != nil {
			refused[r.RuleID] = err
			continue
		}
		g.Rules = append(g.Rules, r.RuleID)
		// 有效期不超过套餐到期时刻
		if t, ok := is.restrict[r.TenantID]; ok && !t.PlanExpiresAt.IsZero() {
			g.TTL = min(g.TTL, t.PlanExpiresAt.Sub(now))
		}
	}
	if len(g.Rules) == 0 {
		return nil, refused, nil
	}
	if _, err := is.l.Append(KindRun, g); err != nil {
		return nil, nil, err
	}
	is.grants[g.ID] = g
	is.noteBootLocked(node, boot)
	return g, refused, nil
}

// pruneRunLocked 丢弃已过期的运行授权（只影响内存；账本保留全部记录）。
func (is *Issuer) pruneRunLocked(now time.Time) {
	for id, g := range is.grants {
		if g.Kind == model.GrantRun && now.Sub(g.IssuedAt) > g.TTL {
			delete(is.grants, id)
		}
	}
}

// BillingRequest 是一次计费授权申请（GrantRequest，kind = BILLING）。
type BillingRequest struct {
	Node, Boot model.UUID
	Rule       RuleState
	RateBps60s uint64 // 近 1 分钟速率（bit/s）
}

// activeVersionLocked 返回生效中的计费版本与下一个版本的生效时刻（没有时为零值）。
func (is *Issuer) activeVersionLocked(now time.Time) (BillingVersion, time.Time, bool) {
	var cur BillingVersion
	found := false
	var next time.Time
	for _, v := range is.versions {
		if !v.EffectiveAt.After(now) {
			cur, found = v, true
		} else {
			next = v.EffectiveAt
			break
		}
	}
	return cur, next, found
}

// IssueBilling 签发计费授权（03 方案 3.4）。
func (is *Issuer) IssueBilling(req BillingRequest) (*Grant, error) {
	now := is.clk.Now()
	r := req.Rule
	if !r.EnforcementPoint {
		return nil, ErrNotEnforcement
	}
	is.mu.Lock()
	defer is.mu.Unlock()
	if is.dead[req.Node][req.Boot] {
		return nil, ErrStaleBoot
	}
	if err := is.checkLocked(r.TenantID, r.RuleID, now); err != nil {
		return nil, err
	}
	if is.revoked[r.TenantID] {
		return nil, ErrBudgetRevoked
	}
	ver, next, ok := is.activeVersionLocked(now)
	if !ok {
		return nil, ErrNoBillingVersion
	}
	want := max(uint64(MinBillingRaw), req.RateBps60s/8*uint64(BillingWindow/time.Second))
	raw := min(want, is.q.MaxRaw(r.TenantID, r.PathCoefMilli))
	if raw == 0 {
		return nil, ErrQuotaExhausted
	}
	ttl := BillingTTL
	if !next.IsZero() { // 跨越下一个计费版本生效时刻的授权在该时刻失效（MTR-003）
		ttl = min(ttl, next.Sub(now))
	}
	g := &Grant{ID: model.NewUUID(), Kind: model.GrantBilling, Tenant: r.TenantID, Node: req.Node, Boot: req.Boot, Rule: r.RuleID,
		BillingVersion: ver.Version, CoefMilli: r.PathCoefMilli, RawBytes: raw, Generation: is.Generation, IssuedAt: now, TTL: ttl}
	g.Reserved = model.CeilBilling(raw, r.PathCoefMilli)
	if _, err := is.q.ReserveWith(r.TenantID, g.ID, raw, r.PathCoefMilli, g); err != nil {
		if errors.Is(err, quota.ErrInsufficient) {
			return nil, ErrQuotaExhausted
		}
		return nil, err
	}
	is.grants[g.ID] = g
	is.noteBootLocked(req.Node, req.Boot)
	return g, nil
}

// ReportConsumption 处理节点上报的计费授权消耗（UsageBatch 中的 GrantConsumption）：final 时结算，
// 否则只记录最近的已用量。返回 final 结算的计费字节。
func (is *Issuer) ReportConsumption(grant model.UUID, usedRaw uint64, final bool) (uint64, error) {
	is.mu.Lock()
	g, ok := is.grants[grant]
	if !ok || g.Kind != model.GrantBilling {
		is.mu.Unlock()
		return 0, nil // 已结算（重复上报）或非本实例签发：忽略，由后端按 HUB-015 处理
	}
	if !final {
		g.Used = max(g.Used, usedRaw)
		is.mu.Unlock()
		return 0, nil
	}
	// 结算与移除授权在同一把锁内完成（锁顺序 is.mu → q.mu，与签发相同），检查点看到的两者状态一致
	defer is.mu.Unlock()
	s, err := is.q.Settle(g.Tenant, grant, usedRaw)
	if err != nil {
		return 0, err
	}
	delete(is.grants, grant)
	return s, nil
}

// Grant 返回一份未结算的授权。
func (is *Issuer) Grant(id model.UUID) (Grant, bool) {
	is.mu.Lock()
	defer is.mu.Unlock()
	g, ok := is.grants[id]
	if !ok {
		return Grant{}, false
	}
	return *g, true
}

// Outstanding 返回某节点某次启动的未结算授权（重启转移 LIM-010 使用）。
func (is *Issuer) Outstanding(node, boot model.UUID) []Grant {
	is.mu.Lock()
	defer is.mu.Unlock()
	var out []Grant
	for _, g := range is.grants {
		if g.Node == node && g.Boot == boot {
			out = append(out, *g)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].IssuedAt.Before(out[j].IssuedAt) })
	return out
}

// KindRevokedBudget 记录 HUB-017 停签的租户集合（整体替换）。
const KindRevokedBudget = "issuer.budget_revoked"

func (is *Issuer) applyRevokedLocked(ids []model.UUID) {
	is.revoked = map[model.UUID]bool{}
	for _, id := range ids {
		is.revoked[id] = true
	}
}

// SetBudgetRevoked 设置额度已被后端回收的租户（ReconcileDirective.budget_revoked_tenant_ids）：对账完成前
// 不为它们签发计费授权（HUB-017）。对账完成时以空集合调用。写账本，重启后保持。
func (is *Issuer) SetBudgetRevoked(ids []model.UUID) error {
	is.mu.Lock()
	defer is.mu.Unlock()
	if ids == nil {
		ids = []model.UUID{}
	}
	if _, err := is.l.Append(KindRevokedBudget, ids); err != nil {
		return err
	}
	is.applyRevokedLocked(ids)
	return nil
}

func (is *Issuer) runTTL() time.Duration {
	if is.RunGrantTTL > 0 {
		return is.RunGrantTTL
	}
	return RunTTL
}
