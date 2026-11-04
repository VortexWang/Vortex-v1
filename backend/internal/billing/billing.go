// Package billing 是套餐与权益（BE-T16；PLN-001～PLN-003、PLN-006、PLN-007、TEN-008、MTR-013）。
//
// 有效权益（PLN-001）= 生效中的主套餐的权益 ∩ 平台硬限制（settings 键 platform_limits）：能力取交集，数值取最小值。
// 规则保存（含回滚的重新校验，RUL-016）按有效权益检查：可用的平台节点组、协议、转发方式、个人组（自托管）、
// 规则数与规则级速率 / 连接数；份额按有效的速率与连接数切分。
//
// 定时任务（Tick）：按月重置基础流量（PLN-006：自开通日起每满一个月重置为套餐基础流量，未用完部分不结转）、
// 作废已过期流量包的余量，并维护租户级限制记录 plan_expired、traffic_exhausted（TEN-008）。余额因此减少时，
// 在同一事务内重算并收回在线实例的区域额度（MTR-013）。
package billing

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"vortex/backend/internal/db"
	p "vortex/backend/internal/platform"
)

// 权益检查的错误码（2.3 节 域_原因）。
const (
	ErrPlanInactive     = "PLAN_INACTIVE"                 // 没有生效中的套餐
	ErrRuleLimit        = "PLAN_RULE_LIMIT"               // 规则数达到上限
	ErrGroupEntitlement = "RULE_GROUP_NOT_ENTITLED"       // 平台节点组未授权
	ErrProtocol         = "RULE_PROTOCOL_NOT_ENTITLED"    // 协议未授权
	ErrForwardMode      = "RULE_MODE_NOT_ENTITLED"        // 转发方式未授权
	ErrSelfHosted       = "RULE_SELF_HOSTED_NOT_ENTITLED" // 未授权使用个人组（自托管节点）
	ErrRuleRate         = "RULE_RATE_ABOVE_PLAN"          // 规则级速率超过套餐
	ErrRuleConns        = "RULE_CONNS_ABOVE_PLAN"         // 规则级连接数超过套餐
)

// Limits 是平台硬限制（settings 键 platform_limits；缺省的项不限制）。
type Limits struct {
	MaxRules     *int64   `json:"max_rules,omitempty"`
	RateBps      *int64   `json:"rate_bps,omitempty"`
	MaxConns     *int64   `json:"max_conns,omitempty"`
	Protocols    []string `json:"protocols,omitempty"`
	ForwardModes []string `json:"forward_modes,omitempty"`
	ReverseDial  *bool    `json:"reverse_dial,omitempty"`
	SelfHosted   *bool    `json:"self_hosted,omitempty"`
}

// SettingLimits 是平台硬限制在 settings 中的键。
const SettingLimits = "platform_limits"

// Effective 是租户的有效权益。
type Effective struct {
	Active       bool // 有生效中的主套餐
	MaxRules     int64
	RateBps      int64
	MaxConns     int64
	Groups       map[p.UUID]bool // 可用的平台节点组
	Protocols    map[string]bool // tcp / udp
	ForwardModes map[string]bool // direct / tunnel / multi_hop
	ReverseDial  bool
	SelfHosted   bool
}

func set(vs []string) map[string]bool {
	m := map[string]bool{}
	for _, v := range vs {
		m[v] = true
	}
	return m
}

// PlatformLimits 读取平台硬限制。
func PlatformLimits(ctx context.Context, q *db.Queries) (Limits, error) {
	var l Limits
	rows, err := q.GetSetting(ctx, SettingLimits)
	if err != nil || len(rows) == 0 {
		return l, err
	}
	return l, json.Unmarshal(rows[0], &l)
}

// Entitlements 计算租户的有效权益（PLN-001）。没有生效中的套餐时 Active = false，其余为零值。
func Entitlements(ctx context.Context, q *db.Queries, tenant p.UUID) (*Effective, error) {
	e := &Effective{Groups: map[p.UUID]bool{}, Protocols: map[string]bool{}, ForwardModes: map[string]bool{}}
	plans, err := q.ActivePlan(ctx, tenant.PG())
	if err != nil || len(plans) == 0 {
		return e, err
	}
	pl := plans[0]
	e.Active, e.MaxRules, e.RateBps, e.MaxConns = true, int64(pl.MaxRules), pl.RateBps, int64(pl.MaxConns)
	ents, err := q.PlanEntitlements(ctx, pl.ID)
	if err != nil {
		return nil, err
	}
	for _, x := range ents {
		switch x.Kind {
		case "node_group":
			var g p.UUID
			if u, err := parseUUID(x.Value); err == nil {
				g = u
				e.Groups[g] = true
			}
		case "protocol":
			e.Protocols[x.Value] = true
		case "forward_mode":
			e.ForwardModes[x.Value] = true
		case "reverse_dial":
			e.ReverseDial = x.Value == "true"
		case "self_hosted":
			e.SelfHosted = x.Value == "true"
		}
	}
	lim, err := PlatformLimits(ctx, q)
	if err != nil {
		return nil, err
	}
	if lim.MaxRules != nil {
		e.MaxRules = min(e.MaxRules, *lim.MaxRules)
	}
	if lim.RateBps != nil {
		e.RateBps = min(e.RateBps, *lim.RateBps)
	}
	if lim.MaxConns != nil {
		e.MaxConns = min(e.MaxConns, *lim.MaxConns)
	}
	if lim.Protocols != nil {
		allow := set(lim.Protocols)
		for k := range e.Protocols {
			if !allow[k] {
				delete(e.Protocols, k)
			}
		}
	}
	if lim.ForwardModes != nil {
		allow := set(lim.ForwardModes)
		for k := range e.ForwardModes {
			if !allow[k] {
				delete(e.ForwardModes, k)
			}
		}
	}
	if lim.ReverseDial != nil && !*lim.ReverseDial {
		e.ReverseDial = false
	}
	if lim.SelfHosted != nil && !*lim.SelfHosted {
		e.SelfHosted = false
	}
	return e, nil
}

func parseUUID(s string) (p.UUID, error) {
	var u p.UUID
	if len(s) != 36 {
		return u, errors.New("bad uuid")
	}
	j := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '-' {
			continue
		}
		if j >= 32 {
			return u, errors.New("bad uuid")
		}
		v, ok := hexval(s[i])
		if !ok {
			return u, errors.New("bad uuid")
		}
		if j%2 == 0 {
			u[j/2] = v << 4
		} else {
			u[j/2] |= v
		}
		j++
	}
	if j != 32 {
		return u, errors.New("bad uuid")
	}
	return u, nil
}

func hexval(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

// ---------------------------------------------------------------- 定时任务

// Budgets 在调用方事务内重算租户的区域额度（leases.Service 实现），返回提交后要做的下发。
type Budgets interface {
	RebudgetIn(ctx context.Context, q *db.Queries, tenant p.UUID) (func(), error)
	BroadcastRestrictions(ctx context.Context) error
}

type Service struct {
	DB      *pgxpool.Pool
	Budgets Budgets
	Log     *slog.Logger
}

func (s *Service) log() *slog.Logger {
	if s.Log == nil {
		return slog.New(slog.DiscardHandler)
	}
	return s.Log
}

// nextPeriodEnd 返回 now 之后的第一个“开通日起每满一个月”的时刻，不超过到期时刻（PLN-006）。
func nextPeriodEnd(started, expires, now time.Time) time.Time {
	for k := 1; ; k++ {
		t := started.AddDate(0, k, 0)
		if t.After(now) {
			return minTime(t, expires)
		}
		if !t.Before(expires) {
			return expires
		}
	}
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// Tick 执行一次：月度重置、流量包作废、限制记录维护。返回余额或限制发生变化的租户数。
func (s *Service) Tick(ctx context.Context) (int, error) {
	changed := 0
	var after []func()
	restrictionsChanged := false
	err := p.InTx(ctx, s.DB, func(tx pgx.Tx) error {
		q := db.New(tx)
		changed, after, restrictionsChanged = 0, nil, false
		touched := map[p.UUID]bool{}
		now := time.Now()
		resets, err := q.DueBaseResets(ctx)
		if err != nil {
			return err
		}
		for _, r := range resets {
			end := nextPeriodEnd(r.StartedAt.Time, r.ExpiresAt.Time, now)
			if err := q.ResetBaseTraffic(ctx, db.ResetBaseTrafficParams{TenantID: r.TenantID, BaseRemainingBytes: r.BaseTrafficBytes,
				BasePeriodEnd: p.TS(end)}); err != nil {
				return err
			}
			touched[p.FromPG(r.TenantID)] = true
		}
		packs, err := q.ExpiredPacks(ctx)
		if err != nil {
			return err
		}
		for _, pk := range packs {
			if err := q.VoidPack(ctx, pk.ID); err != nil {
				return err
			}
			touched[p.FromPG(pk.TenantID)] = true
		}
		standing, err := q.ListTenantsWithStanding(ctx)
		if err != nil {
			return err
		}
		for _, t := range standing {
			id := p.FromPG(t.TenantID)
			if t.Subscribed && !t.PlanActive && !t.ExpiredFlag {
				if err := q.OpenTenantRestriction(ctx, db.OpenTenantRestrictionParams{ID: p.NewUUIDv7().PG(), TenantID: t.TenantID, Kind: "plan_expired"}); err != nil {
					return err
				}
				touched[id], restrictionsChanged = true, true // 套餐到期：流量包随之暂停，余额减少（MTR-013）
			} else if t.PlanActive && t.ExpiredFlag {
				if err := q.ClearTenantRestriction(ctx, db.ClearTenantRestrictionParams{TenantID: t.TenantID, Kind: "plan_expired"}); err != nil {
					return err
				}
				touched[id], restrictionsChanged = true, true
			}
			exhausted := t.PlanActive && t.BalanceBytes <= 0
			if exhausted && !t.ExhaustedFlag {
				if err := q.OpenTenantRestriction(ctx, db.OpenTenantRestrictionParams{ID: p.NewUUIDv7().PG(), TenantID: t.TenantID, Kind: "traffic_exhausted"}); err != nil {
					return err
				}
				restrictionsChanged = true
			} else if !exhausted && t.ExhaustedFlag {
				if err := q.ClearTenantRestriction(ctx, db.ClearTenantRestrictionParams{TenantID: t.TenantID, Kind: "traffic_exhausted"}); err != nil {
					return err
				}
				restrictionsChanged = true
			}
		}
		for _, id := range sorted(touched) {
			if s.Budgets == nil {
				continue
			}
			f, err := s.Budgets.RebudgetIn(ctx, q, id) // MTR-013：同一事务内重算
			if err != nil {
				return err
			}
			after = append(after, f)
		}
		changed = len(touched)
		return nil
	})
	if err != nil {
		return 0, err
	}
	for _, f := range after {
		f()
	}
	if (changed > 0 || restrictionsChanged) && s.Budgets != nil {
		if err := s.Budgets.BroadcastRestrictions(ctx); err != nil {
			s.log().Warn("restrictions after billing changes", "err", err)
		}
	}
	return changed, nil
}

func sorted(m map[p.UUID]bool) []p.UUID {
	out := make([]p.UUID, 0, len(m))
	for id := range m {
		out = append(out, id)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && string(out[j][:]) < string(out[j-1][:]); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// Run 每 every（默认 1 分钟）执行一次 Tick，直到 ctx 结束。
func (s *Service) Run(ctx context.Context, every time.Duration) {
	if every <= 0 {
		every = time.Minute
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if _, err := s.Tick(ctx); err != nil && ctx.Err() == nil {
			s.log().Warn("billing tick", "err", err)
		}
	}
}
