package leases

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"vortex/backend/internal/db"
	p "vortex/backend/internal/platform"
	edgehubv1 "vortex/proto/gen/go/vortex/edgehub/v1"
)

// 失联实例的额度（BE-T22；HUB-015～HUB-017）。
//
// 控制流断开（或后端启动时上次连接的实例）即冻结该实例的全部区域额度：不收回、不重分，计入占用（HubDisconnected、
// Startup）。冻结期间在线实例只能用空闲余额补充（HUB-016，重算时离线实例的额度按原值计入占用）。回收判定（Reclaim）：
//
//	① 该租户在该实例所在区域的全部执行点都已接入其他在线实例，并已在那里报告持有的授权（ForeignGrants）：
//	   委托下调到“已报告的、该实例签发且尚未结算的计费授权”的预留之和；这些授权按节点上报的消耗由后端结算。
//	② 自最后一次联系起超过 ReclaimAfter：该实例必然已停止签发（HUB-008），回收全部未结算部分（委托归 0）。
//
// 回收写审计（budget.reclaim）；实例恢复联系时，冻结期间被回收过的租户随对账 STARTED 下发，对账完成前不得再签发（HUB-017）。

// SafetyMargin 是回收判定 ② 的安全余量。
const SafetyMargin = 5 * time.Minute

type heldGrant struct {
	grant, issuer, tenant, rule p.UUID
	billing                     bool
	amount                      uint64
}

type foreignReport struct {
	hub    p.UUID
	grants []heldGrant
}

// ReportForeign 记录节点经实例 hub 报告的、其他实例签发的授权（HUB-014；ForeignGrants）。
func (s *Service) ReportForeign(hub, node p.UUID, gs []*edgehubv1.HeldGrant) {
	r := foreignReport{hub: hub}
	for _, g := range gs {
		id, ok1 := uidOf(g.GetGrantId())
		iss, ok2 := uidOf(g.GetIssuerHubId())
		t, ok3 := uidOf(g.GetTenantId())
		rule, _ := uidOf(g.GetRuleId())
		if !ok1 || !ok2 || !ok3 {
			continue
		}
		r.grants = append(r.grants, heldGrant{grant: id, issuer: iss, tenant: t, rule: rule,
			billing: g.GetKind() == edgehubv1.GrantKind_GRANT_KIND_BILLING, amount: g.GetAmount()})
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.foreign == nil {
		s.foreign = map[p.UUID]foreignReport{}
	}
	s.foreign[node] = r
}

func uidOf(u interface{ GetValue() []byte }) (p.UUID, bool) {
	if u == nil || len(u.GetValue()) != 16 {
		return p.UUID{}, false
	}
	return p.UUID(u.GetValue()), true
}

// HubDisconnected：实例的控制流断开，冻结它的全部区域额度（HUB-015）。
func (s *Service) HubDisconnected(ctx context.Context, hub p.UUID) error {
	return db.New(s.DB).FreezeHubBudgets(ctx, hub.PG())
}

// Startup：后端启动时，上次运行时连接的实例一律视为失联并冻结其额度。
func (s *Service) Startup(ctx context.Context) error {
	q := db.New(s.DB)
	ids, err := q.MarkAllHubsOffline(ctx)
	if err != nil {
		return err
	}
	for _, h := range ids {
		if err := q.FreezeHubBudgets(ctx, h); err != nil {
			return err
		}
	}
	return nil
}

// thaw 解冻实例的额度，返回本次冻结期间被回收过的租户（HUB-017）。
func (s *Service) thaw(ctx context.Context, hub p.UUID) ([]p.UUID, error) {
	var out []p.UUID
	err := p.InTx(ctx, s.DB, func(tx pgx.Tx) error {
		q := db.New(tx)
		out = nil
		if _, err := q.LockHubBudgets(ctx, hub.PG()); err != nil { // 等进行中的回收提交，避免漏报
			return err
		}
		ts, err := q.ReclaimedTenants(ctx, hub.PG())
		if err != nil {
			return err
		}
		for _, t := range ts {
			out = append(out, p.FromPG(t))
		}
		return q.UnfreezeHubBudgets(ctx, hub.PG())
	})
	return out, err
}

func (s *Service) reclaimAfter() time.Duration {
	if s.ReclaimAfter > 0 {
		return s.ReclaimAfter
	}
	return AutonomyCap + TTL + SafetyMargin
}

// Reclaim 对全部冻结的额度做回收判定；回收后为接管节点的在线实例补充（HUB-016）。
func (s *Service) Reclaim(ctx context.Context) error {
	q := db.New(s.DB)
	rows, err := q.ListFrozenBudgets(ctx)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	eps, err := loadEPs(ctx, q)
	if err != nil {
		return err
	}
	modes, err := q.ListHubModes(ctx)
	if err != nil {
		return err
	}
	online := map[p.UUID]bool{}
	for _, m := range modes {
		online[p.FromPG(m.ID)] = m.Mode == "online"
	}
	now := time.Now()
	touched := map[p.UUID]bool{}
	for _, r := range rows {
		tenant, hub := p.FromPG(r.TenantID), p.FromPG(r.HubID)
		reason, keep, ok := "", uint64(0), false
		if !r.LastContactAt.Valid || now.Sub(r.LastContactAt.Time) > s.reclaimAfter() {
			reason, ok = "timeout", true // ②
		} else if k, all := s.reportedHeld(ctx, q, eps, online, tenant, hub, r.Region); all {
			reason, keep, ok = "reported", k, true // ①
		}
		if !ok {
			continue
		}
		done, err := s.reclaim(ctx, tenant, hub, keep, reason)
		if err != nil {
			return err
		}
		if done {
			touched[tenant] = true
		}
	}
	for _, t := range sortedIDs(touched) {
		if err := s.Rebudget(ctx, t, p.UUID{}); err != nil {
			return err
		}
	}
	return nil
}

// reportedHeld 判定条件 ①：返回已报告的、该实例签发且尚未结算的计费授权的预留之和，以及条件是否满足。
func (s *Service) reportedHeld(ctx context.Context, q *db.Queries, eps map[p.UUID]*epNode, online map[p.UUID]bool, tenant, hub p.UUID, region string) (uint64, bool) {
	var nodes []*epNode
	for _, e := range eps {
		if e.region == region && e.tenants[tenant] {
			nodes = append(nodes, e)
		}
	}
	if len(nodes) == 0 {
		return 0, false // 没有可判定的节点：等待条件 ②
	}
	var keep uint64
	for _, e := range nodes {
		if !e.online || !online[e.hub] { // 失联实例本身不在线，仍连着它的节点不满足条件
			return 0, false
		}
		s.mu.Lock()
		rep, ok := s.foreign[e.node]
		s.mu.Unlock()
		if !ok || rep.hub != e.hub {
			return 0, false // 尚未在当前实例报告
		}
		for _, g := range rep.grants {
			if !g.billing || g.issuer != hub || g.tenant != tenant {
				continue
			}
			info, err := q.GrantInfo(ctx, g.grant.PG())
			if err != nil {
				return 0, false
			}
			switch {
			case len(info) == 1: // 已结算的不在其中
				rs, err := q.OpenGrantReservations(ctx, []pgtype.UUID{g.grant.PG()})
				if err != nil {
					return 0, false
				}
				for _, x := range rs {
					keep += uint64(x.ReservedBillingBytes)
				}
			default: // 签发条目尚未上报：按节点配置中的路径系数估算预留（ceil）
				keep += (g.amount*e.coef[g.rule] + 999) / 1000
			}
		}
	}
	return keep, true
}

// reclaim 在事务内把冻结的委托下调到 keep，写审计。
func (s *Service) reclaim(ctx context.Context, tenant, hub p.UUID, keep uint64, reason string) (bool, error) {
	done := false
	err := p.InTx(ctx, s.DB, func(tx pgx.Tx) error {
		q := db.New(tx)
		done = false
		if _, err := q.LockSubscription(ctx, tenant.PG()); err != nil {
			return err
		}
		cur, err := q.LockFrozenBudget(ctx, db.LockFrozenBudgetParams{TenantID: tenant.PG(), HubID: hub.PG()})
		if err != nil || len(cur) != 1 {
			return err // 已解冻（实例恢复联系）
		}
		if mode, err := q.HubMode(ctx, hub.PG()); err != nil || mode != "offline" {
			return err
		}
		if uint64(cur[0]) <= keep {
			return nil
		}
		if err := q.ReclaimBudget(ctx, db.ReclaimBudgetParams{TenantID: tenant.PG(), HubID: hub.PG(), Delegated: int64(keep)}); err != nil {
			return err
		}
		detail, _ := json.Marshal(map[string]any{"reason": reason, "from": cur[0], "to": keep})
		kind, target := "edge_hub", hub.String()
		done = true
		return q.InsertAudit(ctx, db.InsertAuditParams{ActorKind: "system", TenantID: tenant.PG(), Action: "budget.reclaim",
			TargetKind: &kind, TargetID: &target, Detail: detail})
	})
	if done {
		s.log().Info("regional budget reclaimed", "tenant", tenant.String(), "hub", hub.String(), "reason", reason, "keep", keep)
	}
	return done, err
}
