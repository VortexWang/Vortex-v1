// Package metering 是 Edge Hub 账本条目的入账（BE-T18；MTR-001～MTR-007、MTR-011、PLN-003、PLN-007）。
//
// 账本条目按 (hub_id, seq) 幂等落库（grpcgw），首次落库的条目在同一事务内入账：
//   - 签发（GrantIssued，只记计费授权）：写入 billing_grants（active）。
//   - 结算（GrantSettled）：非 final 只记录已用量；final 时按 MTR-002 用定点数计算计费流量
//     （原始字节 × 路径系数，千分之一字节），超出该实例剩余委托的部分核销、不追缴（C2b H4）；
//     其余与租户累计小数余量相加，满 1 字节的部分先扣当期基础流量、再按最早到期优先扣流量包（PLN-007、PLN-003）；
//     同一事务内把该实例的 delegated 下调结算值（ceil，C2b H3，不低于 0）、累加 settled_acked_total（MTR-004）。
//   - 计量批次（NodeUsage）：按 (node_id, boot_id, seq) 幂等（MTR-006）；只有执行点（带计费授权）的流量计入
//     分钟级与日级用量（MTR-001）；执行点上报的需求信号交给份额分配（LIM-009）。
//   - 重启转移（GrantTransferred）：旧启动的授权进入 settling（MTR-007）。
package metering

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/big"
	"math/bits"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/proto"

	"vortex/backend/internal/db"
	p "vortex/backend/internal/platform"
	backendv1 "vortex/proto/gen/go/vortex/backend/v1"
	commonv1 "vortex/proto/gen/go/vortex/common/v1"
	edgehubv1 "vortex/proto/gen/go/vortex/edgehub/v1"
	runtimev1 "vortex/proto/gen/go/vortex/runtime/v1"
)

// Budgets 是区域额度账户（leases.Service 实现）。
type Budgets interface {
	// SettleBudget 在调用方事务内入账一次结算：cut 时把实例的 delegated 下调 settled（不低于 0），ack 时累加
	// settled_acked_total；版本递增。
	SettleBudget(ctx context.Context, q *db.Queries, tenant, hub p.UUID, settled uint64, cut, ack bool) (db.SettleBudgetRow, error)
	// ReportDemand 记录执行点的需求信号（LIM-009）。
	ReportDemand(node, tenant p.UUID, bytes60s uint64, starvedPermille uint32)
}

type Service struct {
	Budgets Budgets
	Log     *slog.Logger
}

// Effect 是一批条目入账后需要在提交后做的事。
type Effect struct {
	Budgets []db.SettleBudgetRow // 结算后的区域额度，提交后下发给实例
	Tenants map[p.UUID]bool      // 余额变化的租户（重算额度与限制）
	demands []demand
}

type demand struct {
	node, tenant p.UUID
	bytes        uint64
	starved      uint32
}

// Commit 在事务提交后调用：交付需求信号。
func (e *Effect) Commit(b Budgets) {
	for _, d := range e.demands {
		b.ReportDemand(d.node, d.tenant, d.bytes, d.starved)
	}
}

func (s *Service) log() *slog.Logger {
	if s.Log == nil {
		return slog.New(slog.DiscardHandler)
	}
	return s.Log
}

func uid(u *commonv1.Uuid) (p.UUID, bool) {
	if len(u.GetValue()) != 16 {
		return p.UUID{}, false
	}
	return p.UUID(u.GetValue()), true
}

// Apply 在调用方事务内入账一条首次落库的账本条目。
func (s *Service) Apply(ctx context.Context, q *db.Queries, hub p.UUID, e *backendv1.LedgerEntry, eff *Effect) error {
	if eff.Tenants == nil {
		eff.Tenants = map[p.UUID]bool{}
	}
	switch b := e.Body.(type) {
	case *backendv1.LedgerEntry_GrantIssued:
		return s.issued(ctx, q, hub, b.GrantIssued)
	case *backendv1.LedgerEntry_GrantSettled:
		return s.settled(ctx, q, hub, b.GrantSettled, eff)
	case *backendv1.LedgerEntry_NodeUsage:
		at := time.Now()
		if e.RecordedAt != nil {
			at = e.RecordedAt.AsTime()
		}
		return s.usage(ctx, q, hub, b.NodeUsage, at, eff)
	case *backendv1.LedgerEntry_GrantTransferred:
		var ids []pgtype.UUID
		for _, id := range b.GrantTransferred.GetRevokedGrantIds() {
			if u, ok := uid(id); ok {
				ids = append(ids, u.PG())
			}
		}
		if len(ids) == 0 {
			return nil
		}
		return q.MarkGrantsSettling(ctx, ids)
	}
	return nil
}

func (s *Service) issued(ctx context.Context, q *db.Queries, hub p.UUID, g *backendv1.GrantIssued) error {
	if g.GetKind() != edgehubv1.GrantKind_GRANT_KIND_BILLING {
		return nil
	}
	id, ok1 := uid(g.GetGrantId())
	tenant, ok2 := uid(g.GetTenantId())
	rule, ok3 := uid(g.GetRuleId())
	node, ok4 := uid(g.GetNodeId())
	boot, _ := uid(g.GetBootId())
	if !ok1 || !ok2 || !ok3 || !ok4 || g.GetRawBytes() == 0 || g.GetReservedBillingBytes() == 0 || g.GetPathCoefficientMilli() == 0 {
		s.log().Warn("billing grant entry ignored", "hub", hub.String(), "grant", id.String())
		return nil
	}
	n, err := q.InsertBillingGrant(ctx, db.InsertBillingGrantParams{ID: id.PG(), TenantID: tenant.PG(), RuleID: rule.PG(), NodeID: node.PG(),
		BootID: boot.PG(), HubID: hub.PG(), BillingVersion: int64(g.GetBillingVersion()), PathCoefficientMilli: int64(g.GetPathCoefficientMilli()),
		RawBytes: int64(g.GetRawBytes()), ReservedBillingBytes: int64(g.GetReservedBillingBytes())})
	if err == nil && n == 0 {
		s.log().Debug("billing grant not recorded (duplicate, or unknown rule / node)", "grant", id.String())
	}
	return err
}

// settled 入账实例账本中的一次结算。
func (s *Service) settled(ctx context.Context, q *db.Queries, hub p.UUID, m *backendv1.GrantSettled, eff *Effect) error {
	id, ok1 := uid(m.GetGrantId())
	tenant, ok2 := uid(m.GetTenantId())
	if !ok1 || !ok2 {
		return nil
	}
	if !m.GetFinal() {
		return q.NoteGrantUsage(ctx, db.NoteGrantUsageParams{ID: id.PG(), Used: int64(min(m.GetUsedRawBytes(), 1<<62))})
	}
	return s.settle(ctx, q, hub, hub, tenant, id, m.GetUsedRawBytes(), m.GetSettledBillingBytes(), true, eff)
}

// settle 入账一份授权的最终结算。budgetHub 是签发实例（其区域额度下调）；own 表示结算来自签发实例自己的账本，
// 此时累加它的 settled_acked_total 并下发确认；否则是节点在其他实例上报的、失联实例签发的授权（HUB-015 ①
// “已报告授权按正常流程结算”），只扣余额与下调委托，不确认（签发实例没有在本地结算它）。
func (s *Service) settle(ctx context.Context, q *db.Queries, via, budgetHub, tenant, id p.UUID, used, settled uint64, own bool, eff *Effect) error {
	if _, err := q.LockSubscription(ctx, tenant.PG()); err != nil { // MTR-004：先锁订阅行
		return err
	}
	grants, err := q.LockBillingGrant(ctx, id.PG())
	if err != nil {
		return err
	}
	exact := new(big.Int).Mul(new(big.Int).SetUint64(settled), big.NewInt(1000)) // 千分之一字节；授权未知时按结算值
	if len(grants) == 1 {
		g := grants[0]
		if g.Status == "settled" || g.Status == "disputed" {
			if !own {
				return nil
			}
			// 已经入账（例如节点在其他实例上报了最终用量）：只确认，使签发实例的待入账量归零
			row, err := s.Budgets.SettleBudget(ctx, q, tenant, budgetHub, settled, false, true)
			if err != nil {
				return err
			}
			eff.Budgets = append(eff.Budgets, row)
			return nil
		}
		hi, lo := bits.Mul64(used, uint64(g.PathCoefficientMilli))
		exact = new(big.Int).Or(new(big.Int).Lsh(new(big.Int).SetUint64(hi), 64), new(big.Int).SetUint64(lo))
	} else {
		s.log().Warn("settlement for an unknown billing grant", "hub", via.String(), "grant", id.String())
	}
	bs, err := q.LockBudget(ctx, db.LockBudgetParams{TenantID: tenant.PG(), HubID: budgetHub.PG()})
	if err != nil {
		return err
	}
	var delegated uint64
	if len(bs) == 1 {
		delegated = uint64(bs[0].Delegated)
	}
	// C2b H4：超出该实例剩余委托的部分核销，不追缴
	covered := new(big.Int).Mul(new(big.Int).SetUint64(delegated), big.NewInt(1000))
	if exact.Cmp(covered) < 0 {
		covered.Set(exact)
	}
	writtenOff := new(big.Int).Sub(exact, covered)
	// MTR-002：与累计余量相加，满 1 字节的部分扣余额
	carry, err := q.GetBillingCarry(ctx, tenant.PG())
	if err != nil {
		return err
	}
	total := new(big.Int).Add(covered, big.NewInt(0))
	if len(carry) == 1 {
		total.Add(total, big.NewInt(int64(carry[0])))
	}
	whole, rem := new(big.Int).QuoRem(total, big.NewInt(1000), new(big.Int))
	want := whole.Uint64() // covered ≤ delegated × 1000，必在 uint64 内
	got, err := deduct(ctx, q, tenant, want)
	if err != nil {
		return err
	}
	if got < want { // 余额来源不足（套餐已失效等）：差额同样核销
		writtenOff.Add(writtenOff, new(big.Int).Mul(new(big.Int).SetUint64(want-got), big.NewInt(1000)))
		s.log().Warn("settlement exceeds the balance sources; written off", "tenant", tenant.String(), "bytes", want-got)
	}
	if err := q.SetBillingCarry(ctx, db.SetBillingCarryParams{TenantID: tenant.PG(), RemainderMilli: int16(rem.Int64())}); err != nil {
		return err
	}
	// HUB-013 ④：对账中（自治期间）出现的超额消费进入 disputed，72 小时后自动结算（MTR-009）
	dispute := false
	if writtenOff.Sign() > 0 && own {
		mode, err := q.HubMode(ctx, via.PG())
		if err != nil {
			return err
		}
		dispute = mode == "reconciling"
	}
	if len(grants) == 1 {
		st := int64(settled)
		wo := pgtype.Numeric{Int: writtenOff, Exp: -3, Valid: true}
		u := int64(min(used, 1<<62))
		if dispute {
			if err := q.DisputeBillingGrant(ctx, db.DisputeBillingGrantParams{ID: id.PG(), Used: u, Settled: &st, WrittenOff: wo}); err != nil {
				return err
			}
			if err := q.OpenDispute(ctx, id.PG()); err != nil {
				return err
			}
		} else if err := q.SettleBillingGrant(ctx, db.SettleBillingGrantParams{ID: id.PG(), Used: u, Settled: &st, WrittenOff: wo}); err != nil {
			return err
		}
	}
	if writtenOff.Sign() > 0 {
		s.log().Warn("billing written off (MTR-013, C2b H4)", "tenant", tenant.String(), "hub", budgetHub.String(), "grant", id.String(),
			"milli_bytes", writtenOff.String(), "disputed", dispute)
		action := "billing.written_off"
		if dispute {
			action = "billing.disputed"
		}
		if err := audit(ctx, q, budgetHub, tenant, action, id, map[string]any{"written_off_milli": writtenOff.String(), "via_hub": via.String()}); err != nil {
			return err
		}
	}
	row, err := s.Budgets.SettleBudget(ctx, q, tenant, budgetHub, settled, true, own)
	if err != nil {
		return err
	}
	if own {
		eff.Budgets = append(eff.Budgets, row)
	}
	eff.Tenants[tenant] = true
	return nil
}

func audit(ctx context.Context, q *db.Queries, hub, tenant p.UUID, action string, grant p.UUID, detail map[string]any) error {
	detail["grant"] = grant.String()
	raw, _ := json.Marshal(detail)
	kind, target := "billing_grant", grant.String()
	return q.InsertAudit(ctx, db.InsertAuditParams{ActorKind: "hub", ActorID: hub.PG(), TenantID: tenant.PG(), Action: action,
		TargetKind: &kind, TargetID: &target, Detail: raw})
}

// deduct 从租户余额扣减 amount 字节：先当期基础流量，再按最早到期优先扣流量包；套餐无效时流量包暂停（PLN-003）。
// 返回实际扣减量。调用方已锁订阅行。
func deduct(ctx context.Context, q *db.Queries, tenant p.UUID, amount uint64) (uint64, error) {
	if amount == 0 {
		return 0, nil
	}
	var done uint64
	base, err := q.BaseTraffic(ctx, tenant.PG())
	if err != nil {
		return 0, err
	}
	if len(base) == 1 && base[0] > 0 {
		d := min(amount, uint64(base[0]))
		if err := q.DeductBaseTraffic(ctx, db.DeductBaseTrafficParams{TenantID: tenant.PG(), BaseRemainingBytes: int64(d)}); err != nil {
			return 0, err
		}
		done += d
	}
	if done == amount {
		return done, nil
	}
	active, err := q.PlanActive(ctx, tenant.PG())
	if err != nil || !active {
		return done, err
	}
	packs, err := q.LockConsumablePacks(ctx, tenant.PG())
	if err != nil {
		return 0, err
	}
	for _, pk := range packs {
		if done == amount {
			break
		}
		d := min(amount-done, uint64(pk.BytesRemaining))
		if err := q.DeductPack(ctx, db.DeductPackParams{ID: pk.ID, BytesRemaining: int64(d)}); err != nil {
			return 0, err
		}
		done += d
	}
	return done, nil
}

// foreign 入账节点在本实例上报的、其他实例签发的计费授权的消耗（HUB-014 切换后；HUB-015 ①“已报告授权按正常
// 流程结算”）：本实例不结算它们（Edge Hub 只结算自己签发的），由后端在这里结算，下调签发实例的委托。
// 签发实例自己的授权由它的结算条目入账，这里跳过。
func (s *Service) foreign(ctx context.Context, q *db.Queries, via p.UUID, cs []*runtimev1.GrantConsumption, eff *Effect) error {
	for _, c := range cs {
		id, ok := uid(c.GetGrantId())
		if !ok {
			continue
		}
		info, err := q.GrantInfo(ctx, id.PG())
		if err != nil {
			return err
		}
		if len(info) != 1 || p.FromPG(info[0].HubID) == via || info[0].Status == "settled" || info[0].Status == "disputed" {
			continue
		}
		used := c.GetUsedRawBytes()
		if !c.GetFinal() {
			if err := q.NoteGrantUsage(ctx, db.NoteGrantUsageParams{ID: id.PG(), Used: int64(min(used, 1<<62))}); err != nil {
				return err
			}
			continue
		}
		cs, err := q.GrantCoefficient(ctx, id.PG())
		if err != nil || len(cs) != 1 {
			return err
		}
		hi, lo := bits.Mul64(used, uint64(cs[0]))
		if hi != 0 {
			continue
		}
		settled := (lo + 999) / 1000 // C2b H3：ceil
		if err := s.settle(ctx, q, via, p.FromPG(info[0].HubID), p.FromPG(info[0].TenantID), id, used, settled, false, eff); err != nil {
			return err
		}
	}
	return nil
}

// usage 入账一个计量批次。
func (s *Service) usage(ctx context.Context, q *db.Queries, hub p.UUID, u *backendv1.NodeUsage, at time.Time, eff *Effect) error {
	node, ok := uid(u.GetNodeId())
	boot, ok2 := uid(u.GetBatch().GetBootId())
	if !ok || !ok2 || u.GetBatch().GetSeq() == 0 {
		return nil
	}
	raw, err := proto.Marshal(u.GetBatch())
	if err != nil {
		return err
	}
	n, err := q.InsertUsageBatch(ctx, db.InsertUsageBatchParams{NodeID: node.PG(), BootID: boot.PG(), Seq: int64(u.GetBatch().GetSeq()),
		ReceivedViaHub: hub.PG(), Payload: raw})
	if err != nil || n == 0 {
		return err // 重复上报不重复入账（MTR-006）
	}
	snap := u.GetBatch().GetSnapshot()
	if err := s.foreign(ctx, q, hub, snap.GetGrantConsumptions(), eff); err != nil {
		return err
	}
	for _, d := range snap.GetDemands() {
		if t, ok := uid(d.GetTenantId()); ok {
			eff.demands = append(eff.demands, demand{node, t, d.GetBytes_60S(), d.GetStarvedPermille()})
		}
	}
	type key struct{ rule p.UUID }
	type agg struct {
		tenant         p.UUID
		up, down, bill uint64
	}
	sums := map[key]*agg{}
	tenants := map[p.UUID]*p.UUID{}
	coefs := map[p.UUID]uint64{}
	for _, d := range snap.GetDeltas() {
		grant, isEP := uid(d.GetBillingGrantId())
		rule, ok := uid(d.GetRuleId())
		if !isEP || !ok || d.GetRawBytes() == 0 {
			continue // MTR-001：只有执行点的流量计入用量；其他节点只用于展示与对账（后续批次）
		}
		tp, seen := tenants[rule]
		if !seen {
			ts, err := q.RuleTenant(ctx, rule.PG())
			if err != nil {
				return err
			}
			if len(ts) == 1 {
				t := p.FromPG(ts[0])
				tp = &t
			}
			tenants[rule] = tp
		}
		if tp == nil {
			continue
		}
		coef, seen := coefs[grant]
		if !seen {
			cs, err := q.GrantCoefficient(ctx, grant.PG())
			if err != nil {
				return err
			}
			if len(cs) == 1 {
				coef = uint64(cs[0])
			}
			coefs[grant] = coef
		}
		a := sums[key{rule}]
		if a == nil {
			a = &agg{tenant: *tp}
			sums[key{rule}] = a
		}
		if d.GetDirection() == commonv1.Direction_DIRECTION_DOWN {
			a.down += d.GetRawBytes()
		} else {
			a.up += d.GetRawBytes()
		}
		hi, lo := bits.Mul64(d.GetRawBytes(), coef)
		if hi == 0 {
			a.bill += lo / 1000 // 展示用：按批次取整；扣费以结算为准
		}
	}
	minute := at.UTC().Truncate(time.Minute)
	day := pgtype.Date{Time: time.Date(minute.Year(), minute.Month(), minute.Day(), 0, 0, 0, 0, time.UTC), Valid: true}
	for k, a := range sums {
		if err := q.AddUsageMinute(ctx, db.AddUsageMinuteParams{Minute: p.TS(minute), TenantID: a.tenant.PG(), RuleID: k.rule.PG(),
			RawBytesUp: int64(a.up), RawBytesDown: int64(a.down), BilledBytes: int64(a.bill)}); err != nil {
			return err
		}
		if err := q.AddUsageDay(ctx, db.AddUsageDayParams{Day: day, TenantID: a.tenant.PG(), RuleID: k.rule.PG(),
			RawBytes: int64(a.up + a.down), BilledBytes: int64(a.bill)}); err != nil {
			return err
		}
	}
	return nil
}

// ResolveDisputes 自动结算到期的争议（MTR-009）：争议授权在入账时已按 Edge Hub 账本的结算值扣减余额并核销超额，
// 72 小时内无人工调整即转为 settled 并写审计。返回结算的数量。
func ResolveDisputes(ctx context.Context, pool interface {
	Begin(context.Context) (pgx.Tx, error)
}, log *slog.Logger) (int, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := db.New(tx)
	due, err := q.DueDisputes(ctx)
	if err != nil {
		return 0, err
	}
	for _, g := range due {
		if err := q.SettleDisputedGrant(ctx, g); err != nil {
			return 0, err
		}
		if err := q.ResolveDispute(ctx, db.ResolveDisputeParams{GrantID: g, Resolution: []byte(`{"auto":true,"basis":"hub_ledger"}`)}); err != nil {
			return 0, err
		}
		kind, target := "billing_grant", p.FromPG(g).String()
		if err := q.InsertAudit(ctx, db.InsertAuditParams{ActorKind: "system", Action: "billing.dispute_auto_settled", TargetKind: &kind,
			TargetID: &target, Detail: []byte(`{}`)}); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	if len(due) > 0 && log != nil {
		log.Info("disputes auto-settled", "count", len(due))
	}
	return len(due), nil
}
