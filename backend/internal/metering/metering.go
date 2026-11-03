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
	"log/slog"
	"math/big"
	"math/bits"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/proto"

	"vortex/backend/internal/db"
	p "vortex/backend/internal/platform"
	backendv1 "vortex/proto/gen/go/vortex/backend/v1"
	commonv1 "vortex/proto/gen/go/vortex/common/v1"
	edgehubv1 "vortex/proto/gen/go/vortex/edgehub/v1"
)

// Budgets 是区域额度账户（leases.Service 实现）。
type Budgets interface {
	// SettleBudget 在调用方事务内把实例的 delegated 下调 settled（不低于 0）、累加 settled_acked_total，版本递增。
	SettleBudget(ctx context.Context, q *db.Queries, tenant, hub p.UUID, settled uint64) (db.SettleBudgetRow, error)
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

// settled 入账一次结算。
func (s *Service) settled(ctx context.Context, q *db.Queries, hub p.UUID, m *backendv1.GrantSettled, eff *Effect) error {
	id, ok1 := uid(m.GetGrantId())
	tenant, ok2 := uid(m.GetTenantId())
	if !ok1 || !ok2 {
		return nil
	}
	if !m.GetFinal() {
		return q.NoteGrantUsage(ctx, db.NoteGrantUsageParams{ID: id.PG(), Used: int64(min(m.GetUsedRawBytes(), 1<<62))})
	}
	if _, err := q.LockSubscription(ctx, tenant.PG()); err != nil { // MTR-004：先锁订阅行
		return err
	}
	grants, err := q.LockBillingGrant(ctx, id.PG())
	if err != nil {
		return err
	}
	settled := m.GetSettledBillingBytes()
	exact := new(big.Int).Mul(new(big.Int).SetUint64(settled), big.NewInt(1000)) // 千分之一字节；授权未知时按结算值
	if len(grants) == 1 {
		g := grants[0]
		if g.Status == "settled" {
			return nil // 已结算（例如经另一实例的账本）
		}
		hi, lo := bits.Mul64(m.GetUsedRawBytes(), uint64(g.PathCoefficientMilli))
		exact = new(big.Int).Or(new(big.Int).Lsh(new(big.Int).SetUint64(hi), 64), new(big.Int).SetUint64(lo))
	} else {
		s.log().Warn("settlement for an unknown billing grant", "hub", hub.String(), "grant", id.String())
	}
	bs, err := q.LockBudget(ctx, db.LockBudgetParams{TenantID: tenant.PG(), HubID: hub.PG()})
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
	if len(grants) == 1 {
		st := int64(settled)
		if err := q.SettleBillingGrant(ctx, db.SettleBillingGrantParams{ID: id.PG(), Used: int64(min(m.GetUsedRawBytes(), 1<<62)), Settled: &st,
			WrittenOff: pgtype.Numeric{Int: writtenOff, Exp: -3, Valid: true}}); err != nil {
			return err
		}
	}
	if writtenOff.Sign() > 0 {
		s.log().Warn("billing written off (MTR-013, C2b H4)", "tenant", tenant.String(), "hub", hub.String(), "grant", id.String(),
			"milli_bytes", writtenOff.String())
	}
	row, err := s.Budgets.SettleBudget(ctx, q, tenant, hub, settled)
	if err != nil {
		return err
	}
	eff.Budgets = append(eff.Budgets, row)
	eff.Tenants[tenant] = true
	return nil
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
