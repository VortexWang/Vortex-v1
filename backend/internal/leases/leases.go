// Package leases 是运行授权的限制快照、份额租约与区域额度（BE-T20、BE-T21 基础；03/04 方案 3.9）。
//
// 限制快照（HUB-009）：全部租户的账号状态、套餐到期时刻、流量耗尽与规则级停止，全量下发，内容变化时版本递增。
//
// 份额（LIM-002～LIM-006、LIM-009、LIM-010）：用户速率与连接数上限取自生效中的套餐，在事务内切分给该租户
// 当前在线的执行点（执行点由节点运行配置中 enforcement_point 的规则得出）。每次重算：保底 50% 均分、其余按需求
// 信号分配；不在本次切分范围内但仍可能被使用的份额（执行点离线、配置尚未生效的旧执行点）继续计入占用，先从上限
// 中扣除；旧启动的份额立即作废并由新启动接替（LIM-010），已确认生效的新配置中不再是执行点的节点，其份额释放。
// 租约以 5 分钟有效期下发，Edge Hub 在线时按原值为节点续租，因此后端每次重算时把未变化的租约在库中的到期时刻
// 顺延（多留一个续租周期），保证“计入占用”覆盖实例续出的租约；数额变化时生成新租约并作废旧租约，调小先于调大下发。
//
// 区域额度（HUB-006、MTR-004）：每个“租户 × 实例”一行。在线且管理该租户执行点的实例参与切分；离线、自治中的实例
// 额度保持不变并从余额中扣除；在线实例已签发、尚未结算的部分不收回，切分只针对余量。全部实例 Σ delegated ≤ 租户余额
// 在锁住订阅行的事务内保证。结算入账（SettleBudget，由 metering 在入账事务内调用）把 delegated 下调结算值并累加
// settled_acked_total（MTR-004）。失联回收（HUB-015）与对账重算在 BE-T22、BE-T23。
package leases

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"vortex/backend/internal/db"
	p "vortex/backend/internal/platform"
	backendv1 "vortex/proto/gen/go/vortex/backend/v1"
	commonv1 "vortex/proto/gen/go/vortex/common/v1"
	runtimev1 "vortex/proto/gen/go/vortex/runtime/v1"
)

const (
	TTL         = 5 * time.Minute  // LIM-004：与运行授权相同
	RenewEvery  = 60 * time.Second // LIM-009：每次续租重算
	BudgetEvery = 10 * time.Minute // HUB-006：每 10 分钟重算区域额度
)

// Hubs 是当前连接的实例（grpcgw.Server 实现）。
type Hubs interface {
	Sender(hub p.UUID) func(*backendv1.BackendMessage) // 未连接时为 nil
	Connected() []p.UUID
}

type Service struct {
	DB   *pgxpool.Pool
	Hubs Hubs
	Log  *slog.Logger

	mu       sync.Mutex
	rates    map[budgetKey]uint64 // 实例上报的近 1 小时计费速率（BudgetRequest）
	reported map[budgetKey]uint64 // 实例注册时报告的已应用额度版本
	demand   map[demandKey]demandSignal
	rver     uint64
	rdigest  [32]byte
	rsent    uint64
}

type budgetKey struct{ tenant, hub p.UUID }

type demandKey struct{ node, tenant p.UUID }

type demandSignal struct {
	value uint64 // 近 60 秒用量 × (1 + 令牌耗尽占比)
	at    time.Time
}

// DemandTTL：需求信号超过这个时长未更新即视为 0（执行点每秒上报，两个续租周期内都没有即无流量）。
const DemandTTL = 2 * RenewEvery

// ReportDemand 记录执行点上报的需求信号（LIM-009）：近 60 秒用量 × (1 + 令牌耗尽时间占比)。
func (s *Service) ReportDemand(node, tenant p.UUID, bytes60s uint64, starvedPermille uint32) {
	v := mulDiv(bytes60s, 1000+uint64(min(starvedPermille, 1000)), 1000)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.demand == nil {
		s.demand = map[demandKey]demandSignal{}
	}
	s.demand[demandKey{node, tenant}] = demandSignal{v, time.Now()}
}

func (s *Service) demandOf(node, tenant p.UUID, now time.Time) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.demand[demandKey{node, tenant}]
	if !ok || now.Sub(d.at) > DemandTTL {
		return 0
	}
	return d.value
}

func (s *Service) log() *slog.Logger {
	if s.Log == nil {
		return slog.New(slog.DiscardHandler)
	}
	return s.Log
}

func msgUUID(u p.UUID) *commonv1.Uuid { return &commonv1.Uuid{Value: append([]byte(nil), u[:]...)} }

// ---------------------------------------------------------------- 限制快照（HUB-009）

// Restrictions 返回当前限制快照；内容变化时版本递增（取 max(上一版本 + 1, 当前纳秒时刻)，后端重启后仍单调）。
func (s *Service) Restrictions(ctx context.Context) (*backendv1.RestrictionSnapshot, error) {
	q := db.New(s.DB)
	standing, err := q.ListTenantStanding(ctx)
	if err != nil {
		return nil, err
	}
	active, err := q.ListActiveRestrictions(ctx)
	if err != nil {
		return nil, err
	}
	var ruleIDs []db.ListTenantRuleIDsRow
	byTenant := map[p.UUID]*backendv1.TenantRestriction{}
	snap := &backendv1.RestrictionSnapshot{}
	for _, t := range standing {
		id := p.FromPG(t.TenantID)
		tr := &backendv1.TenantRestriction{TenantId: msgUUID(id), AccountStatus: backendv1.AccountStatus_ACCOUNT_STATUS_ACTIVE,
			TrafficExhausted: t.BalanceBytes <= 0}
		if t.ExpiresAt.Valid {
			tr.PlanExpiresAt = timestamppb.New(t.ExpiresAt.Time)
		} else {
			tr.PlanExpiresAt = timestamppb.New(time.Unix(0, 0)) // 没有套餐：按已到期处理
		}
		byTenant[id] = tr
		snap.Tenants = append(snap.Tenants, tr)
	}
	for _, r := range active {
		tr := byTenant[p.FromPG(r.TenantID)]
		if tr == nil {
			continue
		}
		switch r.Kind {
		case "account_banned":
			tr.AccountStatus = backendv1.AccountStatus_ACCOUNT_STATUS_BANNED
		case "account_suspended":
			if tr.AccountStatus != backendv1.AccountStatus_ACCOUNT_STATUS_BANNED {
				tr.AccountStatus = backendv1.AccountStatus_ACCOUNT_STATUS_SUSPENDED
			}
		case "plan_expired":
			tr.PlanExpiresAt = timestamppb.New(time.Unix(0, 0))
		case "traffic_exhausted":
			tr.TrafficExhausted = true
		default: // manual_stop、over_limit、grant_pending：规则级；未指定规则时停止该租户全部规则
			if r.RuleID.Valid {
				tr.StoppedRuleIds = append(tr.StoppedRuleIds, msgUUID(p.FromPG(r.RuleID)))
				continue
			}
			if ruleIDs == nil {
				if ruleIDs, err = q.ListTenantRuleIDs(ctx); err != nil {
					return nil, err
				}
			}
			for _, x := range ruleIDs {
				if x.TenantID == r.TenantID {
					tr.StoppedRuleIds = append(tr.StoppedRuleIds, msgUUID(p.FromPG(x.ID)))
				}
			}
		}
	}
	for _, tr := range snap.Tenants {
		sort.Slice(tr.StoppedRuleIds, func(i, j int) bool {
			return bytes.Compare(tr.StoppedRuleIds[i].Value, tr.StoppedRuleIds[j].Value) < 0
		})
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(snap)
	if err != nil {
		return nil, err
	}
	d := sha256.Sum256(raw)
	s.mu.Lock()
	if d != s.rdigest || s.rver == 0 {
		s.rdigest = d
		s.rver = max(s.rver+1, uint64(time.Now().UnixNano()))
	}
	snap.SnapshotVersion = s.rver
	s.mu.Unlock()
	return snap, nil
}

// BroadcastRestrictions 在快照变化后发给全部已连接的实例。
func (s *Service) BroadcastRestrictions(ctx context.Context) error {
	snap, err := s.Restrictions(ctx)
	if err != nil {
		return err
	}
	s.mu.Lock()
	changed := snap.SnapshotVersion != s.rsent
	s.rsent = snap.SnapshotVersion
	s.mu.Unlock()
	if !changed {
		return nil
	}
	for _, h := range s.Hubs.Connected() {
		if send := s.Hubs.Sender(h); send != nil {
			send(&backendv1.BackendMessage{Body: &backendv1.BackendMessage_Restrictions{Restrictions: snap}})
		}
	}
	return nil
}

// ---------------------------------------------------------------- 执行点

type epNode struct {
	node, hub, boot p.UUID
	online          bool            // 已接入实例（有当前启动）
	applied         bool            // 最新配置已生效
	tenants         map[p.UUID]bool // 本节点为执行点的租户
}

func loadEPs(ctx context.Context, q *db.Queries) (map[p.UUID]*epNode, error) {
	rows, err := q.ListLeaseNodes(ctx)
	if err != nil {
		return nil, err
	}
	out := map[p.UUID]*epNode{}
	for _, r := range rows {
		var cfg runtimev1.RuntimeConfig
		if err := proto.Unmarshal(r.Config, &cfg); err != nil {
			return nil, err
		}
		e := &epNode{node: p.FromPG(r.NodeID), hub: p.FromPG(r.EdgeHubID), boot: p.FromPG(r.CurrentBootID),
			online:  r.Lifecycle == "online" && r.EdgeHubID.Valid && r.CurrentBootID.Valid,
			applied: r.AppliedVersion != nil && *r.AppliedVersion == r.Version, tenants: map[p.UUID]bool{}}
		for _, rule := range cfg.GetRules() {
			if rule.GetEnforcementPoint() && len(rule.GetTenantId().GetValue()) == 16 {
				e.tenants[p.UUID(rule.GetTenantId().GetValue())] = true
			}
		}
		out[e.node] = e
	}
	return out, nil
}

// ---------------------------------------------------------------- 份额（LIM-002～LIM-010）

var kinds = []struct {
	name string
	kind runtimev1.ShareKind
}{{"rate", runtimev1.ShareKind_SHARE_KIND_RATE}, {"conn", runtimev1.ShareKind_SHARE_KIND_CONN}}

type push struct {
	hub, node p.UUID
	delta     int64
	lease     *runtimev1.ShareLease
}

// ReallocateShares 为这些租户重算份额并下发变化的租约。
func (s *Service) ReallocateShares(ctx context.Context, tenants ...p.UUID) error {
	var out []push
	for _, t := range tenants {
		var ps []push
		err := p.InTx(ctx, s.DB, func(tx pgx.Tx) error {
			var err error
			ps, err = s.reallocate(ctx, db.New(tx), t)
			return err
		})
		if err != nil {
			return err
		}
		out = append(out, ps...)
	}
	// LIM-006：调小先于调大下发
	sort.SliceStable(out, func(i, j int) bool { return out[i].delta < out[j].delta })
	for _, x := range out {
		if send := s.Hubs.Sender(x.hub); send != nil {
			send(&backendv1.BackendMessage{Body: &backendv1.BackendMessage_Leases{Leases: &backendv1.ShareLeaseUpdate{
				NodeId: msgUUID(x.node), Leases: []*runtimev1.ShareLease{x.lease}}}})
		}
	}
	return nil
}

func (s *Service) reallocate(ctx context.Context, q *db.Queries, tenant p.UUID) ([]push, error) {
	if err := q.LockTenantLeases(ctx, tenant.PG()); err != nil {
		return nil, err
	}
	eps, err := loadEPs(ctx, q)
	if err != nil {
		return nil, err
	}
	plan, err := q.TenantPlan(ctx, tenant.PG())
	noPlan := errors.Is(err, pgx.ErrNoRows)
	if err != nil && !noPlan {
		return nil, err
	}
	live, err := q.ListLiveLeases(ctx, tenant.PG())
	if err != nil {
		return nil, err
	}
	// 参与切分的执行点：在线（已接入实例）且最新配置中是该租户的执行点
	var set []*epNode
	for _, e := range eps {
		if e.online && e.tenants[tenant] {
			set = append(set, e)
		}
	}
	sort.Slice(set, func(i, j int) bool { return bytes.Compare(set[i].node[:], set[j].node[:]) < 0 })
	inSet := map[p.UUID]*epNode{}
	for _, e := range set {
		inSet[e.node] = e
	}
	now := time.Now()
	expires := now.Add(TTL + RenewEvery) // 覆盖实例按原值续出的租约
	var release []p.UUID
	type cur struct {
		id     p.UUID
		amount int64
	}
	held := map[string]map[p.UUID]cur{"rate": {}, "conn": {}}
	outside := map[string]uint64{}
	for _, l := range live {
		node, boot := p.FromPG(l.NodeID), p.FromPG(l.BootID)
		e := eps[node]
		switch {
		case e != nil && e.online && e.boot != boot:
			release = append(release, p.FromPG(l.ID)) // LIM-010：旧启动的份额立即作废
		case e == nil || (e.applied && !e.tenants[tenant]):
			release = append(release, p.FromPG(l.ID)) // 节点已删除，或已生效的配置中不再是执行点
		case inSet[node] != nil:
			if _, dup := held[l.Kind][node]; dup {
				release = append(release, p.FromPG(l.ID)) // 同一启动只保留最新一份（按到期时刻降序）
				continue
			}
			held[l.Kind][node] = cur{p.FromPG(l.ID), l.Amount}
		default:
			outside[l.Kind] += uint64(l.Amount) // 离线执行点、配置尚未生效：继续计入占用（LIM-004）
		}
	}
	var out []push
	if !noPlan && len(set) > 0 {
		limits := map[string]uint64{"rate": uint64(plan.RateBps), "conn": uint64(plan.MaxConns)}
		for _, k := range kinds {
			avail := limits[k.name] - min(limits[k.name], outside[k.name])
			demand := make([]uint64, len(set))
			for i, e := range set {
				demand[i] = s.demandOf(e.node, tenant, now)
			}
			targets := SplitShare(avail, demand)
			for i, e := range set {
				c, had := held[k.name][e.node]
				if had && uint64(c.amount) == targets[i] {
					if err := q.ExtendShareLease(ctx, db.ExtendShareLeaseParams{ID: c.id.PG(), ExpiresAt: p.TS(expires)}); err != nil {
						return nil, err
					}
					continue
				}
				id := p.NewUUIDv7()
				if err := q.InsertShareLease(ctx, db.InsertShareLeaseParams{ID: id.PG(), TenantID: tenant.PG(), NodeID: e.node.PG(),
					BootID: e.boot.PG(), Kind: k.name, Amount: int64(targets[i]), ExpiresAt: p.TS(expires)}); err != nil {
					return nil, err
				}
				if had {
					release = append(release, c.id)
				}
				out = append(out, push{hub: e.hub, node: e.node, delta: int64(targets[i]) - c.amount, lease: &runtimev1.ShareLease{
					Meta:     &runtimev1.GrantMeta{GrantId: msgUUID(id), BootId: msgUUID(e.boot), Ttl: durationpb.New(TTL)},
					TenantId: msgUUID(tenant), Kind: k.kind, Amount: targets[i]}})
			}
		}
	}
	if len(release) > 0 {
		ids := make([]pgtype.UUID, len(release))
		for i, r := range release {
			ids[i] = r.PG()
		}
		if err := q.ReleaseShareLeases(ctx, ids); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// ---------------------------------------------------------------- 区域额度（HUB-006、MTR-004）

// ReportRate 记录实例上报的近 1 小时计费速率（BudgetRequest）。
func (s *Service) ReportRate(tenant, hub p.UUID, bps uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rates == nil {
		s.rates = map[budgetKey]uint64{}
	}
	s.rates[budgetKey{tenant, hub}] = bps
}

// Rebudget 重算这个租户全部在线实例的区域额度并下发变化（force 指定的实例即使没有变化也下发当前值）。
func (s *Service) Rebudget(ctx context.Context, tenant p.UUID, force p.UUID) error {
	var out []*db.UpsertBudgetRow
	err := p.InTx(ctx, s.DB, func(tx pgx.Tx) error {
		var err error
		out, err = s.rebudget(ctx, db.New(tx), tenant, force)
		return err
	})
	if err != nil {
		return err
	}
	for _, r := range out {
		if send := s.Hubs.Sender(p.FromPG(r.HubID)); send != nil {
			send(&backendv1.BackendMessage{Body: &backendv1.BackendMessage_Budget{Budget: &backendv1.BudgetUpdate{
				TenantId: msgUUID(tenant), BudgetVersion: uint64(r.BudgetVersion), Delegated: uint64(r.Delegated),
				SettledAckedTotal: uint64(r.SettledAckedTotal)}}})
		}
	}
	return nil
}

func (s *Service) rebudget(ctx context.Context, q *db.Queries, tenant, force p.UUID) ([]*db.UpsertBudgetRow, error) {
	if _, err := q.LockSubscription(ctx, tenant.PG()); err != nil { // MTR-004：先锁订阅行
		return nil, err
	}
	bal, err := q.TenantBalance(ctx, tenant.PG())
	if err != nil {
		return nil, err
	}
	balance := uint64(max(bal, 0))
	rows, err := q.ListTenantBudgets(ctx, tenant.PG())
	if err != nil {
		return nil, err
	}
	modes, err := q.ListHubModes(ctx)
	if err != nil {
		return nil, err
	}
	online := map[p.UUID]bool{}
	for _, m := range modes {
		online[p.FromPG(m.ID)] = m.Mode == "online"
	}
	eps, err := loadEPs(ctx, q)
	if err != nil {
		return nil, err
	}
	// 参与切分：在线且管理该租户（在线）执行点的实例
	part := map[p.UUID]bool{}
	for _, e := range eps {
		if e.online && e.tenants[tenant] && online[e.hub] {
			part[e.hub] = true
		}
	}
	type row struct {
		delegated, settled, version int64
		exists                      bool
	}
	cur := map[p.UUID]row{}
	var held uint64 // 离线、自治、对账中的实例：额度保持不变（失联回收在 HUB-015）
	for _, r := range rows {
		h := p.FromPG(r.HubID)
		cur[h] = row{r.Delegated, r.SettledAckedTotal, r.BudgetVersion, true}
		if !online[h] {
			held += uint64(r.Delegated)
		}
	}
	hubs := make([]p.UUID, 0, len(part))
	for h := range part {
		hubs = append(hubs, h)
	}
	sort.Slice(hubs, func(i, j int) bool { return bytes.Compare(hubs[i][:], hubs[j][:]) < 0 })
	s.mu.Lock()
	rates := make([]uint64, len(hubs))
	for i, h := range hubs {
		rates[i] = s.rates[budgetKey{tenant, h}]
	}
	s.mu.Unlock()
	// 在线实例已签发、尚未结算的部分不收回（MTR-013 只收回尚未签发的部分），先从余额中扣除，切分只针对未签发的余量
	outs, err := q.OutstandingByHub(ctx, tenant.PG())
	if err != nil {
		return nil, err
	}
	floor := map[p.UUID]uint64{}
	var floors uint64
	for _, o := range outs {
		h := p.FromPG(o.HubID)
		if c, ok := cur[h]; ok && online[h] {
			floor[h] = min(uint64(max(o.Reserved, 0)), uint64(c.delegated))
			floors += floor[h]
		}
	}
	avail := balance - min(balance, held+floors)
	targets := map[p.UUID]uint64{}
	for i, t := range SplitBudget(avail, rates) {
		targets[hubs[i]] = floor[hubs[i]] + t
	}
	for h := range cur {
		if online[h] && !part[h] {
			targets[h] = floor[h] // 在线但不再管理该租户执行点：收回未签发的部分
		}
	}
	if force != (p.UUID{}) && online[force] {
		if _, ok := targets[force]; !ok {
			targets[force] = 0
		}
	}
	var sum uint64 = held
	var out []*db.UpsertBudgetRow
	ids := make([]p.UUID, 0, len(targets))
	for h := range targets {
		ids = append(ids, h)
	}
	sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i][:], ids[j][:]) < 0 })
	for _, h := range ids {
		t := targets[h]
		sum += t
		c := cur[h]
		s.mu.Lock()
		rep := s.reported[budgetKey{tenant, h}]
		s.mu.Unlock()
		if c.exists && uint64(c.delegated) == t && h != force {
			continue
		}
		if !c.exists && t == 0 && h != force {
			continue
		}
		ver := max(c.version, int64(rep)) + 1 // 实例只应用更大的版本
		r, err := q.UpsertBudget(ctx, db.UpsertBudgetParams{TenantID: tenant.PG(), HubID: h.PG(), Delegated: int64(t), BudgetVersion: ver})
		if err != nil {
			return nil, err
		}
		out = append(out, &r)
	}
	if sum > balance { // 只可能因离线实例持有的额度超过余额（余额被动减少后）；在线实例已为 0
		s.log().Warn("regional budgets exceed balance; held by disconnected hubs", "tenant", tenant.String(), "balance", balance, "held", held)
	}
	return out, nil
}

// SettleBudget 入账一次结算（MTR-004，C2b H3、H4）：在调用方事务内（已锁订阅行）把实例的 delegated 下调结算值
// （不低于 0），累加 settled_acked_total，版本递增。返回新行，提交后由调用方下发 BudgetUpdate。
func (s *Service) SettleBudget(ctx context.Context, q *db.Queries, tenant, hub p.UUID, settled uint64) (db.SettleBudgetRow, error) {
	rows, err := q.LockBudget(ctx, db.LockBudgetParams{TenantID: tenant.PG(), HubID: hub.PG()})
	if err != nil {
		return db.SettleBudgetRow{}, err
	}
	var delegated, acked, version int64
	if len(rows) == 1 {
		delegated, acked, version = rows[0].Delegated, rows[0].SettledAckedTotal, rows[0].BudgetVersion
	}
	s.mu.Lock()
	rep := s.reported[budgetKey{tenant, hub}]
	s.mu.Unlock()
	cut := int64(min(settled, uint64(delegated)))
	return q.SettleBudget(ctx, db.SettleBudgetParams{TenantID: tenant.PG(), HubID: hub.PG(), Delegated: delegated - cut,
		SettledAckedTotal: acked + int64(min(settled, 1<<62)), BudgetVersion: max(version, int64(rep)) + 1})
}

// SendBudget 下发一行区域额度。
func (s *Service) SendBudget(r db.SettleBudgetRow) {
	if send := s.Hubs.Sender(p.FromPG(r.HubID)); send != nil {
		send(&backendv1.BackendMessage{Body: &backendv1.BackendMessage_Budget{Budget: &backendv1.BudgetUpdate{
			TenantId: msgUUID(p.FromPG(r.TenantID)), BudgetVersion: uint64(r.BudgetVersion), Delegated: uint64(r.Delegated),
			SettledAckedTotal: uint64(r.SettledAckedTotal)}}})
	}
}

// ---------------------------------------------------------------- 事件

// HubRegistered：实例注册（在线）时下发限制快照与它的区域额度。reported 是实例本地已应用的额度版本。
func (s *Service) HubRegistered(ctx context.Context, hub p.UUID, reported map[p.UUID]uint64, online bool) error {
	s.mu.Lock()
	if s.reported == nil {
		s.reported = map[budgetKey]uint64{}
	}
	for t, v := range reported {
		s.reported[budgetKey{t, hub}] = v
	}
	s.mu.Unlock()
	snap, err := s.Restrictions(ctx)
	if err != nil {
		return err
	}
	if send := s.Hubs.Sender(hub); send != nil {
		send(&backendv1.BackendMessage{Body: &backendv1.BackendMessage_Restrictions{Restrictions: snap}})
	}
	if !online {
		return nil // 对账完成后（HubOnline）再下发额度
	}
	return s.HubOnline(ctx, hub)
}

// HubOnline：实例（重新）在线后，重算并下发与它相关的租户的区域额度。
func (s *Service) HubOnline(ctx context.Context, hub p.UUID) error {
	q := db.New(s.DB)
	tenants := map[p.UUID]bool{}
	rows, err := q.ListHubBudgets(ctx, hub.PG())
	if err != nil {
		return err
	}
	for _, r := range rows {
		tenants[p.FromPG(r.TenantID)] = true
	}
	eps, err := loadEPs(ctx, q)
	if err != nil {
		return err
	}
	for _, e := range eps {
		if e.hub == hub {
			for t := range e.tenants {
				tenants[t] = true
			}
		}
	}
	for _, t := range sortedIDs(tenants) {
		if err := s.Rebudget(ctx, t, hub); err != nil {
			return err
		}
	}
	return nil
}

// NodeConnected：节点（新启动）接入实例后，为它所服务的租户重算份额与区域额度。
func (s *Service) NodeConnected(ctx context.Context, node p.UUID) error {
	eps, err := loadEPs(ctx, db.New(s.DB))
	if err != nil {
		return err
	}
	e := eps[node]
	if e == nil {
		return nil
	}
	return s.Refresh(ctx, sortedIDs(e.tenants)...)
}

// Refresh 重算这些租户的份额与区域额度（规则部署、节点接入后）。
func (s *Service) Refresh(ctx context.Context, tenants ...p.UUID) error {
	if err := s.ReallocateShares(ctx, tenants...); err != nil {
		return err
	}
	for _, t := range tenants {
		if err := s.Rebudget(ctx, t, p.UUID{}); err != nil {
			return err
		}
	}
	return nil
}

// ActiveTenants 返回有执行点或持有份额、区域额度的租户。
func (s *Service) ActiveTenants(ctx context.Context) ([]p.UUID, error) {
	eps, err := loadEPs(ctx, db.New(s.DB))
	if err != nil {
		return nil, err
	}
	ts := map[p.UUID]bool{}
	for _, e := range eps {
		for t := range e.tenants {
			ts[t] = true
		}
	}
	ids, err := db.New(s.DB).ListLeaseTenants(ctx)
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		ts[p.FromPG(id)] = true
	}
	return sortedIDs(ts), nil
}

// Run 定期（每 every，默认 60 秒）广播变化的限制快照并重算份额，每 BudgetEvery 重算区域额度，直到 ctx 结束。
func (s *Service) Run(ctx context.Context, every time.Duration) {
	if every <= 0 {
		every = RenewEvery
	}
	t := time.NewTicker(every)
	defer t.Stop()
	lastBudget := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if err := s.BroadcastRestrictions(ctx); err != nil && ctx.Err() == nil {
			s.log().Warn("restrictions", "err", err)
		}
		ts, err := s.ActiveTenants(ctx)
		if err != nil {
			if ctx.Err() == nil {
				s.log().Warn("lease tenants", "err", err)
			}
			continue
		}
		if err := s.ReallocateShares(ctx, ts...); err != nil && ctx.Err() == nil {
			s.log().Warn("shares", "err", err)
		}
		if time.Since(lastBudget) >= BudgetEvery {
			lastBudget = time.Now()
			for _, tn := range ts {
				if err := s.Rebudget(ctx, tn, p.UUID{}); err != nil && ctx.Err() == nil {
					s.log().Warn("budget", "tenant", tn.String(), "err", err)
				}
			}
		}
	}
}

func sortedIDs(m map[p.UUID]bool) []p.UUID {
	out := make([]p.UUID, 0, len(m))
	for id := range m {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i][:], out[j][:]) < 0 })
	return out
}
