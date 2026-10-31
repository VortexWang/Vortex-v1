// Package gate 实现运行门控（需求 8.6 节）：
//   - 授权截止点：首次收到时刻 + 有效期 − 安全余量，按单调时钟计算；同一授权重复收到不延长（ENG-025）；
//   - 准入（ENG-024）：运行授权；执行点还须速率与连接份额、连接数余量与计费授权（没有时最多等 200 ms）；
//   - 拷贝点 Hook：租户令牌桶与规则令牌桶叠加（ENG-026）、计费预扣与提交（ENG-037）、账本计量（ENG-039）；
//   - 连接数（ENG-027）：份额下调时立即拒绝超限新会话，超额部分从最近建立者开始，排空期满后强制关闭；
//   - 限制执行（ENG-028）：运行授权或份额失效时关闭受影响会话，产生开始与完成两个事件。
package gate

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"vortex/engine/meter"
	"vortex/engine/pipe"
	"vortex/engine/types"
)

var (
	ErrRunGrant    = errors.New("unauthorized: no valid run grant")
	ErrShare       = errors.New("unauthorized: no valid share lease")
	ErrConnLimited = errors.New("conn_limited")
	ErrQuota       = errors.New("quota_exhausted")
)

const (
	blockMin = 256 << 10 // 预扣块：每方向从 256 KiB 起，用满翻倍，上限 4 MiB（ENG-037）
	blockMax = 4 << 20
	demandN  = 60 // 需求信号窗口：最近 60 次快照（Agent 每秒拉取一次 ≈ 60 秒）
)

type Options struct {
	Margin      func(ttl time.Duration) time.Duration // 默认 max(10%·ttl, 10s)，且不超过 ttl/2
	GrantWait   time.Duration                         // 默认 200 ms（ENG-050）
	DrainWindow time.Duration                         // 默认 60 s（ENG-027）
	MinChunk    int                                   // 令牌最小等待量，默认 16 KiB
	Emit        func(types.Event)
}

func defaultMargin(ttl time.Duration) time.Duration {
	m := ttl / 10
	if m < 10*time.Second {
		m = 10 * time.Second
	}
	if m > ttl/2 {
		m = ttl / 2
	}
	return m
}

type tenantState struct {
	id        types.UUID
	rate      *Bucket
	rateUntil time.Duration
	connLimit uint64
	connUntil time.Duration
	sessions  []*session // 执行点会话，按建立时间排序
	bytes     atomic.Uint64
	ring      [demandN]struct {
		bytes         uint64
		starved, span time.Duration
	}
	ringPos  int
	lastSnap time.Duration
}

type ruleState struct {
	rule     *types.Rule
	account  *meter.Account // 仅执行点
	bucket   *Bucket        // 规则级限速；nil 表示不限
	sessions map[*session]struct{}
}

type session struct {
	g       *Gate
	rs      *ruleState
	t       *tenantState // 非执行点为 nil
	kill    func(reason error)
	endErr  atomic.Pointer[error] // 会话自然结束的原因（额度耗尽）
	start   time.Duration
	killed  atomic.Bool
	ended   atomic.Bool
	ep      bool
	bucket  *Bucket      // 准入时取得的规则级限速桶（快照）；nil 表示不限
	restr   *restriction // 被某个限制关闭时，结束后通知该限制
	drainTm *time.Timer

	// 每方向的未决状态；pipe 对每个方向串行调用 Acquire / Release
	blk    [3]*meter.Block
	block  [3]int
	ledger [3]*atomic.Uint64 // 非执行点按“规则 × 角色 × 方向”累计
}

type restriction struct {
	key       string
	tenant    types.UUID
	rules     []types.UUID
	reason    string
	err       error // 交给 kill 的原因（映射为 RST 原因码）
	remaining int
}

type Gate struct {
	opt    Options
	start  time.Time
	ledger *meter.Ledger

	mu         sync.Mutex
	seen       map[types.UUID]time.Duration // 授权 ID → 首次收到时计算的截止点
	runUntil   map[types.UUID]time.Duration // 规则 → 运行授权截止点
	billing    map[types.UUID][]types.BillingGrant
	tenants    map[types.UUID]*tenantState
	rules      map[types.UUID]*ruleState
	timer      *time.Timer
	restricted map[string]bool // 已产生开始事件、尚未恢复的限制，避免重复事件
}

func New(opt Options) *Gate {
	if opt.Margin == nil {
		opt.Margin = defaultMargin
	}
	if opt.GrantWait == 0 {
		opt.GrantWait = 200 * time.Millisecond
	}
	if opt.DrainWindow == 0 {
		opt.DrainWindow = 60 * time.Second
	}
	if opt.MinChunk == 0 {
		opt.MinChunk = 16 << 10
	}
	if opt.Emit == nil {
		opt.Emit = func(types.Event) {}
	}
	return &Gate{opt: opt, start: time.Now(), ledger: meter.NewLedger(),
		seen: map[types.UUID]time.Duration{}, runUntil: map[types.UUID]time.Duration{},
		billing: map[types.UUID][]types.BillingGrant{}, tenants: map[types.UUID]*tenantState{},
		rules: map[types.UUID]*ruleState{}, restricted: map[string]bool{}}
}

// Now 是引擎的单调时钟（Go 的 time.Since 使用单调读数，墙钟回拨不影响）。
func (g *Gate) Now() time.Duration { return time.Since(g.start) }

func (g *Gate) deadlineLocked(id types.UUID, ttl time.Duration) time.Duration {
	if d, ok := g.seen[id]; ok {
		return d
	}
	d := g.Now() + ttl - g.opt.Margin(ttl)
	g.seen[id] = d
	return d
}

// ---------------------------------------------------------------- 授权更新

// UpdateGrants 以完整的授权集合替换（Engine.UpdateGrants）。
func (g *Gate) UpdateGrants(set *types.GrantSet) {
	g.mu.Lock()
	present := map[types.UUID]bool{}
	runUntil := map[types.UUID]time.Duration{}
	for _, rg := range set.Run {
		present[rg.Meta.GrantID] = true
		d := g.deadlineLocked(rg.Meta.GrantID, rg.Meta.TTL)
		for _, r := range rg.RuleIDs {
			if d > runUntil[r] {
				runUntil[r] = d
			}
		}
	}
	g.runUntil = runUntil

	type share struct {
		amount uint64
		until  time.Duration
	}
	rates, conns := map[types.UUID]share{}, map[types.UUID]share{}
	for _, sl := range set.Shares {
		present[sl.Meta.GrantID] = true
		d := g.deadlineLocked(sl.Meta.GrantID, sl.Meta.TTL)
		m := rates
		if sl.Kind == types.ShareConn {
			m = conns
		}
		if d > m[sl.TenantID].until { // 同一租户同类份额以截止点最晚的租约为准
			m[sl.TenantID] = share{sl.Amount, d}
		}
	}
	ids := map[types.UUID]bool{}
	for id := range rates {
		ids[id] = true
	}
	for id := range conns {
		ids[id] = true
	}
	for id := range g.tenants {
		ids[id] = true
	}
	var downsized []*tenantState
	for id := range ids {
		r, c := rates[id], conns[id]
		t := g.tenants[id]
		if t == nil {
			t = &tenantState{id: id, rate: NewBucket(r.amount), lastSnap: g.Now()}
			g.tenants[id] = t
		} else {
			t.rate.SetRate(r.amount)
		}
		t.rateUntil, t.connUntil = r.until, c.until
		if c.amount < t.connLimit && uint64(len(t.sessions)) > c.amount {
			downsized = append(downsized, t)
		}
		t.connLimit = c.amount
	}

	g.billing = map[types.UUID][]types.BillingGrant{}
	for _, bg := range set.Billing {
		present[bg.Meta.GrantID] = true
		_ = g.deadlineLocked(bg.Meta.GrantID, bg.Meta.TTL)
		g.billing[bg.RuleID] = append(g.billing[bg.RuleID], bg)
	}
	for id := range g.seen {
		if !present[id] {
			delete(g.seen, id)
		}
	}
	var accounts []*ruleState
	for _, rs := range g.rules {
		if rs.account != nil {
			accounts = append(accounts, rs)
		}
	}
	g.mu.Unlock()

	for _, rs := range accounts {
		g.updateAccount(rs)
	}
	for _, t := range downsized {
		g.drainExcess(t)
	}
	g.sweep()
}

func (g *Gate) updateAccount(rs *ruleState) {
	g.mu.Lock()
	gs := append([]types.BillingGrant(nil), g.billing[rs.rule.RuleID]...)
	g.mu.Unlock()
	rs.account.Update(gs, func(bg types.BillingGrant) time.Duration {
		g.mu.Lock()
		defer g.mu.Unlock()
		return g.deadlineLocked(bg.Meta.GrantID, bg.Meta.TTL)
	})
}

// ---------------------------------------------------------------- 准入

func (g *Gate) ruleLocked(r *types.Rule) *ruleState {
	rs := g.rules[r.RuleID]
	if rs == nil {
		rs = &ruleState{rule: r, sessions: map[*session]struct{}{}}
		g.rules[r.RuleID] = rs
	}
	if rs.rule != r || (rs.bucket == nil) != (r.Limits.RateBps == 0) {
		// 只在规则快照变化时调整规则级限速。已有会话持有准入时取得的桶（session.bucket），
		// 这里替换指针不会与它们的读取竞争；同一个桶的速率调整由桶自身加锁完成。
		rs.rule = r
		switch {
		case r.Limits.RateBps == 0:
			rs.bucket = nil
		case rs.bucket == nil:
			rs.bucket = NewBucket(r.Limits.RateBps)
		default:
			rs.bucket.SetRate(r.Limits.RateBps)
		}
	}
	return rs
}

// Admit 实现 route.Admission（ENG-024）。
func (g *Gate) Admit(ctx context.Context, rule *types.Rule, _ netip.AddrPort, kill func(reason error)) (pipe.Hook, func(), error) {
	now := g.Now()
	g.mu.Lock()
	if g.runUntil[rule.RuleID] <= now {
		g.mu.Unlock()
		return nil, nil, ErrRunGrant
	}
	rs := g.ruleLocked(rule)
	s := &session{g: g, rs: rs, kill: kill, start: now, ep: rule.EnforcementPoint}
	if !s.ep {
		for _, d := range []types.Direction{types.DirUp, types.DirDown} {
			s.ledger[d] = g.ledger.Counter(meter.Key{Rule: rule.RuleID, Role: rule.Role, Dir: d})
		}
		rs.sessions[s] = struct{}{}
		g.mu.Unlock()
		return s, s.release, nil
	}
	t := g.tenants[rule.TenantID]
	if t == nil || t.rateUntil <= now || t.connUntil <= now {
		g.mu.Unlock()
		return nil, nil, ErrShare
	}
	epCount := 0
	for o := range rs.sessions {
		if o.ep {
			epCount++
		}
	}
	if uint64(len(t.sessions)) >= t.connLimit || (rule.Limits.MaxConns > 0 && epCount >= int(rule.Limits.MaxConns)) {
		g.mu.Unlock()
		return nil, nil, ErrConnLimited
	}
	s.t = t
	s.bucket = rs.bucket
	t.sessions = append(t.sessions, s)
	rs.sessions[s] = struct{}{}
	created := false
	if rs.account == nil {
		rs.account = meter.NewAccount(rule.RuleID, rule.Role, g.Now, g.ledger, g.opt.Emit)
		created = true
	}
	g.mu.Unlock()
	if created {
		g.updateAccount(rs)
	}
	s.block = [3]int{0, blockMin, blockMin}
	// 计费授权：没有可用额度时最多等待 GrantWait（ENG-050）
	if rs.account.Available() == 0 {
		b := rs.account.ReserveOrWait(ctx, 1, g.opt.GrantWait, rule.TenantID)
		if b == nil {
			s.release()
			return nil, nil, ErrQuota
		}
		rs.account.Commit(b, types.DirUp, 0)
	}
	return s, s.release, nil
}

// ---------------------------------------------------------------- 拷贝点 Hook

func (s *session) Acquire(ctx context.Context, d types.Direction, want int) (int, error) {
	if s.killed.Load() {
		return 0, nil
	}
	if !s.ep {
		return want, nil
	}
	g := s.g
	n, err := s.t.rate.Take(ctx, want, g.opt.MinChunk)
	if err != nil || n == 0 {
		return 0, err
	}
	if rb := s.bucket; rb != nil {
		m, err := rb.Take(ctx, n, g.opt.MinChunk)
		if err != nil {
			s.t.rate.Refund(n)
			return 0, err
		}
		s.t.rate.Refund(n - m)
		n = m
	}
	if n > s.block[d] {
		s.refund(n - s.block[d])
		n = s.block[d]
	}
	blk := s.rs.account.ReserveOrWait(ctx, n, g.opt.GrantWait, s.t.id)
	if blk == nil {
		s.refund(n)
		e := ErrQuota
		s.endErr.CompareAndSwap(nil, &e)
		return 0, nil // 额度耗尽：该方向停止，会话随之结束（ENG-028）
	}
	if blk.N < n {
		s.refund(n - blk.N)
	}
	s.blk[d] = blk
	return blk.N, nil
}

// EndReason 实现 route.EndReasoner：会话因计费授权耗尽结束时返回 ErrQuota。
func (s *session) EndReason() error {
	if p := s.endErr.Load(); p != nil {
		return *p
	}
	return nil
}

func (s *session) refund(n int) {
	if n <= 0 {
		return
	}
	s.t.rate.Refund(n)
	if rb := s.bucket; rb != nil {
		rb.Refund(n)
	}
}

func (s *session) Release(d types.Direction, granted, written int) {
	if !s.ep {
		if written > 0 {
			s.ledger[d].Add(uint64(written))
		}
		return
	}
	blk := s.blk[d]
	s.blk[d] = nil
	if blk == nil {
		return
	}
	s.rs.account.Commit(blk, d, written)
	s.refund(granted - written)
	s.t.bytes.Add(uint64(written))
	if written == s.block[d] && s.block[d] < blockMax {
		s.block[d] *= 2 // 用满翻倍（ENG-037）
	}
}

// release 在会话结束时调用一次。
func (s *session) release() {
	if !s.ended.CompareAndSwap(false, true) {
		return
	}
	g := s.g
	g.mu.Lock()
	delete(s.rs.sessions, s)
	if s.t != nil {
		for i, o := range s.t.sessions {
			if o == s {
				s.t.sessions = append(s.t.sessions[:i], s.t.sessions[i+1:]...)
				break
			}
		}
	}
	if s.drainTm != nil {
		s.drainTm.Stop()
	}
	var done *restriction
	if r := s.restr; r != nil {
		if r.remaining--; r.remaining == 0 {
			done = r
		}
	}
	g.mu.Unlock()
	if done != nil {
		g.emitRestriction(types.EventRestrictionCompleted, done, 0)
	}
}

// ---------------------------------------------------------------- 限制执行

func (g *Gate) emitRestriction(kind types.EventKind, r *restriction, affected int) {
	for _, id := range r.rules {
		g.opt.Emit(types.Event{Kind: kind, At: g.Now(), TenantID: r.tenant, RuleID: id,
			Detail: fmt.Sprintf("%s affected=%d", r.reason, affected)})
	}
	if len(r.rules) == 0 {
		g.opt.Emit(types.Event{Kind: kind, At: g.Now(), TenantID: r.tenant, Detail: fmt.Sprintf("%s affected=%d", r.reason, affected)})
	}
}

// enforce 关闭一组会话，并产生开始事件；全部会话结束后产生完成事件（ENG-028）。调用方不得持有 g.mu。
func (g *Gate) enforce(r *restriction, victims []*session) {
	g.mu.Lock()
	var kill []*session
	for _, s := range victims {
		if !s.ended.Load() && s.restr == nil {
			s.restr = r
			kill = append(kill, s)
		}
	}
	r.remaining = len(kill)
	g.mu.Unlock()
	g.emitRestriction(types.EventRestrictionStarted, r, len(kill))
	if len(kill) == 0 {
		g.emitRestriction(types.EventRestrictionCompleted, r, 0)
		return
	}
	for _, s := range kill {
		if s.killed.CompareAndSwap(false, true) {
			s.kill(r.err)
		}
	}
}

// sweep 检查到期的运行授权与份额，并安排下一次检查。
func (g *Gate) sweep() {
	now := g.Now()
	type job struct {
		r *restriction
		v []*session
	}
	var jobs []job
	g.mu.Lock()
	for id, rs := range g.rules {
		key := "run:" + string(id[:])
		if g.runUntil[id] <= now {
			if !g.restricted[key] {
				g.restricted[key] = true
				var v []*session
				for s := range rs.sessions {
					v = append(v, s)
				}
				jobs = append(jobs, job{&restriction{key: key, tenant: rs.rule.TenantID, rules: []types.UUID{id}, reason: "run_grant_expired", err: ErrRunGrant}, v})
			}
		} else {
			delete(g.restricted, key)
		}
	}
	for id, t := range g.tenants {
		key := "share:" + string(id[:])
		if t.rateUntil <= now || t.connUntil <= now {
			if !g.restricted[key] && len(t.sessions) > 0 {
				g.restricted[key] = true
				jobs = append(jobs, job{&restriction{key: key, tenant: id, reason: "share_expired", err: ErrShare}, append([]*session(nil), t.sessions...)})
			}
		} else {
			delete(g.restricted, key)
		}
	}
	next := time.Duration(-1)
	consider := func(d time.Duration) {
		if d > now && (next < 0 || d < next) {
			next = d
		}
	}
	for _, d := range g.runUntil {
		consider(d)
	}
	for _, t := range g.tenants {
		consider(t.rateUntil)
		consider(t.connUntil)
	}
	if g.timer != nil {
		g.timer.Stop()
	}
	if next >= 0 {
		g.timer = time.AfterFunc(next-now+time.Millisecond, g.sweep) // 到期由引擎自身定时器触发（ENG-025）
	}
	g.mu.Unlock()
	for _, j := range jobs {
		g.enforce(j.r, j.v)
	}
}

// drainExcess：连接份额下调后，超额部分从最近建立的会话开始，排空期满后强制关闭（ENG-027）。
func (g *Gate) drainExcess(t *tenantState) {
	g.mu.Lock()
	sort.SliceStable(t.sessions, func(i, j int) bool { return t.sessions[i].start < t.sessions[j].start })
	var excess []*session
	if n := uint64(len(t.sessions)); n > t.connLimit {
		excess = append(excess, t.sessions[t.connLimit:]...)
	}
	r := &restriction{key: "conn:" + string(t.id[:]), tenant: t.id, reason: "conn_downsized", err: ErrConnLimited}
	for _, s := range excess {
		if s.drainTm == nil {
			s := s
			s.drainTm = time.AfterFunc(g.opt.DrainWindow, func() { g.enforce(r, []*session{s}) })
		}
	}
	g.mu.Unlock()
	if len(excess) > 0 {
		g.opt.Emit(types.Event{Kind: types.EventRestrictionStarted, At: g.Now(), TenantID: t.id,
			Detail: fmt.Sprintf("conn_downsized draining=%d", len(excess))})
	}
}

// ---------------------------------------------------------------- 用量快照

// UsageSnapshot 实现 Engine.DrainUsage 的门控部分：账本增量、逐授权消费量、需求信号（LIM-009）。
func (g *Gate) UsageSnapshot() types.UsageSnapshot {
	snap := types.UsageSnapshot{Deltas: g.ledger.Drain()}
	now := g.Now()
	g.mu.Lock()
	var accounts []*meter.Account
	for _, rs := range g.rules {
		if rs.account != nil {
			accounts = append(accounts, rs.account)
		}
	}
	for id, t := range g.tenants {
		e := &t.ring[t.ringPos%demandN]
		e.bytes, e.starved, e.span = t.bytes.Swap(0), t.rate.TakeStarved(), now-t.lastSnap
		t.ringPos++
		t.lastSnap = now
		var bytes uint64
		var starved, span time.Duration
		for _, x := range t.ring {
			bytes += x.bytes
			starved += x.starved
			span += x.span
		}
		var pm uint32
		if span > 0 {
			if pm = uint32(starved * 1000 / span); pm > 1000 {
				pm = 1000
			}
		}
		snap.Demands = append(snap.Demands, types.TenantDemand{TenantID: id, Bytes60s: bytes, StarvedPermille: pm})
	}
	g.mu.Unlock()
	for _, a := range accounts {
		snap.Consumptions = append(snap.Consumptions, a.Consumptions()...)
	}
	return snap
}
