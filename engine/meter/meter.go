// Package meter 实现引擎的计量（需求 8.8 节）：
//   - Ledger：按“规则 × 角色 × 方向 × 计费授权”累计写成功的原始字节（执行点），其他节点不带授权（ENG-039）；
//     Agent 每秒 Drain 一次；
//   - Account：执行点上一条规则的计费授权账户。每次拷贝前预扣一块，拷贝后按实际写成功字节提交，余量退回；
//     每块只能提交或退回一次（ENG-037）。剩余 30% 时发出 grant_low（每份授权一次）；没有可用授权时，
//     调用方最多等待 200 ms 让 Agent 同步申请（ENG-050）。
package meter

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"vortex/engine/types"
)

// ---------------------------------------------------------------- Ledger

type Key struct {
	Rule  types.UUID
	Role  types.HopRole
	Dir   types.Direction
	Grant types.UUID // 非执行点为零值
}

type Ledger struct {
	mu sync.Mutex
	m  map[Key]*atomic.Uint64
}

func NewLedger() *Ledger { return &Ledger{m: map[Key]*atomic.Uint64{}} }

// Counter 返回某个键的计数器；调用方可缓存指针后无锁累加。
func (l *Ledger) Counter(k Key) *atomic.Uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	c := l.m[k]
	if c == nil {
		c = new(atomic.Uint64)
		l.m[k] = c
	}
	return c
}

// Drain 原子取走全部累计值（Swap 为 0），不会丢失并发写入的字节。
func (l *Ledger) Drain() []types.UsageDelta {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []types.UsageDelta
	for k, c := range l.m {
		if v := c.Swap(0); v > 0 {
			out = append(out, types.UsageDelta{RuleID: k.Rule, Role: k.Role, Direction: k.Dir, RawBytes: v, BillingGrantID: k.Grant})
		}
	}
	sort.Slice(out, func(i, j int) bool { // 稳定顺序，便于测试与对账
		a, b := out[i], out[j]
		if a.RuleID != b.RuleID {
			return string(a.RuleID[:]) < string(b.RuleID[:])
		}
		if a.Direction != b.Direction {
			return a.Direction < b.Direction
		}
		return string(a.BillingGrantID[:]) < string(b.BillingGrantID[:])
	})
	return out
}

// ---------------------------------------------------------------- Account

// Clock 返回单调时间（自引擎启动起）。所有截止点都以它计算，墙钟回拨不影响（ENG-025）。
type Clock func() time.Duration

const LowWaterPermille = 300 // 剩余 30% 时申请下一份（ENG-037）

type grant struct {
	id       types.UUID
	amount   uint64
	reserved uint64 // 已预扣未提交
	used     uint64 // 已提交（写成功）
	deadline time.Duration
	removed  bool // 不在最新 GrantSet 中（如 LIM-010 转移作废）
	lowSent  bool
	counters [3]*atomic.Uint64 // 按方向：DirUp、DirDown
}

func (g *grant) remaining() uint64 { return g.amount - g.reserved - g.used }

// Block 是一次预扣。零值表示没有额度。
type Block struct {
	g    *grant
	N    int
	done atomic.Bool
}

type Account struct {
	Rule types.UUID
	Role types.HopRole

	clock  Clock
	ledger *Ledger
	emit   func(types.Event)

	mu      sync.Mutex
	grants  []*grant
	changed chan struct{} // 每次 Update 关闭并替换，用于唤醒等待授权的会话
}

func NewAccount(rule types.UUID, role types.HopRole, clock Clock, ledger *Ledger, emit func(types.Event)) *Account {
	if emit == nil {
		emit = func(types.Event) {}
	}
	return &Account{Rule: rule, Role: role, clock: clock, ledger: ledger, emit: emit, changed: make(chan struct{})}
}

// Update 以完整的授权集合替换（Agent 每次传入当前持有的全部计费授权）。
// 已有授权不可变（额度与截止点以首次收到时为准）；不再出现的授权停止使用。
// deadline(g) 由门控按 ENG-025 计算（首次收到时刻 + 有效期 − 安全余量），只在授权首次加入账户时调用。
func (a *Account) Update(gs []types.BillingGrant, deadline func(types.BillingGrant) time.Duration) {
	a.mu.Lock()
	seen := map[types.UUID]bool{}
	for _, bg := range gs {
		if bg.RuleID != a.Rule {
			continue
		}
		seen[bg.Meta.GrantID] = true
		exists := false
		for _, g := range a.grants {
			if g.id == bg.Meta.GrantID {
				exists = true
				break
			}
		}
		if !exists {
			g := &grant{id: bg.Meta.GrantID, amount: bg.RawBytes, deadline: deadline(bg)}
			for _, d := range []types.Direction{types.DirUp, types.DirDown} {
				g.counters[d] = a.ledger.Counter(Key{Rule: a.Rule, Role: a.Role, Dir: d, Grant: g.id})
			}
			a.grants = append(a.grants, g)
		}
	}
	for _, g := range a.grants {
		if !seen[g.id] {
			g.removed = true
		}
	}
	// 先到期的先用，避免额度在到期时被浪费
	sort.SliceStable(a.grants, func(i, j int) bool { return a.grants[i].deadline < a.grants[j].deadline })
	close(a.changed)
	a.changed = make(chan struct{})
	a.mu.Unlock()
}

func (a *Account) usable(g *grant, now time.Duration) bool {
	return !g.removed && now < g.deadline && g.remaining() > 0
}

// Reserve 预扣最多 want 字节；没有可用额度时返回 nil。
func (a *Account) Reserve(want int) *Block {
	if want <= 0 {
		return nil
	}
	now := a.clock()
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, g := range a.grants {
		if !a.usable(g, now) {
			continue
		}
		n := uint64(want)
		if r := g.remaining(); n > r {
			n = r
		}
		g.reserved += n
		return &Block{g: g, N: int(n)}
	}
	return nil
}

// ReserveOrWait 先尝试预扣；没有额度时发出 grant_needed 并最多等待 wait（ENG-050，默认 200 ms）。
func (a *Account) ReserveOrWait(ctx context.Context, want int, wait time.Duration, tenant types.UUID) *Block {
	if b := a.Reserve(want); b != nil {
		return b
	}
	a.emit(types.Event{Kind: types.EventGrantNeeded, At: a.clock(), RuleID: a.Rule, TenantID: tenant})
	timer := time.NewTimer(wait)
	defer timer.Stop()
	for {
		a.mu.Lock()
		ch := a.changed
		a.mu.Unlock()
		select {
		case <-ch:
			if b := a.Reserve(want); b != nil {
				return b
			}
		case <-timer.C:
			return a.Reserve(want)
		case <-ctx.Done():
			return nil
		}
	}
}

// Commit 按实际写成功字节提交一块，余量退回，并按方向计入账本。每块只能提交一次；重复调用被忽略并返回 false。
func (a *Account) Commit(b *Block, d types.Direction, written int) bool {
	if b == nil || !b.done.CompareAndSwap(false, true) {
		return false
	}
	if written < 0 {
		written = 0
	}
	if written > b.N {
		written = b.N // 防御：写成功字节不可能超过预扣量
	}
	a.mu.Lock()
	g := b.g
	g.reserved -= uint64(b.N)
	g.used += uint64(written)
	emitLow := !g.lowSent && g.amount > 0 && (g.amount-g.used)*1000 < g.amount*LowWaterPermille
	if emitLow {
		g.lowSent = true
	}
	remaining := g.remaining()
	a.mu.Unlock()
	if written > 0 && (d == types.DirUp || d == types.DirDown) {
		g.counters[d].Add(uint64(written))
	}
	if emitLow {
		a.emit(types.Event{Kind: types.EventGrantLow, At: a.clock(), RuleID: a.Rule, GrantID: g.id,
			Detail: itoa(remaining)})
	}
	return true
}

// Consumptions 报告每份授权的累计已提交字节；不再可用且没有未提交预扣的授权标记为 final，并从账户移除。
func (a *Account) Consumptions() []types.GrantConsumption {
	now := a.clock()
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []types.GrantConsumption
	keep := a.grants[:0]
	for _, g := range a.grants {
		final := g.reserved == 0 && (g.removed || now >= g.deadline || g.remaining() == 0)
		out = append(out, types.GrantConsumption{GrantID: g.id, UsedRawBytes: g.used, Final: final})
		if !final {
			keep = append(keep, g)
		}
	}
	a.grants = keep
	return out
}

// Available 返回当前可用的总额度（测试与诊断用）。
func (a *Account) Available() uint64 {
	now := a.clock()
	a.mu.Lock()
	defer a.mu.Unlock()
	var s uint64
	for _, g := range a.grants {
		if a.usable(g, now) {
			s += g.remaining()
		}
	}
	return s
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}
