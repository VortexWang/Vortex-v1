// Package leases 是 Edge Hub 的份额租约（HUB-002、LIM-004、LIM-006、LIM-009、LIM-010，03 方案 3.5、3.6）。
//
// 在线：转发后端生成的租约（ApplyUpdate），并记录各节点当前持有的份额。
// 自治：进入自治时冻结本实例所辖执行点每个（租户, 种类）的份额总量；每 60 秒为这些执行点续租并再平衡：
// 保底 = 总量 × 50% ÷ 活跃执行点数（近 10 分钟有会话），其余 50% 按需求信号（近 60 秒用量 × (1 + 令牌耗尽占比)）
// 分配；总和恰好等于冻结总量，不得增加。下调先行（LIM-006）：某节点的份额只有在全部节点的有效份额之和不超过
// 冻结总量时才调大——对已下发调小、尚未确认收到的节点，按调小前的份额计算；租约到期后不再计入。
//
// 租约、冻结总量写入账本；重启（包括自治期间）后恢复。
package leases

import (
	"encoding/json"
	"sort"
	"sync"
	"time"

	"vortex/edgehub/clock"
	"vortex/edgehub/ledger"
	"vortex/edgehub/model"
)

const (
	TTL          = 5 * time.Minute  // LIM-004：与运行授权相同
	ActiveWindow = 10 * time.Minute // LIM-009：近 10 分钟有会话为活跃
	FloorRatio   = 0.5

	KindIssue    = "lease.issue"
	KindFreeze   = "lease.freeze"
	KindUnfreeze = "lease.unfreeze"
	KindRevoke   = "lease.revoke"
)

// Key 是份额的（租户, 种类）。
type Key struct {
	Tenant model.UUID      `json:"tenant"`
	Kind   model.GrantKind `json:"kind"` // GrantShareRate / GrantShareConn
}

// Lease 是一份份额租约。
type Lease struct {
	ID       model.UUID      `json:"id"`
	Node     model.UUID      `json:"node"`
	Boot     model.UUID      `json:"boot"`
	Tenant   model.UUID      `json:"tenant"`
	Kind     model.GrantKind `json:"kind"`
	Amount   uint64          `json:"amount"` // RATE：bit/s；CONN：个
	IssuedAt time.Time       `json:"issued_at"`
	TTL      time.Duration   `json:"ttl"`
}

func (l Lease) key() Key { return Key{l.Tenant, l.Kind} }

func (l Lease) validAt(t time.Time) bool { return t.Before(l.IssuedAt.Add(l.TTL)) }

// Demand 是执行点上报的需求信号（UsageBatch.TenantDemand）。
type Demand struct {
	Bytes60s        uint64
	StarvedPermille uint32
}

type nodeKey struct {
	node model.UUID
	key  Key
}

type held struct {
	leases []Lease // 仍可能被节点使用的租约（新的在后）；确认收到后只保留最新一份
}

// Manager 管理份额租约。
type Manager struct {
	mu      sync.Mutex
	clk     clock.Clock
	l       *ledger.Ledger
	held    map[nodeKey]*held
	boot    map[model.UUID]model.UUID // 节点当前的启动
	frozen  map[Key]uint64            // 自治时冻结的总量；nil 表示在线
	demand  map[nodeKey]Demand
	lastAct map[nodeKey]time.Time
}

func New(clk clock.Clock) *Manager {
	return &Manager{clk: clk, held: map[nodeKey]*held{}, boot: map[model.UUID]model.UUID{},
		demand: map[nodeKey]Demand{}, lastAct: map[nodeKey]time.Time{}}
}

func (m *Manager) Bind(l *ledger.Ledger) { m.l = l }

type freezeData struct {
	Totals []struct {
		Key
		Total uint64 `json:"total"`
	} `json:"totals"`
}

type revokeData struct {
	Node model.UUID   `json:"node"`
	IDs  []model.UUID `json:"ids"`
}

// Replay 恢复租约与冻结状态。
func (m *Manager) Replay(e ledger.Entry) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch e.Kind {
	case KindIssue:
		var ls []Lease
		if err := json.Unmarshal(e.Data, &ls); err != nil {
			return true, err
		}
		for _, l := range ls {
			m.addLocked(l)
		}
	case KindRevoke:
		var r revokeData
		if err := json.Unmarshal(e.Data, &r); err != nil {
			return true, err
		}
		m.revokeLocked(r.Node, r.IDs)
	case KindFreeze:
		var f freezeData
		if err := json.Unmarshal(e.Data, &f); err != nil {
			return true, err
		}
		m.frozen = map[Key]uint64{}
		for _, t := range f.Totals {
			m.frozen[t.Key] = t.Total
		}
	case KindUnfreeze:
		m.frozen = nil
	default:
		return false, nil
	}
	return true, nil
}

func (m *Manager) addLocked(l Lease) {
	nk := nodeKey{l.Node, l.key()}
	h := m.held[nk]
	if h == nil {
		h = &held{}
		m.held[nk] = h
	}
	h.leases = append(h.leases, l)
	m.boot[l.Node] = l.Boot
}

func (m *Manager) revokeLocked(node model.UUID, ids []model.UUID) {
	gone := map[model.UUID]bool{}
	for _, id := range ids {
		gone[id] = true
	}
	for nk, h := range m.held {
		if nk.node != node {
			continue
		}
		kept := h.leases[:0]
		for _, l := range h.leases {
			if !gone[l.ID] {
				kept = append(kept, l)
			}
		}
		h.leases = kept
	}
}

// heldAmount：节点可能正在使用的份额——有效租约中的最大值（新租约确认收到前，旧的更大份额仍可能生效）。
func (h *held) amount(now time.Time) uint64 {
	var a uint64
	for _, l := range h.leases {
		if l.validAt(now) {
			a = max(a, l.Amount)
		}
	}
	return a
}

func (h *held) latest(now time.Time) (Lease, bool) {
	for i := len(h.leases) - 1; i >= 0; i-- {
		if h.leases[i].validAt(now) {
			return h.leases[i], true
		}
	}
	return Lease{}, false
}

func (m *Manager) issueLocked(ls []Lease) error {
	if len(ls) == 0 {
		return nil
	}
	if _, err := m.l.Append(KindIssue, ls); err != nil {
		return err
	}
	for _, l := range ls {
		m.addLocked(l)
	}
	return nil
}

// ApplyUpdate 记录后端为节点生成的租约（在线），返回待转发的租约（带本实例填写的签发时刻）。
func (m *Manager) ApplyUpdate(node, boot model.UUID, leases []Lease) ([]Lease, error) {
	now := m.clk.Now()
	out := make([]Lease, len(leases))
	for i, l := range leases {
		l.Node, l.Boot = node, boot
		if l.ID.IsZero() {
			l.ID = model.NewUUID()
		}
		if l.TTL == 0 {
			l.TTL = TTL
		}
		l.IssuedAt = now
		out[i] = l
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.issueLocked(out); err != nil {
		return nil, err
	}
	return out, nil
}

// Ack：节点已收到这些租约。之后该节点只按最新一份计算持有量。
func (m *Manager) Ack(node model.UUID, ids []model.UUID) {
	got := map[model.UUID]bool{}
	for _, id := range ids {
		got[id] = true
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for nk, h := range m.held {
		if nk.node != node {
			continue
		}
		for i, l := range h.leases {
			if got[l.ID] {
				h.leases = append([]Lease(nil), h.leases[i:]...) // 更早的租约已被它取代
				break
			}
		}
	}
}

// ReportDemand 记录执行点的需求信号；用量非零即视为活跃。
func (m *Manager) ReportDemand(node, tenant model.UUID, d Demand) {
	now := m.clk.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, kind := range []model.GrantKind{model.GrantShareRate, model.GrantShareConn} {
		nk := nodeKey{node, Key{tenant, kind}}
		m.demand[nk] = d
		if d.Bytes60s > 0 {
			m.lastAct[nk] = now
		}
	}
}

// Freeze 进入自治：冻结每个（租户, 种类）当前有效份额的总量（HUB-007：不得增加）。
func (m *Manager) Freeze() error {
	now := m.clk.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.frozen != nil {
		return nil
	}
	totals := map[Key]uint64{}
	for nk, h := range m.held {
		if l, ok := h.latest(now); ok {
			totals[nk.key] += l.Amount
		}
	}
	var f freezeData
	keys := make([]Key, 0, len(totals))
	for k := range totals {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return lessKey(keys[i], keys[j]) })
	for _, k := range keys {
		f.Totals = append(f.Totals, struct {
			Key
			Total uint64 `json:"total"`
		}{k, totals[k]})
	}
	if _, err := m.l.Append(KindFreeze, f); err != nil {
		return err
	}
	m.frozen = totals
	return nil
}

// Unfreeze 回到在线：之后由后端重新生成租约。
func (m *Manager) Unfreeze() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.frozen == nil {
		return nil
	}
	if _, err := m.l.Append(KindUnfreeze, struct{}{}); err != nil {
		return err
	}
	m.frozen = nil
	return nil
}

// Frozen 返回冻结总量（在线时为 nil）。
func (m *Manager) Frozen() map[Key]uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.frozen == nil {
		return nil
	}
	out := make(map[Key]uint64, len(m.frozen))
	for k, v := range m.frozen {
		out[k] = v
	}
	return out
}

func lessKey(a, b Key) bool {
	if a.Tenant != b.Tenant {
		return string(a.Tenant[:]) < string(b.Tenant[:])
	}
	return a.Kind < b.Kind
}

// Allocate 是 LIM-009 的分配：total 在 nodes 之间分配，active 中的节点各得保底，其余按需求信号；
// 没有活跃节点时均分。结果之和恰好等于 total（余数按最大余数法分配，次序确定）。
func Allocate(total uint64, nodes []model.UUID, active map[model.UUID]bool, signal map[model.UUID]float64) map[model.UUID]uint64 {
	out := map[model.UUID]uint64{}
	if len(nodes) == 0 {
		return out
	}
	sort.Slice(nodes, func(i, j int) bool { return string(nodes[i][:]) < string(nodes[j][:]) })
	var act []model.UUID
	for _, n := range nodes {
		if active[n] {
			act = append(act, n)
		}
	}
	shares := map[model.UUID]float64{}
	if len(act) == 0 {
		for _, n := range nodes {
			shares[n] = float64(total) / float64(len(nodes))
		}
	} else {
		floor := float64(total) * FloorRatio / float64(len(act))
		var sum float64
		for _, n := range act {
			sum += signal[n]
		}
		rest := float64(total) * (1 - FloorRatio)
		for _, n := range act {
			s := floor
			if sum > 0 {
				s += rest * signal[n] / sum
			} else {
				s += rest / float64(len(act))
			}
			shares[n] = s
		}
	}
	// 最大余数法：先取整，余数按小数部分从大到小（同值按节点 ID）补 1
	var assigned uint64
	type rem struct {
		n model.UUID
		f float64
	}
	var rs []rem
	for _, n := range nodes {
		v := uint64(shares[n])
		out[n] = v
		assigned += v
		rs = append(rs, rem{n, shares[n] - float64(v)})
	}
	sort.SliceStable(rs, func(i, j int) bool { return rs[i].f > rs[j].f })
	for i := 0; assigned < total; i++ {
		out[rs[i%len(rs)].n]++
		assigned++
	}
	for assigned > total { // 浮点误差导致超出时从最大者扣回
		big := nodes[0]
		for _, n := range nodes {
			if out[n] > out[big] {
				big = n
			}
		}
		out[big]--
		assigned--
	}
	return out
}

// Rebalance 在自治中为冻结范围内的执行点续租并再平衡，返回各节点要下发的租约。在线时不做任何事。
// 参与者：持有该（租户, 种类）有效租约的节点。调大受“总和不超过冻结总量”约束，未能下发的调大留待下一轮
// （或调小确认后）。
func (m *Manager) Rebalance() (map[model.UUID][]Lease, error) {
	now := m.clk.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.frozen == nil {
		return nil, nil
	}
	byKey := map[Key][]model.UUID{}
	for nk, h := range m.held {
		if _, ok := h.latest(now); ok {
			byKey[nk.key] = append(byKey[nk.key], nk.node)
		}
	}
	var issue []Lease
	keys := make([]Key, 0, len(m.frozen))
	for k := range m.frozen {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return lessKey(keys[i], keys[j]) })
	for _, k := range keys {
		nodes := byKey[k]
		if len(nodes) == 0 {
			continue
		}
		total := m.frozen[k]
		active, signal := map[model.UUID]bool{}, map[model.UUID]float64{}
		for _, n := range nodes {
			nk := nodeKey{n, k}
			active[n] = now.Sub(m.lastAct[nk]) <= ActiveWindow && !m.lastAct[nk].IsZero()
			d := m.demand[nk]
			signal[n] = float64(d.Bytes60s) * (1 + float64(d.StarvedPermille)/1000)
		}
		target := Allocate(total, nodes, active, signal)
		// 当前持有量（含未确认的旧份额）
		sum := uint64(0)
		cur := map[model.UUID]uint64{}
		for _, n := range nodes {
			cur[n] = m.held[nodeKey{n, k}].amount(now)
			sum += cur[n]
		}
		// 先下发调小与不变（续租），再在容量内下发调大
		var ups []model.UUID
		for _, n := range nodes {
			l, _ := m.held[nodeKey{n, k}].latest(now)
			switch {
			case target[n] > l.Amount:
				ups = append(ups, n)
			default:
				issue = append(issue, Lease{ID: model.NewUUID(), Node: n, Boot: m.boot[n], Tenant: k.Tenant, Kind: k.Kind,
					Amount: target[n], IssuedAt: now, TTL: TTL})
			}
		}
		for _, n := range ups {
			l, _ := m.held[nodeKey{n, k}].latest(now)
			// 调大后该节点的持有量变为 max(cur, target)（未确认的更大旧份额仍计入）
			after := max(cur[n], target[n])
			if sum-cur[n]+after <= total {
				sum = sum - cur[n] + after
				cur[n] = after
				issue = append(issue, Lease{ID: model.NewUUID(), Node: n, Boot: m.boot[n], Tenant: k.Tenant, Kind: k.Kind,
					Amount: target[n], IssuedAt: now, TTL: TTL})
			} else {
				// 暂不调大：按原值续租（LIM-004），等调小确认后下一轮再调大
				issue = append(issue, Lease{ID: model.NewUUID(), Node: n, Boot: m.boot[n], Tenant: k.Tenant, Kind: k.Kind,
					Amount: l.Amount, IssuedAt: now, TTL: TTL})
			}
		}
	}
	if err := m.issueLocked(issue); err != nil {
		return nil, err
	}
	out := map[model.UUID][]Lease{}
	for _, l := range issue {
		out[l.Node] = append(out[l.Node], l)
	}
	return out, nil
}

// Transfer 处理节点重启（LIM-010）：作废旧启动的全部租约，按原值为新启动签发，立即生效（不等旧租约到期）。
// 返回作废的租约 ID 与新租约。
func (m *Manager) Transfer(node, newBoot model.UUID) ([]model.UUID, []Lease, error) {
	now := m.clk.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	var revoked []model.UUID
	var fresh []Lease
	for nk, h := range m.held {
		if nk.node != node {
			continue
		}
		l, ok := h.latest(now)
		for _, x := range h.leases {
			revoked = append(revoked, x.ID)
		}
		if ok && l.Boot != newBoot {
			fresh = append(fresh, Lease{ID: model.NewUUID(), Node: node, Boot: newBoot, Tenant: l.Tenant, Kind: l.Kind,
				Amount: l.Amount, IssuedAt: now, TTL: TTL})
		}
	}
	if len(fresh) == 0 {
		return nil, nil, nil
	}
	if _, err := m.l.Append(KindRevoke, revokeData{Node: node, IDs: revoked}); err != nil {
		return nil, nil, err
	}
	m.revokeLocked(node, revoked)
	if err := m.issueLocked(fresh); err != nil {
		return nil, nil, err
	}
	m.boot[node] = newBoot
	return revoked, fresh, nil
}

// Held 返回节点在某（租户, 种类）上当前可能使用的份额（测试与状态展示）。
func (m *Manager) Held(node model.UUID, k Key) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if h := m.held[nodeKey{node, k}]; h != nil {
		return h.amount(m.clk.Now())
	}
	return 0
}

// Total 返回某（租户, 种类）在全部节点上的当前持有量之和（不变量：自治中 ≤ 冻结总量）。
func (m *Manager) Total(k Key) uint64 {
	now := m.clk.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	var s uint64
	for nk, h := range m.held {
		if nk.key == k {
			s += h.amount(now)
		}
	}
	return s
}
