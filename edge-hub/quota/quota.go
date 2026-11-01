// Package quota 是 Edge Hub 的区域额度账户（HUB-006、MTR-004），语义与契约 C2 第 7 节一致，
// 并与 test/budget-sim 的随机化模型使用同一组不变量（见 sim_test.go）。
//
// 每个租户一个账户（单位为计费字节）：
//
//	delegated      后端委托、尚未结算入账的量（绝对值，随 BudgetUpdate 更新）
//	issued         已签发未结算的预留
//	settled_local  本实例累计结算（只计 final 结算）
//	acked_total    后端已入账的累计结算（settled_acked_total）
//	available      max(0, delegated − issued − (settled_local − acked_total))
//
// 所有变更先写账本再生效；重启后按账本重放得到同一状态。
package quota

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"vortex/edgehub/ledger"
	"vortex/edgehub/model"
)

var (
	ErrInsufficient = errors.New("quota: insufficient regional quota")
	ErrUnknownGrant = errors.New("quota: unknown grant")
	ErrDuplicate    = errors.New("quota: grant already reserved")
)

// 账本条目种类。
const (
	KindIssue  = "quota.issue"
	KindSettle = "quota.settle"
	KindBudget = "quota.budget"
)

// TopUpThreshold：available 低于 delegated 的 30% 时请求补充（03 方案 3.2）。
const TopUpThreshold = 0.30

type reservation struct {
	Raw      uint64 `json:"raw"`
	Coef     uint64 `json:"coef"`
	Reserved uint64 `json:"reserved"`
}

type account struct {
	version   uint64
	delegated uint64
	acked     uint64
	settled   uint64
	issued    uint64
	grants    map[model.UUID]reservation
}

func (a *account) pending() uint64 {
	if a.settled < a.acked { // 不应出现：后端入账不会超过本地结算
		return 0
	}
	return a.settled - a.acked
}

func (a *account) available() uint64 {
	used := a.issued + a.pending()
	if used >= a.delegated {
		return 0
	}
	return a.delegated - used
}

// View 是账户的只读快照。
type View struct {
	Version, Delegated, Issued, SettledLocal, AckedTotal, Pending, Available uint64
	Grants                                                                   int
}

// Accounts 是本实例全部租户的额度账户。
type Accounts struct {
	mu      sync.Mutex
	l       *ledger.Ledger
	tenants map[model.UUID]*account
}

type issueData struct {
	Tenant model.UUID `json:"tenant"`
	Grant  model.UUID `json:"grant"`
	reservation
	Meta json.RawMessage `json:"meta,omitempty"` // 签发方附带的授权信息（节点、启动、规则、计费版本），与预留同一条记录
}

// IssueMeta 从 quota.issue 条目中取出签发方的附带信息。
func IssueMeta(e ledger.Entry) (tenant, grant model.UUID, raw, coef, reserved uint64, meta json.RawMessage, err error) {
	var d issueData
	if err = json.Unmarshal(e.Data, &d); err != nil {
		return
	}
	return d.Tenant, d.Grant, d.Raw, d.Coef, d.Reserved, d.Meta, nil
}

type settleData struct {
	Tenant  model.UUID `json:"tenant"`
	Grant   model.UUID `json:"grant"`
	UsedRaw uint64     `json:"used_raw"`
	Settled uint64     `json:"settled"` // 计费字节（ceil），与后端使用同一数值
}

type budgetData struct {
	Tenant    model.UUID `json:"tenant"`
	Version   uint64     `json:"version"`
	Delegated uint64     `json:"delegated"`
	Acked     uint64     `json:"acked_total"`
}

// New 创建空账户集合；恢复时由 Replay 依次应用账本条目。
func New(l *ledger.Ledger) *Accounts {
	return &Accounts{l: l, tenants: map[model.UUID]*account{}}
}

// SetLedger 在重放完成后绑定账本（ledger.Open 的重放回调先于 Ledger 可用）。
func (q *Accounts) SetLedger(l *ledger.Ledger) { q.l = l }

func (q *Accounts) acct(t model.UUID) *account {
	a := q.tenants[t]
	if a == nil {
		a = &account{grants: map[model.UUID]reservation{}}
		q.tenants[t] = a
	}
	return a
}

// Replay 应用一条账本条目（只处理本包的种类），返回是否处理。
func (q *Accounts) Replay(e ledger.Entry) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	switch e.Kind {
	case KindIssue:
		var d issueData
		if err := json.Unmarshal(e.Data, &d); err != nil {
			return true, err
		}
		q.applyIssue(d)
	case KindSettle:
		var d settleData
		if err := json.Unmarshal(e.Data, &d); err != nil {
			return true, err
		}
		q.applySettle(d)
	case KindBudget:
		var d budgetData
		if err := json.Unmarshal(e.Data, &d); err != nil {
			return true, err
		}
		q.applyBudget(d)
	default:
		return false, nil
	}
	return true, nil
}

func (q *Accounts) applyIssue(d issueData) {
	a := q.acct(d.Tenant)
	a.grants[d.Grant] = d.reservation
	a.issued += d.Reserved
}

func (q *Accounts) applySettle(d settleData) {
	a := q.acct(d.Tenant)
	r := a.grants[d.Grant]
	delete(a.grants, d.Grant)
	a.issued -= r.Reserved
	a.settled += d.Settled
}

func (q *Accounts) applyBudget(d budgetData) {
	a := q.acct(d.Tenant)
	a.version = d.Version
	a.acked = d.Acked         // ① 记录累计值
	a.delegated = d.Delegated // ②③ pending 与 available 由派生得到
}

// Reserve 为计费授权预留 ceil(raw × coef / 1000) 计费字节；超过 available 时返回 ErrInsufficient。
func (q *Accounts) Reserve(tenant, grant model.UUID, raw, coefMilli uint64) (uint64, error) {
	return q.ReserveWith(tenant, grant, raw, coefMilli, nil)
}

// ReserveWith 与 Reserve 相同，并把 meta（JSON）写入同一条账本记录：预留与授权信息原子落盘。
func (q *Accounts) ReserveWith(tenant, grant model.UUID, raw, coefMilli uint64, meta any) (uint64, error) {
	var mj json.RawMessage
	if meta != nil {
		b, err := json.Marshal(meta)
		if err != nil {
			return 0, err
		}
		mj = b
	}
	reserved := model.CeilBilling(raw, coefMilli)
	q.mu.Lock()
	defer q.mu.Unlock()
	a := q.acct(tenant)
	if _, dup := a.grants[grant]; dup {
		return 0, ErrDuplicate
	}
	if reserved > a.available() || raw == 0 {
		return 0, ErrInsufficient
	}
	d := issueData{Tenant: tenant, Grant: grant, reservation: reservation{Raw: raw, Coef: coefMilli, Reserved: reserved}, Meta: mj}
	if _, err := q.l.Append(KindIssue, d); err != nil {
		return 0, err
	}
	q.applyIssue(d)
	return reserved, nil
}

// MaxRaw 返回在当前 available 内可签发的最大原始字节数（ceil(raw × coef / 1000) ≤ available）。
func (q *Accounts) MaxRaw(tenant model.UUID, coefMilli uint64) uint64 {
	if coefMilli == 0 {
		return 0
	}
	q.mu.Lock()
	avail := q.acct(tenant).available()
	q.mu.Unlock()
	// raw ≤ avail × 1000 / coef 时 ceil(raw × coef / 1000) ≤ avail
	hi, lo := mulDiv(avail, 1000, coefMilli)
	if hi != 0 {
		return ^uint64(0)
	}
	return lo
}

func mulDiv(a, b, d uint64) (uint64, uint64) {
	// a × b / d，a ≤ 2^64、b = 1000：用 128 位中间值
	var hi, lo uint64
	{
		const m = 1<<32 - 1
		a0, a1, b0, b1 := a&m, a>>32, b&m, b>>32
		w0 := a0 * b0
		t := a1*b0 + w0>>32
		w1, w2 := t&m, t>>32
		w1 += a0 * b1
		hi, lo = a1*b1+w2+w1>>32, a*b
	}
	if hi >= d {
		return 1, 0
	}
	var q, r uint64 = 0, hi
	for i := 63; i >= 0; i-- {
		c := r >> 63
		r = r<<1 | (lo>>uint(i))&1
		q <<= 1
		if c == 1 || r >= d {
			r -= d
			q |= 1
		}
	}
	return 0, q
}

// Settle 结算一份计费授权（final）：issued 减去预留，settled_local 加上 ceil(used × coef / 1000)。
// used 不超过授权额度（引擎不会超额使用；超出按额度计）。返回计费字节（GrantSettled.settled_billing_bytes）。
func (q *Accounts) Settle(tenant, grant model.UUID, usedRaw uint64) (uint64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	a := q.acct(tenant)
	r, ok := a.grants[grant]
	if !ok {
		return 0, fmt.Errorf("%w %s", ErrUnknownGrant, grant)
	}
	used := min(usedRaw, r.Raw)
	d := settleData{Tenant: tenant, Grant: grant, UsedRaw: used, Settled: model.CeilBilling(used, r.Coef)}
	if _, err := q.l.Append(KindSettle, d); err != nil {
		return 0, err
	}
	q.applySettle(d)
	return d.Settled, nil
}

// ApplyBudget 应用后端的 BudgetUpdate；只应用版本更高的更新（乱序、重复的更新被忽略），返回是否应用。
func (q *Accounts) ApplyBudget(tenant model.UUID, version, delegated, ackedTotal uint64) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	a := q.acct(tenant)
	if version <= a.version {
		return false, nil
	}
	d := budgetData{Tenant: tenant, Version: version, Delegated: delegated, Acked: ackedTotal}
	if _, err := q.l.Append(KindBudget, d); err != nil {
		return false, err
	}
	q.applyBudget(d)
	return true, nil
}

// NeedsTopUp：available 低于 delegated 的 30%（或尚无委托）时应向后端请求补充。
func (q *Accounts) NeedsTopUp(tenant model.UUID) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	a := q.acct(tenant)
	return float64(a.available()) < TopUpThreshold*float64(a.delegated) || a.delegated == 0
}

// View 返回租户账户的快照。
func (q *Accounts) View(tenant model.UUID) View {
	q.mu.Lock()
	defer q.mu.Unlock()
	a := q.acct(tenant)
	return View{Version: a.version, Delegated: a.delegated, Issued: a.issued, SettledLocal: a.settled,
		AckedTotal: a.acked, Pending: a.pending(), Available: a.available(), Grants: len(a.grants)}
}

// Versions 返回各租户已应用的额度版本（HubRegister.budget_versions）。
func (q *Accounts) Versions() map[model.UUID]uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make(map[model.UUID]uint64, len(q.tenants))
	for t, a := range q.tenants {
		out[t] = a.version
	}
	return out
}

// Reserved 返回某授权的预留（未结算时 ok 为 true）。
func (q *Accounts) Reserved(tenant, grant model.UUID) (raw, coef, reserved uint64, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	r, ok := q.acct(tenant).grants[grant]
	return r.Raw, r.Coef, r.Reserved, ok
}

// Settlement 是 quota.settle 条目的内容（上报后端的 GrantSettled）。
type Settlement struct {
	Tenant, Grant model.UUID
	UsedRaw       uint64
	Settled       uint64 // 计费字节
}

// DecodeSettle 解析 quota.settle 条目。
func DecodeSettle(e ledger.Entry) (Settlement, error) {
	var d settleData
	err := json.Unmarshal(e.Data, &d)
	return Settlement{d.Tenant, d.Grant, d.UsedRaw, d.Settled}, err
}
