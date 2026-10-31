package quota

import (
	"math/big"
	"math/rand"
	"testing"
	"time"

	"vortex/edgehub/clock"
	"vortex/edgehub/ledger"
	"vortex/edgehub/model"
)

// 本文件把 test/budget-sim/budget_sim.py 移植为 Go：后端为与 Python 模型逐行对应的模拟，Edge Hub 一侧使用
// 真实的 Accounts（经账本写入）。消息乱序、重复、过期，余额被动减少，超额核销；检查同一组不变量：
//
//	I0 余额与 delegated 不为负；I1 Σ delegated ≤ 租户余额；I2 settled_pending ≥ 0（不多扣）；
//	I3 静止后双方 delegated、acked 逐字节一致且 pending = 0。
//
// 另外检查：任意时刻 issued + pending ≤ delegated，除非 delegated 被后端下调（被动减少）——下调后 available 为 0、
// 已签发部分不收回；以及账本重放得到完全相同的账户。

var coefChoices = []uint64{1000, 1500, 2000, 3500}

type simBackend struct {
	balance    *big.Rat
	delegated  map[string]uint64
	acked      map[string]uint64
	version    map[string]uint64
	applied    map[string]map[uint64]bool
	writtenOff *big.Rat
}

type budgetMsg struct {
	hub                     string
	version, delegated, ack uint64
}

type settleMsg struct {
	hub                string
	seq, used, coef, s uint64
}

func newBackend(hubs []string, balance int64) *simBackend {
	b := &simBackend{balance: big.NewRat(balance, 1), delegated: map[string]uint64{}, acked: map[string]uint64{},
		version: map[string]uint64{}, applied: map[string]map[uint64]bool{}, writtenOff: new(big.Rat)}
	for _, h := range hubs {
		b.applied[h] = map[uint64]bool{}
	}
	return b
}

func (b *simBackend) sumDelegated() uint64 {
	var s uint64
	for _, v := range b.delegated {
		s += v
	}
	return s
}

func (b *simBackend) undelegated() *big.Rat {
	return new(big.Rat).Sub(b.balance, new(big.Rat).SetInt64(int64(b.sumDelegated())))
}

func (b *simBackend) update(h string) budgetMsg {
	b.version[h]++
	return budgetMsg{h, b.version[h], b.delegated[h], b.acked[h]}
}

func floorRat(r *big.Rat) int64 {
	q := new(big.Int).Quo(r.Num(), r.Denom())
	if r.Sign() < 0 && new(big.Int).Mul(q, r.Denom()).Cmp(r.Num()) != 0 {
		q.Sub(q, big.NewInt(1))
	}
	return q.Int64()
}

func (b *simBackend) topup(h string, amount int64) *budgetMsg {
	amount = min(amount, max(0, floorRat(b.undelegated())))
	if amount <= 0 {
		return nil
	}
	b.delegated[h] += uint64(amount)
	m := b.update(h)
	return &m
}

func (b *simBackend) ingestSettle(m settleMsg) *budgetMsg {
	if b.applied[m.hub][m.seq] { // (hub_id, seq) 幂等
		return nil
	}
	b.applied[m.hub][m.seq] = true
	exact := big.NewRat(int64(m.used*m.coef), 1000)
	covered := exact
	if d := new(big.Rat).SetInt64(int64(b.delegated[m.hub])); covered.Cmp(d) > 0 {
		covered = d
	}
	b.writtenOff.Add(b.writtenOff, new(big.Rat).Sub(exact, covered))
	b.balance.Sub(b.balance, covered)
	b.delegated[m.hub] -= min(m.s, b.delegated[m.hub])
	b.acked[m.hub] += m.s
	u := b.update(m.hub)
	return &u
}

func (b *simBackend) passiveReduce(amount int64, hubs []string) []budgetMsg {
	a := new(big.Rat).SetInt64(amount)
	if a.Cmp(b.balance) > 0 {
		a = new(big.Rat).Set(b.balance)
	}
	b.balance.Sub(b.balance, a)
	var out []budgetMsg
	over := new(big.Rat).Sub(new(big.Rat).SetInt64(int64(b.sumDelegated())), b.balance)
	for _, h := range hubs { // 已排序
		if over.Sign() <= 0 {
			break
		}
		ceilOver := -floorRat(new(big.Rat).Neg(over))
		cut := min(b.delegated[h], uint64(ceilOver))
		if cut > 0 {
			b.delegated[h] -= cut
			over.Sub(over, new(big.Rat).SetInt64(int64(cut)))
			out = append(out, b.update(h))
		}
	}
	return out
}

// simHub：Edge Hub 一侧，额度账户为真实实现（单租户）。
type simHub struct {
	name   string
	st     *ledger.MemStore
	q      *Accounts
	seq    uint64
	grants map[model.UUID][2]uint64 // raw, coef
	order  []model.UUID

	overIssued bool
}

var simTenant = model.UUID{0x7e}

func newSimHub(t *testing.T, name string) *simHub {
	st := &ledger.MemStore{}
	l, err := ledger.Open(st, clock.NewFake(time.Unix(0, 0)), nil)
	if err != nil {
		t.Fatal(err)
	}
	return &simHub{name: name, st: st, q: New(l), grants: map[model.UUID][2]uint64{}}
}

func (h *simHub) issue(rng *rand.Rand) {
	coef := coefChoices[rng.Intn(len(coefChoices))]
	raw := uint64(rng.Intn(50_000) + 1)
	g := model.NewUUID()
	if _, err := h.q.Reserve(simTenant, g, raw, coef); err != nil {
		return // available 不足：与 Python 模型相同，不签发
	}
	// 不超发（03 方案第 5 节）：签发之后 issued + pending ≤ delegated
	if v := h.q.View(simTenant); v.Issued+v.Pending > v.Delegated {
		h.overIssued = true
	}
	h.grants[g] = [2]uint64{raw, coef}
	h.order = append(h.order, g)
}

func (h *simHub) settle(t *testing.T, rng *rand.Rand) *settleMsg {
	if len(h.order) == 0 {
		return nil
	}
	i := rng.Intn(len(h.order))
	g := h.order[i]
	h.order = append(h.order[:i], h.order[i+1:]...)
	rc := h.grants[g]
	delete(h.grants, g)
	used := uint64(rng.Int63n(int64(rc[0]) + 1))
	s, err := h.q.Settle(simTenant, g, used)
	if err != nil {
		t.Fatal(err)
	}
	if s != model.CeilBilling(used, rc[1]) {
		t.Fatalf("settled %d, want ceil", s)
	}
	h.seq++
	return &settleMsg{h.name, h.seq, used, rc[1], s}
}

func (h *simHub) apply(t *testing.T, m budgetMsg) {
	if _, err := h.q.ApplyBudget(simTenant, m.version, m.delegated, m.ack); err != nil {
		t.Fatal(err)
	}
}

func runSim(t *testing.T, seed int64) (violations []string, passive bool, wroteOff bool) {
	rng := rand.New(rand.NewSource(seed))
	hubs := []string{"hub-a", "hub-b", "hub-c"}
	be := newBackend(hubs, int64(rng.Intn(18_000_001)+2_000_000))
	hs := map[string]*simHub{}
	for _, n := range hubs {
		hs[n] = newSimHub(t, n)
	}
	var toHub []budgetMsg
	var toBE []settleMsg
	check := func(tag string) {
		if be.balance.Sign() < 0 {
			violations = append(violations, tag+": negative balance")
		}
		if new(big.Rat).SetInt64(int64(be.sumDelegated())).Cmp(be.balance) > 0 {
			violations = append(violations, tag+": Σdelegated > balance")
		}
		for _, h := range hs {
			if h.overIssued {
				violations = append(violations, tag+": "+h.name+" over-issued (issued + pending > delegated after a grant)")
				h.overIssued = false
			}
			v := h.q.View(simTenant)
			if v.SettledLocal < v.AckedTotal {
				violations = append(violations, tag+": "+h.name+" settled_pending < 0")
			}
			// 签发守恒：available 为 0 或 issued + pending + available = delegated
			if v.Available > 0 && v.Issued+v.Pending+v.Available != v.Delegated {
				violations = append(violations, tag+": "+h.name+" issued+pending+available != delegated")
			}
		}
	}
	steps := rng.Intn(601) + 200
	for step := 0; step < steps; step++ {
		op := rng.Float64()
		h := hs[hubs[rng.Intn(len(hubs))]]
		switch {
		case op < 0.25:
			if m := be.topup(h.name, int64(rng.Intn(390_001)+10_000)); m != nil {
				toHub = append(toHub, *m)
			}
		case op < 0.50:
			h.issue(rng)
		case op < 0.70:
			if m := h.settle(t, rng); m != nil {
				toBE = append(toBE, *m)
				if rng.Float64() < 0.3 {
					toBE = append(toBE, *m) // 重复发送
				}
			}
		case op < 0.72:
			toHub = append(toHub, be.passiveReduce(int64(rng.Intn(290_001)+10_000), hubs)...)
			passive = true
		default:
			if len(toBE) > 0 && rng.Float64() < 0.5 {
				i := rng.Intn(len(toBE))
				m := toBE[i]
				toBE = append(toBE[:i], toBE[i+1:]...)
				if r := be.ingestSettle(m); r != nil {
					toHub = append(toHub, *r)
				}
			} else if len(toHub) > 0 {
				i := rng.Intn(len(toHub))
				m := toHub[i]
				toHub = append(toHub[:i], toHub[i+1:]...)
				hs[m.hub].apply(t, m)
				if rng.Float64() < 0.2 {
					hs[m.hub].apply(t, m) // 重复投递
				}
			}
		}
		check("step")
	}
	for _, n := range hubs {
		h := hs[n]
		for len(h.order) > 0 {
			toBE = append(toBE, *h.settle(t, rng))
		}
	}
	for len(toBE) > 0 || len(toHub) > 0 {
		if len(toBE) > 0 {
			i := rng.Intn(len(toBE))
			m := toBE[i]
			toBE = append(toBE[:i], toBE[i+1:]...)
			if r := be.ingestSettle(m); r != nil {
				toHub = append(toHub, *r)
			}
		}
		if len(toHub) > 0 {
			i := rng.Intn(len(toHub))
			m := toHub[i]
			toHub = append(toHub[:i], toHub[i+1:]...)
			hs[m.hub].apply(t, m)
		}
	}
	check("quiescent")
	for _, n := range hubs {
		v := hs[n].q.View(simTenant)
		if v.AckedTotal != be.acked[n] || v.Delegated != be.delegated[n] || v.Pending != 0 {
			violations = append(violations, "quiescent: "+n+" differs from backend")
		}
		// 账本重放得到相同账户
		l2q := New(nil)
		if _, err := ledger.Open(hs[n].st, clock.NewFake(time.Unix(0, 0)), func(e ledger.Entry) error {
			_, err := l2q.Replay(e)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if r := l2q.View(simTenant); r != v {
			violations = append(violations, "replay differs for "+n)
		}
	}
	return violations, passive, be.writtenOff.Sign() > 0
}

func TestBudgetSimulationAgainstRealAccounts(t *testing.T) {
	n := 2000
	if testing.Short() {
		n = 200
	}
	bad, withPassive, wrote := 0, 0, 0
	for seed := int64(0); seed < int64(n); seed++ {
		v, p, w := runSim(t, seed)
		if p {
			withPassive++
		}
		if w {
			wrote++
		}
		if len(v) > 0 {
			bad++
			if bad <= 3 {
				t.Errorf("seed %d: %s", seed, v[0])
			}
		}
	}
	t.Logf("%d scenarios, %d with passive reduction, %d with write-off, %d violating", n, withPassive, wrote, bad)
}
