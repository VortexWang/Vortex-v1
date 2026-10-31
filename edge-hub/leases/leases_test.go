package leases

import (
	"math/rand"
	"testing"
	"time"

	"vortex/edgehub/clock"
	"vortex/edgehub/ledger"
	"vortex/edgehub/model"
)

var (
	tenant = model.UUID{0xa}
	rateK  = Key{tenant, model.GrantShareRate}
	connK  = Key{tenant, model.GrantShareConn}
	nA     = model.UUID{0x1}
	nB     = model.UUID{0x2}
	nC     = model.UUID{0x3}
	bootA  = model.UUID{0x71}
	bootB  = model.UUID{0x72}
	bootC  = model.UUID{0x73}
)

func open(t *testing.T, clk clock.Clock, st *ledger.MemStore) *Manager {
	m := New(clk)
	l, err := ledger.Open(st, clk, func(e ledger.Entry) error { _, err := m.Replay(e); return err })
	if err != nil {
		t.Fatal(err)
	}
	m.Bind(l)
	return m
}

func setup(t *testing.T) (*Manager, *clock.Fake, *ledger.MemStore) {
	clk := clock.NewFake(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC))
	st := &ledger.MemStore{}
	m := open(t, clk, st)
	for _, x := range []struct {
		n, b model.UUID
		rate uint64
	}{{nA, bootA, 60_000_000}, {nB, bootB, 30_000_000}, {nC, bootC, 10_000_000}} {
		ls, err := m.ApplyUpdate(x.n, x.b, []Lease{{Tenant: tenant, Kind: model.GrantShareRate, Amount: x.rate},
			{Tenant: tenant, Kind: model.GrantShareConn, Amount: 100}})
		if err != nil {
			t.Fatal(err)
		}
		m.Ack(x.n, []model.UUID{ls[0].ID, ls[1].ID})
	}
	return m, clk, st
}

func sum(m map[model.UUID]uint64) (s uint64) {
	for _, v := range m {
		s += v
	}
	return
}

func TestAllocate(t *testing.T) {
	nodes := []model.UUID{nA, nB, nC}
	// 全部活跃：保底 50% ÷ 3，其余按信号 3:1:0
	got := Allocate(1200, nodes, map[model.UUID]bool{nA: true, nB: true, nC: true}, map[model.UUID]float64{nA: 3, nB: 1})
	if got[nA] != 200+450 || got[nB] != 200+150 || got[nC] != 200 {
		t.Fatalf("allocate %v", got)
	}
	// 只有 A 活跃：A 拿保底 + 全部需求部分，非活跃节点为 0
	got = Allocate(1000, nodes, map[model.UUID]bool{nA: true}, nil)
	if got[nA] != 1000 || got[nB] != 0 {
		t.Fatalf("one active %v", got)
	}
	// 无活跃：均分；余数分配后总和恰好等于总量
	got = Allocate(1000, nodes, nil, nil)
	if sum(got) != 1000 || got[nA] < 333 || got[nC] < 333 {
		t.Fatalf("no active %v", got)
	}
	// 随机：总和恰好等于总量，活跃节点不低于保底
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 5000; i++ {
		total := uint64(r.Int63n(1 << 40))
		var ns []model.UUID
		act, sig := map[model.UUID]bool{}, map[model.UUID]float64{}
		k := 1 + r.Intn(8)
		nact := 0
		for j := 0; j < k; j++ {
			n := model.UUID{byte(j + 1)}
			ns = append(ns, n)
			if r.Intn(3) > 0 {
				act[n] = true
				nact++
			}
			if r.Intn(2) == 0 {
				sig[n] = r.Float64() * 1e9
			}
		}
		got := Allocate(total, ns, act, sig)
		if sum(got) != total {
			t.Fatalf("sum %d != total %d", sum(got), total)
		}
		if nact > 0 {
			floor := uint64(float64(total) * FloorRatio / float64(nact))
			for n := range act {
				if got[n]+1 < floor {
					t.Fatalf("active node below floor: %d < %d", got[n], floor)
				}
			}
		}
	}
}

// 在线时不再平衡；自治时冻结总量，按需求再分配，任何时刻持有量之和不超过冻结总量（HUB-007、LIM-006、LIM-009）。
func TestAutonomousRebalanceNeverIncreases(t *testing.T) {
	m, clk, _ := setup(t)
	if out, _ := m.Rebalance(); out != nil {
		t.Fatal("rebalance while online")
	}
	if err := m.Freeze(); err != nil {
		t.Fatal(err)
	}
	total := m.Frozen()[rateK]
	if total != 100_000_000 || m.Frozen()[connK] != 300 {
		t.Fatalf("frozen %v", m.Frozen())
	}
	// C 需求最大且令牌耗尽，A 空闲：份额应从 A 移向 C
	m.ReportDemand(nA, tenant, Demand{Bytes60s: 0})
	m.ReportDemand(nB, tenant, Demand{Bytes60s: 100 << 20})
	m.ReportDemand(nC, tenant, Demand{Bytes60s: 300 << 20, StarvedPermille: 1000})
	clk.Advance(time.Minute)
	out, err := m.Rebalance()
	if err != nil {
		t.Fatal(err)
	}
	if m.Total(rateK) > total {
		t.Fatalf("total %d > frozen %d after first round", m.Total(rateK), total)
	}
	// 第一轮：A 调小（未确认，按旧值计），C 的调大放不下，按原值续租
	if l := find(out[nC], rateK); l.Amount != 10_000_000 {
		t.Fatalf("upsize before the downsize was acked: %d", l.Amount)
	}
	// A、B、C 确认后，第二轮完成调大
	for n, ls := range out {
		var ids []model.UUID
		for _, l := range ls {
			ids = append(ids, l.ID)
		}
		m.Ack(n, ids)
	}
	clk.Advance(time.Minute)
	out, _ = m.Rebalance()
	// 保底 25 M（只有 B、C 活跃），需求部分 50 M 按 100 : 600 分
	if a, b, c := m.Held(nA, rateK), m.Held(nB, rateK), m.Held(nC, rateK); a != 0 || c <= b || a+b+c != total {
		t.Fatalf("after rebalance A=%d B=%d C=%d total=%d", a, b, c, total)
	}
	if m.Total(rateK) > total || m.Total(connK) > 300 {
		t.Fatal("total exceeded")
	}
	// 回到在线：解除冻结
	if err := m.Unfreeze(); err != nil || m.Frozen() != nil {
		t.Fatal("unfreeze")
	}
	_ = out
}

func find(ls []Lease, k Key) Lease {
	for _, l := range ls {
		if l.key() == k {
			return l
		}
	}
	return Lease{}
}

// 随机过程：确认可能延迟、丢失，需求随机变化；每一轮之后持有量之和都不超过冻结总量，且续租不中断（无节点因再平衡掉到 0 以外的活跃节点）。
func TestRebalanceRandomInvariant(t *testing.T) {
	for seed := int64(0); seed < 200; seed++ {
		m, clk, _ := setup(t)
		r := rand.New(rand.NewSource(seed))
		_ = m.Freeze()
		total := m.Frozen()[rateK]
		// 独立模型：节点可能在用的份额 = 其最后确认的租约及之后下发的、仍有效的租约中的最大值
		type ml struct {
			amount uint64
			id     model.UUID
			exp    time.Time
		}
		model_ := map[model.UUID][]ml{}
		for _, n := range []model.UUID{nA, nB, nC} {
			model_[n] = []ml{{m.Held(n, rateK), model.UUID{}, clk.Now().Add(TTL)}}
		}
		possible := func() (s uint64) {
			for _, ls := range model_ {
				var a uint64
				for _, l := range ls {
					if clk.Now().Before(l.exp) {
						a = max(a, l.amount)
					}
				}
				s += a
			}
			return
		}
		for round := 0; round < 30; round++ {
			for _, n := range []model.UUID{nA, nB, nC} {
				if r.Intn(4) > 0 {
					m.ReportDemand(n, tenant, Demand{Bytes60s: uint64(r.Int63n(1 << 30)), StarvedPermille: uint32(r.Intn(1001))})
				}
			}
			clk.Advance(time.Minute)
			out, err := m.Rebalance()
			if err != nil {
				t.Fatal(err)
			}
			for n, ls := range out {
				l := find(ls, rateK)
				model_[n] = append(model_[n], ml{l.Amount, l.ID, l.IssuedAt.Add(l.TTL)})
			}
			if got := possible(); got > total {
				t.Fatalf("seed %d round %d: possibly in use %d > frozen %d", seed, round, got, total)
			}
			for n, ls := range out {
				if r.Intn(3) == 0 {
					continue // 确认丢失或延迟
				}
				var ids []model.UUID
				for _, l := range ls {
					ids = append(ids, l.ID)
				}
				m.Ack(n, ids)
				id := find(ls, rateK).ID
				for i, l := range model_[n] {
					if l.id == id {
						model_[n] = model_[n][i:]
						break
					}
				}
				if got, held := possible(), m.Total(rateK); got > total || held != got {
					t.Fatalf("seed %d round %d after ack: possibly in use %d (manager says %d) > frozen %d", seed, round, got, held, total)
				}
			}
		}
	}
}

// 冻结总量与租约在账本中：自治期间重启后冻结总量不变，持有量不变。
func TestFreezeSurvivesRestart(t *testing.T) {
	m, clk, st := setup(t)
	_ = m.Freeze()
	m.ReportDemand(nC, tenant, Demand{Bytes60s: 1 << 30})
	clk.Advance(time.Minute)
	_, _ = m.Rebalance()
	m2 := open(t, clk, st)
	if m2.Frozen()[rateK] != m.Frozen()[rateK] {
		t.Fatalf("frozen after restart %v", m2.Frozen())
	}
	for _, n := range []model.UUID{nA, nB, nC} {
		if m2.Held(n, rateK) != m.Held(n, rateK) {
			t.Fatalf("held %v after restart %d != %d", n, m2.Held(n, rateK), m.Held(n, rateK))
		}
	}
}

// LIM-010：重启节点的份额按原值立即转给新启动，旧租约作废；总量不变；其他节点不受影响；租约 5 分钟后过期。
func TestTransfer(t *testing.T) {
	m, clk, st := setup(t)
	_ = m.Freeze()
	newBoot := model.UUID{0x81}
	revoked, fresh, err := m.Transfer(nA, newBoot)
	if err != nil || len(revoked) != 2 || len(fresh) != 2 {
		t.Fatalf("transfer %v %v %v", revoked, fresh, err)
	}
	for _, l := range fresh {
		if l.Boot != newBoot || l.Node != nA {
			t.Fatalf("fresh lease %+v", l)
		}
	}
	if find(fresh, rateK).Amount != 60_000_000 || m.Total(rateK) != 100_000_000 {
		t.Fatalf("transfer changed amounts: %d total %d", find(fresh, rateK).Amount, m.Total(rateK))
	}
	if m.Held(nB, rateK) != 30_000_000 {
		t.Fatal("transfer touched another node")
	}
	// 同一启动再次接入不重复转移
	if _, fresh, _ := m.Transfer(nA, newBoot); fresh != nil {
		t.Fatal("repeated transfer")
	}
	m2 := open(t, clk, st)
	if m2.Total(rateK) != 100_000_000 || m2.Held(nA, rateK) != 60_000_000 {
		t.Fatal("transfer lost on replay")
	}
	clk.Advance(TTL)
	if m.Held(nB, rateK) != 0 {
		t.Fatal("lease outlived its TTL")
	}
}
