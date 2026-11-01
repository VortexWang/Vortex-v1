package session

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"vortex/edgehub/clock"
	"vortex/edgehub/issuer"
	"vortex/edgehub/leases"
	"vortex/edgehub/ledger"
	"vortex/edgehub/model"
)

func state(t *testing.T, h *Hub) []byte {
	t.Helper()
	c, err := h.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(c)
	return b
}

// 随机操作（签发、续期、计量含 final 结算、重启转移、份额、限制、额度、自治与恢复、检查点、清理、
// 时间推进数天）后重开账本：状态与运行中的实例完全相同；清理不删除未确认条目，序号不回退。
func TestCheckpointReplayRandom(t *testing.T) {
	for seed := int64(0); seed < 60; seed++ {
		checkpointRandom(t, seed, func() ledger.Store { return &ledger.MemStore{} }, nil)
	}
}

// 同一随机过程在 SQLite 存储上：每次比对都关闭并重新打开数据库文件。
func TestCheckpointReplayRandomSQLite(t *testing.T) {
	for seed := int64(100); seed < 108; seed++ {
		path := filepath.Join(t.TempDir(), "ledger.db")
		var cur *ledger.SQLiteStore
		mk := func() ledger.Store {
			s, err := ledger.OpenSQLite(path)
			if err != nil {
				t.Fatal(err)
			}
			cur = s
			return s
		}
		checkpointRandom(t, seed, mk, func() ledger.Store { // 重开：关闭当前连接后打开同一文件
			cur.Close()
			return mk()
		})
		cur.Close()
	}
}

func checkpointRandom(t *testing.T, seed int64, mk func() ledger.Store, reopen func() ledger.Store) {
	{
		rng := rand.New(rand.NewSource(seed))
		clk := clock.NewFake(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC))
		st := mk()
		h := open(t, clk, st, nil)
		_ = h.Issuer.SetBillingVersions([]issuer.BillingVersion{{Version: 1, EffectiveAt: clk.Now().Add(-time.Hour)}})
		_, _ = h.Quota.ApplyBudget(tenant, 1, 1<<34, 0)
		boots := []model.UUID{boot1}
		in := &inbox{}
		s := h.NewSession(node, in.send)
		_ = s.Handle(Hello{Node: node, Boot: boot1})
		_, _ = h.Leases.ApplyUpdate(node, boot1, []leases.Lease{{Tenant: tenant, Kind: model.GrantShareRate, Amount: 1000}})
		var bills []model.UUID
		useq := uint64(0)
		budgetVer := uint64(1)
		for op := 0; op < 150; op++ {
			cur := boots[len(boots)-1]
			switch x := rng.Intn(30); {
			case x < 6:
				_ = s.Handle(GrantRequest{ID: uint64(op), Kind: model.GrantBilling, Rule: ruleA, RateBps60s: uint64(rng.Intn(1e9))})
				for _, m := range in.take() {
					if r, ok := m.(GrantResponse); ok {
						for _, g := range r.Billing {
							bills = append(bills, g.ID)
						}
					}
				}
			case x < 9:
				_ = s.Handle(GrantRequest{ID: uint64(op), Kind: model.GrantRun})
				_ = s.Handle(GrantRequest{ID: uint64(op), Kind: model.GrantShareRate, Tenant: tenant})
			case x < 15:
				useq++
				b := UsageBatch{Boot: cur, Seq: useq, Demands: []Demand{{Tenant: tenant, Bytes60s: uint64(rng.Intn(1 << 20))}}}
				if len(bills) > 0 && rng.Intn(2) == 0 {
					i := rng.Intn(len(bills))
					b.Consumptions = []Consumption{{Grant: bills[i], Used: uint64(rng.Intn(8 << 20)), Final: rng.Intn(2) == 0}}
				}
				_ = s.Handle(b)
				if rng.Intn(4) == 0 {
					_ = s.Handle(b) // 重发
				}
			case x < 16:
				nb := model.UUID{0xb0, byte(len(boots))}
				boots = append(boots, nb)
				s.Close()
				s = h.NewSession(node, in.send)
				_ = s.Handle(Hello{Node: node, Boot: nb})
				useq = 0
			case x < 18:
				budgetVer++
				_, _ = h.Quota.ApplyBudget(tenant, budgetVer, uint64(rng.Int63n(1<<34)), 0)
			case x < 19:
				_ = h.Issuer.SetRestrictions(uint64(op), []issuer.TenantRestriction{{Tenant: model.UUID{0x99}, Status: issuer.AccountBanned}})
				if rng.Intn(2) == 0 {
					_ = h.Issuer.SetBudgetRevoked([]model.UUID{tenant})
				} else {
					_ = h.Issuer.SetBudgetRevoked(nil)
				}
			case x < 21:
				clk.Advance(time.Duration(rng.Intn(4)) * 24 * time.Hour)
				_ = h.Ledger.Ack(h.Ledger.LastSeq() - uint64(rng.Intn(5)))
			case x < 23:
				clk.Advance(31 * time.Second) // 失联进入自治（后端未连接）
				_ = h.Tick()
				_ = h.Rebalance()
			case x < 24:
				_ = h.Mode.Contact()
				_ = h.Mode.ReconcileCompleted()
				_ = h.Tick()
			case x < 27:
				before := h.Ledger.LastSeq()
				if _, err := h.Compact(); err != nil {
					t.Fatal(err)
				}
				if h.Ledger.LastSeq() < before {
					t.Fatal("seq went back")
				}
			default:
				if _, err := h.Checkpoint(); err != nil {
					t.Fatal(err)
				}
			}
			in.take()
			if rng.Intn(10) == 0 || op == 149 {
				want := state(t, h)
				last, acked := h.Ledger.LastSeq(), h.Ledger.Acked()
				unacked, _ := h.Ledger.Unacked()
				st2 := st
				if reopen != nil {
					st2 = reopen()
					st = st2
				}
				h2 := open(t, clk, st2, nil)
				if reopen != nil { // 运行中的实例改用新连接继续（同一文件）
					h = open(t, clk, st2, nil)
					s.Close()
					s = h.NewSession(node, in.send)
					_ = s.Handle(Hello{Node: node, Boot: boots[len(boots)-1]})
					in.take()
				}
				if got := state(t, h2); !bytes.Equal(got, want) {
					t.Fatalf("seed %d op %d: state after reopen differs\n got %s\nwant %s", seed, op, got, want)
				}
				if h2.Ledger.LastSeq() != last || h2.Ledger.Acked() != acked {
					t.Fatalf("seed %d: seq %d/%d acked %d/%d", seed, h2.Ledger.LastSeq(), last, h2.Ledger.Acked(), acked)
				}
				un2, _ := h2.Ledger.Unacked()
				if len(un2) != len(unacked) {
					t.Fatalf("seed %d: unacked entries %d → %d after reopen", seed, len(unacked), len(un2))
				}
			}
		}
	}
}

// 保留期：只清理检查点之前、已确认、超过 7 天的条目；不在线时不清理。
func TestCompactRetention(t *testing.T) {
	f := setup(t, false)
	s, in := connect(t, f.h, boot1)
	for i := 1; i <= 5; i++ {
		_ = s.Handle(UsageBatch{Boot: boot1, Seq: uint64(i)})
	}
	in.take()
	n0 := f.st.Len()
	f.clk.Advance(Retention)
	if n, _ := f.h.Compact(); n != 0 {
		t.Fatal("pruned unacked entries")
	}
	if _, ok := open(t, f.clk, f.st, nil).usage[usageKey{node, boot1, 1}]; !ok {
		t.Fatal("dedupe key of an unacked batch dropped from the checkpoint")
	}
	_ = s.Handle(UsageBatch{Boot: boot1, Seq: 7}) // 刚写入、确认后也不足 7 天
	_ = f.h.Ledger.Ack(f.h.Ledger.LastSeq())
	young := f.h.Ledger.LastSeq()
	_ = s.Handle(UsageBatch{Boot: boot1, Seq: 8})
	_ = f.h.Ledger.Ack(f.h.Ledger.LastSeq())
	_ = s.Handle(UsageBatch{Boot: boot1, Seq: 6}) // 新条目：不足 7 天
	n, err := f.h.Compact()
	if err != nil || n < 5 || f.st.Len() >= n0 {
		t.Fatalf("pruned %d (%v), entries %d → %d", n, err, n0, f.st.Len())
	}
	kept := false
	_ = f.st.Load(func(e ledger.Entry) error {
		if e.Seq == young {
			kept = true
		}
		return nil
	})
	if !kept {
		t.Fatal("acked entry younger than 7 days pruned")
	}
	// 清理后重发最近 7 天内的批次仍被去重；更早的幂等键已丢弃（Agent 不会重发已确认的批次）
	_ = s.Handle(UsageBatch{Boot: boot1, Seq: 6})
	h2 := open(t, f.clk, f.st, nil)
	if _, ok := h2.usage[usageKey{node, boot1, 6}]; !ok {
		t.Fatal("recent dedupe key lost")
	}
	if _, ok := h2.usage[usageKey{node, boot1, 1}]; ok {
		t.Fatal("dedupe key beyond retention kept")
	}
	// 不在线：不清理
	f.clk.Advance(31 * time.Second)
	_ = f.h.Tick()
	f.clk.Advance(Retention)
	_ = f.h.Ledger.Ack(f.h.Ledger.LastSeq())
	if n, _ := f.h.Compact(); n != 0 {
		t.Fatal("pruned while autonomous (HUB-012: not before reconciliation completes)")
	}
}

// 并发签发与检查点：重开后状态一致（检查点在快照期间有写入时重试）。
func TestCheckpointConcurrent(t *testing.T) {
	f := setup(t, false)
	s, in := connect(t, f.h, boot1)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 300; i++ {
			_ = s.Handle(GrantRequest{ID: uint64(i), Kind: model.GrantBilling, Rule: ruleA})
			var g model.UUID
			for _, m := range in.take() {
				if r, ok := m.(GrantResponse); ok && len(r.Billing) > 0 {
					g = r.Billing[0].ID
				}
			}
			_ = s.Handle(UsageBatch{Boot: boot1, Seq: uint64(i + 1), Consumptions: []Consumption{{Grant: g, Used: 1, Final: true}}})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			if _, err := f.h.Checkpoint(); err != nil {
				t.Error(err)
			}
		}
	}()
	wg.Wait()
	want := state(t, f.h)
	if got := state(t, open(t, f.clk, f.st, nil)); !bytes.Equal(got, want) {
		t.Fatal("state differs after concurrent checkpoints")
	}
}

// 文件存储：清理是原子替换，重开后条目与序号正确。
func TestFileStoreCompact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	fs, err := ledger.OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	clk := clock.NewFake(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC))
	h := open(t, clk, fs, nil)
	_, _ = h.Quota.ApplyBudget(tenant, 1, 1<<30, 0)
	s := h.NewSession(node, func(HubMsg) {})
	_ = s.Handle(Hello{Node: node, Boot: boot1})
	for i := 1; i <= 20; i++ {
		_ = s.Handle(UsageBatch{Boot: boot1, Seq: uint64(i)})
	}
	clk.Advance(Retention)
	_ = h.Ledger.Ack(h.Ledger.LastSeq())
	if n, err := h.Compact(); err != nil || n < 20 {
		t.Fatalf("compact %d %v", n, err)
	}
	want, last := state(t, h), h.Ledger.LastSeq()
	_ = s.Handle(UsageBatch{Boot: boot1, Seq: 21}) // 清理后继续追加
	fs.Close()
	fs2, err := ledger.OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer fs2.Close()
	h2 := open(t, clk, fs2, nil)
	if h2.Ledger.LastSeq() != last+1 {
		t.Fatalf("seq after reopen %d, want %d", h2.Ledger.LastSeq(), last+1)
	}
	_ = want
	if _, ok := h2.usage[usageKey{node, boot1, 21}]; !ok {
		t.Fatal("entry appended after compaction lost")
	}
}
