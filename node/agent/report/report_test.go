package report

import (
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"vortex/engine/types"
	"vortex/node/agent/store"
	"vortex/node/internal/clock"
)

var (
	b1    = types.UUID{0xb1}
	b2    = types.UUID{0xb2}
	b3    = types.UUID{0xb3}
	ruleA = types.UUID{0xa}
	g1    = types.UUID{0x91}
	ten   = types.UUID{0x7}
)

func t0() *clock.Fake { return clock.NewFake(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)) }

func delta(n uint64) types.UsageSnapshot {
	return types.UsageSnapshot{Deltas: []types.UsageDelta{{RuleID: ruleA, Role: 1, Direction: types.DirUp, RawBytes: n, BillingGrantID: g1}}}
}

func open(t *testing.T, clk clock.Clock, log store.Log, boot types.UUID) *Reporter {
	t.Helper()
	r, err := Open(clk, log, boot)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func raw(b Batch) (s uint64) {
	for _, d := range b.Snapshot.Deltas {
		s += d.RawBytes
	}
	return
}

func TestMerge(t *testing.T) {
	var s types.UsageSnapshot
	Merge(&s, types.UsageSnapshot{
		Deltas:       []types.UsageDelta{{RuleID: ruleA, RawBytes: 5}, {RuleID: ruleA, Direction: types.DirDown, RawBytes: 1}},
		Demands:      []types.TenantDemand{{TenantID: ten, Bytes60s: 10}},
		Consumptions: []types.GrantConsumption{{GrantID: g1, UsedRawBytes: 100, Final: true}},
	})
	Merge(&s, types.UsageSnapshot{
		Deltas:       []types.UsageDelta{{RuleID: ruleA, RawBytes: 7}},
		Demands:      []types.TenantDemand{{TenantID: ten, Bytes60s: 3, StarvedPermille: 9}},
		Consumptions: []types.GrantConsumption{{GrantID: g1, UsedRawBytes: 90}},
	})
	if len(s.Deltas) != 2 || s.Deltas[0].RawBytes != 12 || s.Deltas[1].RawBytes != 1 {
		t.Fatalf("deltas %+v", s.Deltas)
	}
	if len(s.Demands) != 1 || s.Demands[0].Bytes60s != 3 || s.Demands[0].StarvedPermille != 9 {
		t.Fatalf("demand must be the latest %+v", s.Demands)
	}
	if c := s.Consumptions[0]; c.UsedRawBytes != 100 || !c.Final {
		t.Fatalf("consumption %+v (max used, final sticky)", c)
	}
}

func TestSealAckAndRestart(t *testing.T) {
	clk, log := t0(), &store.Mem{}
	r := open(t, clk, log, b1)
	if b, _ := r.Seal(); b != nil {
		t.Fatal("empty batch sealed")
	}
	for i := 1; i <= 3; i++ {
		_ = r.Add(delta(uint64(i)))
		clk.Advance(time.Second)
		b, err := r.Seal()
		if err != nil || b.Seq != uint64(i) || b.Boot != b1 || raw(*b) != uint64(i) {
			t.Fatalf("batch %d: %+v %v", i, b, err)
		}
	}
	if r.FirstUnacked() != 1 {
		t.Fatal("first unacked")
	}
	if err := r.Ack(b1, 99); err != nil { // 超过已封序号的确认截断到已封的最大序号
		t.Fatal(err)
	}
	if len(r.Pending()) != 0 || r.FirstUnacked() != 0 {
		t.Fatalf("ack beyond the last sealed seq %+v", r.Pending())
	}
	_ = r.Add(delta(10))
	if b, _ := r.Seal(); b.Seq != 4 {
		t.Fatalf("seq after clamped ack %d", b.Seq)
	}
	_ = r.Add(delta(20)) // 未封：重启时封为 b1 的最后一批
	if err := r.Ack(b3, 1); !errors.Is(err, ErrUnknownBoot) {
		t.Fatalf("unknown boot %v", err)
	}

	r2 := open(t, clk, log, b2)
	_ = r2.Add(delta(30))
	_, _ = r2.Seal()
	p := r2.Pending()
	if len(p) != 3 || p[0].Boot != b1 || p[0].Seq != 4 || p[1].Seq != 5 || raw(p[1]) != 20 || p[2].Boot != b2 || p[2].Seq != 1 {
		t.Fatalf("pending after restart %+v (previous boot first, new boot from seq 1)", p)
	}
	_ = r2.Ack(b1, 4)
	_ = r2.Ack(b1, 3) // 回退无效果
	if p := r2.Pending(); len(p) != 2 || p[0].Seq != 5 {
		t.Fatalf("after partial ack %+v", p)
	}
	// 再次重启：状态一致，不重复封批
	r3 := open(t, clk, log, b3)
	if p := r3.Pending(); len(p) != 2 || p[0].Boot != b1 || p[1].Boot != b2 {
		t.Fatalf("pending after second restart %+v", p)
	}
}

func TestAddFailureLeavesStateUnchanged(t *testing.T) {
	clk, log := t0(), &store.Mem{}
	r := open(t, clk, log, b1)
	_ = r.Add(delta(1))
	log.Fail = errors.New("disk full")
	if err := r.Add(delta(100)); err == nil {
		t.Fatal("no error")
	}
	if _, err := r.Seal(); err == nil {
		t.Fatal("seal must fail too")
	}
	log.Fail = nil
	b, _ := r.Seal()
	if b.Seq != 1 || raw(*b) != 1 {
		t.Fatalf("after failures %+v", b)
	}
	if r2 := open(t, clk, log, b2); len(r2.Pending()) != 1 || raw(r2.Pending()[0]) != 1 {
		t.Fatal("memory and disk disagree after a failed append")
	}
}

func TestRetention(t *testing.T) {
	clk, log := t0(), &store.Mem{}
	r := open(t, clk, log, b1)
	for i := 0; i < 5; i++ {
		_ = r.Add(delta(1))
		_, _ = r.Seal()
	}
	_ = r.Ack(b1, 3)
	clk.Advance(Retention - time.Second)
	if n, _ := r.Prune(); n != 0 {
		t.Fatal("pruned before 7 days")
	}
	clk.Advance(time.Second)
	_ = r.Add(delta(7)) // 未封部分在压缩后保留
	n, err := r.Prune()
	if err != nil || n != 3 {
		t.Fatalf("pruned %d %v", n, err)
	}
	clk.Advance(100 * Retention)
	if n, _ := r.Prune(); n != 0 {
		t.Fatal("unacked batches must never be pruned")
	}
	r2 := open(t, clk, log, b1) // 同一启动重新打开（模拟进程内重载）
	b, _ := r2.Seal()
	if b == nil || b.Seq != 6 || raw(*b) != 7 {
		t.Fatalf("after compaction %+v (seq must continue, open delta kept)", b)
	}
	if p := r2.Pending(); len(p) != 3 || p[0].Seq != 4 {
		t.Fatalf("pending after compaction %+v", p)
	}
	_ = r2.Ack(b1, 2) // 已确认位置在压缩后保留：不会回退
	if len(r2.Pending()) != 3 {
		t.Fatal("ack position lost by compaction")
	}
}

// 断电：最后一行写了一半时截掉，已封批次不丢、不重复；中间行损坏拒绝打开。
func TestFileCrash(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "usage.log")
	clk := t0()
	f, err := store.OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	r := open(t, clk, f, b1)
	for i := 0; i < 4; i++ {
		_ = r.Add(delta(1))
		_, _ = r.Seal()
	}
	_ = r.Ack(b1, 1)
	_ = r.Add(delta(5))
	f.Close()
	fh, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	_, _ = fh.WriteString(`{"k":"usage.seal","d":{"boot":[17`) // 写了一半
	fh.Close()

	f2, err := store.OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	r2 := open(t, clk, f2, b2)
	p := r2.Pending()
	if len(p) != 4 || p[0].Seq != 2 || p[3].Seq != 5 || raw(p[3]) != 5 {
		t.Fatalf("after torn write %+v", p)
	}
	_, _ = r2.Prune()
	f2.Close()

	b, _ := os.ReadFile(path)
	b[3] = '!'
	_ = os.WriteFile(path, b, 0o600)
	if _, err := store.OpenFile(path); !errors.Is(err, store.ErrCorrupt) {
		t.Fatalf("corrupt middle line: %v", err)
	}
}

// 随机操作（落盘、封批、确认、重启、清理、写失败）与模型对照：每个启动的序号从 1 连续，没有重复或缺号；
// 每个启动累计的原始字节（已封 + 未封）等于成功落盘的总和；已确认的不再出现在待上报中。
func TestRandomAgainstModel(t *testing.T) {
	for seed := int64(0); seed < 300; seed++ {
		rng := rand.New(rand.NewSource(seed))
		clk, log := t0(), &store.Mem{}
		boots := []types.UUID{{1, byte(seed)}}
		r := open(t, clk, log, boots[0])
		added := map[types.UUID]uint64{}
		acked := map[types.UUID]uint64{}
		for op := 0; op < 120; op++ {
			cur := boots[len(boots)-1]
			switch x := rng.Intn(20); {
			case x < 8:
				n := uint64(rng.Intn(1000) + 1)
				fail := rng.Intn(10) == 0
				if fail {
					log.Fail = errors.New("io")
				}
				if err := r.Add(delta(n)); err == nil {
					added[cur] += n
				} else if !fail {
					t.Fatal(err)
				}
				log.Fail = nil
			case x < 13:
				_, _ = r.Seal()
			case x < 16:
				p := r.Pending()
				if len(p) > 0 {
					b := p[rng.Intn(len(p))]
					_ = r.Ack(b.Boot, b.Seq)
					acked[b.Boot] = max(acked[b.Boot], b.Seq)
				}
			case x < 18:
				clk.Advance(time.Duration(rng.Intn(4)) * 24 * time.Hour)
				if _, err := r.Prune(); err != nil {
					t.Fatal(err)
				}
			default:
				nb := types.UUID{byte(len(boots) + 1), byte(seed)}
				boots = append(boots, nb)
				r = open(t, clk, log, nb)
			}
		}
		// 重启一次把所有未封部分封掉，再核对
		final := types.UUID{0xff, byte(seed)}
		r = open(t, clk, log, final)
		r2 := open(t, clk, log, types.UUID{0xfe, byte(seed)}) // 第二次打开也必须一致
		for _, rr := range []*Reporter{r, r2} {
			seen := map[types.UUID]map[uint64]bool{}
			sum := map[types.UUID]uint64{}
			for _, b := range rr.batches {
				if seen[b.Boot] == nil {
					seen[b.Boot] = map[uint64]bool{}
				}
				if seen[b.Boot][b.Seq] {
					t.Fatalf("seed %d: duplicate seq %d", seed, b.Seq)
				}
				seen[b.Boot][b.Seq] = true
				sum[b.Boot] += raw(*b)
			}
			for _, bt := range boots {
				s := rr.boots[bt]
				if s == nil { // 没有任何计量的启动不留记录
					if added[bt] != 0 || acked[bt] != 0 {
						t.Fatalf("seed %d: boot with usage lost", seed)
					}
					continue
				}
				for q := s.lastSeq; q > 0; q-- {
					if !seen[bt][q] && q > s.acked { // 已清理的只能是已确认的
						t.Fatalf("seed %d: missing unacked seq %d of boot %v", seed, q, bt)
					}
				}
				if s.acked != acked[bt] {
					t.Fatalf("seed %d: acked %d, model %d", seed, s.acked, acked[bt])
				}
				if s.open != nil {
					t.Fatalf("seed %d: old boot still has an open batch", seed)
				}
			}
			for _, p := range rr.Pending() {
				if p.Seq <= acked[p.Boot] {
					t.Fatalf("seed %d: acked batch pending", seed)
				}
			}
			// 没有清理过的启动：字节总数与模型一致
			for _, bt := range boots {
				if s := rr.boots[bt]; s != nil && len(seen[bt]) == int(s.lastSeq) && sum[bt] != added[bt] {
					t.Fatalf("seed %d: boot bytes %d, model %d", seed, sum[bt], added[bt])
				}
			}
		}
	}
}

func TestDiskGuard(t *testing.T) {
	free := uint64(10 << 30)
	var lows, oks int
	g := &DiskGuard{Low: 1 << 30, High: 2 << 30, OnLow: func(uint64) { lows++ }, OnOK: func(uint64) { oks++ },
		Free: func(string) (uint64, error) { return free, nil }}
	for i, step := range []struct {
		free uint64
		low  bool
	}{{10 << 30, false}, {900 << 20, true}, {800 << 20, true}, {1500 << 20, true}, {3 << 30, false}, {1500 << 20, false}} {
		free = step.free
		if low, _ := g.Check(); low != step.low {
			t.Fatalf("step %d: low = %v (hysteresis between Low and High)", i, low)
		}
	}
	if lows != 1 || oks != 1 {
		t.Fatalf("low %d ok %d (hysteresis between Low and High)", lows, oks)
	}
	g.Free = func(string) (uint64, error) { return 0, errors.New("statfs") }
	if low, err := g.Check(); err == nil || low {
		t.Fatal("error must keep the current state")
	}
	g2 := &DiskGuard{Path: t.TempDir(), Low: 1}
	if low, err := g2.Check(); err != nil || low {
		t.Fatalf("statfs on a real dir: %v %v", low, err)
	}
}
