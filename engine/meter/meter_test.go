package meter

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"vortex/engine/types"
)

type fakeClock struct{ t atomic.Int64 }

func (c *fakeClock) now() time.Duration  { return time.Duration(c.t.Load()) }
func (c *fakeClock) add(d time.Duration) { c.t.Add(int64(d)) }
func fixedDeadline(c *fakeClock) func(types.BillingGrant) time.Duration {
	return func(g types.BillingGrant) time.Duration { return c.now() + g.Meta.TTL }
}

var rule = types.UUID{0xa1}

func bg(id byte, raw uint64, ttl time.Duration) types.BillingGrant {
	return types.BillingGrant{Meta: types.GrantMeta{GrantID: types.UUID{id}, TTL: ttl}, RuleID: rule, RawBytes: raw}
}

func TestLedgerDrainIsLossless(t *testing.T) {
	l := NewLedger()
	k := Key{Rule: rule, Role: types.RoleEntry, Dir: types.DirUp}
	var wg sync.WaitGroup
	var drained atomic.Uint64
	stop := make(chan struct{})
	go func() { // 并发 Drain：任何时刻取走的总和 + 剩余 必须等于写入总和
		for {
			select {
			case <-stop:
				return
			default:
			}
			for _, d := range l.Drain() {
				drained.Add(d.RawBytes)
			}
		}
	}()
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := l.Counter(k)
			for j := 0; j < 100000; j++ {
				c.Add(3)
			}
		}()
	}
	wg.Wait()
	close(stop)
	for _, d := range l.Drain() {
		drained.Add(d.RawBytes)
	}
	if drained.Load() != 8*100000*3 {
		t.Fatalf("drained %d, want %d", drained.Load(), 8*100000*3)
	}
}

func TestReserveCommitRefundOnce(t *testing.T) {
	c := &fakeClock{}
	l := NewLedger()
	a := NewAccount(rule, types.RoleEntry, c.now, l, nil)
	a.Update([]types.BillingGrant{bg(1, 1000, time.Minute)}, fixedDeadline(c))
	b := a.Reserve(600)
	if b == nil || b.N != 600 || a.Available() != 400 {
		t.Fatalf("reserve: %+v available %d", b, a.Available())
	}
	if b2 := a.Reserve(600); b2 == nil || b2.N != 400 { // 只能拿到剩余部分
		t.Fatalf("second reserve %+v", b2)
	} else {
		a.Commit(b2, types.DirDown, 0) // 全额退回
	}
	if !a.Commit(b, types.DirUp, 250) {
		t.Fatal("first commit")
	}
	if a.Commit(b, types.DirUp, 250) {
		t.Fatal("block committed twice（ENG-037 每笔预扣只能提交或退回一次）")
	}
	if a.Available() != 750 {
		t.Fatalf("available %d, want 750 (250 used, 350 refunded)", a.Available())
	}
	ds := l.Drain()
	if len(ds) != 1 || ds[0].RawBytes != 250 || ds[0].Direction != types.DirUp || ds[0].BillingGrantID != (types.UUID{1}) {
		t.Fatalf("ledger %+v", ds)
	}
}

func TestEarliestDeadlineFirstAndExpiry(t *testing.T) {
	c := &fakeClock{}
	a := NewAccount(rule, types.RoleEntry, c.now, NewLedger(), nil)
	a.Update([]types.BillingGrant{bg(1, 100, 10*time.Second), bg(2, 100, 2*time.Second)}, fixedDeadline(c))
	b := a.Reserve(50)
	if b.g.id != (types.UUID{2}) {
		t.Fatal("must consume the grant that expires first")
	}
	a.Commit(b, types.DirUp, 50)
	c.add(3 * time.Second) // 授权 2 到期：剩余 50 不可再用
	if a.Available() != 100 {
		t.Fatalf("expired grant still usable: available %d", a.Available())
	}
	cs := a.Consumptions()
	if len(cs) != 2 || !cs[1].Final && !cs[0].Final {
		t.Fatalf("expired grant must be reported final: %+v", cs)
	}
	for _, x := range cs {
		if x.GrantID == (types.UUID{2}) && (!x.Final || x.UsedRawBytes != 50) {
			t.Fatalf("grant 2 consumption %+v", x)
		}
	}
	if len(a.Consumptions()) != 1 {
		t.Fatal("final grants are reported once then dropped")
	}
}

func TestRemovedGrantStopsAndOutstandingBlockDelaysFinal(t *testing.T) {
	c := &fakeClock{}
	a := NewAccount(rule, types.RoleEntry, c.now, NewLedger(), nil)
	a.Update([]types.BillingGrant{bg(1, 100, time.Minute)}, fixedDeadline(c))
	b := a.Reserve(40)
	a.Update(nil, fixedDeadline(c)) // LIM-010：重启转移后旧授权作废
	if a.Reserve(1) != nil {
		t.Fatal("removed grant still usable")
	}
	if cs := a.Consumptions(); len(cs) != 1 || cs[0].Final {
		t.Fatalf("grant with outstanding reservation must not be final yet: %+v", cs)
	}
	a.Commit(b, types.DirUp, 40)
	if cs := a.Consumptions(); len(cs) != 1 || !cs[0].Final || cs[0].UsedRawBytes != 40 {
		t.Fatalf("after commit: %+v", cs)
	}
}

func TestLowWaterEventOncePerGrant(t *testing.T) {
	c := &fakeClock{}
	var evs []types.Event
	a := NewAccount(rule, types.RoleEntry, c.now, NewLedger(), func(e types.Event) { evs = append(evs, e) })
	a.Update([]types.BillingGrant{bg(1, 1000, time.Minute)}, fixedDeadline(c))
	for i := 0; i < 9; i++ {
		a.Commit(a.Reserve(100), types.DirUp, 100)
	}
	if len(evs) != 1 || evs[0].Kind != types.EventGrantLow || evs[0].Detail != "200" { // 第 8 次提交后剩 20%，首次低于 30%
		t.Fatalf("events %+v, want exactly one grant_low when remaining < 30%%", evs)
	}
}

func TestReserveOrWaitWakesOnUpdate(t *testing.T) {
	c := &fakeClock{}
	var needed atomic.Int64
	a := NewAccount(rule, types.RoleEntry, c.now, NewLedger(), func(e types.Event) {
		if e.Kind == types.EventGrantNeeded {
			needed.Add(1)
		}
	})
	go func() {
		time.Sleep(30 * time.Millisecond)
		a.Update([]types.BillingGrant{bg(1, 500, time.Minute)}, fixedDeadline(c))
	}()
	start := time.Now()
	b := a.ReserveOrWait(context.Background(), 100, 200*time.Millisecond, types.UUID{})
	if b == nil || b.N != 100 || time.Since(start) > 150*time.Millisecond || needed.Load() != 1 {
		t.Fatalf("block %+v after %v, needed %d", b, time.Since(start), needed.Load())
	}
	// 没有授权到达：200 ms 后放弃（ENG-050）
	empty := NewAccount(rule, types.RoleEntry, c.now, NewLedger(), nil)
	start = time.Now()
	if empty.ReserveOrWait(context.Background(), 1, 200*time.Millisecond, types.UUID{}) != nil {
		t.Fatal("no grant must return nil")
	}
	if el := time.Since(start); el < 190*time.Millisecond || el > 400*time.Millisecond {
		t.Fatalf("wait %v, want ≈200ms", el)
	}
}

func TestConcurrentReserveNeverOverdraws(t *testing.T) {
	c := &fakeClock{}
	a := NewAccount(rule, types.RoleEntry, c.now, NewLedger(), nil)
	a.Update([]types.BillingGrant{bg(1, 1_000_000, time.Minute)}, fixedDeadline(c))
	var wg sync.WaitGroup
	var used atomic.Int64
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				b := a.Reserve(777)
				if b == nil {
					return
				}
				used.Add(int64(b.N))
				a.Commit(b, types.DirUp, b.N)
			}
		}()
	}
	wg.Wait()
	if used.Load() != 1_000_000 {
		t.Fatalf("used %d, want exactly the grant amount", used.Load())
	}
}
