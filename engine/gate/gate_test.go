package gate

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"vortex/engine/types"
)

var (
	tenantA = types.UUID{0x7a}
	ruleEP  = types.UUID{0xe1}
	ruleMid = types.UUID{0xe2}
)

func meta(id byte, ttl time.Duration) types.GrantMeta {
	return types.GrantMeta{GrantID: types.UUID{id}, TTL: ttl}
}

// smallMargin 让测试可以用很短的有效期。
func smallMargin(ttl time.Duration) time.Duration { return ttl / 10 }

type evlog struct {
	mu  sync.Mutex
	evs []types.Event
}

func (l *evlog) emit(e types.Event) { l.mu.Lock(); l.evs = append(l.evs, e); l.mu.Unlock() }
func (l *evlog) count(k types.EventKind) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, e := range l.evs {
		if e.Kind == k {
			n++
		}
	}
	return n
}

func epRule() *types.Rule {
	return &types.Rule{RuleID: ruleEP, TenantID: tenantA, Role: types.RoleEntry, EnforcementPoint: true}
}

func fullSet(runTTL time.Duration, rateBps, conns uint64, billing uint64) *types.GrantSet {
	gs := &types.GrantSet{
		Run: []types.RunGrant{{Meta: meta(1, runTTL), RuleIDs: []types.UUID{ruleEP, ruleMid}}},
		Shares: []types.ShareLease{
			{Meta: meta(2, time.Hour), TenantID: tenantA, Kind: types.ShareRate, Amount: rateBps},
			{Meta: meta(3, time.Hour), TenantID: tenantA, Kind: types.ShareConn, Amount: conns},
		},
	}
	if billing > 0 {
		gs.Billing = []types.BillingGrant{{Meta: meta(4, time.Hour), RuleID: ruleEP, RawBytes: billing}}
	}
	return gs
}

func TestBucketRateAndStarvation(t *testing.T) {
	b := NewBucket(8_000_000) // 1 MB/s，容量 1 秒 = 1 MB
	ctx := context.Background()
	start := time.Now()
	var got int
	for time.Since(start) < 1500*time.Millisecond {
		n, err := b.Take(ctx, 64<<10, 16<<10)
		if err != nil {
			t.Fatal(err)
		}
		got += n
	}
	el := time.Since(start).Seconds()
	want := 1_000_000 + el*1_000_000 // 初始满桶 1 秒突发 + 持续速率（ENG-026：突发不超过 1 秒份额）
	if r := float64(got) / want; r < 0.93 || r > 1.05 {
		t.Fatalf("took %d bytes in %.2fs, want ≈%.0f (ratio %.3f)", got, el, want, r)
	}
	if s := b.TakeStarved(); s < 800*time.Millisecond {
		t.Fatalf("starved %v while saturated for ~1s", s)
	}
	b.SetRate(0) // 速率为 0：一直等到取消
	cctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if n, err := b.Take(cctx, 1, 1); n != 0 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("zero-rate take returned %d, %v", n, err)
	}
}

func TestDeadlineFromFirstReceiptNotExtendedByResend(t *testing.T) {
	g := New(Options{Margin: smallMargin})
	g.UpdateGrants(fullSet(200*time.Millisecond, 1e9, 10, 1<<20))
	d1 := g.runUntil[ruleEP]
	time.Sleep(50 * time.Millisecond)
	g.UpdateGrants(fullSet(200*time.Millisecond, 1e9, 10, 1<<20)) // 同一授权 ID 再次收到
	if g.runUntil[ruleEP] != d1 {
		t.Fatal("re-sending the same grant must not extend its deadline (ENG-025)")
	}
	if want := 200*time.Millisecond - 20*time.Millisecond; d1 > want+10*time.Millisecond || d1 < want-10*time.Millisecond {
		t.Fatalf("deadline %v, want ≈ ttl − margin = %v", d1, want)
	}
	renew := fullSet(200*time.Millisecond, 1e9, 10, 1<<20)
	renew.Run[0].Meta.GrantID = types.UUID{99} // 续期：新授权 ID
	g.UpdateGrants(renew)
	if g.runUntil[ruleEP] <= d1 {
		t.Fatal("renewal with a new grant must extend")
	}
	if defaultMargin(5*time.Minute) != 30*time.Second || defaultMargin(time.Minute) != 10*time.Second || defaultMargin(4*time.Second) != 2*time.Second {
		t.Fatal("default margin: max(10%, 10s), capped at ttl/2")
	}
}

func TestAdmissionChecks(t *testing.T) {
	g := New(Options{Margin: smallMargin, GrantWait: 50 * time.Millisecond})
	ctx := context.Background()
	if _, _, err := g.Admit(ctx, epRule(), noAddr, func(error) {}); !errors.Is(err, ErrRunGrant) {
		t.Fatalf("no run grant: %v", err)
	}
	// 只有运行授权：非执行点可准入，执行点因缺份额被拒
	g.UpdateGrants(&types.GrantSet{Run: []types.RunGrant{{Meta: meta(1, time.Hour), RuleIDs: []types.UUID{ruleEP, ruleMid}}}})
	mid := &types.Rule{RuleID: ruleMid, TenantID: tenantA, Role: types.RoleRelay}
	if _, rel, err := g.Admit(ctx, mid, noAddr, func(error) {}); err != nil {
		t.Fatalf("non enforcement point: %v", err)
	} else {
		rel()
	}
	if _, _, err := g.Admit(ctx, epRule(), noAddr, func(error) {}); !errors.Is(err, ErrShare) {
		t.Fatalf("missing share: %v", err)
	}
	// 有份额无计费授权：等待 GrantWait 后拒绝
	g.UpdateGrants(fullSet(time.Hour, 1e9, 2, 0))
	start := time.Now()
	if _, _, err := g.Admit(ctx, epRule(), noAddr, func(error) {}); !errors.Is(err, ErrQuota) || time.Since(start) < 45*time.Millisecond {
		t.Fatalf("no billing grant: %v after %v", err, time.Since(start))
	}
	// 计费授权在等待期间到达：准入成功
	go func() { time.Sleep(20 * time.Millisecond); g.UpdateGrants(fullSet(time.Hour, 1e9, 2, 1<<20)) }()
	_, r1, err := g.Admit(ctx, epRule(), noAddr, func(error) {})
	if err != nil {
		t.Fatalf("grant arrived during wait: %v", err)
	}
	_, r2, err := g.Admit(ctx, epRule(), noAddr, func(error) {})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := g.Admit(ctx, epRule(), noAddr, func(error) {}); !errors.Is(err, ErrConnLimited) {
		t.Fatalf("third session with conn share 2: %v", err)
	}
	r1()
	r1() // release 幂等
	if _, r3, err := g.Admit(ctx, epRule(), noAddr, func(error) {}); err != nil {
		t.Fatalf("after release: %v", err)
	} else {
		r3()
	}
	r2()
}

func TestHookMeteringRefundAndBlockGrowth(t *testing.T) {
	g := New(Options{Margin: smallMargin})
	g.UpdateGrants(fullSet(time.Hour, 1e12, 10, 10<<20))
	h, rel, err := g.Admit(context.Background(), epRule(), noAddr, func(error) {})
	if err != nil {
		t.Fatal(err)
	}
	s := h.(*session)
	ctx := context.Background()
	n, _ := h.Acquire(ctx, types.DirUp, 1<<20)
	if n != blockMin {
		t.Fatalf("first block %d, want %d", n, blockMin)
	}
	h.Release(types.DirUp, n, n) // 用满 → 翻倍
	if s.block[types.DirUp] != 2*blockMin {
		t.Fatalf("block not doubled: %d", s.block[types.DirUp])
	}
	n, _ = h.Acquire(ctx, types.DirUp, 1<<20)
	h.Release(types.DirUp, n, 1000) // 只写成功 1000：余量退回
	n, _ = h.Acquire(ctx, types.DirDown, 500)
	h.Release(types.DirDown, n, 500)
	rel()
	snap := g.UsageSnapshot()
	var up, down uint64
	for _, d := range snap.Deltas {
		if d.BillingGrantID != (types.UUID{4}) {
			t.Fatalf("enforcement point usage must carry the billing grant id: %+v", d)
		}
		if d.Direction == types.DirUp {
			up += d.RawBytes
		} else {
			down += d.RawBytes
		}
	}
	if up != blockMin+1000 || down != 500 {
		t.Fatalf("ledger up %d down %d", up, down)
	}
	if len(snap.Consumptions) != 1 || snap.Consumptions[0].UsedRawBytes != blockMin+1500 {
		t.Fatalf("consumption %+v", snap.Consumptions)
	}
	if len(snap.Demands) != 1 || snap.Demands[0].Bytes60s != blockMin+1500 {
		t.Fatalf("demand %+v", snap.Demands)
	}
	if rs := g.rules[ruleEP]; rs.account.Available() != 10<<20-(blockMin+1500) {
		t.Fatalf("unused bytes not refunded: available %d", rs.account.Available())
	}
}

func TestRunGrantExpiryKillsSessionsWithEvents(t *testing.T) {
	ev := &evlog{}
	g := New(Options{Margin: smallMargin, Emit: ev.emit})
	g.UpdateGrants(fullSet(300*time.Millisecond, 1e9, 10, 1<<20))
	var killed sync.WaitGroup
	var rels []func()
	for i := 0; i < 3; i++ {
		killed.Add(1)
		_, rel, err := g.Admit(context.Background(), epRule(), noAddr, func(error) { killed.Done() })
		if err != nil {
			t.Fatal(err)
		}
		rels = append(rels, rel)
	}
	done := make(chan struct{})
	go func() { killed.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sessions not killed at run grant expiry")
	}
	if ev.count(types.EventRestrictionStarted) != 1 || ev.count(types.EventRestrictionCompleted) != 0 {
		t.Fatal("started once; completed only after all sessions end")
	}
	for _, r := range rels {
		r()
	}
	if ev.count(types.EventRestrictionCompleted) != 1 {
		t.Fatal("completed event after last session released")
	}
	if _, _, err := g.Admit(context.Background(), epRule(), noAddr, func(error) {}); !errors.Is(err, ErrRunGrant) {
		t.Fatalf("new sessions must be rejected after expiry: %v", err)
	}
}

func TestConnDownsizeDrainsNewestAfterWindow(t *testing.T) {
	g := New(Options{Margin: smallMargin, DrainWindow: 150 * time.Millisecond})
	g.UpdateGrants(fullSet(time.Hour, 1e9, 5, 1<<20))
	killed := make([]bool, 5)
	var mu sync.Mutex
	for i := 0; i < 5; i++ {
		i := i
		if _, _, err := g.Admit(context.Background(), epRule(), noAddr, func(error) { mu.Lock(); killed[i] = true; mu.Unlock() }); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond) // 区分建立顺序
	}
	down := fullSet(time.Hour, 1e9, 3, 1<<20)
	down.Shares[1].Meta.GrantID = types.UUID{33} // 新的连接份额租约：3
	g.UpdateGrants(down)
	if _, _, err := g.Admit(context.Background(), epRule(), noAddr, func(error) {}); !errors.Is(err, ErrConnLimited) {
		t.Fatalf("over-limit new session must be rejected immediately: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	early := killed[3] || killed[4]
	mu.Unlock()
	if early {
		t.Fatal("excess sessions must get the drain window before being closed")
	}
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if killed[0] || killed[1] || killed[2] || !killed[3] || !killed[4] {
		t.Fatalf("killed %v, want only the two newest", killed)
	}
}

func TestStarvationSignalInDemand(t *testing.T) {
	g := New(Options{Margin: smallMargin})
	g.UpdateGrants(fullSet(time.Hour, 8_000_000, 10, 100<<20)) // 1 MB/s
	h, rel, _ := g.Admit(context.Background(), epRule(), noAddr, func(error) {})
	defer rel()
	_ = g.UsageSnapshot()
	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		n, _ := h.Acquire(context.Background(), types.DirUp, 1<<20)
		h.Release(types.DirUp, n, n)
	}
	d := g.UsageSnapshot().Demands[0]
	if d.StarvedPermille < 300 {
		t.Fatalf("rate-limited tenant must show starvation, got %d‰", d.StarvedPermille)
	}
}

var noAddr = netipAddr()
