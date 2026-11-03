package quota

import (
	"testing"
	"time"

	"vortex/edgehub/clock"
	"vortex/edgehub/ledger"
	"vortex/edgehub/model"
)

// 近 1 小时平均计费速率（BudgetRequest，HUB-006）：按签发的计费字节统计，超过 1 小时的签发不再计入；重放账本按条目时刻恢复。
func TestRateBps1h(t *testing.T) {
	clk := clock.NewFake(time.Unix(1_000_000, 0))
	st := &ledger.MemStore{}
	l, err := ledger.Open(st, clk, nil)
	if err != nil {
		t.Fatal(err)
	}
	q := New(l)
	q.Now = clk.Now
	tenant := model.UUID{1}
	if _, err := q.ApplyBudget(tenant, 1, 1<<30, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Reserve(tenant, model.UUID{2}, 3600, 1000); err != nil {
		t.Fatal(err)
	}
	clk.Advance(30 * time.Minute)
	if _, err := q.Reserve(tenant, model.UUID{3}, 1000, 1500); err != nil { // 1500 计费字节
		t.Fatal(err)
	}
	if got := q.RateBps1h(tenant, clk.Now()); got != (3600+1500)*8/3600 {
		t.Fatalf("rate %d", got)
	}
	clk.Advance(31 * time.Minute)
	if got := q.RateBps1h(tenant, clk.Now()); got != 1500*8/3600 {
		t.Fatalf("rate after an hour %d", got)
	}
	if got := q.RateBps1h(model.UUID{9}, clk.Now()); got != 0 {
		t.Fatalf("unknown tenant %d", got)
	}
	// 重放：样本取账本条目的时刻
	r := New(nil)
	if _, err := ledger.Open(st, clk, func(e ledger.Entry) error { _, err := r.Replay(e); return err }); err != nil {
		t.Fatal(err)
	}
	if got := r.RateBps1h(tenant, clk.Now()); got != 1500*8/3600 {
		t.Fatalf("rate after replay %d", got)
	}
}
