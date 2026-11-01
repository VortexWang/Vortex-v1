package runner

import (
	"context"
	"sync"
	"testing"
	"time"

	"vortex/edgehub/clock"
	"vortex/edgehub/issuer"
	"vortex/edgehub/leases"
	"vortex/edgehub/ledger"
	"vortex/edgehub/model"
	"vortex/edgehub/session"
	"vortex/edgehub/upstream"
)

func TestRunLoop(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC))
	st := &ledger.MemStore{}
	node, boot, tenant := model.UUID{0xd}, model.UUID{0xb}, model.UUID{0x7}
	h, err := session.Open(session.Options{HubID: model.UUID{1}, Clock: clk, Store: st,
		Rules: session.RulesFunc(func(model.UUID) []issuer.RuleState { return nil })})
	if err != nil {
		t.Fatal(err)
	}
	l := upstream.New(h)
	var mu sync.Mutex
	var ups []upstream.Up
	_ = l.Connected(func(m upstream.Up) { mu.Lock(); ups = append(ups, m); mu.Unlock() })
	var msgs []session.HubMsg
	s := h.NewSession(node, func(m session.HubMsg) { mu.Lock(); msgs = append(msgs, m); mu.Unlock() })
	_ = s.Handle(session.Hello{Node: node, Boot: boot})
	_, _ = h.Leases.ApplyUpdate(node, boot, []leases.Lease{{Tenant: tenant, Kind: model.GrantShareRate, Amount: 100}})
	r := &Runner{Hub: h, Link: l, Tick: 5 * time.Millisecond, Rebalance: 10 * time.Millisecond, Compact: 15 * time.Millisecond,
		OnError: func(step string, err error) { t.Errorf("%s: %v", step, err) }}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()

	// 在线：清理与检查点照常进行
	deadline := time.Now().Add(2 * time.Second)
	cps := func() int {
		n := 0
		_ = st.Load(func(e ledger.Entry) error {
			if e.Kind == session.KindCheckpoint {
				n++
			}
			return nil
		})
		return n
	}
	for cps() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if cps() == 0 {
		t.Fatal("no checkpoint written by the loop")
	}
	// 与后端断开 30 秒：下一次 Tick 进入自治，冻结份额，再平衡推送续租
	l.Disconnected()
	clk.Advance(31 * time.Second)
	for (h.Mode.Mode() != model.ModeAutonomous || h.Leases.Frozen() == nil) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if h.Mode.Mode() != model.ModeAutonomous || h.Leases.Frozen() == nil {
		t.Fatal("loop did not enter autonomy / freeze shares")
	}
	var shares bool
	for !shares && time.Now().Before(deadline) {
		mu.Lock()
		for _, m := range msgs {
			if g, ok := m.(session.GrantResponse); ok && len(g.Shares) > 0 {
				shares = true
			}
		}
		mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	if !shares {
		t.Fatal("no rebalance push during autonomy")
	}
	n := cps()
	time.Sleep(60 * time.Millisecond)
	if cps() != n {
		t.Fatal("checkpoint/compaction while autonomous")
	}
	cancel()
	<-done
}
