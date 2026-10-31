package grants

import (
	"errors"
	"testing"
	"time"

	"vortex/engine/types"
	"vortex/node/internal/clock"
)

var (
	hubA  = types.UUID{0xa1}
	hubB  = types.UUID{0xb1}
	boot  = types.UUID{0xb0}
	old   = types.UUID{0xbf}
	ruleX = types.UUID{0x11}
	ruleY = types.UUID{0x12}
	ten   = types.UUID{0x7}
)

func meta(id byte, hub, boot types.UUID, ttl time.Duration) types.GrantMeta {
	return types.GrantMeta{GrantID: types.UUID{id}, IssuerHub: hub, BootID: boot, TTL: ttl}
}

func setup() (*Manager, *clock.Fake) {
	clk := clock.NewFake(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC))
	m := New(clk, boot)
	m.Connected(hubA)
	return m, clk
}

func runSet(id byte, hub, b types.UUID, rules ...types.UUID) types.GrantSet {
	return types.GrantSet{Run: []types.RunGrant{{Meta: meta(id, hub, b, 5*time.Minute), RuleIDs: rules}}}
}

// HUB-014、AGT-009：只收当前实例的应答、该实例签发、本次启动的授权。
func TestAcceptOnlyCurrentHubAndBoot(t *testing.T) {
	m, _ := setup()
	if _, err := m.Apply(hubB, Response{Set: runSet(1, hubB, boot, ruleX)}); !errors.Is(err, ErrForeignIssuer) {
		t.Fatalf("response from another hub: %v", err)
	}
	ch, _ := m.Apply(hubA, Response{Set: types.GrantSet{
		Run:     []types.RunGrant{{Meta: meta(2, hubB, boot, time.Minute), RuleIDs: []types.UUID{ruleX}}}, // 他实例签发
		Billing: []types.BillingGrant{{Meta: meta(3, hubA, old, time.Minute), RuleID: ruleX}},             // 上次启动
		Shares:  []types.ShareLease{{Meta: meta(4, hubA, boot, 0), TenantID: ten}},                        // 无有效期
	}})
	if ch || len(m.EngineSet().Run)+len(m.EngineSet().Billing)+len(m.EngineSet().Shares) != 0 {
		t.Fatalf("accepted a grant it must not: %+v", m.EngineSet())
	}
	if ch, _ := m.Apply(hubA, Response{Set: runSet(5, hubA, boot, ruleX)}); !ch {
		t.Fatal("valid grant not accepted")
	}
	if ch, _ := m.Apply(hubA, Response{Set: runSet(5, hubA, boot, ruleX)}); ch {
		t.Fatal("duplicate grant changed the set")
	}
	m2 := New(clock.System{}, boot)
	if _, err := m2.Apply(hubA, Response{}); !errors.Is(err, ErrForeignIssuer) {
		t.Fatal("accepted a response while not connected")
	}
}

// AGT-005：TTL 原样交给 Engine；到期后从集合中移除；断线不主动停止（AGT-011）。
func TestEngineSetAndExpiry(t *testing.T) {
	m, clk := setup()
	_, _ = m.Apply(hubA, Response{Set: runSet(1, hubA, boot, ruleX)})
	clk.Advance(2 * time.Minute)
	m.Disconnected()
	set := m.EngineSet()
	if len(set.Run) != 1 || set.Run[0].Meta.TTL != 5*time.Minute {
		t.Fatalf("set %+v (TTL must be the original relative value)", set)
	}
	if rem := m.RunRemaining()[ruleX]; rem != 3*time.Minute {
		t.Fatalf("remaining %v", rem)
	}
	clk.Advance(3 * time.Minute)
	if set := m.EngineSet(); len(set.Run) != 0 {
		t.Fatal("expired grant still handed to the engine")
	}
}

// 每 60 秒续期：只续最新的一份，在途不重复，异步超时后重发；未连接时不续。
func TestRenewal(t *testing.T) {
	m, clk := setup()
	_, _ = m.Apply(hubA, Response{Set: types.GrantSet{
		Run:    []types.RunGrant{{Meta: meta(1, hubA, boot, 5*time.Minute), RuleIDs: []types.UUID{ruleX, ruleY}}},
		Shares: []types.ShareLease{{Meta: meta(2, hubA, boot, 5*time.Minute), TenantID: ten, Kind: types.ShareRate, Amount: 100}},
	}})
	if d := m.Due(); len(d) != 0 {
		t.Fatalf("renewed too early %+v", d)
	}
	clk.Advance(RenewEvery)
	d := m.Due()
	if len(d) != 2 {
		t.Fatalf("due %+v", d)
	}
	for _, r := range d {
		if (r.Kind == KindRun && r.CurrentGrant != (types.UUID{1})) || (r.Kind == KindShareRate && (r.CurrentGrant != types.UUID{2} || r.Tenant != ten)) {
			t.Fatalf("request %+v", r)
		}
	}
	if d := m.Due(); len(d) != 0 {
		t.Fatal("duplicate renewal while in flight")
	}
	clk.Advance(AsyncTimeout)
	if d := m.Due(); len(d) != 2 {
		t.Fatal("no resend after timeout")
	}
	// 续期应答到达：新授权更新，下一次只续新的一份
	var runReq Request
	for _, r := range m.pendingList() {
		if r.Kind == KindRun {
			runReq = r
		}
	}
	_, _ = m.Apply(hubA, Response{RequestID: runReq.ID, Set: types.GrantSet{Run: []types.RunGrant{{Meta: meta(3, hubA, boot, 5*time.Minute), RuleIDs: []types.UUID{ruleX, ruleY}}}}})
	clk.Advance(RenewEvery)
	n := 0
	for _, r := range m.Due() {
		if r.Kind == KindRun {
			n++
			if r.CurrentGrant != (types.UUID{3}) {
				t.Fatalf("renewed the older grant %+v", r)
			}
		}
	}
	if n != 1 {
		t.Fatalf("run renewals %d", n)
	}
	if m.Answered() {
		t.Fatal("a renewal answer is not the bootstrap answer")
	}
	m.Connected(types.UUID{})
	clk.Advance(RenewEvery)
	if d := m.Due(); d != nil {
		t.Fatal("renewal while not connected")
	}
}

func (m *Manager) pendingList() []Request {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Request
	for _, p := range m.pending {
		out = append(out, *p)
	}
	return out
}

// ENG-050：grant_low 异步、grant_needed 同步；同一规则不重复；异步在途时仍发同步；同步 200 ms 超时可重发。
func TestBillingRequests(t *testing.T) {
	m, clk := setup()
	low := types.Event{Kind: types.EventGrantLow, RuleID: ruleX, TenantID: ten}
	need := types.Event{Kind: types.EventGrantNeeded, RuleID: ruleX, TenantID: ten}
	r, _ := m.OnEvent(low, 8_000_000)
	if r == nil || r.Sync || r.Kind != KindBilling || r.Rule != ruleX || r.RateBps60s != 8_000_000 {
		t.Fatalf("low %+v", r)
	}
	if r, _ := m.OnEvent(low, 0); r != nil {
		t.Fatal("duplicate async request")
	}
	s, _ := m.OnEvent(need, 0)
	if s == nil || !s.Sync {
		t.Fatal("sync request suppressed by an async one in flight")
	}
	if r, _ := m.OnEvent(need, 0); r != nil {
		t.Fatal("duplicate sync request")
	}
	clk.Advance(SyncTimeout)
	if r, _ := m.OnEvent(need, 0); r == nil {
		t.Fatal("no retry after the sync timeout")
	}
	if r, _ := m.OnEvent(types.Event{Kind: types.EventGrantLow, RuleID: ruleY}, 0); r == nil {
		t.Fatal("another rule blocked")
	}
	if r, _ := m.OnEvent(types.Event{Kind: types.EventPeerDown}, 0); r != nil {
		t.Fatal("unrelated event")
	}
	// 拒绝原因按规则记录，拿到授权后清除
	_, _ = m.Apply(hubA, Response{RequestID: s.ID, Error: "QUOTA_EXHAUSTED"})
	if m.LastError(ruleX) != "QUOTA_EXHAUSTED" {
		t.Fatal("error not recorded")
	}
	_, _ = m.Apply(hubA, Response{Set: types.GrantSet{Billing: []types.BillingGrant{{Meta: meta(9, hubA, boot, time.Minute), RuleID: ruleX, RawBytes: 8 << 20}}}})
	if m.LastError(ruleX) != "" {
		t.Fatal("error not cleared")
	}
	m.Disconnected()
	if _, err := m.OnEvent(low, 0); err != nil {
		t.Fatalf("disconnected manager still knows its hub until Connected: %v", err)
	}
	m2 := New(clk, boot)
	if _, err := m2.OnEvent(low, 0); !errors.Is(err, ErrNoHub) {
		t.Fatal("request without a hub")
	}
}

// 计费授权 final 后移出集合；已用量随 HeldGrantsReport 报告。
func TestConsumption(t *testing.T) {
	m, _ := setup()
	_, _ = m.Apply(hubA, Response{Set: types.GrantSet{Billing: []types.BillingGrant{{Meta: meta(1, hubA, boot, time.Minute), RuleID: ruleX, RawBytes: 100}}}})
	if m.Observe([]types.GrantConsumption{{GrantID: types.UUID{1}, UsedRawBytes: 40}}) {
		t.Fatal("non-final changed the set")
	}
	if h := m.Held(); len(h) != 1 || h[0].Used != 40 || h[0].Amount != 100 || h[0].Rule != ruleX {
		t.Fatalf("held %+v", h)
	}
	if !m.Observe([]types.GrantConsumption{{GrantID: types.UUID{1}, UsedRawBytes: 100, Final: true}}) || len(m.EngineSet().Billing) != 0 {
		t.Fatal("final grant still in the set")
	}
}

// HUB-014：切换实例后报告全部持有授权（含原实例签发的），原授权继续交给 Engine；只收新实例签发的新授权；
// 在途申请作废（应答只会来自旧连接）。
func TestSwitchHub(t *testing.T) {
	m, clk := setup()
	_, _ = m.Apply(hubA, Response{Set: types.GrantSet{
		Run:     []types.RunGrant{{Meta: meta(1, hubA, boot, 5*time.Minute), RuleIDs: []types.UUID{ruleX}}},
		Billing: []types.BillingGrant{{Meta: meta(2, hubA, boot, 10*time.Minute), RuleID: ruleX, RawBytes: 1 << 20}},
		Shares:  []types.ShareLease{{Meta: meta(3, hubA, boot, 5*time.Minute), TenantID: ten, Kind: types.ShareConn, Amount: 50}},
	}})
	_ = m.Observe([]types.GrantConsumption{{GrantID: types.UUID{2}, UsedRawBytes: 300}})
	_ = m.Observe([]types.GrantConsumption{{GrantID: types.UUID{2}, UsedRawBytes: 200}}) // 乱序的旧值不回退
	clk.Advance(time.Minute)
	_, _ = m.OnEvent(types.Event{Kind: types.EventGrantLow, RuleID: ruleY}, 0)
	if rep, sw := m.Connected(hubA); sw || rep != nil {
		t.Fatal("reconnect to the same hub reported as a switch")
	}
	if r, _ := m.OnEvent(types.Event{Kind: types.EventGrantLow, RuleID: ruleY}, 0); r == nil {
		t.Fatal("request of the dropped connection still in flight after reconnect")
	}
	req, _ := m.OnEvent(types.Event{Kind: types.EventGrantLow, RuleID: ruleX}, 0)
	rep, sw := m.Connected(hubB)
	if !sw || len(rep) != 3 {
		t.Fatalf("held report %+v", rep)
	}
	for _, h := range rep {
		if h.Issuer != hubA {
			t.Fatalf("issuer %+v", h)
		}
		switch h.Kind {
		case KindBilling:
			if h.Used != 300 || h.RemainingTTL != 9*time.Minute {
				t.Fatalf("billing %+v", h)
			}
		case KindShareConn:
			if h.Amount != 50 || h.Tenant != ten || h.RemainingTTL != 4*time.Minute {
				t.Fatalf("share %+v", h)
			}
		}
	}
	if set := m.EngineSet(); len(set.Run) != 1 || len(set.Billing) != 1 || len(set.Shares) != 1 {
		t.Fatal("grants from the previous hub dropped on switch")
	}
	if _, err := m.Apply(hubA, Response{RequestID: req.ID, Set: runSet(7, hubA, boot, ruleX)}); !errors.Is(err, ErrForeignIssuer) {
		t.Fatal("accepted a late answer from the previous hub")
	}
	if ch, _ := m.Apply(hubB, Response{Set: runSet(8, hubA, boot, ruleX)}); ch {
		t.Fatal("new hub relayed a grant issued by the old one")
	}
	if r, _ := m.OnEvent(types.Event{Kind: types.EventGrantLow, RuleID: ruleX}, 0); r == nil {
		t.Fatal("in-flight request of the old connection still blocks")
	}
}

// LIM-010：签发方主动推送（request_id = 0）的作废与新授权。
func TestRevokePush(t *testing.T) {
	m, _ := setup()
	_, _ = m.Apply(hubA, Response{Set: runSet(1, hubA, boot, ruleX)})
	ch, err := m.Apply(hubA, Response{Revoked: []types.UUID{{1}}, Set: runSet(2, hubA, boot, ruleX)})
	if err != nil || !ch {
		t.Fatal(err)
	}
	if set := m.EngineSet(); len(set.Run) != 1 || set.Run[0].Meta.GrantID != (types.UUID{2}) {
		t.Fatalf("after revoke %+v", set)
	}
}

// AGT-010：没有有效运行授权时发启动申请（不带当前授权 ID），在途不重复；作答后 Answered；有授权后不再发。
func TestBootstrap(t *testing.T) {
	m, clk := setup()
	r := m.Bootstrap()
	if r == nil || r.Kind != KindRun || r.CurrentGrant != (types.UUID{}) {
		t.Fatalf("bootstrap %+v", r)
	}
	if m.Bootstrap() != nil {
		t.Fatal("duplicate bootstrap in flight")
	}
	if m.Answered() {
		t.Fatal("answered before a response")
	}
	_, _ = m.Apply(hubA, Response{RequestID: r.ID, Error: "RULE_STOPPED"})
	if !m.Answered() {
		t.Fatal("a refusal is an answer")
	}
	_, _ = m.Apply(hubA, Response{Set: runSet(1, hubA, boot, ruleX)})
	clk.Advance(AsyncTimeout)
	if m.Bootstrap() != nil {
		t.Fatal("bootstrap with a valid run grant")
	}
	clk.Advance(5 * time.Minute) // 运行授权全部到期：重新申请
	if m.Bootstrap() == nil {
		t.Fatal("no bootstrap after all run grants expired")
	}
	if New(clk, boot).Bootstrap() != nil {
		t.Fatal("bootstrap without a hub")
	}
}
