package gate_test

import (
	"context"
	"crypto/rand"
	"io"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"vortex/engine/gate"
	"vortex/engine/internal/lifecycle"
	"vortex/engine/route"
	"vortex/engine/types"
)

// 端到端：直接转发入口 + 真实门控。目标只读不写，统计收到的字节，便于与计量逐字节比对。

var (
	tenant = types.UUID{0x7a}
	ruleID = types.UUID{0xe1}
)

type sink struct {
	ln    net.Listener
	bytes atomic.Int64
}

func newSink(t *testing.T) *sink {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &sink{ln: ln}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				n, _ := io.Copy(io.Discard, c)
				s.bytes.Add(n)
			}()
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return s
}

func grants(runTTL time.Duration, rateBps uint64, billing uint64) *types.GrantSet {
	m := func(id byte, ttl time.Duration) types.GrantMeta {
		return types.GrantMeta{GrantID: types.UUID{id}, TTL: ttl}
	}
	return &types.GrantSet{
		Run: []types.RunGrant{{Meta: m(1, runTTL), RuleIDs: []types.UUID{ruleID}}},
		Shares: []types.ShareLease{
			{Meta: m(2, time.Hour), TenantID: tenant, Kind: types.ShareRate, Amount: rateBps},
			{Meta: m(3, time.Hour), TenantID: tenant, Kind: types.ShareConn, Amount: 100},
		},
		Billing: []types.BillingGrant{{Meta: m(4, time.Hour), RuleID: ruleID, RawBytes: billing}},
	}
}

func setup(t *testing.T, g *gate.Gate, s *sink) string {
	ap := netip.MustParseAddrPort(s.ln.Addr().String())
	var ts route.Tables
	ts.Swap(route.NewTable(1, []types.Rule{{RuleID: ruleID, TenantID: tenant, RuleVersion: 1, Role: types.RoleEntry,
		EnforcementPoint: true, Targets: []types.Target{{Host: ap.Addr().String(), Port: ap.Port(), Weight: 1}}}}))
	r, err := route.ReserveTCP(netip.MustParseAddrPort("127.0.0.1:0"))
	if err != nil {
		t.Fatal(err)
	}
	ln, err := r.CommitTCP(128)
	if err != nil {
		t.Fatal(err)
	}
	root := lifecycle.NewRoot(context.Background())
	e := &route.DirectEntry{RuleID: ruleID, Tables: &ts, Listener: ln, Scope: root, Admission: g,
		Resolver: route.NewResolver(nil, 0), Guard: route.NewGuard(types.TargetPolicy{SelfHostedNode: true}, nil, nil)}
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	return ln.Addr().String()
}

func send(t *testing.T, addr string, n int) (sent int, err error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(20 * time.Second))
	buf := make([]byte, 64<<10)
	_, _ = rand.Read(buf)
	for sent < n {
		m := len(buf)
		if n-sent < m {
			m = n - sent
		}
		w, err := c.Write(buf[:m])
		sent += w
		if err != nil {
			return sent, err
		}
	}
	_ = c.(*net.TCPConn).CloseWrite()
	_, _ = io.Copy(io.Discard, c)
	return sent, nil
}

func waitFor(t *testing.T, cond func() bool) {
	deadline := time.Now().Add(5 * time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
}

func meteredUp(snap types.UsageSnapshot) (n uint64) {
	for _, d := range snap.Deltas {
		if d.Direction == types.DirUp {
			n += d.RawBytes
		}
	}
	return
}

func TestE2ERateLimitAccuracy(t *testing.T) {
	g := gate.New(gate.Options{})
	g.UpdateGrants(grants(time.Hour, 16_000_000, 1<<40)) // 2 MB/s，突发 2 MB（1 秒份额）
	s := newSink(t)
	addr := setup(t, g, s)
	const total = 8 << 20
	start := time.Now()
	if _, err := send(t, addr, total); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return s.bytes.Load() == total })
	el := time.Since(start).Seconds()
	want := (float64(total) - 2e6) / 2e6 // 扣除初始 1 秒突发
	t.Logf("8 MiB at 16 Mbps share: %.2fs (theory %.2fs)", el, want)
	if el < want*0.9 || el > want*1.2 {
		t.Fatalf("elapsed %.2fs, want %.2fs ±10%%/20%% (ENG-026)", el, want)
	}
}

func TestE2EMeteringMatchesDeliveredBytes(t *testing.T) {
	g := gate.New(gate.Options{})
	g.UpdateGrants(grants(time.Hour, 1e12, 1<<40))
	s := newSink(t)
	addr := setup(t, g, s)
	var want int64
	for i, n := range []int{1, 1000, 65536, 300_000, 3 << 20, 7_777_777} {
		if _, err := send(t, addr, n); err != nil {
			t.Fatalf("session %d: %v", i, err)
		}
		want += int64(n)
	}
	waitFor(t, func() bool { return s.bytes.Load() == want })
	snap := g.UsageSnapshot()
	if got := meteredUp(snap); int64(got) != want || s.bytes.Load() != want {
		t.Fatalf("metered %d, delivered %d, sent %d — must be equal (NFR-021)", got, s.bytes.Load(), want)
	}
	if snap.Consumptions[0].UsedRawBytes != uint64(want) {
		t.Fatalf("grant consumption %d, want %d", snap.Consumptions[0].UsedRawBytes, want)
	}
}

func TestE2EBillingExhaustionStopsSession(t *testing.T) {
	g := gate.New(gate.Options{GrantWait: 100 * time.Millisecond})
	const grant = 2 << 20
	g.UpdateGrants(grants(time.Hour, 1e12, grant))
	s := newSink(t)
	addr := setup(t, g, s)
	start := time.Now()
	_, _ = send(t, addr, 16<<20) // 发 16 MiB，额度只有 2 MiB
	waitFor(t, func() bool { return time.Since(start) > 300*time.Millisecond })
	snap := g.UsageSnapshot()
	if s.bytes.Load() != grant || meteredUp(snap) != grant {
		t.Fatalf("delivered %d, metered %d, want exactly the grant %d", s.bytes.Load(), meteredUp(snap), grant)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("session should end soon after the grant is exhausted")
	}
}

func TestE2ERunGrantExpiryClosesIdleSession(t *testing.T) {
	g := gate.New(gate.Options{Margin: func(ttl time.Duration) time.Duration { return ttl / 10 }})
	g.UpdateGrants(grants(400*time.Millisecond, 1e12, 1<<30))
	s := newSink(t)
	addr := setup(t, g, s)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, _ = c.Write([]byte("hello")) // 之后保持空闲：会话阻塞在读取上，不会调用 Hook
	start := time.Now()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, err = io.ReadAll(c)
	el := time.Since(start)
	if err != nil || el > time.Second {
		t.Fatalf("idle session not closed at run grant expiry: %v after %v (ENG-028)", err, el)
	}
}
