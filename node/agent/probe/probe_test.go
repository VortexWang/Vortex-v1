package probe

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"vortex/engine/types"
)

var (
	m1 = types.UUID{0x1}
	m2 = types.UUID{0x2}
)

func cfg() *types.RuntimeConfig {
	return &types.RuntimeConfig{
		Peers: []types.Peer{{NodeID: m1, Address: "10.0.0.1", LinkPort: 7443, Outbound: true}, {NodeID: m2}},
		Rules: []types.Rule{
			{RuleID: types.UUID{0xa}, NextHop: &types.NextHop{Members: []types.Member{{NodeID: m1}, {NodeID: m2}}}},
			{RuleID: types.UUID{0xb}, Networks: []types.Network{types.NetworkTCP}, Targets: []types.Target{{Host: "1.1.1.1", Port: 80}, {Host: "1.1.1.1", Port: 80}}},
			{RuleID: types.UUID{0xc}, Networks: []types.Network{types.NetworkUDP}, Targets: []types.Target{{Host: "8.8.8.8", Port: 53}}},
		},
	}
}

func TestSubjects(t *testing.T) {
	s := Subjects(cfg())
	if len(s) != 2 || s[0].Node != m1 || s[0].addr != "10.0.0.1:7443" || s[1].Target != "1.1.1.1:80" {
		t.Fatalf("subjects %+v (reverse-dial member and UDP-only target are not probed; duplicates merged)", s)
	}
}

type fakeDial struct {
	mu   sync.Mutex
	fail map[string]bool
}

func (f *fakeDial) dial(_ context.Context, addr string) (time.Duration, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail[addr] {
		return 0, errors.New("refused")
	}
	return 7 * time.Millisecond, nil
}

func (f *fakeDial) set(addr string, fail bool) { f.mu.Lock(); f.fail[addr] = fail; f.mu.Unlock() }

// 连续 3 次失败才不可用，连续 2 次成功才恢复；中间的成功或失败清零计数。
func TestHysteresis(t *testing.T) {
	fd := &fakeDial{fail: map[string]bool{}}
	p := New()
	p.Dial = fd.dial
	p.SetSubjects(Subjects(cfg()))
	ctx := context.Background()
	if !p.PeerHealthy(m1) || !p.TargetHealthy("1.1.1.1:80") || !p.PeerHealthy(m2) || !p.TargetHealthy("8.8.8.8:53") {
		t.Fatal("initial state must be healthy")
	}
	fd.set("10.0.0.1:7443", true)
	p.Round(ctx)
	p.Round(ctx)
	if !p.PeerHealthy(m1) {
		t.Fatal("down after 2 failures")
	}
	fd.set("10.0.0.1:7443", false)
	p.Round(ctx) // 成功一次：失败计数清零
	fd.set("10.0.0.1:7443", true)
	p.Round(ctx)
	p.Round(ctx)
	if !p.PeerHealthy(m1) {
		t.Fatal("failure count not reset by a success")
	}
	r := p.Round(ctx)
	if p.PeerHealthy(m1) || r[0].Reachable || r[0].RTT != 0 {
		t.Fatalf("not down after 3 consecutive failures %+v", r[0])
	}
	if !p.TargetHealthy("1.1.1.1:80") || r[1].RTT != 7*time.Millisecond {
		t.Fatalf("target %+v", r[1])
	}
	fd.set("10.0.0.1:7443", false)
	p.Round(ctx)
	if p.PeerHealthy(m1) {
		t.Fatal("recovered after 1 success")
	}
	fd.set("10.0.0.1:7443", true)
	p.Round(ctx) // 恢复中失败：成功计数清零
	fd.set("10.0.0.1:7443", false)
	p.Round(ctx)
	if p.PeerHealthy(m1) {
		t.Fatal("success count not reset by a failure")
	}
	p.Round(ctx)
	if !p.PeerHealthy(m1) {
		t.Fatal("not recovered after 2 consecutive successes")
	}
	// 移除的对象丢弃；保留的对象保持状态
	fd.set("1.1.1.1:80", true)
	for i := 0; i < 3; i++ {
		p.Round(ctx)
	}
	c := cfg()
	c.Rules = c.Rules[1:]
	p.SetSubjects(Subjects(c))
	if len(p.Results()) != 1 || p.TargetHealthy("1.1.1.1:80") {
		t.Fatalf("after SetSubjects %+v", p.Results())
	}
}

// 真实 TCP：监听中的端口可达并测得 RTT，关闭的端口不可达。
func TestTCPDial(t *testing.T) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	open := netip.MustParseAddrPort(ln.Addr().String())
	cl, _ := net.Listen("tcp4", "127.0.0.1:0")
	closed := netip.MustParseAddrPort(cl.Addr().String())
	cl.Close()
	p := New()
	p.Timeout = time.Second
	p.SetSubjects(Subjects(&types.RuntimeConfig{Rules: []types.Rule{{Targets: []types.Target{
		{Host: "127.0.0.1", Port: open.Port()}, {Host: "127.0.0.1", Port: closed.Port()}}}}}))
	var r []Result
	for i := 0; i < FailAfter; i++ {
		r = p.Round(context.Background())
	}
	var okOpen, okClosed bool
	for _, x := range r {
		switch x.Target {
		case open.String():
			okOpen = x.Reachable && x.RTT > 0
		case closed.String():
			okClosed = !x.Reachable && x.RTT == 0
		}
	}
	if !okOpen || !okClosed {
		t.Fatalf("results %+v", r)
	}
}
