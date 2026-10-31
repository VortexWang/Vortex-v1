package engine_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"vortex/engine"
	"vortex/engine/enginetest"
	"vortex/engine/types"
	"vortex/engine/vlink"
)

// 隧道端到端（ENG-T24、ENG-T25）：同一进程内多个引擎节点，各自有证书、链路监听与模拟 Agent。

var (
	nodeA = types.UUID{0xa1} // 入口
	nodeB = types.UUID{0xb2} // 中继
	nodeC = types.UUID{0xc3} // 出口
	nodeD = types.UUID{0xd4} // 第二个出口 / 攻击者
	ruleT = types.UUID{0x77}
)

type node struct {
	id   types.UUID
	rt   *engine.Runtime
	ag   *enginetest.Agent
	link netip.AddrPort
	cfg  *types.RuntimeConfig
}

func newNode(t *testing.T, pki *enginetest.PKI, id types.UUID) *node {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rt := engine.New(engine.Options{Tunnel: &engine.TunnelOptions{Identity: pki.Node(id), Roots: pki.Roots,
		BootID: types.UUID{0xb0, id[0]}, LinkListener: ln, DialTimeout: time.Second}})
	n := &node{id: id, rt: rt, ag: enginetest.Start(rt, 20*time.Millisecond), link: netip.MustParseAddrPort(ln.Addr().String())}
	t.Cleanup(func() { n.ag.Stop(); _ = rt.Close() })
	return n
}

func (n *node) peer(outbound bool) types.Peer {
	return types.Peer{NodeID: n.id, Address: n.link.Addr().String(), LinkPort: n.link.Port(), Outbound: outbound}
}

func (n *node) apply(t *testing.T, cfg *types.RuntimeConfig) {
	t.Helper()
	cfg.NodeID = n.id
	if _, _, err := n.ag.Apply(context.Background(), cfg); err != nil {
		t.Fatalf("node %x apply: %v", n.id[0], err)
	}
	n.ag.GrantAll(cfg, enginetest.Grants{TTL: time.Hour})
	n.cfg = cfg
}

func members(ns ...*node) *types.NextHop {
	nh := &types.NextHop{MemberSchedule: types.ScheduleWeightedRoundRobin, Dial: types.DialForward}
	for _, n := range ns {
		nh.Members = append(nh.Members, types.Member{NodeID: n.id, Weight: 1})
	}
	return nh
}

func hop(role types.HopRole, idx uint32, ep bool, upstream ...*node) types.Rule {
	r := types.Rule{RuleID: ruleT, TenantID: tenant, RuleVersion: 3, PathGeneration: 2, Role: role, HopIndex: idx,
		EnforcementPoint: ep, Networks: []types.Network{types.NetworkTCP}, TargetSchedule: types.ScheduleWeightedRoundRobin}
	for _, u := range upstream {
		r.AllowedUpstreams = append(r.AllowedUpstreams, u.id)
	}
	return r
}

type topo struct {
	pki     *enginetest.PKI
	a, b, c *node
	entry   netip.AddrPort
	target  netip.AddrPort
}

// threeHop 建立 入口 A → 中继 B → 出口 C → 目标。
func threeHop(t *testing.T) *topo {
	pki := enginetest.NewPKI()
	a, b, c := newNode(t, pki, nodeA), newNode(t, pki, nodeB), newNode(t, pki, nodeC)
	target := tagServer(t, 'T')
	ra := hop(types.RoleEntry, 0, true)
	ra.NextHop = members(b)
	rb := hop(types.RoleRelay, 1, false, a)
	rb.NextHop = members(c)
	rc := hop(types.RoleExit, 2, false, b)
	rc.Targets = []types.Target{{Host: target.Addr().String(), Port: target.Port(), Weight: 1}}
	l := tcpL(ruleT)
	pol := types.TargetPolicy{SelfHostedNode: true}
	c.apply(t, &types.RuntimeConfig{Version: 1, Generation: 1, Rules: []types.Rule{rc}, Peers: []types.Peer{b.peer(false)}, TargetPolicy: pol})
	b.apply(t, &types.RuntimeConfig{Version: 1, Generation: 1, Rules: []types.Rule{rb}, Peers: []types.Peer{a.peer(false), c.peer(true)}, TargetPolicy: pol})
	a.apply(t, &types.RuntimeConfig{Version: 1, Generation: 1, Listeners: []types.Listener{l}, Rules: []types.Rule{ra}, Peers: []types.Peer{b.peer(true)}, TargetPolicy: pol})
	return &topo{pki: pki, a: a, b: b, c: c, entry: a.rt.ListenerAddr(l), target: target}
}

func waitUsage(n *node, dir types.Direction, want uint64) uint64 {
	deadline := time.Now().Add(3 * time.Second)
	for {
		n.ag.Pull()
		got := n.ag.Usage(ruleT, dir)
		if got >= want || time.Now().After(deadline) {
			return got
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestThreeHopTunnelForwardsAndMetersEveryHop(t *testing.T) {
	tp := threeHop(t)
	c, tag := dial(t, tp.entry)
	if tag != 'T' {
		t.Fatalf("tag %q", tag)
	}
	data := make([]byte, 8<<20)
	_, _ = rand.Read(data)
	back := make([]byte, len(data))
	go func() { _, _ = c.Write(data) }()
	if _, err := io.ReadFull(c, back); err != nil || !bytes.Equal(back, data) {
		t.Fatalf("echo through 3 hops: %v", err)
	}
	c.Close()
	// 每一跳都计量（ENG-035）：入口为执行点（计费授权），中继与出口只计原始字节
	for _, n := range []*node{tp.a, tp.b, tp.c} {
		if up := waitUsage(n, types.DirUp, uint64(len(data))); up != uint64(len(data)) {
			t.Errorf("node %x metered up %d, want %d", n.id[0], up, len(data))
		}
		if down := waitUsage(n, types.DirDown, uint64(len(data))+1); down != uint64(len(data))+1 {
			t.Errorf("node %x metered down %d, want %d", n.id[0], down, len(data)+1)
		}
	}
	// 多个并发会话共用同一条链路
	for i := 0; i < 20; i++ {
		if _, tag := dial(t, tp.entry); tag != 'T' {
			t.Fatal("concurrent session failed")
		}
	}
}

// attacker 以合法证书（同一平台 CA）直接连到 to 的链路监听，返回可任意开流的链路。
func attacker(t *testing.T, tp *topo, id types.UUID, to *node) *vlink.Link {
	t.Helper()
	nc, err := net.Dial("tcp", to.link.String())
	if err != nil {
		t.Fatal(err)
	}
	ident := tp.pki.Node(id)
	l := &vlink.Local{Identity: ident, Trust: &vlink.Trust{Roots: tp.pki.Roots}, BootID: types.UUID{9}, Generation: 1}
	est, err := vlink.Handshake(context.Background(), tls.Client(nc, vlink.ClientConfig(ident, l.Trust, to.id)), l, vlink.Expect{NodeID: to.id})
	if err != nil {
		t.Fatal(err)
	}
	link := vlink.NewLink(est, vlink.LinkOptions{Initiator: true})
	t.Cleanup(func() { link.Close() })
	return link
}

// openErr 开流并返回对端的拒绝原因（0 表示被接受）。
func openErr(t *testing.T, l *vlink.Link, o *vlink.Open) vlink.ErrorCode {
	t.Helper()
	s, err := l.Open(o)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_ = s.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, err = s.Read(make([]byte, 1))
	var se *vlink.StreamError
	if errors.As(err, &se) {
		return se.Code
	}
	return 0
}

func (tp *topo) rejections(n *node, peer types.UUID) []types.Event {
	var out []types.Event
	for _, ev := range n.ag.Events(types.EventOpenRejected) {
		if ev.PeerID == peer {
			out = append(out, ev)
		}
	}
	return out
}

func TestOpenChecksPreventBypass(t *testing.T) {
	tp := threeHop(t)
	client := netip.MustParseAddrPort("198.51.100.1:5000")
	good := func() *vlink.Open {
		return &vlink.Open{RuleID: ruleT, RuleVersion: 3, PathGeneration: 2, HopIndex: 2, Network: types.NetworkTCP,
			Visited: []types.UUID{nodeA, nodeB}, Client: client}
	}
	// 入口 A 跳过中继直接连出口：A 不在出口允许的上游集合中（中继不可绕过）
	bypass := attacker(t, tp, nodeA, tp.c)
	if code := openErr(t, bypass, good()); code != vlink.CodeUnauthorized {
		t.Fatalf("entry bypassing relay: code %v", code)
	}
	// 任意合法节点 D
	d := attacker(t, tp, nodeD, tp.c)
	if code := openErr(t, d, good()); code != vlink.CodeUnauthorized {
		t.Fatalf("foreign node: code %v", code)
	}
	// 冒充中继 B（同一 node_id，合法证书）时逐项检查 ①～④
	b := attacker(t, tp, nodeB, tp.c)
	cases := []struct {
		name string
		mut  func(*vlink.Open)
		code vlink.ErrorCode
	}{
		{"rule not active", func(o *vlink.Open) { o.RuleID = types.UUID{0x99} }, vlink.CodeNoRoute},
		{"stale rule version", func(o *vlink.Open) { o.RuleVersion = 2 }, vlink.CodeUnauthorized},
		{"stale path generation", func(o *vlink.Open) { o.PathGeneration = 1 }, vlink.CodeUnauthorized},
		{"wrong hop index", func(o *vlink.Open) { o.HopIndex = 1 }, vlink.CodeUnauthorized},
		{"loop: exit already visited", func(o *vlink.Open) { o.Visited = []types.UUID{nodeA, nodeC} }, vlink.CodeUnauthorized},
		{"udp not enabled", func(o *vlink.Open) { o.Network = types.NetworkUDP }, vlink.CodeUnauthorized},
	}
	for _, c := range cases {
		o := good()
		c.mut(o)
		if code := openErr(t, b, o); code != c.code {
			t.Errorf("%s: code %v, want %v", c.name, code, c.code)
		}
	}
	// 每次拒绝都有审计事件，含对端与规则（ENG-023）
	deadline := time.Now().Add(2 * time.Second)
	for len(tp.rejections(tp.c, nodeB)) < len(cases) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	evs := tp.rejections(tp.c, nodeB)
	if len(evs) != len(cases) || len(tp.rejections(tp.c, nodeA)) != 1 || len(tp.rejections(tp.c, nodeD)) != 1 {
		t.Fatalf("audit events: B %d A %d D %d", len(evs), len(tp.rejections(tp.c, nodeA)), len(tp.rejections(tp.c, nodeD)))
	}
	for _, ev := range evs {
		if ev.Detail == "" {
			t.Fatalf("event without reason: %+v", ev)
		}
	}
	// 正确的 OPEN 被接受，目标由出口本地配置决定（ENG-021）
	s, _ := b.Open(good())
	_ = s.SetReadDeadline(time.Now().Add(3 * time.Second))
	var tag [1]byte
	if _, err := io.ReadFull(s, tag[:]); err != nil || tag[0] != 'T' {
		t.Fatalf("valid OPEN: %v %q", err, tag)
	}
	s.Close()
}

func TestRelayChecksUpstreamToo(t *testing.T) {
	tp := threeHop(t)
	d := attacker(t, tp, nodeD, tp.b) // D 冒充入口直接连中继
	o := &vlink.Open{RuleID: ruleT, RuleVersion: 3, PathGeneration: 2, HopIndex: 1, Network: types.NetworkTCP,
		Visited: []types.UUID{nodeD}, Client: netip.MustParseAddrPort("198.51.100.1:5000")}
	if code := openErr(t, d, o); code != vlink.CodeUnauthorized {
		t.Fatalf("relay accepted unknown upstream: %v", code)
	}
}

func TestReasonPropagatesToEntry(t *testing.T) {
	tp := threeHop(t)
	// 出口没有运行授权：出口拒绝（no_route），原因经中继回传到入口，入口记事件
	tp.c.rt.UpdateGrants(&types.GrantSet{})
	c, tag := dial(t, tp.entry)
	if tag != 0 {
		t.Fatal("session admitted although exit has no run grant")
	}
	c.Close()
	deadline := time.Now().Add(3 * time.Second)
	for {
		for _, ev := range tp.a.ag.Events(types.EventOpenRejected) {
			if ev.PeerID == nodeB && strings.Contains(ev.Detail, "no_route") {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("entry events: %+v", tp.a.ag.Events(types.EventOpenRejected))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestTargetUnreachableReason(t *testing.T) {
	tp := threeHop(t)
	cfg := *tp.c.cfg
	r := cfg.Rules[0]
	r.Targets = []types.Target{{Host: "127.0.0.1", Port: freePort(t).Port(), Weight: 1}} // 没有服务
	cfg.Rules = []types.Rule{r}
	cfg.Version = 2
	tp.c.apply(t, &cfg)
	c, tag := dial(t, tp.entry)
	if tag != 0 {
		t.Fatal("unexpected data")
	}
	c.Close()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, ev := range tp.a.ag.Events(types.EventOpenRejected) {
			if strings.Contains(ev.Detail, "target_unreachable") {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("entry events: %+v", tp.a.ag.Events(types.EventOpenRejected))
}

func TestMemberFailoverAndNoRoute(t *testing.T) {
	pki := enginetest.NewPKI()
	a, c := newNode(t, pki, nodeA), newNode(t, pki, nodeC)
	target := tagServer(t, 'T')
	rc := hop(types.RoleExit, 1, false, a)
	rc.Targets = []types.Target{{Host: target.Addr().String(), Port: target.Port(), Weight: 1}}
	pol := types.TargetPolicy{SelfHostedNode: true}
	c.apply(t, &types.RuntimeConfig{Version: 1, Rules: []types.Rule{rc}, Peers: []types.Peer{a.peer(false)}, TargetPolicy: pol})
	// 下一组两个成员：D 的地址上没有服务（主），C 正常（备）
	deadPort := freePort(t)
	ra := hop(types.RoleEntry, 0, true)
	ra.NextHop = &types.NextHop{MemberSchedule: types.SchedulePrimaryBackup, Dial: types.DialForward,
		Members: []types.Member{{NodeID: nodeD, Weight: 1, Priority: 0}, {NodeID: nodeC, Weight: 1, Priority: 1}}}
	l := tcpL(ruleT)
	a.apply(t, &types.RuntimeConfig{Version: 1, Listeners: []types.Listener{l}, Rules: []types.Rule{ra}, TargetPolicy: pol,
		Peers: []types.Peer{{NodeID: nodeD, Address: "127.0.0.1", LinkPort: deadPort.Port(), Outbound: true}, c.peer(true)}})
	if _, tag := dial(t, a.rt.ListenerAddr(l)); tag != 'T' {
		t.Fatal("did not fail over to the backup member (ENG-022)")
	}
	// 下一组只剩不可达成员：拒绝新会话，不跳过下一组
	ra.NextHop = &types.NextHop{MemberSchedule: types.ScheduleWeightedRoundRobin, Dial: types.DialForward,
		Members: []types.Member{{NodeID: nodeD, Weight: 1}}}
	a.apply(t, &types.RuntimeConfig{Version: 2, Listeners: []types.Listener{l}, Rules: []types.Rule{ra}, TargetPolicy: pol,
		Peers: []types.Peer{{NodeID: nodeD, Address: "127.0.0.1", LinkPort: deadPort.Port(), Outbound: true}}})
	if _, tag := dial(t, a.rt.ListenerAddr(l)); tag != 0 {
		t.Fatal("session admitted with no reachable member")
	}
}

func TestRemovedMemberClosesSessionsAndRevocationClosesLinks(t *testing.T) {
	tp := threeHop(t)
	c, _ := dial(t, tp.entry)
	// 中继的下一组移除 C：权限收紧，已有会话关闭
	orig := *tp.b.cfg
	cfg := orig
	r := cfg.Rules[0]
	r.NextHop = members(&node{id: nodeD})
	cfg.Rules, cfg.Version = []types.Rule{r}, 2
	tp.b.apply(t, &cfg)
	if !closedSoon(c) {
		t.Fatal("session survived removal of the next-group member")
	}
	restored := orig
	restored.Version = 3
	tp.b.apply(t, &restored)
	if _, tag := dial(t, tp.entry); tag != 'T' {
		t.Fatal("path not restored")
	}
	if tp.a.rt.NumPeerLinks(nodeB) != 1 {
		t.Fatalf("entry has %d links to B", tp.a.rt.NumPeerLinks(nodeB))
	}

	// 入口吊销中继 B 的节点身份：到 B 的链路被关闭
	cfgA := *tp.a.cfg
	cfgA.Version = 2
	cfgA.Revocations = types.RevocationList{NodeIDs: []types.UUID{nodeB}}
	tp.a.apply(t, &cfgA)
	deadline := time.Now().Add(2 * time.Second)
	for tp.a.rt.NumPeerLinks(nodeB) != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := tp.a.rt.NumPeerLinks(nodeB); n != 0 {
		t.Fatalf("link to revoked relay still open (%d)", n)
	}
	if _, tag := dial(t, tp.entry); tag != 0 {
		t.Fatal("entry still reaches a revoked relay")
	}
}

// 中继选择下一跳时排除已经过的节点（ENG-022）：B 的下一组为 [A（主）, C（备）]，A 已经过，必须选 C。
func TestNextHopExcludesVisitedNodes(t *testing.T) {
	tp := threeHop(t)
	cfg := *tp.b.cfg
	r := cfg.Rules[0]
	r.NextHop = &types.NextHop{MemberSchedule: types.SchedulePrimaryBackup, Dial: types.DialForward,
		Members: []types.Member{{NodeID: nodeA, Weight: 1, Priority: 0}, {NodeID: nodeC, Weight: 1, Priority: 1}}}
	cfg.Rules, cfg.Version = []types.Rule{r}, 2
	cfg.Peers = []types.Peer{tp.a.peer(true), tp.c.peer(true)}
	tp.b.apply(t, &cfg)
	for i := 0; i < 5; i++ {
		if _, tag := dial(t, tp.entry); tag != 'T' {
			t.Fatal("relay routed back to an already visited node")
		}
	}
}

// threeHopUDP 与 threeHop 相同，但规则为 UDP，目标为回显服务。
func threeHopUDP(t *testing.T, exitIdle time.Duration, exitPolicy types.TargetPolicy) (*topo, netip.AddrPort) {
	pki := enginetest.NewPKI()
	a, b, c := newNode(t, pki, nodeA), newNode(t, pki, nodeB), newNode(t, pki, nodeC)
	echo := udpEcho(t)
	udp := []types.Network{types.NetworkUDP}
	ra := hop(types.RoleEntry, 0, true)
	ra.Networks, ra.NextHop = udp, members(b)
	rb := hop(types.RoleRelay, 1, false, a)
	rb.Networks, rb.NextHop = udp, members(c)
	rc := hop(types.RoleExit, 2, false, b)
	rc.Networks, rc.UDPIdle = udp, exitIdle
	rc.Targets = []types.Target{{Host: echo.Addr().String(), Port: echo.Port(), Weight: 1}}
	l := types.Listener{Bind: anyLocal, Network: types.NetworkUDP, RuleID: ruleT}
	pol := types.TargetPolicy{SelfHostedNode: true}
	c.apply(t, &types.RuntimeConfig{Version: 1, Rules: []types.Rule{rc}, Peers: []types.Peer{b.peer(false)}, TargetPolicy: exitPolicy})
	b.apply(t, &types.RuntimeConfig{Version: 1, Rules: []types.Rule{rb}, Peers: []types.Peer{a.peer(false), c.peer(true)}, TargetPolicy: pol})
	a.apply(t, &types.RuntimeConfig{Version: 1, Listeners: []types.Listener{l}, Rules: []types.Rule{ra}, Peers: []types.Peer{b.peer(true)}, TargetPolicy: pol})
	return &topo{pki: pki, a: a, b: b, c: c}, a.rt.ListenerAddr(l)
}

func udpRoundTrips(t *testing.T, addr netip.AddrPort, n int) int {
	t.Helper()
	c, err := net.Dial("udp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	buf := make([]byte, 2000)
	ok := 0
	for i := 0; i < n; i++ {
		p := bytes.Repeat([]byte{byte(i)}, 1200)
		_, _ = c.Write(p)
		_ = c.SetReadDeadline(time.Now().Add(time.Second))
		m, err := c.Read(buf)
		if err == nil && bytes.Equal(buf[:m], p) {
			ok++
		}
	}
	return ok
}

func TestUDPThreeHopTunnel(t *testing.T) {
	tp, addr := threeHopUDP(t, 0, types.TargetPolicy{SelfHostedNode: true})
	if ok := udpRoundTrips(t, addr, 200); ok != 200 {
		t.Fatalf("%d of 200 datagrams echoed through 3 hops", ok)
	}
	for _, n := range []*node{tp.a, tp.b, tp.c} { // 每一跳整包计量
		if up := waitUsage(n, types.DirUp, 200*1200); up != 200*1200 {
			t.Errorf("node %x metered up %d", n.id[0], up)
		}
		if down := waitUsage(n, types.DirDown, 200*1200); down != 200*1200 {
			t.Errorf("node %x metered down %d", n.id[0], down)
		}
	}
}

func TestUDPTunnelIdleAndReopen(t *testing.T) {
	_, addr := threeHopUDP(t, 300*time.Millisecond, types.TargetPolicy{SelfHostedNode: true})
	c, _ := net.Dial("udp", addr.String())
	defer c.Close()
	buf := make([]byte, 2000)
	echo := func() bool {
		_, _ = c.Write([]byte("ping"))
		_ = c.SetReadDeadline(time.Now().Add(time.Second))
		n, err := c.Read(buf)
		return err == nil && string(buf[:n]) == "ping"
	}
	if !echo() {
		t.Fatal("first datagram")
	}
	// 出口空闲 300 ms 后回收会话并 RST 回传；入口会话随之结束，下一个数据报重新开流
	time.Sleep(900 * time.Millisecond)
	ok := false
	for i := 0; i < 3 && !ok; i++ {
		ok = echo()
	}
	if !ok {
		t.Fatal("session not re-established after exit idle timeout")
	}
}

func TestUDPTunnelTargetForbiddenReason(t *testing.T) {
	tp, addr := threeHopUDP(t, 0, types.TargetPolicy{}) // 平台出口：回环目标被 SEC-011 拒绝
	if ok := udpRoundTrips(t, addr, 1); ok != 0 {
		t.Fatal("forbidden target reached")
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, ev := range tp.a.ag.Events(types.EventOpenRejected) {
			if strings.Contains(ev.Detail, "target_unreachable") {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("entry events %+v", tp.a.ag.Events(types.EventOpenRejected))
}

func TestUDPOpenChecks(t *testing.T) {
	tp, _ := threeHopUDP(t, 0, types.TargetPolicy{SelfHostedNode: true})
	d := attacker(t, tp, nodeD, tp.c)
	o := &vlink.Open{RuleID: ruleT, RuleVersion: 3, PathGeneration: 2, HopIndex: 2, Network: types.NetworkUDP,
		Visited: []types.UUID{nodeA, nodeB}, Client: netip.MustParseAddrPort("198.51.100.1:5000")}
	if code := openErr(t, d, o); code != vlink.CodeUnauthorized {
		t.Fatalf("udp open from unknown node: %v", code)
	}
	b := attacker(t, tp, nodeB, tp.c)
	tcp := *o
	tcp.Network = types.NetworkTCP // 规则只启用了 UDP
	if code := openErr(t, b, &tcp); code != vlink.CodeUnauthorized {
		t.Fatalf("tcp open on udp-only rule: %v", code)
	}
	s, _ := b.Open(o)
	_ = s.SendDgram([]byte("hi"))
	_ = s.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 100)
	if n, err := s.ReadDgram(buf); err != nil || string(buf[:n]) != "hi" {
		t.Fatalf("valid udp open: %q %v", buf[:n], err)
	}
}
