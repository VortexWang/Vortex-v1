package engine_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"io"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"vortex/engine/enginetest"
	"vortex/engine/types"
	"vortex/engine/vlink"
)

// 多跳（ENG-T31、NFR-006）与执行点（ENG-T32、ENG-049）。

type chain struct {
	pki   *enginetest.PKI
	nodes []*node // 入口、中继……、出口
	entry netip.AddrPort
}

type chainOpt struct {
	relays    int
	ep        int    // 执行点所在的跳位（0 = 入口）
	rateBps   uint64 // 执行点的租户速率份额（0 = 不限）
	noBilling bool   // 执行点不下发计费授权
}

func nodeIDAt(i int) types.UUID { return types.UUID{0x40, byte(i + 1)} }

// newChain 建立 入口 → 中继 × relays → 出口 → 目标（回显，接入时先写标识 'T'）。
func newChain(t *testing.T, o chainOpt) *chain {
	t.Helper()
	pki := enginetest.NewPKI()
	n := o.relays + 2
	ch := &chain{pki: pki}
	for i := 0; i < n; i++ {
		ch.nodes = append(ch.nodes, newNode(t, pki, nodeIDAt(i)))
	}
	target := tagServer(t, 'T')
	l := tcpL(ruleT)
	// 从出口往入口应用，入口最后（与后端的下发顺序一致：下游先就绪）
	for i := n - 1; i >= 0; i-- {
		nd := ch.nodes[i]
		role := types.RoleRelay
		switch i {
		case 0:
			role = types.RoleEntry
		case n - 1:
			role = types.RoleExit
		}
		r := hop(role, uint32(i), i == o.ep)
		var peers []types.Peer
		if i > 0 {
			r.AllowedUpstreams = []types.UUID{ch.nodes[i-1].id}
			peers = append(peers, ch.nodes[i-1].peer(false))
		}
		if i < n-1 {
			r.NextHop = members(ch.nodes[i+1])
			peers = append(peers, ch.nodes[i+1].peer(true))
		} else {
			r.Targets = []types.Target{{Host: target.Addr().String(), Port: target.Port(), Weight: 1}}
		}
		cfg := &types.RuntimeConfig{Version: 1, Generation: 1, Rules: []types.Rule{r}, Peers: peers,
			TargetPolicy: types.TargetPolicy{SelfHostedNode: true}}
		if i == 0 {
			cfg.Listeners = []types.Listener{l}
		}
		nd.cfg = cfg
		cfg.NodeID = nd.id
		if _, _, err := nd.ag.Apply(context.Background(), cfg); err != nil {
			t.Fatalf("node %d apply: %v", i, err)
		}
		set := nd.ag.GrantAll(cfg, enginetest.Grants{TTL: time.Hour, RateBps: o.rateBps})
		if i == o.ep && o.noBilling {
			set.Billing = nil
			nd.rt.UpdateGrants(set)
		}
	}
	ch.entry = ch.nodes[0].rt.ListenerAddr(l)
	// 预热：每一跳到下一跳的链路就绪
	deadline := time.Now().Add(3 * time.Second)
	for i := 0; i < n-1; i++ {
		for ch.nodes[i].rt.PeerStats(ch.nodes[i+1].id).Mux == 0 && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
	}
	return ch
}

func TestThreeRelayPathLargeFileIntegrity(t *testing.T) {
	ch := newChain(t, chainOpt{relays: 3})
	c, tag := dial(t, ch.entry)
	if tag != 'T' {
		t.Fatalf("tag %q through 3 relays", tag)
	}
	data := make([]byte, 32<<20) // NFR-006：大文件校验一致
	_, _ = rand.Read(data)
	h := sha256.New()
	go func() { _, _ = c.Write(data); _ = c.(*net.TCPConn).CloseWrite() }()
	n, err := io.Copy(h, c)
	if err != nil || n != int64(len(data)) {
		t.Fatalf("echo through 5 nodes: %d bytes, %v", n, err)
	}
	if want := sha256.Sum256(data); !bytes.Equal(h.Sum(nil), want[:]) {
		t.Fatal("checksum mismatch through 3 relays")
	}
	for i, nd := range ch.nodes { // 每一跳都计量（入口为执行点）
		if up := waitUsage(nd, types.DirUp, uint64(len(data))); up != uint64(len(data)) {
			t.Errorf("hop %d metered up %d", i, up)
		}
	}
}

// 已经过节点列表上限为入口加 3 个中继（R10）：4 个中继的路径在第 4 个中继处无法继续开流。
func TestFourRelaysExceedVisitedLimit(t *testing.T) {
	ch := newChain(t, chainOpt{relays: 4})
	if _, tag := dial(t, ch.entry); tag != 0 {
		t.Fatal("path with 4 relays must not be established")
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, ev := range ch.nodes[0].ag.Events(types.EventOpenRejected) {
			if strings.Contains(ev.Detail, "no_route") {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("entry events %+v", ch.nodes[0].ag.Events(types.EventOpenRejected))
}

// 多跳顺序不可绕过（NFR-006）：中继 2 的上游只能是中继 1；入口或中继 1 之外的节点直接连中继 2 / 中继 3 都被拒绝，
// 跳位不符（跳过一个中继）被拒绝。
func TestMultiHopOrderCannotBeBypassed(t *testing.T) {
	ch := newChain(t, chainOpt{relays: 3})
	tp := &topo{pki: ch.pki}
	r2, r3 := ch.nodes[2], ch.nodes[3]
	client := netip.MustParseAddrPort("198.51.100.1:5000")
	open := func(hop uint8, visited ...types.UUID) *vlink.Open {
		return &vlink.Open{RuleID: ruleT, RuleVersion: 3, PathGeneration: 2, HopIndex: hop, Network: types.NetworkTCP,
			Visited: visited, Client: client}
	}
	// 中继 1 冒充入口直接连中继 3（跳过中继 2）：中继 1 是中继 3 的已知对端吗？不是 → 链路即被拒
	refused(t, attacker(t, tp, ch.nodes[1].id, r3), r3, ch.nodes[1].id)
	// 中继 2 的已知对端（中继 3，下一组成员）反向开流：不是允许的上游
	l3 := attacker(t, tp, r3.id, r2)
	if code := openErr(t, l3, open(2, ch.nodes[0].id, ch.nodes[1].id)); code != vlink.CodeUnauthorized {
		t.Fatalf("downstream opening upstream: %v", code)
	}
	// 合法上游（中继 1）但跳位不符：试图让中继 2 以为自己是第 3 跳
	l1 := attacker(t, tp, ch.nodes[1].id, r2)
	if code := openErr(t, l1, open(3, ch.nodes[0].id, ch.nodes[1].id)); code != vlink.CodeUnauthorized {
		t.Fatalf("hop index mismatch: %v", code)
	}
	// 合法上游、合法跳位，但已经过列表含中继 2（环路）
	if code := openErr(t, l1, open(2, ch.nodes[0].id, r2.id)); code != vlink.CodeUnauthorized {
		t.Fatalf("loop: %v", code)
	}
	// 正确的 OPEN 经中继 2 → 中继 3 → 出口到达目标（下一跳只取本地配置）
	s, _ := l1.Open(open(2, ch.nodes[0].id, ch.nodes[1].id))
	_ = s.SetReadDeadline(time.Now().Add(3 * time.Second))
	var tag [1]byte
	if _, err := io.ReadFull(s, tag[:]); err != nil || tag[0] != 'T' {
		t.Fatalf("valid open from the real upstream: %v %q", err, tag)
	}
	s.Close()
}

// ENG-049：入口为自托管节点时，执行点是第一个平台节点（中继 1）。计费与份额在中继 1 执行，
// 入口只计原始字节（不持有计费授权）。
func TestEnforcementPointAtFirstPlatformNode(t *testing.T) {
	ch := newChain(t, chainOpt{relays: 1, ep: 1})
	c, tag := dial(t, ch.entry)
	if tag != 'T' {
		t.Fatal("session through self-hosted entry")
	}
	data := make([]byte, 1<<20)
	go func() { _, _ = c.Write(data) }()
	if _, err := io.ReadFull(c, make([]byte, len(data))); err != nil {
		t.Fatal(err)
	}
	c.Close()
	entry, ep := ch.nodes[0], ch.nodes[1]
	if waitUsage(entry, types.DirUp, 1<<20) != 1<<20 || waitUsage(ep, types.DirUp, 1<<20) != 1<<20 {
		t.Fatal("both hops must meter raw bytes")
	}
	// 入口规则不是执行点；计费授权只在执行点被消耗（上行 + 下行）
	if entry.cfg.Rules[0].EnforcementPoint || !ep.cfg.Rules[0].EnforcementPoint {
		t.Fatal("enforcement point must be the first platform node")
	}
	if used := epBillingUsed(ep); used != 2<<20+1 {
		t.Fatalf("enforcement point consumed %d bytes of billing grant, want %d (up + down + tag)", used, 2<<20+1)
	}
	if used := epBillingUsed(entry); used != 0 {
		t.Fatalf("self-hosted entry consumed %d bytes of billing grant", used)
	}
}

func epBillingUsed(n *node) uint64 {
	n.ag.Pull()
	var used uint64
	for _, c := range n.ag.Consumptions() {
		used += c.UsedRawBytes
	}
	return used
}

func TestEnforcementPointWithoutBillingRejectsAndReasonReachesEntry(t *testing.T) {
	ch := newChain(t, chainOpt{relays: 1, ep: 1, noBilling: true})
	if _, tag := dial(t, ch.entry); tag != 0 {
		t.Fatal("session admitted although the enforcement point has no billing grant")
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, ev := range ch.nodes[0].ag.Events(types.EventOpenRejected) {
			if strings.Contains(ev.Detail, "quota_exhausted") {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("entry events %+v", ch.nodes[0].ag.Events(types.EventOpenRejected))
}

// 执行点的速率份额对整条路径生效（ENG-026 在中继执行）：16 Mbps 份额下 4 MiB 用时约 2 秒（突发 1 秒份额）。
func TestEnforcementPointRateLimitsWholePath(t *testing.T) {
	ch := newChain(t, chainOpt{relays: 1, ep: 1, rateBps: 16_000_000})
	c, _ := dial(t, ch.entry)
	data := make([]byte, 4<<20)
	start := time.Now()
	go func() { _, _ = c.Write(data) }()
	if _, err := io.ReadFull(c, make([]byte, len(data))); err != nil {
		t.Fatal(err)
	}
	took := time.Since(start)
	// 上下行共用租户份额：8 MiB 经过执行点，速率 2 MB/s，容量 2 MB → 理论约 (8.39 − 2) / 2 ≈ 3.2 秒
	if took < 2500*time.Millisecond || took > 5*time.Second {
		t.Fatalf("4 MiB echo took %v under a 16 Mbps share at the enforcement point", took)
	}
}
