package tunnel

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"net/netip"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"vortex/engine/internal/lifecycle"
	"vortex/engine/types"
	"vortex/engine/vlink"
)

// 本包测试不经过引擎运行时：两个 Peers 实例在回环上互连。

type testPKI struct {
	roots *x509.CertPool
	ca    *x509.Certificate
	key   *ecdsa.PrivateKey
	sn    int64
}

func newTestPKI(t *testing.T) *testPKI {
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ca"}, IsCA: true, BasicConstraintsValid: true,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &k.PublicKey, k)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(der)
	p := &testPKI{roots: x509.NewCertPool(), ca: ca, key: k, sn: 10}
	p.roots.AddCert(ca)
	return p
}

func (p *testPKI) node(id types.UUID) *vlink.Identity {
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	u, _ := url.Parse(vlink.NodeURI(id))
	p.sn++
	tpl := &x509.Certificate{SerialNumber: big.NewInt(p.sn), URIs: []*url.URL{u}, KeyUsage: x509.KeyUsageDigitalSignature,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, _ := x509.CreateCertificate(rand.Reader, tpl, p.ca, &k.PublicKey, p.key)
	return &vlink.Identity{NodeID: id, Cert: tls.Certificate{Certificate: [][]byte{der}, PrivateKey: k}}
}

var (
	upID   = types.UUID{0x01}
	downID = types.UUID{0x02}
)

type pair struct {
	up, down *Peers
	addr     netip.AddrPort
	held     chan *vlink.Stream // 下游收到的流（不读取，直到测试结束）
}

// wrap 非 nil 时改写上游连接下游所用的地址（例如插入可断开的代理）。
func newPair(t *testing.T, mode types.LinkMode, opt vlink.LinkOptions, tune func(up, down *Peers), wrap ...func(netip.AddrPort) netip.AddrPort) *pair {
	t.Helper()
	pki := newTestPKI(t)
	tr := &vlink.Trust{Roots: pki.roots}
	local := func(id *vlink.Identity) func() *vlink.Local {
		return func() *vlink.Local {
			return &vlink.Local{Identity: id, Trust: tr, BootID: types.UUID{0xb0, id.NodeID[0]}, Capabilities: vlink.CapDgram}
		}
	}
	pr := &pair{held: make(chan *vlink.Stream, 1024)}
	upI, downI := pki.node(upID), pki.node(downID)
	pr.down = &Peers{Local: local(downI), LinkOptions: opt, Tick: 50 * time.Millisecond,
		Handle: func(ctx context.Context, _ *lifecycle.Scope, _ *vlink.Link, s *vlink.Stream) {
			s.Accept()
			pr.held <- s
			<-ctx.Done()
		}}
	pr.up = &Peers{Local: local(upI), LinkOptions: opt, Tick: 50 * time.Millisecond,
		Handle: func(context.Context, *lifecycle.Scope, *vlink.Link, *vlink.Stream) {}}
	if tune != nil {
		tune(pr.up, pr.down)
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	pr.addr = netip.MustParseAddrPort(ln.Addr().String())
	rootD, rootU := lifecycle.NewRoot(context.Background()), lifecycle.NewRoot(context.Background())
	if err := pr.down.Start(rootD, ln); err != nil {
		t.Fatal(err)
	}
	if err := pr.up.Start(rootU, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rootU.Close(); _ = rootD.Close() })
	pr.down.SetPeers([]types.Peer{{NodeID: upID}})
	dst := pr.addr
	for _, w := range wrap {
		dst = w(dst)
	}
	pr.up.SetPeers([]types.Peer{{NodeID: downID, Address: "127.0.0.1", LinkPort: dst.Port(), Outbound: true, LinkMode: mode}})
	return pr
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func topen(n types.Network) *vlink.Open {
	return &vlink.Open{RuleID: types.UUID{7}, Network: n, Client: netip.MustParseAddrPort("192.0.2.1:1")}
}

func TestMuxWarmupBeforeAnySession(t *testing.T) {
	pr := newPair(t, types.LinkModeMux, vlink.LinkOptions{}, nil)
	eventually(t, "2 pre-warmed mux links", func() bool { return pr.up.Stats(downID).Mux == 2 })
	time.Sleep(200 * time.Millisecond)
	if st := pr.up.Stats(downID); st.Mux != 2 || st.PoolIdle != 0 {
		t.Fatalf("stats %+v", st)
	}
	// 会话不触发建链
	s, err := pr.up.OpenStream(context.Background(), downID, topen(types.NetworkTCP), false)
	if err != nil || s.Link().Mode() != vlink.ModeMux {
		t.Fatal(err)
	}
	if n := pr.up.NumLinks(downID); n != 2 {
		t.Fatalf("session caused a dial: %d links", n)
	}
}

func TestMuxBalanceGrowAndShrink(t *testing.T) {
	// 链路窗口设为 1 MiB（两端一致），不读取的流很快把链路的在途字节推过一半
	opt := vlink.LinkOptions{LinkWindow: 1 << 20}
	pr := newPair(t, types.LinkModeMux, opt, func(p, _ *Peers) { p.IdleClose = 300 * time.Millisecond })
	eventually(t, "warm-up", func() bool { return pr.up.Stats(downID).Mux == 2 })
	var streams []*vlink.Stream
	links := map[*vlink.Link]int{}
	for i := 0; i < 30; i++ {
		s, err := pr.up.OpenStream(context.Background(), downID, topen(types.NetworkTCP), false)
		if err != nil {
			t.Fatal(err)
		}
		_ = s.SetWriteDeadline(time.Now().Add(2 * time.Second))
		_, _ = s.Write(make([]byte, 100<<10))
		links[s.Link()]++
		streams = append(streams, s)
		time.Sleep(20 * time.Millisecond)
		if m := pr.up.Stats(downID).Mux; m > DefaultMaxMux {
			t.Fatalf("%d mux links, above the maximum", m)
		}
	}
	if pr.up.Stats(downID).Mux <= 2 {
		t.Fatal("mux links did not grow when all were busy")
	}
	if len(links) < 3 {
		t.Fatalf("streams not spread across links: %v", links)
	}
	for _, s := range streams {
		s.Reset(vlink.CodeNoRoute, "")
	}
	eventually(t, "shrink back to 2 idle links", func() bool { return pr.up.Stats(downID).Mux == 2 })
}

func TestPoolReuseAndOneStreamPerLink(t *testing.T) {
	pr := newPair(t, types.LinkModePool, vlink.LinkOptions{}, nil)
	eventually(t, "pool warm-up", func() bool { return pr.up.Stats(downID).PoolIdle >= 2 })
	s1, _ := pr.up.OpenStream(context.Background(), downID, topen(types.NetworkTCP), false)
	s2, _ := pr.up.OpenStream(context.Background(), downID, topen(types.NetworkTCP), false)
	if s1.Link().Mode() != vlink.ModePool || s2.Link().Mode() != vlink.ModePool || s1.Link() == s2.Link() {
		t.Fatal("pool streams must each get their own pool link")
	}
	if st := pr.up.Stats(downID); st.PoolBusy != 2 {
		t.Fatalf("stats %+v", st)
	}
	// 接受方从 HELLO 看到的是 pool 链路
	in := <-pr.held
	if in.Link().Mode() != vlink.ModePool {
		t.Fatal("acceptor does not see pool mode")
	}
	l1 := s1.Link()
	s1.Reset(vlink.CodeNoRoute, "")
	eventually(t, "link returned to pool", func() bool { return pr.up.Stats(downID).PoolBusy == 1 })
	reused := false
	for i := 0; i < 4 && !reused; i++ { // 池中的空闲链路之一就是刚归还的那条
		s, _ := pr.up.OpenStream(context.Background(), downID, topen(types.NetworkTCP), false)
		reused = s.Link() == l1
	}
	if !reused {
		t.Fatal("returned pool link not reused")
	}
	// UDP 固定使用 mux（ENG-017）
	u, _ := pr.up.OpenStream(context.Background(), downID, topen(types.NetworkUDP), false)
	if u.Link().Mode() != vlink.ModeMux {
		t.Fatal("udp session on a pool link")
	}
}

func TestPoolExhaustedFallsBackToMuxAndGrows(t *testing.T) {
	pr := newPair(t, types.LinkModePool, vlink.LinkOptions{}, nil)
	eventually(t, "pool warm-up", func() bool { st := pr.up.Stats(downID); return st.PoolIdle == 2 && st.Mux == 2 })
	var ss []*vlink.Stream
	for i := 0; i < 6; i++ {
		start := time.Now()
		s, err := pr.up.OpenStream(context.Background(), downID, topen(types.NetworkTCP), false)
		if err != nil {
			t.Fatal(err)
		}
		if time.Since(start) > 50*time.Millisecond {
			t.Fatalf("session %d waited %v for a link", i, time.Since(start))
		}
		ss = append(ss, s)
	}
	fallback := 0
	for _, s := range ss {
		if s.Link().Mode() == vlink.ModeMux {
			fallback++
		}
	}
	if fallback == 0 {
		t.Fatal("pool exhaustion did not fall back to mux")
	}
	// 并发峰值 6：空闲目标 = max(2, 6 × 0.5) = 3
	eventually(t, "pool grows to the new target", func() bool { return pr.up.Stats(downID).PoolIdle == 3 })
	for _, s := range ss {
		s.Reset(vlink.CodeNoRoute, "")
	}
	eventually(t, "excess idle pool links closed", func() bool {
		st := pr.up.Stats(downID)
		return st.PoolBusy == 0 && st.PoolIdle == 3
	})
	if !pr.up.Healthy(downID) { // 本端主动关闭多余的池链路不得把对端标记为不可用
		t.Fatal("closing excess pool links marked the peer down")
	}
}

func TestDurationsMedianWindow(t *testing.T) {
	now := time.Unix(1000, 0)
	ds := durations{now: func() time.Time { return now }}
	r := types.UUID{1}
	if _, ok := ds.median(r); ok || ds.short(r) {
		t.Fatal("no history must mean mux")
	}
	for _, d := range []time.Duration{1, 2, 3, 30, 40} {
		ds.record(r, d*time.Second)
	}
	if m, _ := ds.median(r); m != 3*time.Second || !ds.short(r) {
		t.Fatalf("median %v", m)
	}
	for i := 0; i < 4; i++ {
		ds.record(r, time.Minute)
	}
	now = now.Add(2 * time.Second)                                // 越过 1 秒缓存
	if m, _ := ds.median(r); m != 40*time.Second || ds.short(r) { // 1,2,3,30,40,60,60,60,60 的中位数
		t.Fatalf("median %v", m)
	}
	now = now.Add(11 * time.Minute) // 全部超出 10 分钟窗口
	if _, ok := ds.median(r); ok {
		t.Fatal("old samples must expire")
	}
	ds.record(r, time.Second)
	ds.record(r, 3*time.Second)
	if m, _ := ds.median(r); m != 2*time.Second {
		t.Fatalf("even count median %v", m)
	}
}

func TestUsePool(t *testing.T) {
	cases := []struct {
		mode  types.LinkMode
		net   types.Network
		short bool
		want  bool
	}{
		{types.LinkModeAuto, types.NetworkTCP, true, true},
		{types.LinkModeAuto, types.NetworkTCP, false, false},
		{types.LinkModeUnspecified, types.NetworkTCP, true, true},
		{types.LinkModePool, types.NetworkTCP, false, true},
		{types.LinkModePool, types.NetworkUDP, true, false},
		{types.LinkModeMux, types.NetworkTCP, true, false},
		{types.LinkModeAuto, types.NetworkUDP, true, false},
	}
	for _, c := range cases {
		if got := usePool(c.mode, c.net, c.short); got != c.want {
			t.Errorf("%+v: %v", c, got)
		}
	}
}

// 反向建链方连不上上游时按指数退避重试（ENG-018）。
func TestReverseDialBackoffGrows(t *testing.T) {
	pki := newTestPKI(t)
	tr := &vlink.Trust{Roots: pki.roots}
	id := pki.node(upID)
	var mu sync.Mutex
	var at []time.Time
	p := &Peers{Local: func() *vlink.Local { return &vlink.Local{Identity: id, Trust: tr} }, Tick: 20 * time.Millisecond,
		MinMux: 1, ReverseBackoff: 100 * time.Millisecond, ReverseBackoffMax: 2 * time.Second,
		Handle: func(context.Context, *lifecycle.Scope, *vlink.Link, *vlink.Stream) {},
		Emit: func(ev types.Event) {
			mu.Lock()
			at = append(at, time.Now())
			mu.Unlock()
		}}
	root := lifecycle.NewRoot(context.Background())
	defer root.Close()
	if err := p.Start(root, nil); err != nil {
		t.Fatal(err)
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	port := netip.MustParseAddrPort(ln.Addr().String()).Port()
	ln.Close() // 没有服务的端口
	p.SetPeers([]types.Peer{{NodeID: downID, Address: "127.0.0.1", LinkPort: port, Outbound: true, ServedRuleIDs: []types.UUID{{7}}}})
	time.Sleep(2500 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	// 0、0.1、0.3、0.7、1.5 秒附近各一次（±20%）：约 5 次；固定周期重试会有上百次
	if len(at) < 4 || len(at) > 7 {
		t.Fatalf("%d attempts in 2.5 s", len(at))
	}
	for i := 2; i < len(at); i++ {
		prev, cur := at[i-1].Sub(at[i-2]), at[i].Sub(at[i-1])
		if cur < prev*13/10 {
			t.Fatalf("interval %d did not grow: %v then %v", i, prev, cur)
		}
	}
}

// 握手成功但对端随即关闭链路（对端尚未把本节点加入配置）时同样退避，而不是每个维护周期重连。
func TestReverseDialBacksOffWhenPeerRefusesLink(t *testing.T) {
	var mu sync.Mutex
	refused := 0
	pr := newPair(t, types.LinkModeMux, vlink.LinkOptions{}, func(p, down *Peers) {
		p.MinMux, p.ReverseBackoff, p.ReverseBackoffMax = 1, 100*time.Millisecond, 2*time.Second
		down.Emit = func(ev types.Event) {
			mu.Lock()
			refused++
			mu.Unlock()
		}
	})
	pr.down.SetPeers(nil) // 下游不认识上游：接受的链路立即关闭
	pr.up.SetPeers([]types.Peer{{NodeID: downID, Address: "127.0.0.1", LinkPort: pr.addr.Port(), Outbound: true, ServedRuleIDs: []types.UUID{{7}}}})
	time.Sleep(2500 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if refused == 0 || refused > 10 { // 维护周期 50 ms：不退避时约 50 次
		t.Fatalf("%d refused link attempts in 2.5 s", refused)
	}
}

// holeProxy 转发 TCP；hole 置位后停止转发但不关闭连接（模拟对端失联：链路两端都收不到任何帧）。
type holeProxy struct {
	addr netip.AddrPort
	hole atomic.Bool
}

func newHoleProxy(t *testing.T, to netip.AddrPort) *holeProxy {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	hp := &holeProxy{addr: netip.MustParseAddrPort(ln.Addr().String())}
	t.Cleanup(func() { ln.Close() })
	pump := func(dst, src net.Conn) {
		buf := make([]byte, 32<<10)
		for {
			n, err := src.Read(buf)
			if err != nil {
				dst.Close()
				return
			}
			for hp.hole.Load() {
				time.Sleep(10 * time.Millisecond)
			}
			if _, err := dst.Write(buf[:n]); err != nil {
				src.Close()
				return
			}
		}
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			u, err := net.Dial("tcp", to.String())
			if err != nil {
				c.Close()
				continue
			}
			t.Cleanup(func() { c.Close(); u.Close() })
			go pump(u, c)
			go pump(c, u)
		}
	}()
	return hp
}

func TestDeadLinkMarksPeerDownAndRecovers(t *testing.T) {
	ka := vlink.LinkOptions{KeepaliveIdle: 100 * time.Millisecond, PingWait: 100 * time.Millisecond, DeadAfter: 400 * time.Millisecond}
	var hp *holeProxy
	pr := newPair(t, types.LinkModeMux, ka, func(up, _ *Peers) { up.DownBackoff, up.StableAfter = 500*time.Millisecond, 50*time.Millisecond },
		func(a netip.AddrPort) netip.AddrPort { hp = newHoleProxy(t, a); return hp.addr })
	eventually(t, "links through the proxy", func() bool { return pr.up.Stats(downID).Mux == 2 })
	time.Sleep(200 * time.Millisecond) // 保活正常时链路保持
	if pr.up.Stats(downID).Mux != 2 || !pr.up.Healthy(downID) {
		t.Fatal("links dropped while the path is healthy")
	}
	hp.hole.Store(true)
	start := time.Now()
	eventually(t, "peer marked down", func() bool { return !pr.up.Healthy(downID) })
	if took := time.Since(start); took > 900*time.Millisecond {
		t.Fatalf("failure detected after %v", took)
	}
	// 选路以 Healthy 为准（不再选它）；全部链路随后也被判定失效，此时开流失败
	eventually(t, "all links declared dead", func() bool { return pr.up.Stats(downID).Mux == 0 })
	if _, err := pr.up.OpenStream(context.Background(), downID, topen(types.NetworkTCP), false); err == nil {
		t.Fatal("opened a stream to a peer marked down")
	}
	hp.hole.Store(false)
	eventually(t, "links re-established after backoff", func() bool {
		return pr.up.Healthy(downID) && pr.up.Stats(downID).Mux >= 1
	})
}

func TestDrainingPeerNotSelectedAndNotRedialed(t *testing.T) {
	pr := newPair(t, types.LinkModeMux, vlink.LinkOptions{}, nil)
	eventually(t, "warm-up", func() bool { return pr.up.Stats(downID).Mux == 2 })
	old, _ := pr.up.OpenStream(context.Background(), downID, topen(types.NetworkTCP), false)
	in := <-pr.held
	pr.down.DrainAll(vlink.GoawayDraining)
	eventually(t, "peer seen as draining", func() bool { return !pr.up.Healthy(downID) })
	if _, err := pr.up.OpenStream(context.Background(), downID, topen(types.NetworkTCP), false); err == nil {
		t.Fatal("new stream to a draining peer")
	}
	time.Sleep(200 * time.Millisecond) // 若误判为缺少链路会重新建链
	if n := pr.up.NumLinks(downID); n != 2 {
		t.Fatalf("%d links: redialed a draining peer", n)
	}
	// 已有流继续
	_, _ = old.Write([]byte("x"))
	_ = in.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := in.Read(make([]byte, 1)); err != nil {
		t.Fatalf("existing stream on draining peer: %v", err)
	}
}

// AGT-007：Agent 的探测结果判定对端不可用时不再选它；恢复后重新可选。
func TestProbeResultMarksPeerDown(t *testing.T) {
	var down atomic.Bool
	pr := newPair(t, types.LinkModeMux, vlink.LinkOptions{}, func(up, _ *Peers) {
		up.Probe = func(types.UUID) bool { return !down.Load() }
	})
	eventually(t, "links", func() bool { return pr.up.Stats(downID).Mux >= 1 })
	if !pr.up.Healthy(downID) {
		t.Fatal("healthy peer reported down")
	}
	down.Store(true)
	if pr.up.Healthy(downID) {
		t.Fatal("probe result ignored")
	}
	down.Store(false)
	if !pr.up.Healthy(downID) {
		t.Fatal("not recovered")
	}
}
