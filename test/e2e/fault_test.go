package e2e

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"vortex/edgehub/clock"
	"vortex/edgehub/model"
	"vortex/engine/types"
	"vortex/node/agent/control"
	"vortex/node/agent/probe"
	"vortex/node/agent/reconcile"
	"vortex/node/agent/store"
)

// 故障注入（HUB-T18、AGT-T18；NFR-016～018、NFR-022、NFR-026、GEN-006）。授权有效期与续期周期缩短到秒级，
// Edge Hub 用可注入时钟把 12 小时自治压缩为一次时钟推进；结论按比例对应到默认参数（见 docs/m3-progress.md）。

func newLogs() logs { return logs{&store.Mem{}, &store.Mem{}, &store.Mem{}} }

func t0() *clock.Fake { return clock.NewFake(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)) }

// tagTarget 是测试目标：接入后先写一个标识字节，然后回显。
func tagTarget(t *testing.T, tag byte) (netip.AddrPort, net.Listener) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { defer c.Close(); _, _ = c.Write([]byte{tag}); _, _ = io.Copy(c, c) }()
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return netip.MustParseAddrPort(ln.Addr().String()), ln
}

// transfer 经入口发送 size 字节并收回，返回收回的字节数与标识字节；deadline 内收不齐即返回已收数。
func transfer(addr netip.AddrPort, size int, deadline time.Duration) (int, byte) {
	c, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		return 0, 0
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(deadline))
	tag := make([]byte, 1)
	if _, err := io.ReadFull(c, tag); err != nil {
		return 0, 0
	}
	go func() { _, _ = c.Write(make([]byte, size)) }()
	n, _ := io.ReadFull(c, make([]byte, size))
	return n, tag[0]
}

func ok(addr netip.AddrPort) bool { n, _ := transfer(addr, 1024, 2*time.Second); return n == 1024 }

// stopsWithin 持续发起新会话，返回首次失败距 start 的时间（max 内一直成功则返回 -1）。
func stopsWithin(addr netip.AddrPort, start time.Time, max time.Duration) time.Duration {
	for time.Since(start) < max {
		if !ok(addr) {
			return time.Since(start)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return -1
}

func online(t *testing.T, n *nodeProc) {
	t.Helper()
	eventually(t, "applied", func() bool { return n.a.Status().Phase == reconcile.PhaseApplied })
	eventually(t, "run grant", func() bool { return n.a.Status().RunRemaining[ruleA] > 0 })
}

// NFR-018、GEN-006 ②：节点与全部 Edge Hub 失联后继续转发，最迟在运行授权到期时停止；连接期间续期使转发
// 不受有效期限制。
func TestAllHubsLostStopsAtRunGrantExpiry(t *testing.T) {
	const ttl = 4 * time.Second
	cfg := config(t)
	addr := cfg.Listeners[0].Bind
	a := newHubOpt(t, hubA, t0(), cfg, hubOpts{runTTL: ttl})
	d := Dialer{Node: nodeID, Hubs: map[types.UUID]*Hub{hubA: a}}
	n := startNodeOpt(t, newLogs(), d, []control.Endpoint{{ID: hubA}}, nodeOpts{renew: 300 * time.Millisecond})
	defer n.stop()
	online(t, n)
	// 连接期间：运行超过 2 个有效期仍在转发（续期生效）
	if s := stopsWithin(addr, time.Now(), 2*ttl); s >= 0 {
		t.Fatalf("forwarding stopped after %v while connected", s)
	}
	a.Kill()
	killed := time.Now()
	s := stopsWithin(addr, killed, 3*ttl)
	if s < 0 || s > ttl+time.Second {
		t.Fatalf("stopped %v after losing every hub, want ≤ %v (run grant TTL)", s, ttl)
	}
	if s < 200*time.Millisecond {
		t.Fatalf("stopped immediately (%v): the agent must not stop forwarding on disconnect (AGT-011)", s)
	}
	t.Logf("NFR-018: forwarding stopped %v after losing all hubs (TTL %v, engine margin TTL/2)", s.Round(time.Millisecond), ttl)
}

// GEN-006：计费授权耗尽早于运行授权到期时，以先到者为准——失联后执行点只能用完已持有的计费授权。
func TestBillingExhaustionBeforeRunExpiry(t *testing.T) {
	cfg := config(t)
	addr := cfg.Listeners[0].Bind
	a := newHub(t, hubA, t0(), cfg)
	d := Dialer{Node: nodeID, Hubs: map[types.UUID]*Hub{hubA: a}}
	n := startNode(t, newLogs(), d, []control.Endpoint{{ID: hubA}})
	defer n.stop()
	online(t, n)
	if !ok(addr) { // 首个会话取得 8 MiB 计费授权（速率低，取起步额度）
		t.Fatal("no forwarding")
	}
	a.Kill()
	got, _ := transfer(addr, 32<<20, 3*time.Second)
	// 执行点按双向计量（ENG-039）：回显 got 字节即上下行共 2 × got 原始字节
	if got >= 32<<20 || 2*got > 9<<20 {
		t.Fatalf("transferred %d bytes after losing every hub with an 8 MiB billing grant", got)
	}
	if n.a.Status().RunRemaining[ruleA] <= 0 {
		t.Fatal("run grant expired: the stop must come from billing exhaustion")
	}
	t.Logf("GEN-006: echoed %.1f MiB (%.1f MiB both directions) of 32 MiB before the 8 MiB billing grant ran out; run grant still valid",
		float64(got)/(1<<20), float64(2*got)/(1<<20))
}

// NFR-022：后端停止 → 自治：既有规则持续转发（自治中续期）、期间配置变更不下发、自治中重启的节点
// 60 秒内恢复转发、自治满 12 小时后停止续期，节点在运行授权到期时停止。
func TestBackendDown12h(t *testing.T) {
	const ttl = 3 * time.Second
	cfg := config(t)
	addr := cfg.Listeners[0].Bind
	clk := t0()
	a := newHubOpt(t, hubA, clk, cfg, hubOpts{runTTL: ttl})
	d := Dialer{Node: nodeID, Hubs: map[types.UUID]*Hub{hubA: a}}
	l := newLogs()
	n := startNodeOpt(t, l, d, []control.Endpoint{{ID: hubA}}, nodeOpts{renew: 300 * time.Millisecond})
	online(t, n)

	// 后端停止：30 秒后自治；不断流
	var stalls atomic.Int32
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // 进入自治前后持续发起会话
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if !ok(addr) {
				stalls.Add(1)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	a.Link.Disconnected()
	clk.Advance(31 * time.Second)
	if err := a.Tick(); err != nil {
		t.Fatal(err)
	}
	if a.Mode.Mode() != model.ModeAutonomous {
		t.Fatal("not autonomous")
	}
	time.Sleep(3 * ttl) // 自治中续期
	close(stop)
	wg.Wait()
	if s := stalls.Load(); s != 0 {
		t.Fatalf("%d failed sessions around entering autonomy", s)
	}

	// 期间的配置变更不下发（HUB-007）
	v2 := *cfg
	v2.Version = 2
	raw, _ := jsonMarshal(&v2)
	a.SetDesired(toModel(nodeID), 2, raw)
	time.Sleep(200 * time.Millisecond)
	if n.a.Status().AppliedVersion != 1 {
		t.Fatal("config change delivered during autonomy")
	}

	// 自治中重启：60 秒内恢复转发（LIM-010 由 Edge Hub 转移）
	n.stop()
	restart := time.Now()
	n = startNodeOpt(t, l, d, []control.Endpoint{{ID: hubA}}, nodeOpts{renew: 300 * time.Millisecond})
	defer n.stop()
	eventually(t, "recovered during autonomy", func() bool { return ok(addr) })
	took := time.Since(restart)
	if took > 60*time.Second {
		t.Fatalf("restart during autonomy took %v", took)
	}
	if n.a.Status().AppliedVersion != 1 {
		t.Fatal("queued config applied during autonomy")
	}

	// 自治满 12 小时：停止续期，运行授权到期时停止
	clk.Advance(12 * time.Hour)
	if err := a.Tick(); err != nil {
		t.Fatal(err)
	}
	expired := time.Now()
	s := stopsWithin(addr, expired, 4*ttl)
	if s < 0 || s > ttl+time.Second {
		t.Fatalf("still forwarding %v after autonomy expired (TTL %v)", s, ttl)
	}
	t.Logf("NFR-022: no stall entering autonomy; restart during autonomy recovered in %v; stopped %v after the 12 h limit",
		took.Round(time.Millisecond), s.Round(time.Millisecond))
}

// NFR-022：自治期间任一租户的消费不超过其区域额度。
func TestAutonomyQuotaNotExceeded(t *testing.T) {
	const delegated = 24 << 20 // 计费字节；路径系数 1.5 → 最多 16 MiB 原始字节
	cfg := config(t)
	addr := cfg.Listeners[0].Bind
	clk := t0()
	a := newHubOpt(t, hubA, clk, cfg, hubOpts{delegated: delegated})
	d := Dialer{Node: nodeID, Hubs: map[types.UUID]*Hub{hubA: a}}
	n := startNode(t, newLogs(), d, []control.Endpoint{{ID: hubA}})
	defer n.stop()
	online(t, n)
	a.Link.Disconnected()
	clk.Advance(31 * time.Second)
	_ = a.Tick()
	total := 0
	for i := 0; i < 12; i++ {
		got, _ := transfer(addr, 4<<20, 3*time.Second)
		total += got
		if got < 4<<20 {
			break
		}
	}
	v := a.Quota.View(toModel(tenant))
	if v.Issued+v.SettledLocal > delegated {
		t.Fatalf("regional quota exceeded: %+v", v)
	}
	if max := delegated * 1000 / 1500; 2*total > max+(1<<20) { // 双向计量
		t.Fatalf("forwarded %d raw bytes both directions, quota allows %d", 2*total, max)
	}
	t.Logf("NFR-022: forwarded %.1f MiB (both directions) of the %.1f MiB raw allowance during autonomy; issued+settled %d ≤ delegated %d",
		float64(2*total)/(1<<20), float64(delegated*1000/1500)/(1<<20), v.Issued+v.SettledLocal, delegated)
}

// NFR-026：实例在线时宕机——Agent 切到另一实例，业务不中断（已持有的授权继续有效，新实例签发新授权），不超发。
func TestHubCrashWhileOnline(t *testing.T) {
	cfg := config(t)
	addr := cfg.Listeners[0].Bind
	a, b := newHub(t, hubA, t0(), cfg), newHub(t, hubB, t0(), cfg)
	d := Dialer{Node: nodeID, Hubs: map[types.UUID]*Hub{hubA: a, hubB: b}}
	n := startNode(t, newLogs(), d, []control.Endpoint{{ID: hubA}, {ID: hubB}})
	defer n.stop()
	online(t, n)
	var fails atomic.Int32
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if !ok(addr) {
				fails.Add(1)
			}
		}
	}()
	time.Sleep(200 * time.Millisecond)
	a.Kill()
	crash := time.Now()
	eventually(t, "switched to B", func() bool { cur, _ := n.c.Current(); return cur == hubB })
	took := time.Since(crash)
	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()
	if f := fails.Load(); f != 0 {
		t.Fatalf("%d failed sessions across the hub crash", f)
	}
	if took > 60*time.Second {
		t.Fatalf("switch took %v", took)
	}
	for _, h := range []*Hub{a, b} {
		if v := h.Quota.View(toModel(tenant)); v.Issued+v.SettledLocal > 1<<30 {
			t.Fatalf("hub over-issued %+v", v)
		}
	}
	t.Logf("NFR-026: switched to the other hub %v after the crash, no failed session", took.Round(time.Millisecond))
}

// NFR-016、NFR-017、AGT-007：目标故障由探测在 3 轮内判定（默认 3 秒一轮即 9 秒 ≤ 15 秒），之后新会话
// 只走健康目标；判定之前新会话也能成功（连接失败时换下一个目标）。
func TestTargetFailureDetection(t *testing.T) {
	const interval = 200 * time.Millisecond
	addrA, lnA := tagTarget(t, 'A')
	addrB, _ := tagTarget(t, 'B')
	cfg := config(t)
	cfg.Rules[0].Targets = []types.Target{{Host: addrA.Addr().String(), Port: addrA.Port(), Weight: 1},
		{Host: addrB.Addr().String(), Port: addrB.Port(), Weight: 1}}
	addr := cfg.Listeners[0].Bind
	a := newHub(t, hubA, t0(), cfg)
	d := Dialer{Node: nodeID, Hubs: map[types.UUID]*Hub{hubA: a}}
	pr := probe.New()
	pr.Timeout = interval
	n := startNodeOpt(t, newLogs(), d, []control.Endpoint{{ID: hubA}}, nodeOpts{healthy: pr.TargetHealthy,
		applied: func(c *types.RuntimeConfig) { pr.SetSubjects(probe.Subjects(c)) }})
	defer n.stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pr.Run(ctx, interval, n.a.ReportProbes)
	online(t, n)
	seen := map[byte]bool{}
	for i := 0; i < 20; i++ {
		_, tag := transfer(addr, 16, time.Second)
		seen[tag] = true
	}
	if !seen['A'] || !seen['B'] {
		t.Fatalf("both targets should be used while healthy: %v", seen)
	}
	lnA.Close()
	down := time.Now()
	eventually(t, "target A marked down", func() bool { return !pr.TargetHealthy(addrA.String()) })
	detect := time.Since(down)
	if rounds := float64(detect) / float64(interval); rounds > probe.FailAfter+1.5 {
		t.Fatalf("detection took %.1f rounds", rounds)
	}
	for i := 0; i < 20; i++ {
		got, tag := transfer(addr, 16, time.Second)
		if got != 16 || tag != 'B' {
			t.Fatalf("session %d after detection: %d bytes via %q", i, got, tag)
		}
	}
	t.Logf("NFR-016: target failure detected in %v (%.1f probe rounds; default 3 s rounds → ≤ %v)", detect.Round(time.Millisecond),
		float64(detect)/float64(interval), time.Duration(probe.FailAfter+1)*probe.Interval)
}

func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }

// NFR-023：后端恢复后对账：限制先于账本下达，自治期间的计量批次全部到达后端、无遗漏（按幂等键无重复），
// 对账完成回到在线。
func TestReconciliationAccounting(t *testing.T) {
	cfg := config(t)
	addr := cfg.Listeners[0].Bind
	clk := t0()
	a := newHub(t, hubA, clk, cfg)
	d := Dialer{Node: nodeID, Hubs: map[types.UUID]*Hub{hubA: a}}
	n := startNode(t, newLogs(), d, []control.Endpoint{{ID: hubA}})
	defer n.stop()
	online(t, n)
	a.Link.Disconnected()
	clk.Advance(31 * time.Second)
	_ = a.Tick()
	sealed := 0
	for i := 0; i < 5; i++ {
		if !ok(addr) {
			t.Fatal("no forwarding during autonomy")
		}
		n.a.Tick(context.Background())
		if n.a.SealTick() != nil {
			sealed++
		}
	}
	eventually(t, "acked locally during autonomy", func() bool { return n.a.Status().PendingBatches == 0 })
	boot := n.a.Status().Boot
	before := a.B.Batches(nodeID, boot)
	recovered := time.Now()
	if err := a.B.Connect(); err != nil {
		t.Fatal(err)
	}
	eventually(t, "back online", func() bool { return a.Mode.Mode() == model.ModeOnline })
	took := time.Since(recovered)
	if a.Issuer.RestrictionVersion() == 0 {
		t.Fatal("restrictions not delivered during reconciliation")
	}
	if got := a.B.Batches(nodeID, boot); got != before+sealed {
		t.Fatalf("backend has %d batches of this boot, want %d (+%d from autonomy)", got, before+sealed, sealed)
	}
	es, _ := a.Ledger.Unacked()
	if len(es) != 0 {
		t.Fatalf("%d ledger entries unacked after reconciliation", len(es))
	}
	t.Logf("NFR-023: reconciliation completed in %v; %d autonomous batches delivered, none missing", took.Round(time.Millisecond), sealed)
}
