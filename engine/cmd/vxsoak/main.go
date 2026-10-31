// vxsoak 是引擎的长稳与泄漏回归（ENG-T39；需求 13.3 节 NFR-008～NFR-011 的缩短版）。
//
// 同一进程内运行三个引擎节点（入口 → 中继 → 出口，入口为执行点）与一条直接转发规则，施加混合负载：
// 长 TCP 会话（请求 / 回显，持续）、短 TCP 连接（隧道与直接转发）、不断更替的 UDP 会话（隧道）。
// 负载结束后清空会话、等待回收，然后与负载前的空闲基线比较：
//
//	NFR-008  无崩溃、死锁、持续积压；无重复计费：计量 = 客户端实际收发；计费授权消耗 = 执行点计量
//	NFR-009  负载后半段平均 RSS ≤ 前半段平均的 120%
//	NFR-010  清空后会话与连接归零
//	NFR-011  文件描述符与 goroutine：清空后 ≤ 空闲基线 + max(⌈基线 × 10%⌉, 20)
//
// 用法：vxsoak -load 20m -drain 2m -out DIR；结果写入 DIR/soak.json 与 DIR/samples.csv，判定不通过时退出码 1。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"vortex/engine"
	"vortex/engine/enginetest"
	"vortex/engine/types"
)

var (
	nodeE, nodeR, nodeX = types.UUID{0x50, 1}, types.UUID{0x50, 2}, types.UUID{0x50, 3}
	ruleTCP, ruleUDP    = types.UUID{0x51, 1}, types.UUID{0x51, 2}
	ruleDirect          = types.UUID{0x51, 3}
	tenant              = types.UUID{0x52}
)

type node struct {
	id   types.UUID
	rt   *engine.Runtime
	ag   *enginetest.Agent
	link netip.AddrPort
}

func newNode(pki *enginetest.PKI, id types.UUID) *node {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	check(err)
	rt := engine.New(engine.Options{Tunnel: &engine.TunnelOptions{Identity: pki.Node(id), Roots: pki.Roots,
		BootID: types.UUID{0xb0, id[1]}, LinkListener: ln, DialTimeout: 3 * time.Second}})
	return &node{id: id, rt: rt, ag: enginetest.Start(rt, time.Second), link: netip.MustParseAddrPort(ln.Addr().String())}
}

func (n *node) peer(out bool) types.Peer {
	return types.Peer{NodeID: n.id, Address: n.link.Addr().String(), LinkPort: n.link.Port(), Outbound: out}
}

func (n *node) apply(cfg *types.RuntimeConfig) {
	cfg.NodeID = n.id
	_, _, err := n.ag.Apply(context.Background(), cfg)
	check(err)
	n.ag.GrantAll(cfg, enginetest.Grants{TTL: 24 * time.Hour})
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "vxsoak:", err)
		os.Exit(2)
	}
}

func rule(id types.UUID, role types.HopRole, hop uint32, ep bool, nets ...types.Network) types.Rule {
	return types.Rule{RuleID: id, TenantID: tenant, RuleVersion: 1, PathGeneration: 1, Role: role, HopIndex: hop,
		EnforcementPoint: ep, Networks: nets, TargetSchedule: types.ScheduleWeightedRoundRobin, UDPIdle: 5 * time.Second}
}

// ---------------------------------------------------------------- 目标服务

func echoTCP() netip.AddrPort {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	check(err)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}()
		}
	}()
	return netip.MustParseAddrPort(ln.Addr().String())
}

func echoUDP() netip.AddrPort {
	c, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	check(err)
	go func() {
		buf := make([]byte, 65535)
		for {
			n, a, err := c.ReadFromUDP(buf)
			if err != nil {
				return
			}
			_, _ = c.WriteToUDP(buf[:n], a)
		}
	}()
	return netip.MustParseAddrPort(c.LocalAddr().String())
}

func tgt(ap netip.AddrPort) []types.Target {
	return []types.Target{{Host: ap.Addr().String(), Port: ap.Port(), Weight: 1}}
}

// ---------------------------------------------------------------- 观测

func fds() int {
	es, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return -1
	}
	return len(es)
}

func rssKiB() int {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return -1
	}
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "VmRSS:") {
			v, _ := strconv.Atoi(strings.Fields(l)[1])
			return v
		}
	}
	return -1
}

type sample struct {
	T          float64 `json:"t"`
	Phase      string  `json:"phase"`
	Goroutines int     `json:"goroutines"`
	FDs        int     `json:"fds"`
	RSSKiB     int     `json:"rss_kib"`
	Sessions   int     `json:"sessions"`
	Links      int     `json:"links"`
}

// ---------------------------------------------------------------- 负载

type counters struct {
	tcpUp, tcpDown       atomic.Int64 // 隧道 TCP 客户端实际发送、收到的字节
	directUp, directDown atomic.Int64
	udpSent, udpRecv     atomic.Int64
	shortOK, shortFail   atomic.Int64
	longChunks           atomic.Int64
	errors               atomic.Int64
}

// longSession 请求 / 回显：每次写 64 KiB，读满 64 KiB 后再写下一块；停止时在两块之间退出，所有字节都已交付。
func longSession(ctx context.Context, addr netip.AddrPort, c *counters, wg *sync.WaitGroup) {
	defer wg.Done()
	buf, back := make([]byte, 64<<10), make([]byte, 64<<10)
	for ctx.Err() == nil {
		conn, err := net.DialTimeout("tcp", addr.String(), 3*time.Second)
		if err != nil {
			c.errors.Add(1)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		for ctx.Err() == nil {
			_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
			if _, err := conn.Write(buf); err != nil {
				c.errors.Add(1)
				break
			}
			c.tcpUp.Add(int64(len(buf)))
			if _, err := io.ReadFull(conn, back); err != nil {
				c.errors.Add(1)
				break
			}
			c.tcpDown.Add(int64(len(back)))
			c.longChunks.Add(1)
			time.Sleep(5 * time.Millisecond) // 每会话约 100 Mbit/s 以下，避免占满 CPU
		}
		_ = conn.Close()
	}
}

func shortSessions(ctx context.Context, addr netip.AddrPort, perSec int, up, down *atomic.Int64, c *counters, wg *sync.WaitGroup) {
	defer wg.Done()
	t := time.NewTicker(time.Second / time.Duration(perSec))
	defer t.Stop()
	msg := make([]byte, 4096)
	var inner sync.WaitGroup
	defer inner.Wait()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		inner.Add(1)
		go func() {
			defer inner.Done()
			conn, err := net.DialTimeout("tcp", addr.String(), 3*time.Second)
			if err != nil {
				c.shortFail.Add(1)
				return
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			if _, err := conn.Write(msg); err != nil {
				c.shortFail.Add(1)
				return
			}
			up.Add(int64(len(msg)))
			if _, err := io.ReadFull(conn, make([]byte, len(msg))); err != nil {
				c.shortFail.Add(1)
				return
			}
			down.Add(int64(len(msg)))
			c.shortOK.Add(1)
		}()
	}
}

// udpChurn：每个客户端存活 lifetime 后关闭、换新端口重开；每秒 pps 个 1,200 字节数据报。
func udpChurn(ctx context.Context, addr netip.AddrPort, pps int, lifetime time.Duration, c *counters, wg *sync.WaitGroup) {
	defer wg.Done()
	p := make([]byte, 1200)
	for ctx.Err() == nil {
		conn, err := net.Dial("udp", addr.String())
		if err != nil {
			c.errors.Add(1)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			buf := make([]byte, 2048)
			for {
				_ = conn.SetReadDeadline(time.Now().Add(time.Second))
				if _, err := conn.Read(buf); err != nil {
					if ne, ok := err.(net.Error); ok && ne.Timeout() && ctx.Err() == nil {
						continue
					}
					return
				}
				c.udpRecv.Add(1)
			}
		}()
		end := time.Now().Add(lifetime)
		t := time.NewTicker(time.Second / time.Duration(pps))
		for ctx.Err() == nil && time.Now().Before(end) {
			<-t.C
			if _, err := conn.Write(p); err == nil {
				c.udpSent.Add(1)
			}
		}
		t.Stop()
		_ = conn.Close()
		<-done
	}
}

// ---------------------------------------------------------------- 主流程

type result struct {
	LoadSec, DrainSec        float64
	Baseline, AfterDrain     sample
	PeakDuringLoad           sample
	RSSFirstHalfKiB          float64
	RSSSecondHalfKiB         float64
	TCPUp, TCPDown           int64
	DirectUp, DirectDown     int64
	MeteredTunnelUp          uint64
	MeteredTunnelDown        uint64
	MeteredDirectUp          uint64
	MeteredDirectDown        uint64
	BillingUsed              uint64
	EntryMeteredAll          uint64
	UDPSent, UDPRecv         int64
	ShortOK, ShortFail       int64
	LongChunks, ClientErrors int64
	Verdicts                 map[string]bool
	Notes                    []string
}

func main() {
	load := flag.Duration("load", 20*time.Minute, "负载时长")
	drain := flag.Duration("drain", 2*time.Minute, "清空后等待时长（需求为 10 分钟）")
	out := flag.String("out", "soak-out", "结果目录")
	every := flag.Duration("sample", 5*time.Second, "采样间隔")
	longN := flag.Int("long", 8, "长 TCP 会话数")
	shortRate := flag.Int("short", 30, "每秒短连接数（隧道与直接转发各一半）")
	udpN := flag.Int("udp", 10, "并发 UDP 客户端数")
	warm := flag.Duration("warm", 0, "NFR-009 比较前排除的预热时长（默认 max(负载 × 25%, 60 秒)）")
	flag.Parse()
	check(os.MkdirAll(*out, 0o755))

	pki := enginetest.NewPKI()
	e, r, x := newNode(pki, nodeE), newNode(pki, nodeR), newNode(pki, nodeX)
	tcpT, udpT, dirT := echoTCP(), echoUDP(), echoTCP()
	any := netip.MustParseAddrPort("127.0.0.1:0")
	tcpL := types.Listener{Bind: any, Network: types.NetworkTCP, RuleID: ruleTCP}
	udpL := types.Listener{Bind: any, Network: types.NetworkUDP, RuleID: ruleUDP}
	dirL := types.Listener{Bind: netip.MustParseAddrPort("127.0.0.2:0"), Network: types.NetworkTCP, RuleID: ruleDirect} // 监听键按配置的地址区分
	pol := types.TargetPolicy{SelfHostedNode: true}

	var xr, rr, er []types.Rule
	for _, id := range []types.UUID{ruleTCP, ruleUDP} {
		nw := types.NetworkTCP
		t := tgt(tcpT)
		if id == ruleUDP {
			nw, t = types.NetworkUDP, tgt(udpT)
		}
		a := rule(id, types.RoleExit, 2, false, nw)
		a.AllowedUpstreams, a.Targets = []types.UUID{nodeR}, t
		b := rule(id, types.RoleRelay, 1, false, nw)
		b.AllowedUpstreams = []types.UUID{nodeE}
		b.NextHop = &types.NextHop{MemberSchedule: types.ScheduleWeightedRoundRobin, Dial: types.DialForward, Members: []types.Member{{NodeID: nodeX, Weight: 1}}}
		c := rule(id, types.RoleEntry, 0, true, nw)
		c.NextHop = &types.NextHop{MemberSchedule: types.ScheduleWeightedRoundRobin, Dial: types.DialForward, Members: []types.Member{{NodeID: nodeR, Weight: 1}}}
		xr, rr, er = append(xr, a), append(rr, b), append(er, c)
	}
	d := rule(ruleDirect, types.RoleEntry, 0, true, types.NetworkTCP)
	d.Targets = tgt(dirT)
	er = append(er, d)
	x.apply(&types.RuntimeConfig{Version: 1, Generation: 1, Rules: xr, Peers: []types.Peer{r.peer(false)}, TargetPolicy: pol})
	r.apply(&types.RuntimeConfig{Version: 1, Generation: 1, Rules: rr, Peers: []types.Peer{e.peer(false), x.peer(true)}, TargetPolicy: pol})
	e.apply(&types.RuntimeConfig{Version: 1, Generation: 1, Rules: er, Peers: []types.Peer{r.peer(true)}, TargetPolicy: pol,
		Listeners: []types.Listener{tcpL, udpL, dirL}})
	tcpA, udpA, dirA := e.rt.ListenerAddr(tcpL), e.rt.ListenerAddr(udpL), e.rt.ListenerAddr(dirL)

	start := time.Now()
	var samples []sample
	var smu sync.Mutex
	nodes := []*node{e, r, x}
	snap := func(phase string) sample {
		runtime.GC()
		s := sample{T: time.Since(start).Seconds(), Phase: phase, Goroutines: runtime.NumGoroutine(), FDs: fds(), RSSKiB: rssKiB()}
		for _, n := range nodes {
			s.Sessions += n.rt.Sessions()
		}
		s.Links = e.rt.NumPeerLinks(nodeR) + r.rt.NumPeerLinks(nodeX)
		smu.Lock()
		samples = append(samples, s)
		smu.Unlock()
		return s
	}
	// 空闲基线：链路预热完成、目标服务就绪之后
	time.Sleep(10 * time.Second)
	base := snap("idle")
	fmt.Printf("baseline: %+v\n", base)

	var c counters
	ctx, cancel := context.WithTimeout(context.Background(), *load)
	var wg sync.WaitGroup
	for i := 0; i < *longN; i++ {
		wg.Add(1)
		go longSession(ctx, tcpA, &c, &wg)
	}
	wg.Add(2)
	go shortSessions(ctx, tcpA, max(1, *shortRate/2), &c.tcpUp, &c.tcpDown, &c, &wg)
	go shortSessions(ctx, dirA, max(1, *shortRate/2), &c.directUp, &c.directDown, &c, &wg)
	for i := 0; i < *udpN; i++ {
		wg.Add(1)
		go udpChurn(ctx, udpA, 200, 20*time.Second+time.Duration(i)*time.Second, &c, &wg)
	}
	tick := time.NewTicker(*every)
	var loadSamples []sample
	var peak sample
	func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				s := snap("load")
				loadSamples = append(loadSamples, s)
				if s.Goroutines > peak.Goroutines {
					peak = s
				}
				fmt.Printf("%6.0fs load: goroutines %d fds %d rss %d KiB sessions %d links %d\n", s.T, s.Goroutines, s.FDs, s.RSSKiB, s.Sessions, s.Links)
			}
		}
	}()
	cancel()
	waitDone := make(chan struct{})
	go func() { wg.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
	case <-time.After(30 * time.Second):
		fmt.Println("load generators did not stop within 30 s (possible deadlock)")
	}
	loadEnd := time.Now()
	for time.Since(loadEnd) < *drain {
		<-tick.C
		s := snap("drain")
		fmt.Printf("%6.0fs drain: goroutines %d fds %d rss %d KiB sessions %d links %d\n", s.T, s.Goroutines, s.FDs, s.RSSKiB, s.Sessions, s.Links)
	}
	tick.Stop()
	after := snap("after")

	res := result{LoadSec: load.Seconds(), DrainSec: drain.Seconds(), Baseline: base, AfterDrain: after, PeakDuringLoad: peak,
		TCPUp: c.tcpUp.Load(), TCPDown: c.tcpDown.Load(), DirectUp: c.directUp.Load(), DirectDown: c.directDown.Load(),
		UDPSent: c.udpSent.Load(), UDPRecv: c.udpRecv.Load(), ShortOK: c.shortOK.Load(), ShortFail: c.shortFail.Load(),
		LongChunks: c.longChunks.Load(), ClientErrors: c.errors.Load(), Verdicts: map[string]bool{}}
	// 需求做法：预热 1 小时后比较前后两个 6 小时窗口。缩短版：排除预热（默认 max(负载 × 25%, 60 秒)），其余分前后两半比较
	w := *warm
	if w == 0 {
		w = max(*load/4, time.Minute)
	}
	var steady []sample
	for _, s := range loadSamples {
		if s.T-base.T >= w.Seconds() {
			steady = append(steady, s)
		}
	}
	h := len(steady) / 2
	avg := func(ss []sample) float64 {
		if len(ss) == 0 {
			return math.NaN()
		}
		t := 0.0
		for _, s := range ss {
			t += float64(s.RSSKiB)
		}
		return t / float64(len(ss))
	}
	res.RSSFirstHalfKiB, res.RSSSecondHalfKiB = avg(steady[:h]), avg(steady[h:])

	for _, n := range nodes {
		n.ag.Pull()
	}
	res.MeteredTunnelUp, res.MeteredTunnelDown = e.ag.Usage(ruleTCP, types.DirUp), e.ag.Usage(ruleTCP, types.DirDown)
	res.MeteredDirectUp, res.MeteredDirectDown = e.ag.Usage(ruleDirect, types.DirUp), e.ag.Usage(ruleDirect, types.DirDown)
	for _, id := range []types.UUID{ruleTCP, ruleUDP, ruleDirect} {
		res.EntryMeteredAll += e.ag.Usage(id, types.DirUp) + e.ag.Usage(id, types.DirDown)
	}
	for _, cs := range e.ag.Consumptions() {
		res.BillingUsed += cs.UsedRawBytes
	}

	allow := func(b int) int { return b + max(int(math.Ceil(float64(b)*0.1)), 20) }
	res.Verdicts["NFR-008 tunnel metering == client bytes"] = res.MeteredTunnelUp == uint64(res.TCPUp) && res.MeteredTunnelDown == uint64(res.TCPDown)
	res.Verdicts["NFR-008 direct metering == client bytes"] = res.MeteredDirectUp == uint64(res.DirectUp) && res.MeteredDirectDown == uint64(res.DirectDown)
	res.Verdicts["NFR-008 billing used == entry metering (no double charge)"] = res.BillingUsed == res.EntryMeteredAll
	res.Verdicts["NFR-008 no deadlock (generators stopped)"] = func() bool {
		select {
		case <-waitDone:
			return true
		default:
			return false
		}
	}()
	res.Verdicts["NFR-009 RSS second half <= 120% first half (after warm-up)"] = res.RSSSecondHalfKiB <= 1.2*res.RSSFirstHalfKiB
	res.Verdicts["NFR-010 sessions zero after drain"] = after.Sessions == 0
	res.Verdicts["NFR-011 goroutines <= baseline + max(10%,20)"] = after.Goroutines <= allow(base.Goroutines)
	res.Verdicts["NFR-011 fds <= baseline + max(10%,20)"] = after.FDs <= allow(base.FDs)
	res.Notes = append(res.Notes, fmt.Sprintf("links after drain %d (pre-warmed mux links persist; baseline %d)", after.Links, base.Links))

	j, _ := json.MarshalIndent(res, "", "  ")
	check(os.WriteFile(filepath.Join(*out, "soak.json"), j, 0o644))
	var csv strings.Builder
	csv.WriteString("t,phase,goroutines,fds,rss_kib,sessions,links\n")
	for _, s := range samples {
		fmt.Fprintf(&csv, "%.0f,%s,%d,%d,%d,%d,%d\n", s.T, s.Phase, s.Goroutines, s.FDs, s.RSSKiB, s.Sessions, s.Links)
	}
	check(os.WriteFile(filepath.Join(*out, "samples.csv"), []byte(csv.String()), 0o644))
	fmt.Println(string(j))
	for _, ok := range res.Verdicts {
		if !ok {
			os.Exit(1)
		}
	}
}
