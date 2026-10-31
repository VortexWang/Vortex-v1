// vxbench 是隧道基准（M1 退出：NFR-002、NFR-004、NFR-007）在各网络命名空间中运行的进程。
//
//	vxbench pki       -dir D                         生成测试 CA 与 entry / exit 两个节点证书
//	vxbench entry     -dir D -listen A -peer A [-link-listen A] [-reverse] [-link-mode mux|pool|auto]
//	vxbench exit      -dir D -target A [-link-listen A] [-peer A] [-reverse]
//	vxbench tls-entry -dir D -listen A -peer A       最小基准：每个会话一条 TLS 1.3（AES-GCM）连接
//	vxbench tls-exit  -dir D -listen A -target A
//	vxbench relay     -dir D -name r1 -hop 1 -upstream entry -next r2 -link-listen A -peer A   多跳中继
//	vxbench tls-relay -dir D -name r1 -listen A -peer A                                       最小基准的中继
//	（entry 用 -next 指定下一跳名称，exit 用 -hop 与 -upstream；名称：entry、r1～r3、exit）
//	vxbench delay     -listen A -to A -delay 10ms    用户态单向延迟代理（容器内没有 netem）
//	vxbench tag       -listen A                      目标：接入后立即写 1 字节，然后丢弃输入
//	vxbench ttfb      -connect A -n 200              测量从发起连接到收到首字节的时间，输出 JSON
//
// 引擎节点就绪后在标准输出打印 READY；收到 SIGINT / SIGTERM 时打印 JSON 统计并退出。
// 入口规则为执行点，持有运行授权、份额与计费授权：正式计量与门控全程生效（需求 13.2 节）。
package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/netip"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"syscall"
	"time"

	"vortex/engine"
	"vortex/engine/enginetest"
	"vortex/engine/types"
	"vortex/engine/vlink"
)

// idOf 返回节点名称对应的 node_id：entry、exit、r1～r9。
func idOf(name string) types.UUID {
	switch name {
	case "entry":
		return entryID
	case "exit":
		return exitID
	}
	if len(name) == 2 && name[0] == 'r' && name[1] >= '1' && name[1] <= '9' {
		return types.UUID{0xe0, 10 + name[1] - '0'}
	}
	check(fmt.Errorf("unknown node name %q", name))
	return types.UUID{}
}

var nodeNames = []string{"entry", "r1", "r2", "r3", "exit"}

var (
	entryID = types.UUID{0xe0, 1}
	exitID  = types.UUID{0xe0, 2}
	ruleID  = types.UUID{0xbe, 7}
	tenant  = types.UUID{0xbe, 8}
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: vxbench pki|entry|exit|tls-entry|tls-exit|delay|tag|ttfb ...")
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	dir := fs.String("dir", "", "证书目录")
	listen := fs.String("listen", "", "监听地址")
	peer := fs.String("peer", "", "对端链路地址 ip:port")
	linkListen := fs.String("link-listen", "", "链路监听地址")
	target := fs.String("target", "", "目标 ip:port")
	reverse := fs.Bool("reverse", false, "出口反向建链（出口连接入口）")
	linkMode := fs.String("link-mode", "auto", "mux | pool | auto")
	to := fs.String("to", "", "延迟代理的转发地址")
	delay := fs.Duration("delay", 10*time.Millisecond, "单向延迟")
	connect := fs.String("connect", "", "ttfb 连接地址")
	n := fs.Int("n", 200, "ttfb 次数")
	gap := fs.Duration("gap", 20*time.Millisecond, "ttfb 两次之间的间隔")
	name := fs.String("name", "", "节点名称（relay / tls-relay）")
	hop := fs.Int("hop", -1, "本节点跳位（relay、exit；exit 默认 1）")
	upstream := fs.String("upstream", "entry", "上游节点名称")
	next := fs.String("next", "exit", "下一跳节点名称")
	_ = fs.Parse(args)

	switch cmd {
	case "pki":
		check(genPKI(*dir))
	case "entry":
		runEntry(*dir, *listen, *peer, *linkListen, *reverse, *linkMode, *next)
	case "exit":
		h := *hop
		if h < 0 {
			h = 1
		}
		runExit(*dir, *target, *linkListen, *peer, *reverse, uint32(h), *upstream)
	case "relay":
		runRelay(*dir, *name, uint32(*hop), *upstream, *next, *linkListen, *peer)
	case "tls-relay":
		runTLSRelay(*dir, *name, *listen, *peer)
	case "tls-entry":
		runTLSEntry(*dir, *listen, *peer)
	case "tls-exit":
		runTLSExit(*dir, *listen, *target)
	case "delay":
		runDelay(*listen, *to, *delay)
	case "tag":
		runTag(*listen)
	case "ttfb":
		runTTFB(*connect, *n, *gap)
	default:
		fmt.Fprintln(os.Stderr, "unknown command", cmd)
		os.Exit(2)
	}
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "vxbench:", err)
		os.Exit(1)
	}
}

func ap(s string) netip.AddrPort {
	a, err := netip.ParseAddrPort(s)
	check(err)
	return a
}

// ---------------------------------------------------------------- 证书

func writePEM(path, typ string, der []byte) error {
	return os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600)
}

func genPKI(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	ck, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	now := time.Now()
	ct := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "vxbench CA"}, IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(48 * time.Hour)}
	cder, err := x509.CreateCertificate(rand.Reader, ct, ct, &ck.PublicKey, ck)
	if err != nil {
		return err
	}
	ca, _ := x509.ParseCertificate(cder)
	if err := writePEM(filepath.Join(dir, "ca.pem"), "CERTIFICATE", cder); err != nil {
		return err
	}
	for i, nm := range nodeNames {
		n := struct {
			name string
			id   types.UUID
		}{nm, idOf(nm)}
		k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		u, _ := url.Parse(vlink.NodeURI(n.id))
		t := &x509.Certificate{SerialNumber: big.NewInt(int64(10 + i)), Subject: pkix.Name{CommonName: n.name}, URIs: []*url.URL{u},
			KeyUsage: x509.KeyUsageDigitalSignature, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(48 * time.Hour),
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}}
		der, err := x509.CreateCertificate(rand.Reader, t, ca, &k.PublicKey, ck)
		if err != nil {
			return err
		}
		kd, _ := x509.MarshalECPrivateKey(k)
		if err := writePEM(filepath.Join(dir, n.name+".pem"), "CERTIFICATE", der); err != nil {
			return err
		}
		if err := writePEM(filepath.Join(dir, n.name+".key"), "EC PRIVATE KEY", kd); err != nil {
			return err
		}
	}
	return nil
}

func load(dir, name string, id types.UUID) (*vlink.Identity, *x509.CertPool) {
	cert, err := tls.LoadX509KeyPair(filepath.Join(dir, name+".pem"), filepath.Join(dir, name+".key"))
	check(err)
	caPEM, err := os.ReadFile(filepath.Join(dir, "ca.pem"))
	check(err)
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEM)
	return &vlink.Identity{NodeID: id, Cert: cert}, pool
}

// ---------------------------------------------------------------- 引擎节点

type stats struct {
	MeteredUp     uint64  `json:"metered_up_bytes"`
	MeteredDown   uint64  `json:"metered_down_bytes"`
	BillingUsed   uint64  `json:"billing_used_bytes,omitempty"`
	Events        int     `json:"events"`
	Rejections    int     `json:"open_rejections"`
	Goroutines    int     `json:"goroutines_at_exit"`
	CPUUserSec    float64 `json:"cpu_user_seconds"`
	CPUSysSec     float64 `json:"cpu_sys_seconds"`
	WallSec       float64 `json:"wall_seconds"`
	MaxRSSKiB     int64   `json:"max_rss_kib"`
	AESHardware   bool    `json:"aes_hardware"`
	PeerMuxLinks  int     `json:"peer_mux_links"`
	PeerPoolIdle  int     `json:"peer_pool_idle"`
	DroppedEvents uint64  `json:"dropped_events"`
}

func linkModeOf(s string) types.LinkMode {
	switch s {
	case "mux":
		return types.LinkModeMux
	case "pool":
		return types.LinkModePool
	}
	return types.LinkModeAuto
}

func listenOrNil(addr string) net.Listener {
	if addr == "" {
		return nil
	}
	ln, err := net.Listen("tcp", addr)
	check(err)
	return ln
}

func newRuntime(dir, name string, id types.UUID, linkListen string) *engine.Runtime {
	ident, roots := load(dir, name, id)
	return engine.New(engine.Options{Tunnel: &engine.TunnelOptions{Identity: ident, Roots: roots,
		BootID: types.UUID{0xb0, id[1]}, LinkListener: listenOrNil(linkListen), DialTimeout: 3 * time.Second}})
}

func baseRule(role types.HopRole, hop uint32, ep bool) types.Rule {
	return types.Rule{RuleID: ruleID, TenantID: tenant, RuleVersion: 1, PathGeneration: 1, Role: role, HopIndex: hop,
		EnforcementPoint: ep, Networks: []types.Network{types.NetworkTCP}, TargetSchedule: types.ScheduleWeightedRoundRobin,
		PathCoefMilli: 1000, BillingVersion: 1}
}

func peerOf(id types.UUID, addr string, outbound bool, mode types.LinkMode, served []types.UUID) types.Peer {
	p := types.Peer{NodeID: id, Outbound: outbound, LinkMode: mode, ServedRuleIDs: served}
	if addr != "" {
		a := ap(addr)
		p.Address, p.LinkPort = a.Addr().String(), a.Port()
	}
	return p
}

func runEntry(dir, listen, peer, linkListen string, reverse bool, mode, next string) {
	rt := newRuntime(dir, "entry", entryID, linkListen)
	nid := idOf(next)
	r := baseRule(types.RoleEntry, 0, true)
	r.NextHop = &types.NextHop{MemberSchedule: types.ScheduleWeightedRoundRobin, Dial: types.DialForward,
		Members: []types.Member{{NodeID: nid, Weight: 1}}}
	p := peerOf(nid, peer, true, linkModeOf(mode), nil)
	if reverse {
		r.NextHop.Dial = types.DialReverse
		p = peerOf(nid, "", false, types.LinkModeMux, nil)
	}
	cfg := &types.RuntimeConfig{Version: 1, Generation: 1, NodeID: entryID, Rules: []types.Rule{r}, Peers: []types.Peer{p},
		Listeners: []types.Listener{{Bind: ap(listen), Network: types.NetworkTCP, RuleID: ruleID}}}
	serve(rt, cfg, func() (int, int) { st := rt.PeerStats(nid); return st.Mux, st.PoolIdle })
}

func runRelay(dir, name string, hop uint32, upstream, next, linkListen, peer string) {
	id, up, nx := idOf(name), idOf(upstream), idOf(next)
	rt := newRuntime(dir, name, id, linkListen)
	r := baseRule(types.RoleRelay, hop, false)
	r.AllowedUpstreams = []types.UUID{up}
	r.NextHop = &types.NextHop{MemberSchedule: types.ScheduleWeightedRoundRobin, Dial: types.DialForward,
		Members: []types.Member{{NodeID: nx, Weight: 1}}}
	cfg := &types.RuntimeConfig{Version: 1, Generation: 1, NodeID: id, Rules: []types.Rule{r},
		Peers: []types.Peer{peerOf(up, "", false, types.LinkModeAuto, nil), peerOf(nx, peer, true, types.LinkModeMux, nil)}}
	serve(rt, cfg, func() (int, int) { st := rt.PeerStats(nx); return st.Mux, st.PoolIdle })
}

func runExit(dir, target, linkListen, peer string, reverse bool, hop uint32, upstream string) {
	rt := newRuntime(dir, "exit", exitID, linkListen)
	up := idOf(upstream)
	r := baseRule(types.RoleExit, hop, false)
	r.AllowedUpstreams = []types.UUID{up}
	t := ap(target)
	r.Targets = []types.Target{{Host: t.Addr().String(), Port: t.Port(), Weight: 1}}
	p := peerOf(up, "", false, types.LinkModeAuto, nil)
	if reverse {
		p = peerOf(up, peer, true, types.LinkModeMux, []types.UUID{ruleID})
	}
	cfg := &types.RuntimeConfig{Version: 1, Generation: 1, NodeID: exitID, Rules: []types.Rule{r}, Peers: []types.Peer{p},
		TargetPolicy: types.TargetPolicy{AllowPrivatePrefix: []netip.Prefix{netip.PrefixFrom(t.Addr(), t.Addr().BitLen()).Masked()}}}
	serve(rt, cfg, func() (int, int) { st := rt.PeerStats(up); return st.Mux, st.PoolIdle })
}

func serve(rt *engine.Runtime, cfg *types.RuntimeConfig, peerStats func() (int, int)) {
	ag := enginetest.Start(rt, time.Second)
	if _, _, err := ag.Apply(context.Background(), cfg); err != nil {
		check(err)
	}
	set := ag.GrantAll(cfg, enginetest.Grants{TTL: 24 * time.Hour})
	start := time.Now()
	fmt.Println("READY")
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	ag.Stop()
	st := stats{MeteredUp: ag.Usage(ruleID, types.DirUp), MeteredDown: ag.Usage(ruleID, types.DirDown),
		Events: len(ag.Events(0)), Rejections: len(ag.Events(types.EventOpenRejected)), DroppedEvents: rt.DroppedEvents(),
		WallSec: time.Since(start).Seconds(), AESHardware: aesHW()}
	st.PeerMuxLinks, st.PeerPoolIdle = peerStats()
	if len(set.Billing) > 0 {
		if c, ok := ag.Consumption(set.Billing[0].Meta.GrantID); ok {
			st.BillingUsed = c.UsedRawBytes
		}
	}
	_ = rt.Close()
	st.Goroutines = runtime.NumGoroutine()
	fill(&st)
	out, _ := json.Marshal(st)
	fmt.Println(string(out))
}

func fill(st *stats) {
	var ru syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &ru) == nil {
		st.CPUUserSec = float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6
		st.CPUSysSec = float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6
		st.MaxRSSKiB = ru.Maxrss
	}
}

func waitSignal(start time.Time) {
	fmt.Println("READY")
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	st := stats{WallSec: time.Since(start).Seconds(), AESHardware: aesHW()}
	fill(&st)
	out, _ := json.Marshal(st)
	fmt.Println(string(out))
}

// ---------------------------------------------------------------- 最小基准：TLS 1.3 代理

func tlsConfig(dir, name string, id types.UUID) *tls.Config {
	ident, roots := load(dir, name, id)
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{ident.Cert}, RootCAs: roots, ClientCAs: roots,
		ClientAuth: tls.RequireAndVerifyClientCert, ServerName: "vxbench", InsecureSkipVerify: true, DynamicRecordSizingDisabled: true}
}

func relay(a, b net.Conn) {
	var wg sync.WaitGroup
	cp := func(dst, src net.Conn) {
		defer wg.Done()
		_, _ = io.Copy(dst, src)
		if cw, ok := dst.(interface{ CloseWrite() error }); ok {
			_ = cw.CloseWrite()
		} else {
			_ = dst.Close()
		}
	}
	wg.Add(2)
	go cp(a, b)
	go cp(b, a)
	wg.Wait()
	_ = a.Close()
	_ = b.Close()
}

func runTLSEntry(dir, listen, peer string) {
	cfg := tlsConfig(dir, "entry", entryID)
	ln := listenOrNil(listen)
	start := time.Now()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				u, err := tls.Dial("tcp", peer, cfg)
				if err != nil {
					_ = c.Close()
					return
				}
				relay(c, u)
			}()
		}
	}()
	waitSignal(start)
}

// runTLSRelay 是最小基准的中继：接受 TLS，再为每个会话建立一条到下一跳的 TLS 连接。
func runTLSRelay(dir, name, listen, peer string) {
	cfg := tlsConfig(dir, name, idOf(name))
	ln, err := tls.Listen("tcp", listen, cfg)
	check(err)
	start := time.Now()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				u, err := tls.Dial("tcp", peer, cfg)
				if err != nil {
					_ = c.Close()
					return
				}
				relay(c, u)
			}()
		}
	}()
	waitSignal(start)
}

func runTLSExit(dir, listen, target string) {
	cfg := tlsConfig(dir, "exit", exitID)
	ln, err := tls.Listen("tcp", listen, cfg)
	check(err)
	start := time.Now()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				t, err := net.Dial("tcp", target)
				if err != nil {
					_ = c.Close()
					return
				}
				relay(c, t)
			}()
		}
	}()
	waitSignal(start)
}

// ---------------------------------------------------------------- 延迟代理、目标、首字节时间

// runDelay 转发 TCP，每个方向把数据推迟 d 后写出（按到达时间排队，不改变顺序）。
func runDelay(listen, to string, d time.Duration) {
	ln := listenOrNil(listen)
	type chunk struct {
		at time.Time
		b  []byte
	}
	pump := func(dst, src net.Conn) {
		q := make(chan chunk, 4096)
		go func() {
			for c := range q {
				time.Sleep(time.Until(c.at))
				if _, err := dst.Write(c.b); err != nil {
					break
				}
			}
			_ = dst.Close()
		}()
		for {
			buf := make([]byte, 64<<10)
			n, err := src.Read(buf)
			if n > 0 {
				q <- chunk{time.Now().Add(d), buf[:n]}
			}
			if err != nil {
				close(q)
				return
			}
		}
	}
	start := time.Now()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				u, err := net.Dial("tcp", to)
				if err != nil {
					_ = c.Close()
					return
				}
				go pump(u, c)
				go pump(c, u)
			}()
		}
	}()
	waitSignal(start)
}

func runTag(listen string) {
	ln := listenOrNil(listen)
	start := time.Now()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				if _, err := c.Write([]byte{'T'}); err != nil {
					return
				}
				_, _ = io.Copy(io.Discard, c)
			}()
		}
	}()
	waitSignal(start)
}

type ttfbResult struct {
	N        int     `json:"n"`
	Failed   int     `json:"failed"`
	MedianMs float64 `json:"median_ms"`
	P90Ms    float64 `json:"p90_ms"`
	MinMs    float64 `json:"min_ms"`
	ConnMs   float64 `json:"median_connect_ms"` // TCP 建连（客户端到入口的 RTT）
}

func runTTFB(addr string, n int, gap time.Duration) {
	var ds, cs []float64
	failed := 0
	for i := 0; i < n; i++ {
		t0 := time.Now()
		c, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			failed++
			continue
		}
		t1 := time.Now()
		_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
		var b [1]byte
		_, err = io.ReadFull(c, b[:])
		t2 := time.Now()
		_ = c.Close()
		if err != nil {
			failed++
			continue
		}
		ds = append(ds, float64(t2.Sub(t0).Microseconds())/1000)
		cs = append(cs, float64(t1.Sub(t0).Microseconds())/1000)
		time.Sleep(gap)
	}
	sort.Float64s(ds)
	sort.Float64s(cs)
	r := ttfbResult{N: len(ds), Failed: failed}
	if len(ds) > 0 {
		r.MedianMs, r.P90Ms, r.MinMs, r.ConnMs = ds[len(ds)/2], ds[len(ds)*9/10], ds[0], cs[len(cs)/2]
	}
	out, _ := json.Marshal(r)
	fmt.Println(string(out))
}

func aesHW() bool {
	ok, _ := vlink.AESHardware()
	return ok
}
