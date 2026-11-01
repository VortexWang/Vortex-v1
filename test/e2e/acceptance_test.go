package e2e

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"vortex/edgehub/backendgrpc"
	"vortex/engine/types"
	"vortex/engine/vlink"
	"vortex/node/agent/upgrade"
	nodewire "vortex/node/agent/wire"
	nodeapp "vortex/node/app"
	backendv1 "vortex/proto/gen/go/vortex/backend/v1"
	edgehubv1 "vortex/proto/gen/go/vortex/edgehub/v1"
)

// M3 验收（NFR-016～018，附加：真实 systemd 下的安装、升级与回退）。需要 root、ip netns；VORTEX_ACCEPTANCE=systemd
// 时以真实 systemd 单元运行（CI 的 acceptance 工作流），=local 时以进程方式运行同样的命令行（开发机调试用）。
//
// 拓扑：一个只含网桥的命名空间 vxa-lan 连接主机（10.77.0.1 后端、目标、发布服务器；10.77.0.2 Edge Hub）与三个节点
// 命名空间：入口 E（10.77.0.10）、出口 X1（.11）、出口 X2（.12）。E 的规则以 X1、X2 为下一跳组（加权轮询），
// X1、X2 经 VLink（7443）接受 E 的链路并转发到目标。客户端在主机上每 100 毫秒向 E 发起一个新会话。
//
//   - NFR-016 / 017 链路故障：断开 X1 的网桥端口（不发 RST）。检测 = E 报告 X1 链路失效（PeerDown 到达后端）的时刻；
//     恢复 = 此后新会话全部成功的起点。
//   - NFR-016 / 017 节点故障：冻结 X2 进程（SIGSTOP，不关闭连接），同样测量。
//   - 升级（AGT-008）：后端下发 2.0.0 → systemd 重启 → 健康；下发一启动就崩溃的 4.0.0 → ExecStartPre 的 upgrade-guard 回退。
//   - NFR-018：E 到 Edge Hub 的路由黑洞（E 与全部 Edge Hub 失联），默认参数（运行授权 5 分钟），测量转发停止的时刻。
//
// 结果写入 ACCEPTANCE_OUT（默认临时目录）下的 m3-acceptance.json 与 .md。
func TestM3Acceptance(t *testing.T) {
	mode := os.Getenv("VORTEX_ACCEPTANCE")
	if mode != "systemd" && mode != "local" {
		t.Skip("set VORTEX_ACCEPTANCE=systemd|local (root, ip netns)")
	}
	if os.Geteuid() != 0 {
		t.Fatal("acceptance needs root")
	}
	sd := mode == "systemd"
	testT = t
	a := &acceptance{t: t, sd: sd, dir: t.TempDir(), res: map[string]any{"mode": mode, "started_at": time.Now().UTC()}}
	a.out = os.Getenv("ACCEPTANCE_OUT")
	if a.out == "" {
		a.out = a.dir
	}
	_ = os.MkdirAll(a.out, 0o755)
	defer a.write()
	a.build()
	a.network()
	a.platform()
	a.nodes()
	a.linkFailure()
	a.nodeFailure()
	a.upgrades()
	a.hubLoss()
}

var (
	nodeX1 = types.UUID{0x1e}
	nodeX2 = types.UUID{0x2e}
)

const (
	hostIP    = "10.77.0.1"
	hubIP     = "10.77.0.2"
	entryIP   = "10.77.0.10"
	x1IP      = "10.77.0.11"
	x2IP      = "10.77.0.12"
	entryPort = 8000
	linkPort  = 7443
)

type acceptance struct {
	t       *testing.T
	sd      bool
	dir     string
	out     string
	root    string // 安装根（systemd 模式为 ""，即 /）
	res     map[string]any
	mu      sync.Mutex
	v1, v2  []byte
	v4      []byte
	hubBin  string
	nodeBin string
	p       *pki
	caFile  string
	relKey  string
	priv    ed25519.PrivateKey
	be      *enrollBackend
	rel     string // 发布服务器地址
	units   map[string]unit
	entry   netip.AddrPort
}

// unit 是一个运行中的服务：systemd 单元或模拟 systemd 的进程。
type unit interface {
	signal(sig string)
	logs() string
}

type sdUnit struct{ name string }

func (u sdUnit) signal(sig string) { _ = exec.Command("systemctl", "kill", "-s", sig, u.name).Run() }
func (u sdUnit) logs() string {
	b, _ := exec.Command("journalctl", "-u", u.name, "--no-pager", "-o", "cat", "-n", "300").CombinedOutput()
	return string(b)
}

type localUnit struct{ s *service }

func (u localUnit) signal(sig string) {
	m := map[string]syscall.Signal{"SIGSTOP": syscall.SIGSTOP, "SIGCONT": syscall.SIGCONT, "SIGKILL": syscall.SIGKILL}
	u.s.mu.Lock()
	defer u.s.mu.Unlock()
	if u.s.cmd != nil && u.s.cmd.Process != nil {
		_ = u.s.cmd.Process.Signal(m[sig])
	}
}
func (u localUnit) logs() string { return u.s.log() }

func (a *acceptance) record(k string, v any) {
	a.mu.Lock()
	a.res[k] = v
	a.mu.Unlock()
	a.t.Logf("%s = %v", k, v)
}

func (a *acceptance) write() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.res["passed"] = !a.t.Failed()
	b, _ := json.MarshalIndent(a.res, "", "  ")
	_ = os.WriteFile(filepath.Join(a.out, "m3-acceptance.json"), b, 0o644)
	var keys []string
	for k := range a.res {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var md strings.Builder
	md.WriteString("# M3 验收结果\n\n| 项 | 值 |\n| --- | --- |\n")
	for _, k := range keys {
		fmt.Fprintf(&md, "| %s | %v |\n", k, a.res[k])
	}
	_ = os.WriteFile(filepath.Join(a.out, "m3-acceptance.md"), []byte(md.String()), 0o644)
	if a.t.Failed() {
		for _, sock := range []string{"run/vortex/node.sock", "run/vortex-x1/node.sock", "run/vortex-x2/node.sock"} {
			b, _ := exec.Command(filepath.Join(a.root, "opt/vortex/node/vortex-node"), "status", "-socket", filepath.Join(a.root, sock)).CombinedOutput()
			a.t.Logf("status %s: %s", sock, b)
		}
		if a.be != nil {
			a.be.mu.Lock()
			a.t.Logf("node events:\n%s", tail(strings.Join(a.be.events, "\n"), 60))
			a.be.mu.Unlock()
		}
		for name, u := range a.units {
			a.t.Logf("---- %s ----\n%s", name, tail(u.logs(), 80))
		}
	}
}

func tail(s string, n int) string {
	l := strings.Split(s, "\n")
	if len(l) > n {
		l = l[len(l)-n:]
	}
	return strings.Join(l, "\n")
}

func (a *acceptance) sh(args ...string) {
	a.t.Helper()
	if b, err := exec.Command(args[0], args[1:]...).CombinedOutput(); err != nil {
		a.t.Fatalf("%v: %v\n%s", args, err, b)
	}
}

func (a *acceptance) build() {
	b := filepath.Join(a.dir, "build")
	a.hubBin, a.nodeBin = filepath.Join(b, "edge-hub"), filepath.Join(b, "vortex-node")
	buildHub(a.t, a.hubBin)
	a.v1 = buildNode(a.t, a.nodeBin, "1.0.0")
	a.v2 = buildNode(a.t, filepath.Join(b, "v2"), "2.0.0")
	a.v4 = buildNode(a.t, filepath.Join(b, "v4"), "4.0.0", "-X vortex/node/app.faultCrash=at-start")
	// 构建产物要能被服务用户读取
	_ = os.Chmod(a.dir, 0o755)
	_ = os.Chmod(b, 0o755)
}

// network 建立拓扑；结束时删除。
func (a *acceptance) network() {
	t := a.t
	nss := []string{"vxa-lan", "vxa-e", "vxa-x1", "vxa-x2"}
	cleanup := func() {
		_ = exec.Command("ip", "link", "del", "vxa-host").Run()
		for _, ns := range nss {
			_ = exec.Command("ip", "netns", "del", ns).Run()
		}
	}
	cleanup()
	t.Cleanup(cleanup)
	for _, ns := range nss {
		a.sh("ip", "netns", "add", ns)
		a.sh("ip", "-n", ns, "link", "set", "lo", "up")
	}
	a.sh("ip", "-n", "vxa-lan", "link", "add", "br0", "type", "bridge")
	a.sh("ip", "-n", "vxa-lan", "link", "set", "br0", "up")
	port := func(name, peerNS, peerName string, addrs ...string) {
		a.sh("ip", "-n", "vxa-lan", "link", "add", name, "type", "veth", "peer", "name", peerName)
		if peerNS != "" {
			a.sh("ip", "-n", "vxa-lan", "link", "set", peerName, "netns", peerNS)
		} else {
			a.sh("ip", "-n", "vxa-lan", "link", "set", peerName, "netns", "1")
		}
		a.sh("ip", "-n", "vxa-lan", "link", "set", name, "master", "br0")
		a.sh("ip", "-n", "vxa-lan", "link", "set", name, "up")
		ns := []string{"ip"}
		if peerNS != "" {
			ns = append(ns, "-n", peerNS)
		}
		for _, ad := range addrs {
			a.sh(append(append([]string(nil), ns...), "addr", "add", ad+"/24", "dev", peerName)...)
		}
		a.sh(append(append([]string(nil), ns...), "link", "set", peerName, "up")...)
	}
	port("p-host", "", "vxa-host", hostIP, hubIP)
	port("p-e", "vxa-e", "eth0", entryIP)
	port("p-x1", "vxa-x1", "eth0", x1IP)
	port("p-x2", "vxa-x2", "eth0", x2IP)
	// 连通性：各节点命名空间能连到主机
	ln, err := net.Listen("tcp4", hostIP+":0")
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
	_, lport, _ := net.SplitHostPort(ln.Addr().String())
	for _, ns := range nss[1:] {
		deadline := time.Now().Add(5 * time.Second)
		for exec.Command("ip", "netns", "exec", ns, "bash", "-c", "echo > /dev/tcp/"+hostIP+"/"+lport).Run() != nil {
			if time.Now().After(deadline) {
				t.Fatalf("no connectivity from %s", ns)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	a.entry = netip.MustParseAddrPort(fmt.Sprintf("%s:%d", entryIP, entryPort))
}

// platform：目标、发布服务器、模拟后端，以及用安装脚本安装并启动的 Edge Hub。
func (a *acceptance) platform() {
	t := a.t
	tln, err := net.Listen("tcp4", hostIP+":0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := tln.Accept()
			if err != nil {
				return
			}
			go func() { defer c.Close(); _, _ = c.Write([]byte{'T'}); _, _ = io.Copy(c, c) }()
		}
	}()
	t.Cleanup(func() { tln.Close() })
	target := netip.MustParseAddrPort(tln.Addr().String())

	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	a.priv = priv
	der, _ := x509.MarshalPKIXPublicKey(pub)
	a.relKey = filepath.Join(a.dir, "release.pub")
	_ = os.WriteFile(a.relKey, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}), 0o644)
	rln, _ := net.Listen("tcp4", hostIP+":0")
	pkgs := map[string][]byte{"/2.0.0": a.v2, "/4.0.0": a.v4}
	rs := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if b, ok := pkgs[r.URL.Path]; ok {
			_, _ = w.Write(b)
			return
		}
		http.NotFound(w, r)
	})}
	go func() { _ = rs.Serve(rln) }()
	t.Cleanup(func() { rs.Close() })
	a.rel = "http://" + rln.Addr().String()

	a.p = newPKI(t)
	a.caFile = filepath.Join(a.dir, "ca.crt")
	_ = os.WriteFile(a.caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: a.p.ca.Raw}), 0o644)

	// 目标在测试网段（私有地址），须显式放行（SEC-011）
	pol := types.TargetPolicy{AllowPrivatePrefix: []netip.Prefix{netip.MustParsePrefix("10.77.0.0/24")}}
	members := &types.NextHop{MemberSchedule: types.ScheduleWeightedRoundRobin, Dial: types.DialForward,
		Members: []types.Member{{NodeID: nodeX1, Weight: 1}, {NodeID: nodeX2, Weight: 1}}}
	entryCfg := &types.RuntimeConfig{Version: 1, Generation: 1, NodeID: nodeID,
		Listeners: []types.Listener{{Bind: a.entry, Network: types.NetworkTCP, RuleID: ruleA}},
		Rules: []types.Rule{{RuleID: ruleA, TenantID: tenant, RuleVersion: 1, PathGeneration: 1, Role: types.RoleEntry, EnforcementPoint: true,
			Networks: []types.Network{types.NetworkTCP}, TargetSchedule: types.ScheduleWeightedRoundRobin, PathCoefMilli: 1500, BillingVersion: 1,
			NextHop: members}},
		Peers:        []types.Peer{{NodeID: nodeX1, Address: x1IP, LinkPort: linkPort, Outbound: true}, {NodeID: nodeX2, Address: x2IP, LinkPort: linkPort, Outbound: true}},
		TargetPolicy: pol}
	exit := func(id types.UUID) *types.RuntimeConfig {
		return &types.RuntimeConfig{Version: 1, Generation: 1, NodeID: id,
			Rules: []types.Rule{{RuleID: ruleA, TenantID: tenant, RuleVersion: 1, PathGeneration: 1, Role: types.RoleExit, HopIndex: 1,
				Networks: []types.Network{types.NetworkTCP}, TargetSchedule: types.ScheduleWeightedRoundRobin, BillingVersion: 1,
				AllowedUpstreams: []types.UUID{nodeID}, Targets: []types.Target{{Host: target.Addr().String(), Port: target.Port(), Weight: 1}}}},
			Peers: []types.Peer{{NodeID: nodeID}}, TargetPolicy: pol}
	}
	hubAddr := hubIP + ":7400"
	a.be = &enrollBackend{protoBackend: &protoBackend{cfg: entryCfg, more: []*types.RuntimeConfig{exit(nodeX1), exit(nodeX2)}, pushed: map[string]bool{}},
		p: a.p, nodeLife: 24 * time.Hour, hubAddr: hubAddr}
	bln, err := net.Listen("tcp4", hostIP+":0")
	if err != nil {
		t.Fatal(err)
	}
	bsrv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{a.p.issue(t, backendgrpc.BackendURI, x509.ExtKeyUsageServerAuth)},
		ClientCAs: a.p.roots, ClientAuth: tls.VerifyClientCertIfGiven, MinVersion: tls.VersionTLS13})))
	backendv1.RegisterHubControlServer(bsrv, a.be)
	go func() { _ = bsrv.Serve(bln) }()
	t.Cleanup(bsrv.Stop)

	// Edge Hub：安装脚本（systemd 模式为真实安装：用户、单元、systemctl enable --now）
	if !a.sd {
		a.root = filepath.Join(a.dir, "root")
	}
	a.units = map[string]unit{}
	args := []string{"--binary", a.hubBin, "--ca", a.caFile, "--backend", bln.Addr().String(), "--region", "test", "--listen", hubAddr}
	if out, err := a.install("edge-hub", []string{"VORTEX_BOOTSTRAP_TOKEN=hub-token"}, args...); err != nil {
		t.Fatalf("edge-hub install: %v\n%s", err, out)
	}
	if a.sd {
		t.Cleanup(func() {
			a.purge("edge-hub.service", "/opt/vortex/edge-hub", "/var/lib/vortex-edge-hub", "/etc/vortex/edge-hub.json")
		})
		a.units["edge-hub"] = sdUnit{"edge-hub.service"}
	} else {
		a.units["edge-hub"] = localUnit{startService(t, filepath.Join(a.root, "opt/vortex/edge-hub/edge-hub"), nil, "run", "-config", filepath.Join(a.root, "etc/vortex/edge-hub.json"))}
	}
}

func (a *acceptance) install(which string, env []string, args ...string) (string, error) {
	cmd := exec.Command("sh", append([]string{filepath.Join(repoRoot(a.t), "deploy", which, "install.sh")}, args...)...)
	cmd.Env = append(append(os.Environ(), "ROOT="+a.root), env...)
	b, err := cmd.CombinedOutput()
	return string(b), err
}

// purge 停止并删除 systemd 模式安装的服务与文件。
func (a *acceptance) purge(unit string, paths ...string) {
	_ = exec.Command("systemctl", "disable", "--now", unit).Run()
	_ = os.Remove(filepath.Join("/etc/systemd/system", unit))
	_ = os.RemoveAll(filepath.Join("/etc/systemd/system", unit+".d"))
	for _, p := range paths {
		_ = os.RemoveAll(p)
	}
	_ = exec.Command("systemctl", "daemon-reload").Run()
}

// nodes：入口 E 用安装脚本注册与安装（单元加入命名空间），出口 X1、X2 用同一单元文件改写路径后安装。
func (a *acceptance) nodes() {
	t := a.t
	args := []string{"--binary", a.nodeBin, "--ca", a.caFile, "--release-key", a.relKey, "--hub", hubIP + ":7400", "--no-start"}
	var out string
	var err error
	deadline := time.Now().Add(60 * time.Second)
	for {
		if out, err = a.install("node", []string{"VORTEX_ENROLL_TOKEN=node-token"}, args...); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("node install: %v\n%s", err, out)
		}
		time.Sleep(500 * time.Millisecond)
	}
	unitSrc, err := os.ReadFile(filepath.Join(repoRoot(t), "deploy", "node", "vortex-node.service"))
	if err != nil {
		t.Fatal(err)
	}
	if a.sd {
		t.Cleanup(func() { a.purge("vortex-node.service", "/opt/vortex/node", "/var/lib/vortex", "/etc/vortex/node.json") })
		dropIn := "/etc/systemd/system/vortex-node.service.d"
		_ = os.MkdirAll(dropIn, 0o755)
		_ = os.WriteFile(filepath.Join(dropIn, "netns.conf"), []byte("[Service]\nNetworkNamespacePath=/run/netns/vxa-e\n"), 0o644)
	}
	a.startExit("x1", nodeX1, x1IP, string(unitSrc))
	a.startExit("x2", nodeX2, x2IP, string(unitSrc))
	if a.sd {
		a.sh("systemctl", "daemon-reload")
		a.sh("systemctl", "start", "vortex-node.service")
		a.units["e"] = sdUnit{"vortex-node.service"}
	} else {
		cfg := filepath.Join(a.root, "etc/vortex/node.json")
		a.units["e"] = localUnit{startServicePrefix(t, []string{"ip", "netns", "exec", "vxa-e"}, filepath.Join(a.root, "opt/vortex/node/vortex-node"),
			[]string{"upgrade-guard", "-config", cfg}, "run", "-config", cfg)}
	}
	a.waitFor("all three nodes online on 1.0.0", 60*time.Second, func() bool {
		v := a.be.versions()
		return len(v) == 3 && v[string(nodeID[:])] == "1.0.0"
	})
	a.waitFor("forwarding through both exits", 30*time.Second, func() bool { return ok(a.entry) })
	a.record("install", "edge-hub and entry via deploy/*/install.sh; exits via the same unit file")
}

// startExit 安装一个出口节点：证书直接由测试 CA 签发（入口已验证注册流程），单元由 deploy/node/vortex-node.service 改写路径。
func (a *acceptance) startExit(name string, id types.UUID, ip, unitSrc string) {
	t := a.t
	r := a.root
	bindir := filepath.Join(r, "opt/vortex/node-"+name)
	data := filepath.Join(r, "var/lib/vortex-"+name)
	cfgPath := filepath.Join(r, "etc/vortex/node-"+name+".json")
	_ = os.MkdirAll(bindir, 0o755)
	_ = os.MkdirAll(data, 0o700)
	bin := filepath.Join(bindir, "vortex-node")
	if err := os.WriteFile(bin, a.v1, 0o755); err != nil {
		t.Fatal(err)
	}
	c, k := writePEM(t, data, "node", a.p.node(t, id))
	cfg := &nodeapp.Config{NodeID: vlink.FormatUUID(id), DataDir: data, Socket: filepath.Join(r, "run/vortex-"+name, "node.sock"),
		EdgeHubs: []nodeapp.HubEntry{{HubID: vlink.FormatUUID(hubA), Address: hubIP + ":7400"}}, DiskLowBytes: 1, DiskHighBytes: 1,
		CertFile: c, KeyFile: k, CAFile: filepath.Join(r, "etc/vortex/ca.crt"), LinkListen: fmt.Sprintf("%s:%d", ip, linkPort),
		ReleaseKeyFile: filepath.Join(r, "etc/vortex/release.pub"), Binary: bin}
	b, _ := json.Marshal(cfg)
	if err := os.WriteFile(cfgPath, b, 0o640); err != nil {
		t.Fatal(err)
	}
	if !a.sd {
		a.units[name] = localUnit{startServicePrefix(t, []string{"ip", "netns", "exec", "vxa-" + name}, bin, nil, "run", "-config", cfgPath)}
		return
	}
	a.sh("chown", "-R", "vortex:vortex", bindir, data)
	a.sh("chown", "root:vortex", cfgPath)
	var lines []string
	for _, l := range strings.Split(unitSrc, "\n") {
		switch {
		case l == "StateDirectory=vortex":
			l = "StateDirectory=vortex-" + name
		case l == "RuntimeDirectory=vortex":
			l = "RuntimeDirectory=vortex-" + name
		case l == "[Service]":
			l += "\nNetworkNamespacePath=/run/netns/vxa-" + name
		}
		l = strings.ReplaceAll(l, "/opt/vortex/node", "/opt/vortex/node-"+name)
		l = strings.ReplaceAll(l, "/etc/vortex/node.json", cfgPath)
		lines = append(lines, l)
	}
	unitName := "vortex-node-" + name + ".service"
	_ = os.WriteFile(filepath.Join("/etc/systemd/system", unitName), []byte(strings.Join(lines, "\n")), 0o644)
	t.Cleanup(func() { a.purge(unitName, bindir, data, cfgPath) })
	a.sh("systemctl", "daemon-reload")
	a.sh("systemctl", "start", unitName)
	a.units[name] = sdUnit{unitName}
}

func (a *acceptance) waitFor(what string, d time.Duration, f func() bool) {
	a.t.Helper()
	deadline := time.Now().Add(d)
	for !f() {
		if time.Now().After(deadline) {
			a.t.Fatalf("timeout (%v): %s", d, what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// attempt 是一次新会话尝试。
type attempt struct {
	at time.Time
	ok bool
}

// sessions 每 every 发起一个新会话（并发），直到 stop 关闭；返回按开始时间排序的结果。
func sessions(addr netip.AddrPort, every time.Duration, stop <-chan struct{}) func() []attempt {
	var mu sync.Mutex
	var res []attempt
	var wg sync.WaitGroup
	done := make(chan struct{})
	go func() {
		defer close(done)
		tk := time.NewTicker(every)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				wg.Wait()
				return
			case <-tk.C:
				wg.Add(1)
				go func(at time.Time) {
					defer wg.Done()
					n, _ := transfer(addr, 1024, 3*time.Second)
					mu.Lock()
					res = append(res, attempt{at, n == 1024})
					mu.Unlock()
				}(time.Now())
			}
		}
	}()
	return func() []attempt {
		<-done
		sort.Slice(res, func(i, j int) bool { return res[i].at.Before(res[j].at) })
		return res
	}
}

// recoveredAt 返回此后所有尝试都成功的起点（第一次失败之后最后一次失败的下一次尝试）。
func recoveredAt(rs []attempt, from time.Time) (time.Time, int) {
	last, fails := -1, 0
	for i, r := range rs {
		if !r.at.Before(from) && !r.ok {
			last = i
			fails++
		}
	}
	if last == -1 {
		return from, 0
	}
	if last+1 < len(rs) {
		return rs[last+1].at, fails
	}
	return time.Time{}, fails
}

// failover 注入故障 inject，测量检测（PeerDown 到达后端）与恢复时间，最后执行 restore 并等待两出口都恢复。
func (a *acceptance) failover(name string, peer types.UUID, inject, restore func()) {
	t := a.t
	a.waitFor("forwarding before "+name, 30*time.Second, func() bool { return ok(a.entry) })
	stop := make(chan struct{})
	collect := sessions(a.entry, 100*time.Millisecond, stop)
	time.Sleep(2 * time.Second)
	t0 := time.Now()
	inject()
	var detected time.Time
	a.waitFor(name+": PeerDown reported", 40*time.Second, func() bool {
		detected = a.be.peerDownAfter(peer, t0)
		return !detected.IsZero()
	})
	time.Sleep(20 * time.Second) // 检测后观察 20 秒
	close(stop)
	rs := collect()
	rec, fails := recoveredAt(rs, t0)
	detect := detected.Sub(t0)
	a.record(name+".detect_s", round(detect))
	a.record(name+".failed_sessions", fails)
	a.record(name+".attempts", len(rs))
	if rec.IsZero() {
		t.Fatalf("%s: new sessions never recovered", name)
	}
	recovery := rec.Sub(t0)
	afterDetect := rec.Sub(detected)
	if afterDetect < 0 {
		afterDetect = 0 // 探测先于链路失效判定把成员移出选路
	}
	a.record(name+".recovered_s_since_fault", round(recovery))
	a.record(name+".recovered_s_after_detect", round(afterDetect))
	if detect > 15*time.Second {
		t.Errorf("NFR-016 %s: detection %v > 15s", name, detect)
	}
	if afterDetect > 15*time.Second {
		t.Errorf("NFR-017 %s: recovery %v after detection > 15s", name, afterDetect)
	}
	restore()
	a.waitFor(name+": restored", 60*time.Second, func() bool {
		for i := 0; i < 10; i++ {
			if !ok(a.entry) {
				return false
			}
		}
		return true
	})
}

func round(d time.Duration) float64 { return float64(d.Milliseconds()) / 1000 }

func (a *acceptance) linkFailure() {
	a.failover("link_failure_x1", nodeX1,
		func() { a.sh("ip", "-n", "vxa-lan", "link", "set", "p-x1", "down") },
		func() { a.sh("ip", "-n", "vxa-lan", "link", "set", "p-x1", "up") })
}

func (a *acceptance) nodeFailure() {
	a.failover("node_hang_x2", nodeX2,
		func() { a.units["x2"].signal("SIGSTOP") },
		func() { a.units["x2"].signal("SIGCONT") })
}

// upgrades：真实 systemd（Restart=always、ExecStartPre 的 upgrade-guard）下的升级与回退。
func (a *acceptance) upgrades() {
	push := func(v string, body []byte) {
		sum := sha256.Sum256(body)
		a.be.push(&backendv1.BackendMessage{Body: &backendv1.BackendMessage_NodeOperation{NodeOperation: &backendv1.NodeOperation{
			NodeId: nodewire.UUID(nodeID), Op: &backendv1.NodeOperation_Upgrade{Upgrade: &edgehubv1.UpgradeCommand{Version: v,
				DownloadUrl: a.rel + "/" + v, Sha256: sum[:], Signature: ed25519.Sign(a.priv, upgrade.SignedMessage(v, sum[:], false))}}}}})
	}
	start := time.Now()
	push("2.0.0", a.v2)
	a.waitFor("entry upgraded to 2.0.0 and healthy", 120*time.Second, func() bool {
		return a.be.upgraded("2.0.0", edgehubv1.UpgradePhase_UPGRADE_PHASE_HEALTHY, "") && a.be.versions()[string(nodeID[:])] == "2.0.0"
	})
	a.record("upgrade.to_healthy_s", round(time.Since(start)))
	a.waitFor("forwarding after upgrade", 30*time.Second, func() bool { return ok(a.entry) })
	start = time.Now()
	push("4.0.0", a.v4)
	a.waitFor("crashing 4.0.0 rolled back by the guard", 180*time.Second, func() bool {
		return a.be.upgraded("4.0.0", edgehubv1.UpgradePhase_UPGRADE_PHASE_ROLLED_BACK, "3 starts") && a.be.versions()[string(nodeID[:])] == "2.0.0"
	})
	a.record("upgrade.rollback_s", round(time.Since(start)))
	a.waitFor("forwarding after rollback", 30*time.Second, func() bool { return ok(a.entry) })
}

// hubLoss：E 与全部 Edge Hub 失联后继续转发，最迟在运行授权（默认 5 分钟）到期时停止（NFR-018）。
func (a *acceptance) hubLoss() {
	t := a.t
	a.waitFor("forwarding before hub loss", 30*time.Second, func() bool { return ok(a.entry) })
	t0 := time.Now()
	a.sh("ip", "-n", "vxa-e", "route", "add", "blackhole", hubIP+"/32")
	defer func() { _ = exec.Command("ip", "-n", "vxa-e", "route", "del", "blackhole", hubIP+"/32").Run() }()
	time.Sleep(60 * time.Second)
	if !ok(a.entry) {
		t.Errorf("NFR-018: forwarding stopped within 60s of losing all Edge Hubs (should continue until grants expire)")
	}
	stopped := time.Duration(-1)
	for time.Since(t0) < 330*time.Second {
		if !ok(a.entry) && !ok(a.entry) {
			stopped = time.Since(t0)
			break
		}
		time.Sleep(time.Second)
	}
	if stopped < 0 {
		t.Fatalf("NFR-018: still forwarding %v after losing all Edge Hubs", time.Since(t0).Round(time.Second))
	}
	a.record("hub_loss.stopped_s", round(stopped))
	if stopped > 300*time.Second {
		t.Errorf("NFR-018: stopped after %v > 5 min", stopped)
	}
}
