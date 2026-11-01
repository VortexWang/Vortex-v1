package e2e

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	hubapp "vortex/edgehub/app"
	"vortex/edgehub/backendgrpc"
	"vortex/engine/vlink"
	"vortex/node/agent/upgrade"
	nodewire "vortex/node/agent/wire"
	nodeapp "vortex/node/app"
	backendv1 "vortex/proto/gen/go/vortex/backend/v1"
	edgehubv1 "vortex/proto/gen/go/vortex/edgehub/v1"
)

// buildNode 构建 vortex-node（版本由 -ldflags 注入）。
func buildNode(t *testing.T, out, version string, ldflags ...string) []byte {
	t.Helper()
	flags := append([]string{"-X vortex/node/app.Version=" + version}, ldflags...)
	cmd := exec.Command("go", "build", "-o", out, "-ldflags", strings.Join(flags, " "), "vortex/node/cmd/vortex-node")
	cmd.Env = append(os.Environ(), "GOTOOLCHAIN=local", "CGO_ENABLED=0")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", version, err, b)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// service 模拟 systemd（Restart=always）：程序退出后 100 毫秒以同样的参数重新启动安装路径上的程序；
// 每次启动前像 deploy/node/vortex-node.service 的 ExecStartPre 一样执行 `<程序>.prev upgrade-guard`（失败忽略）。
type service struct {
	mu    sync.Mutex
	exits []int
	logs  bytes.Buffer
	cmd   *exec.Cmd
	stop  bool
	done  chan struct{}

	guardArgs []string
}

func startService(t *testing.T, bin string, guard []string, args ...string) *service {
	s := &service{done: make(chan struct{}), guardArgs: guard}
	go func() {
		defer close(s.done)
		for {
			s.mu.Lock()
			if s.stop {
				s.mu.Unlock()
				return
			}
			if _, err := os.Stat(bin + ".prev"); err == nil && len(s.guardArgs) > 0 {
				g := exec.Command(bin+".prev", s.guardArgs...)
				g.Stderr = &lockedWriter{mu: &s.mu, w: &s.logs}
				s.mu.Unlock()
				_ = g.Run()
				s.mu.Lock()
			}
			cmd := exec.Command(bin, args...)
			cmd.Stderr = &lockedWriter{mu: &s.mu, w: &s.logs}
			err := cmd.Start()
			s.cmd = cmd
			s.mu.Unlock()
			code := 0
			if err == nil {
				err = cmd.Wait()
			}
			var ee *exec.ExitError
			if errors.As(err, &ee) {
				code = ee.ExitCode()
			} else if err != nil {
				code = -1
			}
			s.mu.Lock()
			s.exits = append(s.exits, code)
			s.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
		}
	}()
	t.Cleanup(func() {
		s.mu.Lock()
		s.stop = true
		if s.cmd != nil && s.cmd.Process != nil {
			_ = s.cmd.Process.Signal(os.Interrupt)
		}
		s.mu.Unlock()
		select {
		case <-s.done:
		case <-time.After(20 * time.Second):
			t.Error("node did not stop")
		}
		if t.Failed() {
			t.Logf("node exits %v\nlogs:\n%s", s.exitCodes(), s.log())
		}
	})
	return s
}

type lockedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

func (s *service) exitCodes() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int(nil), s.exits...)
}

func (s *service) log() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.logs.String()
}

// upgradeLogged 报告节点日志中是否出现某个升级阶段。
func (s *service) upgradeLogged(target string, phase upgrade.Phase) bool {
	for _, line := range strings.Split(s.log(), "\n") {
		var e struct{ Msg, Target, Phase string }
		if json.Unmarshal([]byte(line), &e) == nil && e.Msg == "upgrade" && e.Target == target && e.Phase == phase.String() {
			return true
		}
	}
	return false
}

func onlineVersion(be *protoBackend) string {
	for _, v := range be.versions() {
		return v
	}
	return ""
}

// 升级与回退（AGT-008、AGT-T13）：真实的 vortex-node 进程在模拟 systemd 下运行，经 edge-hub 接入模拟后端。
// ① 后端下发 2.0.0：下载、校验签名与摘要、执行新程序读取兼容信息、排空、保留 .prev、原子替换、以退出码 75
// 请求重启；新版本连接 Edge Hub 并恢复配置后报告健康，转发恢复。② 下发 3.0.0（每次启动都在启动检查后崩溃）：
// 连续 3 次启动未健康，第 4 次启动前由上一版本的 upgrade-guard（ExecStartPre）回退到 2.0.0。④ 下发 4.0.0（一启动就崩溃，
// 走不到自己的启动计数）：同样由 Guard 回退。
// 连续 3 次启动未健康，第 4 次启动时回退到 2.0.0 并重启，旧版本报告已回退。③ 签名错误的指令被拒绝，程序不变。
func TestUpgradeAndRollback(t *testing.T) {
	if testing.Short() {
		t.Skip("builds binaries")
	}
	dir := t.TempDir()
	v1 := buildNode(t, filepath.Join(dir, "build", "v1"), "1.0.0")
	v2 := buildNode(t, filepath.Join(dir, "build", "v2"), "2.0.0")
	v3 := buildNode(t, filepath.Join(dir, "build", "v3"), "3.0.0", "-X vortex/node/app.faultCrash=after-upgrade-check")
	v4 := buildNode(t, filepath.Join(dir, "build", "v4"), "4.0.0", "-X vortex/node/app.faultCrash=at-start")

	// 发布：Ed25519 签名，公钥随安装包分发
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	der, _ := x509.MarshalPKIXPublicKey(pub)
	keyFile := filepath.Join(dir, "release.pub")
	_ = os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}), 0o644)
	if k, err := nodeapp.LoadReleaseKey(keyFile); err != nil || !k.Equal(pub) {
		t.Fatalf("release key %v", err)
	}
	pkgs := map[string][]byte{"/2.0.0": v2, "/3.0.0": v3, "/4.0.0": v4}
	rel := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, ok := pkgs[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(b)
	}))
	defer rel.Close()
	command := func(version string, body []byte, signer ed25519.PrivateKey) *backendv1.BackendMessage {
		sum := sha256.Sum256(body)
		return &backendv1.BackendMessage{Body: &backendv1.BackendMessage_NodeOperation{NodeOperation: &backendv1.NodeOperation{
			NodeId: nodewire.UUID(nodeID), Op: &backendv1.NodeOperation_Upgrade{Upgrade: &edgehubv1.UpgradeCommand{Version: version,
				DownloadUrl: rel.URL + "/" + version, Sha256: sum[:], Signature: ed25519.Sign(signer, upgrade.SignedMessage(version, sum[:], false))}}}}}
	}

	// 后端与 Edge Hub
	p := newPKI(t)
	caFile := filepath.Join(dir, "ca.crt")
	_ = os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: p.ca.Raw}), 0o600)
	cfg := config(t)
	addr := cfg.Listeners[0].Bind
	be := &protoBackend{cfg: cfg, pushed: map[string]bool{}}
	bln, _ := net.Listen("tcp4", "127.0.0.1:0")
	bsrv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{p.issue(t, backendgrpc.BackendURI, x509.ExtKeyUsageServerAuth)},
		ClientCAs: p.roots, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13})))
	backendv1.RegisterHubControlServer(bsrv, be)
	go func() { _ = bsrv.Serve(bln) }()
	defer bsrv.Stop()
	hc, hk := writePEM(t, dir, "hub", p.hub(t, hubA))
	hubCfg := &hubapp.Config{HubID: vlink.FormatUUID(hubA), Region: "test", DataDir: filepath.Join(dir, "hub"), Listen: "127.0.0.1:0",
		CertFile: hc, KeyFile: hk, CAFile: caFile, BackendAddress: bln.Addr().String()}
	ctx, cancel := context.WithCancel(context.Background())
	listen := make(chan string, 1)
	hubDone := make(chan error, 1)
	go func() {
		hubDone <- hubapp.Run(ctx, hubCfg, slog.New(slog.NewTextHandler(io.Discard, nil)), hubapp.Options{OnListen: func(a string) { listen <- a }})
	}()
	defer func() { cancel(); <-hubDone }()
	hubAddr := <-listen

	// 安装 1.0.0 并以服务方式运行
	bin := filepath.Join(dir, "opt", "vortex-node")
	_ = os.MkdirAll(filepath.Dir(bin), 0o755)
	_ = os.WriteFile(bin, v1, 0o755)
	nc, nk := writePEM(t, dir, "node", p.node(t, nodeID))
	nodeCfg := &nodeapp.Config{NodeID: vlink.FormatUUID(nodeID), DataDir: filepath.Join(dir, "node"), Socket: filepath.Join(dir, "node.sock"),
		EdgeHubs: []nodeapp.HubEntry{{HubID: vlink.FormatUUID(hubA), Address: hubAddr}}, DiskLowBytes: 1, DiskHighBytes: 1,
		CertFile: nc, KeyFile: nk, CAFile: caFile, ReleaseKeyFile: keyFile, Binary: bin}
	cb, _ := json.Marshal(nodeCfg)
	cfgPath := filepath.Join(dir, "node.json")
	_ = os.WriteFile(cfgPath, cb, 0o600)
	svc := startService(t, bin, []string{"upgrade-guard", "-config", cfgPath}, "run", "-config", cfgPath)

	long := func(what string, f func() bool) {
		t.Helper()
		deadline := time.Now().Add(60 * time.Second)
		for !f() {
			if time.Now().After(deadline) {
				t.Fatalf("timeout: %s", what)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	long("1.0.0 online and forwarding", func() bool { return onlineVersion(be) == "1.0.0" && ok(addr) })

	// ③ 签名不是平台发布密钥：拒绝，程序不变、不重启
	_, other, _ := ed25519.GenerateKey(rand.Reader)
	be.push(command("2.0.0", v2, other))
	long("bad signature reported to the backend", func() bool {
		return be.upgraded("2.0.0", edgehubv1.UpgradePhase_UPGRADE_PHASE_FAILED, "signature")
	})
	if b, _ := os.ReadFile(bin); !bytes.Equal(b, v1) || len(svc.exitCodes()) != 0 || !ok(addr) {
		t.Fatal("unsigned upgrade changed the node")
	}

	// ① 升级到 2.0.0
	be.push(command("2.0.0", v2, priv))
	long("2.0.0 online", func() bool { return onlineVersion(be) == "2.0.0" })
	long("2.0.0 healthy, reported to the backend", func() bool {
		return svc.upgradeLogged("2.0.0", upgrade.PhaseHealthy) && be.upgraded("2.0.0", edgehubv1.UpgradePhase_UPGRADE_PHASE_HEALTHY, "")
	})
	for _, ph := range []edgehubv1.UpgradePhase{edgehubv1.UpgradePhase_UPGRADE_PHASE_DOWNLOADING, edgehubv1.UpgradePhase_UPGRADE_PHASE_DRAINING,
		edgehubv1.UpgradePhase_UPGRADE_PHASE_SWITCHED} {
		if !be.upgraded("2.0.0", ph, "") {
			t.Fatalf("backend did not see phase %v", ph)
		}
	}
	long("forwarding after upgrade", func() bool { return ok(addr) })
	if b, _ := os.ReadFile(bin); !bytes.Equal(b, v2) {
		t.Fatal("binary not replaced")
	}
	if b, _ := os.ReadFile(bin + ".prev"); !bytes.Equal(b, v1) {
		t.Fatal("previous version not kept")
	}
	if ex := svc.exitCodes(); len(ex) != 1 || ex[0] != 75 {
		t.Fatalf("exit codes %v, want [75]", ex)
	}

	// ② 升级到每次启动都崩溃的 3.0.0：回退到 2.0.0
	be.push(command("3.0.0", v3, priv))
	long("rolled back to 2.0.0", func() bool {
		return be.upgraded("3.0.0", edgehubv1.UpgradePhase_UPGRADE_PHASE_ROLLED_BACK, "3 starts") && onlineVersion(be) == "2.0.0"
	})
	long("forwarding after rollback", func() bool { return ok(addr) })
	if b, _ := os.ReadFile(bin); !bytes.Equal(b, v2) {
		t.Fatal("binary not restored")
	}
	// 2.0.0 → 3.0.0（75）→ 崩溃 ×3 → 第 4 次启动前 Guard 回退，2.0.0 直接启动
	checkCrashes := func(ex []int) {
		t.Helper()
		if len(ex) != 4 || ex[0] != 75 || ex[1] == 75 || ex[2] == 75 || ex[3] == 75 || ex[1] == 0 {
			t.Fatalf("exit codes %v, want [75 crash crash crash]", ex)
		}
	}
	checkCrashes(svc.exitCodes()[1:])

	// ④ 升级到一启动就崩溃的 4.0.0（走不到新版本自己的启动计数）：上一版本的 Guard 计数并回退
	be.push(command("4.0.0", v4, priv))
	long("4.0.0 rolled back by the guard", func() bool {
		return be.upgraded("4.0.0", edgehubv1.UpgradePhase_UPGRADE_PHASE_ROLLED_BACK, "3 starts") && onlineVersion(be) == "2.0.0"
	})
	long("forwarding after the second rollback", func() bool { return ok(addr) })
	if b, _ := os.ReadFile(bin); !bytes.Equal(b, v2) {
		t.Fatal("binary not restored")
	}
	checkCrashes(svc.exitCodes()[5:])
}
