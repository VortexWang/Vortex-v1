package e2e

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	hubapp "vortex/edgehub/app"
	"vortex/edgehub/backendgrpc"
	"vortex/engine/vlink"
	nodeapp "vortex/node/app"
	backendv1 "vortex/proto/gen/go/vortex/backend/v1"
)

func buildHub(t *testing.T, out string) {
	t.Helper()
	cmd := exec.Command("go", "build", "-o", out, "vortex/edgehub/cmd/edge-hub")
	cmd.Env = append(os.Environ(), "GOTOOLCHAIN=local", "CGO_ENABLED=0")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build edge-hub: %v\n%s", err, b)
	}
}

func repoRoot(t *testing.T) string {
	wd, _ := os.Getwd()
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

func install(t *testing.T, script, root string, env []string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command("sh", append([]string{script}, args...)...)
	cmd.Env = append(append(os.Environ(), "ROOT="+root), env...)
	b, err := cmd.CombinedOutput()
	return string(b), err
}

func mode(t *testing.T, path string) os.FileMode {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Mode().Perm()
}

// 部署（AGT-T17、HUB-T17）：用 deploy/ 下的安装脚本把 edge-hub 与 vortex-node 安装到一个目录树（ROOT 模式），
// 分别用引导令牌、注册令牌完成身份获取，再按 systemd 单元中的命令行把安装好的程序作为真实进程运行：
// 节点经 edge-hub 接入后端并转发流量。重复安装只更新程序，不重新注册；令牌不写入任何文件。
func TestInstallScripts(t *testing.T) {
	if testing.Short() {
		t.Skip("builds binaries")
	}
	testT = t
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	repo := repoRoot(t)
	hubBin, nodeBin := filepath.Join(dir, "build", "edge-hub"), filepath.Join(dir, "build", "vortex-node")
	buildHub(t, hubBin)
	buildNode(t, nodeBin, "1.0.0")

	p := newPKI(t)
	caFile := filepath.Join(dir, "ca.crt")
	_ = os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: p.ca.Raw}), 0o644)
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	der, _ := x509.MarshalPKIXPublicKey(pub)
	relKey := filepath.Join(dir, "release.pub")
	_ = os.WriteFile(relKey, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}), 0o644)

	cfg := config(t)
	addr := cfg.Listeners[0].Bind
	hubListen := freePort(t).String()
	be := &enrollBackend{protoBackend: &protoBackend{cfg: cfg, pushed: map[string]bool{}}, p: p, nodeLife: time.Hour, hubAddr: hubListen}
	bln, _ := net.Listen("tcp4", "127.0.0.1:0")
	bsrv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{p.issue(t, backendgrpc.BackendURI, x509.ExtKeyUsageServerAuth)},
		ClientCAs: p.roots, ClientAuth: tls.VerifyClientCertIfGiven, MinVersion: tls.VersionTLS13})))
	backendv1.RegisterHubControlServer(bsrv, be)
	go func() { _ = bsrv.Serve(bln) }()
	defer bsrv.Stop()

	// Edge Hub：参数错误、令牌错误时失败且不留下配置
	hubScript := filepath.Join(repo, "deploy", "edge-hub", "install.sh")
	if out, err := install(t, hubScript, root, nil, "--binary", hubBin, "--ca", caFile); err == nil || !strings.Contains(out, "required") {
		t.Fatalf("install without token: %v %s", err, out)
	}
	hubArgs := []string{"--binary", hubBin, "--ca", caFile, "--backend", bln.Addr().String(), "--region", "test", "--listen", hubListen}
	if _, err := install(t, hubScript, root, []string{"VORTEX_BOOTSTRAP_TOKEN=wrong"}, hubArgs...); err == nil {
		t.Fatal("bad bootstrap token accepted")
	}
	hubCfgPath := filepath.Join(root, "etc", "vortex", "edge-hub.json")
	if _, err := os.Stat(hubCfgPath); err == nil {
		t.Fatal("config left after a failed enrollment")
	}
	if out, err := install(t, hubScript, root, []string{"VORTEX_BOOTSTRAP_TOKEN=hub-token"}, hubArgs...); err != nil {
		t.Fatalf("edge-hub install: %v\n%s", err, out)
	}
	hubCfg, err := hubapp.LoadConfig(hubCfgPath)
	if err != nil || hubCfg.HubID != vlink.FormatUUID(hubA) || !strings.HasPrefix(hubCfg.DataDir, root) {
		t.Fatalf("hub config %+v %v", hubCfg, err)
	}
	if mode(t, filepath.Join(root, "var", "lib", "vortex-edge-hub")) != 0o700 || mode(t, hubCfg.KeyFile) != 0o600 ||
		mode(t, filepath.Join(root, "opt", "vortex", "edge-hub", "edge-hub")) != 0o755 {
		t.Fatal("hub file modes")
	}
	// 按单元的 ExecStart 运行
	hubSvc := startService(t, filepath.Join(root, "opt", "vortex", "edge-hub", "edge-hub"), nil, "run", "-config", hubCfgPath)
	_ = hubSvc

	// 节点：Edge Hub 连上后端之前注册会失败，安装脚本可重试
	nodeScript := filepath.Join(repo, "deploy", "node", "install.sh")
	nodeArgs := []string{"--binary", nodeBin, "--ca", caFile, "--release-key", relKey, "--hub", hubListen, "--link-listen", "127.0.0.1:0"}
	if _, err := install(t, nodeScript, root, []string{"VORTEX_ENROLL_TOKEN=nope"}, nodeArgs...); err == nil {
		t.Fatal("bad enrollment token accepted")
	}
	var out string
	deadline := time.Now().Add(30 * time.Second)
	for {
		out, err = install(t, nodeScript, root, []string{"VORTEX_ENROLL_TOKEN=node-token"}, nodeArgs...)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("node install: %v\n%s", err, out)
		}
		time.Sleep(200 * time.Millisecond)
	}
	nodeCfgPath := filepath.Join(root, "etc", "vortex", "node.json")
	nodeCfg, err := nodeapp.LoadConfig(nodeCfgPath)
	installed := filepath.Join(root, "opt", "vortex", "node", "vortex-node")
	if err != nil || nodeCfg.NodeID != vlink.FormatUUID(nodeID) || nodeCfg.Binary != installed || nodeCfg.LinkListen != "127.0.0.1:0" ||
		nodeCfg.ReleaseKeyFile != filepath.Join(root, "etc", "vortex", "release.pub") || nodeCfg.Socket != filepath.Join(root, "run", "vortex", "node.sock") {
		t.Fatalf("node config %+v %v", nodeCfg, err)
	}
	if mode(t, filepath.Join(root, "var", "lib", "vortex")) != 0o700 || mode(t, nodeCfg.KeyFile) != 0o600 || mode(t, installed) != 0o755 {
		t.Fatal("node file modes")
	}
	for _, f := range []string{hubCfgPath, nodeCfgPath} {
		if b, _ := os.ReadFile(f); containsToken(b) {
			t.Fatalf("token written to %s", f)
		}
	}
	nodeSvc := startService(t, installed, []string{"upgrade-guard", "-config", nodeCfgPath}, "run", "-config", nodeCfgPath)
	eventually30 := func(what string, f func() bool) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for !f() {
			if time.Now().After(deadline) {
				t.Fatalf("timeout: %s\nnode log:\n%s\nhub log:\n%s", what, nodeSvc.log(), hubSvc.log())
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	eventually30("installed node forwarding through the installed edge-hub", func() bool { return ok(addr) })

	// 重复安装：只更新程序，不重新注册（不需要令牌），配置不变
	before, _ := os.ReadFile(nodeCfgPath)
	out, err = install(t, nodeScript, root, nil, "--binary", nodeBin, "--ca", caFile, "--release-key", relKey)
	if err != nil || !strings.Contains(out, "already enrolled") {
		t.Fatalf("reinstall: %v %s", err, out)
	}
	if after, _ := os.ReadFile(nodeCfgPath); string(after) != string(before) {
		t.Fatal("reinstall changed the config")
	}
	if !ok(addr) {
		t.Fatal("forwarding after reinstall")
	}
	var st struct {
		Connected bool `json:"connected"`
	}
	b, err := exec.Command(installed, "status", "-socket", nodeCfg.Socket).Output()
	if err != nil || json.Unmarshal(b, &st) != nil || !st.Connected {
		t.Fatalf("status %s %v", b, err)
	}
}
