package e2e

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	hubapp "vortex/edgehub/app"
	"vortex/edgehub/backendgrpc"
	"vortex/engine/types"
	"vortex/engine/vlink"
	"vortex/node/agent"
	"vortex/node/agent/certs"
	"vortex/node/agent/control"
	"vortex/node/agent/grpcconn"
	"vortex/node/agent/localapi"
	nodewire "vortex/node/agent/wire"
	nodeapp "vortex/node/app"
	backendv1 "vortex/proto/gen/go/vortex/backend/v1"
	edgehubv1 "vortex/proto/gen/go/vortex/edgehub/v1"
)

// enrollBackend 在 protoBackend 之上实现平台 CA：引导令牌换实例证书，注册令牌换节点证书，按 node_id 续签。
type enrollBackend struct {
	*protoBackend
	p        *pki
	hubAddr  string
	nodeLife time.Duration // 首次签发的节点证书有效期（短，用于触发续签）
	hubLife  time.Duration // 实例证书有效期（0 = 1 小时）

	mu          sync.Mutex
	renewals    int
	hubRenewals int
	remotes     []string
}

func (b *enrollBackend) sign(t *testing.T, csrDER []byte, uri string, life time.Duration, usage ...x509.ExtKeyUsage) ([]byte, error) {
	csr, err := x509.ParseCertificateRequest(csrDER)
	if err != nil || csr.CheckSignature() != nil {
		return nil, status.Error(codes.InvalidArgument, "bad csr")
	}
	u, _ := url.Parse(uri)
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	tpl := &x509.Certificate{SerialNumber: serial, NotBefore: time.Now().Add(-time.Second), NotAfter: time.Now().Add(life),
		URIs: []*url.URL{u}, ExtKeyUsage: usage, KeyUsage: x509.KeyUsageDigitalSignature}
	return x509.CreateCertificate(rand.Reader, tpl, b.p.ca, csr.PublicKey, b.p.key)
}

var testT *testing.T

func (b *enrollBackend) EnrollHub(_ context.Context, r *backendv1.EnrollHubRequest) (*backendv1.EnrollHubResponse, error) {
	if r.BootstrapToken != "hub-token" {
		return nil, status.Error(codes.PermissionDenied, "bad bootstrap token")
	}
	der, err := b.sign(testT, r.CsrDer, grpcconn.HubURI(hubA), b.hubLifetime(), x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth)
	if err != nil {
		return nil, err
	}
	return &backendv1.EnrollHubResponse{HubId: nodewire.UUID(hubA), CertificateChainDer: [][]byte{der}, CaBundleDer: [][]byte{b.p.ca.Raw}}, nil
}

func (b *enrollBackend) EnrollNode(_ context.Context, r *backendv1.EnrollNodeRequest) (*edgehubv1.EnrollResponse, error) {
	if r.GetRequest().GetEnrollToken() == "mismatch-token" { // 响应中的 node_id 与证书不符
		der, _ := b.sign(testT, r.Request.CsrDer, vlink.NodeURI(nodeID), b.nodeLife, x509.ExtKeyUsageClientAuth)
		return &edgehubv1.EnrollResponse{NodeId: nodewire.UUID(types.UUID{0x99}), CertificateChainDer: [][]byte{der}, CaBundleDer: [][]byte{b.p.ca.Raw}}, nil
	}
	if r.GetRequest().GetEnrollToken() != "node-token" {
		return nil, status.Error(codes.PermissionDenied, "bad enrollment token")
	}
	b.mu.Lock()
	b.remotes = append(b.remotes, r.ObservedRemoteAddress)
	b.mu.Unlock()
	der, err := b.sign(testT, r.Request.CsrDer, vlink.NodeURI(nodeID), b.nodeLife, x509.ExtKeyUsageClientAuth)
	if err != nil {
		return nil, err
	}
	return &edgehubv1.EnrollResponse{NodeId: nodewire.UUID(nodeID), CertificateChainDer: [][]byte{der}, CaBundleDer: [][]byte{b.p.ca.Raw},
		EdgeHubs: []*edgehubv1.HubEndpoint{{HubId: nodewire.UUID(hubA), Address: b.hubAddr, Region: "test"}}}, nil
}

func (b *enrollBackend) RenewNodeCert(_ context.Context, r *backendv1.RenewNodeCertRequest) (*edgehubv1.RenewCertResponse, error) {
	id, err := nodewire.FromUUID(r.NodeId)
	if err != nil {
		return nil, err
	}
	der, err := b.sign(testT, r.CsrDer, vlink.NodeURI(id), time.Hour, x509.ExtKeyUsageClientAuth)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	b.renewals++
	b.mu.Unlock()
	return &edgehubv1.RenewCertResponse{CertificateChainDer: [][]byte{der}}, nil
}

func (b *enrollBackend) hubLifetime() time.Duration {
	if b.hubLife == 0 {
		return time.Hour
	}
	return b.hubLife
}

// RenewHubCert（CC-02）：身份取自 mTLS 客户端证书（须为平台 CA 签发的有效实例证书），签发同一 hub_id。
func (b *enrollBackend) RenewHubCert(ctx context.Context, r *edgehubv1.RenewCertRequest) (*edgehubv1.RenewCertResponse, error) {
	pr, ok := peer.FromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "no peer")
	}
	ti, ok := pr.AuthInfo.(credentials.TLSInfo)
	if !ok || len(ti.State.VerifiedChains) == 0 {
		return nil, status.Error(codes.Unauthenticated, "no verified client certificate")
	}
	leaf := ti.State.VerifiedChains[0][0]
	if len(leaf.URIs) != 1 || !strings.HasPrefix(leaf.URIs[0].String(), "spiffe://vortex/hub/") {
		return nil, status.Error(codes.PermissionDenied, "not a hub certificate")
	}
	der, err := b.sign(testT, r.CsrDer, leaf.URIs[0].String(), b.hubLifetime(), x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	b.hubRenewals++
	b.mu.Unlock()
	return &edgehubv1.RenewCertResponse{CertificateChainDer: [][]byte{der}}, nil
}

func serial(t *testing.T, certFile string) *big.Int {
	b, err := os.ReadFile(certFile)
	if err != nil {
		t.Fatal(err)
	}
	blk, _ := pem.Decode(b)
	c, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return c.SerialNumber
}

// AGT-001、AGT-012、HUB-T03、HUB-T19：实例用引导令牌换证书 → 节点用注册令牌经 Edge Hub 换证书（私钥只在本地）
// → 节点以注册得到的身份接入并转发 → 证书剩余 1/3 时经 Edge Hub 续签（新私钥、节点 ID 不变、文件原子替换）。
// 令牌错误被拒绝；Edge Hub 自治时不接受注册。
func TestEnrollmentAndRenewal(t *testing.T) {
	testT = t
	p := newPKI(t)
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.crt")
	_ = os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: p.ca.Raw}), 0o600)
	cfg := config(t)
	addr := cfg.Listeners[0].Bind

	be := &enrollBackend{protoBackend: &protoBackend{cfg: cfg, pushed: map[string]bool{}}, p: p, nodeLife: 3 * time.Second}
	bln, _ := net.Listen("tcp4", "127.0.0.1:0")
	bsrv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{p.issue(t, backendgrpc.BackendURI, x509.ExtKeyUsageServerAuth)},
		ClientCAs: p.roots, ClientAuth: tls.VerifyClientCertIfGiven, MinVersion: tls.VersionTLS13})))
	backendv1.RegisterHubControlServer(bsrv, be)
	go func() { _ = bsrv.Serve(bln) }()
	defer bsrv.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 实例引导
	hp := hubapp.EnrollParams{Token: "wrong", Backend: bln.Addr().String(), CAFile: caFile, Region: "test", Listen: "127.0.0.1:0",
		DataDir: filepath.Join(dir, "hub"), ConfigPath: filepath.Join(dir, "hub.json")}
	if _, err := hubapp.Enroll(ctx, hp); err == nil {
		t.Fatal("bad bootstrap token accepted")
	}
	hp.Token = "hub-token"
	hubCfg, err := hubapp.Enroll(ctx, hp)
	if err != nil {
		t.Fatal(err)
	}
	if fi, _ := os.Stat(hubCfg.KeyFile); fi.Mode().Perm() != 0o600 {
		t.Fatalf("hub key mode %v", fi.Mode().Perm())
	}
	loadedHub, err := hubapp.LoadConfig(hp.ConfigPath)
	if err != nil || loadedHub.HubID != vlink.FormatUUID(hubA) {
		t.Fatalf("hub config %+v %v", loadedHub, err)
	}
	listen := make(chan string, 1)
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	hubDone := make(chan error, 1)
	go func() {
		hubDone <- hubapp.Run(ctx, loadedHub, quiet, hubapp.Options{OnListen: func(a string) { listen <- a }})
	}()
	be.hubAddr = <-listen

	// 节点注册（经 Edge Hub 转发到后端）
	np := nodeapp.EnrollParams{Token: "nope", Hub: be.hubAddr, CAFile: caFile, DataDir: filepath.Join(dir, "node"),
		ConfigPath: filepath.Join(dir, "node.json"), Socket: filepath.Join(dir, "node.sock")}
	eventually(t, "hub online", func() bool {
		_, err := nodeapp.Enroll(ctx, np)
		return status.Code(err) == codes.PermissionDenied // 后端已连接，令牌错误被后端拒绝
	})
	np.Token = "mismatch-token"
	if _, err := nodeapp.Enroll(ctx, np); err == nil {
		t.Fatal("response node_id that differs from the certificate accepted")
	}
	np.Token = "node-token"
	nodeCfg, err := nodeapp.Enroll(ctx, np)
	if err != nil {
		t.Fatal(err)
	}
	if nodeCfg.NodeID != vlink.FormatUUID(nodeID) || len(nodeCfg.EdgeHubs) != 1 || nodeCfg.EdgeHubs[0].Address != be.hubAddr {
		t.Fatalf("node config %+v", nodeCfg)
	}
	if fi, _ := os.Stat(nodeCfg.KeyFile); fi.Mode().Perm() != 0o600 {
		t.Fatalf("node key mode %v", fi.Mode().Perm())
	}
	if b, _ := os.ReadFile(np.ConfigPath); len(b) == 0 || containsToken(b) {
		t.Fatal("token written to the config")
	}
	be.mu.Lock()
	remote := be.remotes[len(be.remotes)-1]
	be.mu.Unlock()
	if remote == "" {
		t.Fatal("hub did not forward the observed remote address")
	}
	first := serial(t, nodeCfg.CertFile)

	// 以注册得到的身份运行，并在剩余 1/3 时续签
	loaded, err := nodeapp.LoadConfig(np.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	loaded.SetRenewEvery(200 * time.Millisecond)
	dialer, err := nodeapp.Dialer(loaded)
	if err != nil {
		t.Fatal(err)
	}
	nodeDone := make(chan error, 1)
	go func() { nodeDone <- nodeapp.Run(ctx, loaded, dialer, quiet) }()
	defer func() {
		cancel()
		<-nodeDone
		<-hubDone
	}()
	eventually(t, "forwarding with the enrolled identity", func() bool { return ok(addr) })
	eventually(t, "certificate renewed", func() bool { return serial(t, nodeCfg.CertFile).Cmp(first) != 0 })
	if _, err := os.Stat(nodeCfg.KeyFile + ".prev"); err != nil {
		t.Fatal("previous key not kept for crash recovery")
	}
	renewed, err := certs.Load(nodeCfg.CertFile, nodeCfg.KeyFile)
	if err != nil {
		t.Fatalf("renewed certificate and key do not match: %v", err)
	}
	if id, _ := vlink.NodeIDFromCert(mustLeaf(t, renewed)); id != nodeID {
		t.Fatal("renewal changed the node id")
	}
	// 原证书过期后，节点仍能建立新的控制连接（使用续签后的证书）
	time.Sleep(3 * time.Second)
	st, err := localapi.GetStatus(context.Background(), loaded.Socket)
	if err != nil || !st.Connected {
		t.Fatalf("status %+v %v", st, err)
	}
	if !ok(addr) {
		t.Fatal("forwarding after the original certificate expired")
	}
	conn, err := dialer.Dial(ctx, control.Endpoint{ID: hubA, Address: be.hubAddr})
	if err != nil {
		t.Fatalf("new control connection after the original certificate expired: %v", err)
	}
	defer conn.Close()
	if err := conn.Send(agent.Hello{Node: nodeID, Boot: types.UUID{0x5e}}); err != nil {
		t.Fatal(err)
	}
	rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
	defer rcancel()
	if m, err := conn.Recv(rctx); err != nil {
		t.Fatalf("hub refused the renewed certificate: %v", err)
	} else if _, isHello := m.(agent.HubHello); !isHello {
		t.Fatalf("first message %T", m)
	}
}

func mustLeaf(t *testing.T, c tls.Certificate) *x509.Certificate {
	l, err := x509.ParseCertificate(c.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func containsToken(b []byte) bool {
	for _, tok := range []string{"node-token", "hub-token"} {
		if string(b) != "" && len(b) >= len(tok) {
			for i := 0; i+len(tok) <= len(b); i++ {
				if string(b[i:i+len(tok)]) == tok {
					return true
				}
			}
		}
	}
	return false
}

// 实例证书续签（C2 变更 CC-02）：实例证书有效期 4 秒，剩余 1/3 时经 RenewHubCert 续签（新私钥、hub_id 不变、
// 文件原子替换）；原证书过期后，节点仍能与 Edge Hub 建立新的控制连接（服务端证书热更新），后端重启后
// Edge Hub 以新证书重新连接（客户端证书热更新），节点照常转发。
func TestHubCertRenewal(t *testing.T) {
	testT = t
	p := newPKI(t)
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.crt")
	_ = os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: p.ca.Raw}), 0o600)
	cfg := config(t)
	addr := cfg.Listeners[0].Bind
	be := &enrollBackend{protoBackend: &protoBackend{cfg: cfg, pushed: map[string]bool{}}, p: p, nodeLife: time.Hour, hubLife: 4 * time.Second}
	backendTLS := credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{p.issue(t, backendgrpc.BackendURI, x509.ExtKeyUsageServerAuth)},
		ClientCAs: p.roots, ClientAuth: tls.VerifyClientCertIfGiven, MinVersion: tls.VersionTLS13})
	bln, _ := net.Listen("tcp4", "127.0.0.1:0")
	baddr := bln.Addr().String()
	bsrv := grpc.NewServer(grpc.Creds(backendTLS))
	backendv1.RegisterHubControlServer(bsrv, be)
	go func() { _ = bsrv.Serve(bln) }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hp := hubapp.EnrollParams{Token: "hub-token", Backend: baddr, CAFile: caFile, Region: "test", Listen: "127.0.0.1:0",
		DataDir: filepath.Join(dir, "hub"), ConfigPath: filepath.Join(dir, "hub.json")}
	hubCfg, err := hubapp.Enroll(ctx, hp)
	if err != nil {
		t.Fatal(err)
	}
	first := serial(t, hubCfg.CertFile)
	hubCfg.SetRenewEvery(200 * time.Millisecond)
	listen := make(chan string, 1)
	hubDone := make(chan error, 1)
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	go func() {
		hubDone <- hubapp.Run(ctx, hubCfg, quiet, hubapp.Options{OnListen: func(a string) { listen <- a }})
	}()
	hubAddr := <-listen
	start := time.Now()

	d := grpcconn.Dialer{Cert: p.node(t, nodeID), Roots: p.roots, DialTimeout: 2 * time.Second}
	n := startNodeGRPC(t, newLogs(), d, []control.Endpoint{{ID: hubA, Address: hubAddr}})
	online(t, n)
	eventually(t, "hub certificate renewed", func() bool { return serial(t, hubCfg.CertFile).Cmp(first) != 0 })
	if _, err := os.Stat(hubCfg.KeyFile + ".prev"); err != nil {
		t.Fatal("previous hub key not kept")
	}
	// 原证书过期之后
	time.Sleep(time.Until(start.Add(5 * time.Second)))
	// 另一个节点身份新建连接（不取代正在转发的节点的会话）
	probeD := grpcconn.Dialer{Cert: p.node(t, nodeX1), Roots: p.roots, DialTimeout: 2 * time.Second}
	conn, err := probeD.Dial(ctx, control.Endpoint{ID: hubA, Address: hubAddr})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Send(agent.Hello{Node: nodeX1, Boot: types.UUID{0x5f}}); err != nil {
		t.Fatal(err)
	}
	rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
	defer rcancel()
	if m, err := conn.Recv(rctx); err != nil {
		t.Fatalf("new control connection after the original hub certificate expired: %v", err)
	} else if _, ok := m.(agent.HubHello); !ok {
		t.Fatalf("first message %T", m)
	}
	conn.Close()
	eventually(t, "forwarding after the original hub certificate expired", func() bool { return ok(addr) })
	// 后端重启：Edge Hub 以续签后的客户端证书重新连接并注册
	bsrv.Stop()
	be.protoBackend.mu.Lock()
	regs := be.registers
	be.protoBackend.mu.Unlock()
	bln2, err := net.Listen("tcp4", baddr)
	if err != nil {
		t.Fatal(err)
	}
	bsrv2 := grpc.NewServer(grpc.Creds(backendTLS))
	backendv1.RegisterHubControlServer(bsrv2, be)
	go func() { _ = bsrv2.Serve(bln2) }()
	defer bsrv2.Stop()
	eventually30s(t, "hub reconnected to the backend with the renewed certificate", func() bool {
		be.protoBackend.mu.Lock()
		defer be.protoBackend.mu.Unlock()
		return be.registers > regs
	})
	be.mu.Lock()
	renewals := be.hubRenewals
	be.mu.Unlock()
	if renewals == 0 {
		t.Fatal("no RenewHubCert call")
	}
	eventually(t, "forwarding", func() bool { return ok(addr) })
	cancel()
	<-hubDone
}

func eventually30s(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
