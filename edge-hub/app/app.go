// Package app 组装并运行 Edge Hub（03 方案 2；HUB-T01）：SQLite 账本（重放恢复，包括自治中重启）、模式、
// 额度、签发、份额、会话、运行循环，面向 Agent 的 NodeControl gRPC 服务与到后端的 HubControl gRPC 客户端。
// cmd/edge-hub 与集成测试都通过它启动实例。
package app

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"

	"vortex/edgehub/backendgrpc"
	"vortex/edgehub/ledger"
	"vortex/edgehub/model"
	"vortex/edgehub/nodegrpc"
	"vortex/edgehub/runner"
	"vortex/edgehub/session"
	"vortex/edgehub/upstream"
	"vortex/edgehub/wire"
	backendv1 "vortex/proto/gen/go/vortex/backend/v1"
	edgehubv1 "vortex/proto/gen/go/vortex/edgehub/v1"
	runtimev1 "vortex/proto/gen/go/vortex/runtime/v1"
)

// Version 由构建时注入。
var Version = "dev"

// Config 是 Edge Hub 配置文件。
type Config struct {
	HubID   string `json:"hub_id"`
	Region  string `json:"region"`
	DataDir string `json:"data_dir"`
	// Listen 是面向 Agent 的 NodeControl 监听地址，如 ":7400"。
	Listen string `json:"listen"`
	// 实例证书（SAN URI spiffe://vortex/hub/<hub_id>，契约补充 L5）与平台 CA。
	CertFile string `json:"cert_file"`
	KeyFile  string `json:"key_file"`
	CAFile   string `json:"ca_file"`
	// BackendAddress 是后端 HubControl 地址；为空时不连接后端（始终失联，30 秒后自治）。
	BackendAddress string `json:"backend_address"`
}

func ParseUUID(s string) (model.UUID, error) {
	var u model.UUID
	b, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	if err != nil || len(b) != 16 {
		return u, fmt.Errorf("invalid uuid %q", s)
	}
	copy(u[:], b)
	return u, nil
}

func LoadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if c.DataDir == "" {
		c.DataDir = "/var/lib/vortex-edge-hub"
	}
	return &c, nil
}

// Options 供测试使用。
type Options struct {
	OnListen func(addr string) // NodeControl 开始监听时调用（Listen 为 ":0" 时取实际端口）
	OnReady  func(h *session.Hub, l *upstream.Link)
}

func loadTLS(cfg *Config) (tls.Certificate, *x509.CertPool, error) {
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("hub certificate: %w", err)
	}
	pem, err := os.ReadFile(cfg.CAFile)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return tls.Certificate{}, nil, errors.New("ca_file: no certificates")
	}
	return cert, roots, nil
}

// revoked 按后端下发的吊销列表判定节点或证书是否已吊销（SEC-002）。
func revoked(link *upstream.Link) func(model.UUID, []byte) bool {
	return func(node model.UUID, serial []byte) bool {
		_, raw := link.Revocations()
		if len(raw) == 0 {
			return false
		}
		var rl runtimev1.RevocationList
		if proto.Unmarshal(raw, &rl) != nil {
			return false
		}
		for _, id := range rl.GetNodeIds() {
			if v, err := wire.FromUUID(id); err == nil && v == node {
				return true
			}
		}
		for _, s := range rl.GetCertSerials() {
			if bytes.Equal(bytes.TrimLeft(s, "\x00"), bytes.TrimLeft(serial, "\x00")) {
				return true
			}
		}
		return false
	}
}

// Run 运行 Edge Hub 直至 ctx 结束。
func Run(ctx context.Context, cfg *Config, log *slog.Logger, opt Options) error {
	id, err := ParseUUID(cfg.HubID)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return err
	}
	st, err := ledger.OpenSQLite(filepath.Join(cfg.DataDir, "ledger.db"))
	if err != nil {
		return err
	}
	defer st.Close()
	h, err := session.Open(session.Options{HubID: id, Store: st, ParseRules: wire.RulesFromConfig})
	if err != nil {
		return err
	}
	link := upstream.New(h)
	log.Info("edge-hub started", "hub", cfg.HubID, "region", cfg.Region, "mode", h.Mode.Mode().String(),
		"ledger_last_seq", h.Ledger.LastSeq(), "version", Version)
	h.Mode.OnChange(func(m model.Mode) { log.Warn("mode changed", "mode", m.String()) })
	if opt.OnReady != nil {
		opt.OnReady(h, link)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	var srv *grpc.Server
	if cfg.Listen != "" || cfg.BackendAddress != "" {
		cert, roots, err := loadTLS(cfg)
		if err != nil {
			return err
		}
		var bc *backendgrpc.Client
		if cfg.BackendAddress != "" {
			bc = &backendgrpc.Client{Link: link, Addr: cfg.BackendAddress, TLS: backendgrpc.ClientTLS(cert, roots), Region: cfg.Region,
				Version: Version, OnError: func(err error) { log.Warn("backend connection", "err", err) }}
			defer bc.Close()
		}
		if cfg.Listen != "" {
			ln, err := net.Listen("tcp", cfg.Listen)
			if err != nil {
				return err
			}
			srv = grpc.NewServer(grpc.Creds(credentials.NewTLS(nodegrpc.ServerTLS(cert, roots))))
			ns := &nodegrpc.Server{Hub: h, Region: cfg.Region, Revoked: revoked(link)}
			if bc != nil {
				ns.Backend = bc // 注册与续签经本实例转发到后端（HUB-T03）
			}
			edgehubv1.RegisterNodeControlServer(srv, ns)
			go func() { _ = srv.Serve(ln) }()
			log.Info("NodeControl listening", "addr", ln.Addr().String())
			if opt.OnListen != nil {
				opt.OnListen(ln.Addr().String())
			}
		}
		if bc != nil {
			go func() { bc.Run(ctx); close(done) }()
		}
	}
	if cfg.BackendAddress == "" {
		close(done)
		log.Warn("no backend address: the hub will run autonomous")
	}
	r := &runner.Runner{Hub: h, Link: link, OnError: func(step string, err error) { log.Error("loop", "step", step, "err", err) }}
	r.Run(ctx)
	if srv != nil {
		srv.Stop()
	}
	cancel()
	<-done
	return nil
}

// ---------------------------------------------------------------- 实例引导（HUB-T19）

// EnrollParams 是 edge-hub enroll 的参数。
type EnrollParams struct {
	Token      string // 一次性引导令牌
	Backend    string // 后端 HubControl 地址
	CAFile     string // 平台 CA（随安装包分发）
	Region     string
	Listen     string
	DataDir    string
	ConfigPath string
}

// Enroll 本地生成私钥与 CSR，用引导令牌向后端换取实例证书与 hub_id，校验后写入数据目录并写配置。
func Enroll(ctx context.Context, p EnrollParams) (*Config, error) {
	pemCA, err := os.ReadFile(p.CAFile)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pemCA) {
		return nil, errors.New("ca file: no certificates")
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		return nil, err
	}
	tc := backendgrpc.ClientTLS(tls.Certificate{}, roots)
	tc.Certificates = nil // 引导时还没有证书
	cc, err := grpc.NewClient(p.Backend, grpc.WithTransportCredentials(credentials.NewTLS(tc)))
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	resp, err := backendv1.NewHubControlClient(cc).EnrollHub(ctx, &backendv1.EnrollHubRequest{BootstrapToken: p.Token, CsrDer: csr, Region: p.Region, HubVersion: Version})
	if err != nil {
		return nil, fmt.Errorf("enroll hub: %w", err)
	}
	id, err := wire.FromUUID(resp.GetHubId())
	if err != nil || id.IsZero() {
		return nil, fmt.Errorf("enroll hub: bad hub_id")
	}
	if err := verifyHubCert(resp.GetCertificateChainDer(), key, roots, id); err != nil {
		return nil, fmt.Errorf("enroll hub: issued certificate: %w", err)
	}
	if err := os.MkdirAll(p.DataDir, 0o700); err != nil {
		return nil, err
	}
	cfg := &Config{HubID: id.String(), Region: p.Region, DataDir: p.DataDir, Listen: p.Listen, BackendAddress: p.Backend,
		CertFile: filepath.Join(p.DataDir, "hub.crt"), KeyFile: filepath.Join(p.DataDir, "hub.key"), CAFile: filepath.Join(p.DataDir, "ca.crt")}
	var caOut, chainOut bytes.Buffer
	for _, d := range resp.GetCaBundleDer() {
		_ = pem.Encode(&caOut, &pem.Block{Type: "CERTIFICATE", Bytes: d})
	}
	if caOut.Len() == 0 {
		caOut.Write(pemCA)
	}
	for _, d := range resp.GetCertificateChainDer() {
		_ = pem.Encode(&chainOut, &pem.Block{Type: "CERTIFICATE", Bytes: d})
	}
	kd, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}
	for _, f := range []struct {
		path string
		data []byte
		mode os.FileMode
	}{
		{cfg.KeyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kd}), 0o600},
		{cfg.CertFile, chainOut.Bytes(), 0o644},
		{cfg.CAFile, caOut.Bytes(), 0o644},
	} {
		if err := writeAtomic(f.path, f.data, f.mode); err != nil {
			return nil, err
		}
	}
	b, _ := json.MarshalIndent(cfg, "", "  ")
	if err := writeAtomic(p.ConfigPath, b, 0o600); err != nil {
		return nil, err
	}
	return cfg, nil
}

// verifyHubCert：公钥与本地私钥一致，链到平台 CA，SAN URI 为该 hub_id，扩展用途含 serverAuth 与 clientAuth（L5）。
func verifyHubCert(chain [][]byte, key *ecdsa.PrivateKey, roots *x509.CertPool, id model.UUID) error {
	if len(chain) == 0 {
		return errors.New("empty chain")
	}
	var certs []*x509.Certificate
	for _, d := range chain {
		c, err := x509.ParseCertificate(d)
		if err != nil {
			return err
		}
		certs = append(certs, c)
	}
	leaf := certs[0]
	if pub, ok := leaf.PublicKey.(*ecdsa.PublicKey); !ok || !pub.Equal(&key.PublicKey) {
		return errors.New("public key mismatch")
	}
	inter := x509.NewCertPool()
	for _, c := range certs[1:] {
		inter.AddCert(c)
	}
	for _, ku := range []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth} {
		if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: inter, KeyUsages: []x509.ExtKeyUsage{ku}}); err != nil {
			return err
		}
	}
	want := nodegrpc.HubURI(id)
	for _, u := range leaf.URIs {
		if u.String() == want {
			return nil
		}
	}
	return fmt.Errorf("certificate does not carry %s", want)
}

func writeAtomic(path string, data []byte, mode os.FileMode) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, mode); err != nil {
		return err
	}
	f, err := os.Open(tmp)
	if err == nil {
		_ = f.Sync()
		f.Close()
	}
	return os.Rename(tmp, path)
}
