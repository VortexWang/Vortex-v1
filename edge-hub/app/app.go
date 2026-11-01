// Package app 组装并运行 Edge Hub（03 方案 2；HUB-T01）：SQLite 账本（重放恢复，包括自治中重启）、模式、
// 额度、签发、份额、会话、运行循环，面向 Agent 的 NodeControl gRPC 服务与到后端的 HubControl gRPC 客户端。
// cmd/edge-hub 与集成测试都通过它启动实例。
package app

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
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
		if cfg.Listen != "" {
			ln, err := net.Listen("tcp", cfg.Listen)
			if err != nil {
				return err
			}
			srv = grpc.NewServer(grpc.Creds(credentials.NewTLS(nodegrpc.ServerTLS(cert, roots))))
			edgehubv1.RegisterNodeControlServer(srv, &nodegrpc.Server{Hub: h, Region: cfg.Region, Revoked: revoked(link)})
			go func() { _ = srv.Serve(ln) }()
			log.Info("NodeControl listening", "addr", ln.Addr().String())
			if opt.OnListen != nil {
				opt.OnListen(ln.Addr().String())
			}
		}
		if cfg.BackendAddress != "" {
			c := &backendgrpc.Client{Link: link, Addr: cfg.BackendAddress, TLS: backendgrpc.ClientTLS(cert, roots), Region: cfg.Region,
				Version: Version, OnError: func(err error) { log.Warn("backend connection", "err", err) }}
			go func() { c.Run(ctx); close(done) }()
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
