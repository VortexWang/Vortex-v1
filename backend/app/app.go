// Package app 组装并运行后端（cmd/backend 与集成测试共用）：数据库迁移、平台 CA、面向 Edge Hub 的 HubControl 服务。
// HTTP API（C4）在后续批次接入。
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"vortex/backend/internal/fleet"
	"vortex/backend/internal/grpcgw"
	"vortex/backend/internal/pki"
	"vortex/backend/internal/platform"
	backendv1 "vortex/proto/gen/go/vortex/backend/v1"
)

// Version 由构建时注入。
var Version = "dev"

type Config = platform.Config

func LoadConfig(path string) (*Config, error) { return platform.LoadConfig(path) }

// Options 供测试使用。
type Options struct {
	OnListen       func(addr string)
	OnReady        func(*Backend)
	ReconcileQuiet time.Duration
	// 证书有效期覆盖（测试用）
	NodeCertLife, HubCertLife time.Duration
}

// Backend 是运行中的后端，暴露运维与测试所需的操作。
type Backend struct {
	Fleet *fleet.Service
}

// CreateHubBootstrapToken 签发实例引导令牌。
func (b *Backend) CreateHubBootstrapToken(ctx context.Context, region string, ttl time.Duration) (string, error) {
	return b.Fleet.CreateHubBootstrapToken(ctx, region, ttl)
}

// CreateEnrollToken 签发节点注册令牌（平台节点：tenant 为零值）。
func (b *Backend) CreateEnrollToken(ctx context.Context, tenant, createdBy [16]byte) (string, error) {
	return b.Fleet.CreateEnrollToken(ctx, tenant, createdBy)
}

// RevokeCertificate 吊销证书。
func (b *Backend) RevokeCertificate(ctx context.Context, serial []byte, reason string) error {
	return b.Fleet.Revoke(ctx, serial, reason)
}

// LoadCA 按配置加载平台 CA（中间 CA 私钥口令取自环境变量）。
func LoadCA(cfg *Config) (*pki.CA, error) {
	pass := os.Getenv(cfg.CA.KeyPassphraseEnv)
	if pass == "" {
		return nil, fmt.Errorf("CA key passphrase: environment variable %s is not set", cfg.CA.KeyPassphraseEnv)
	}
	return pki.Load(cfg.CA.RootCertFile, cfg.CA.IntermediateCertFile, cfg.CA.IntermediateKeyFile, []byte(pass))
}

// Run 运行后端直至 ctx 结束。
func Run(ctx context.Context, cfg *Config, log *slog.Logger, opt Options) error {
	if cfg.DatabaseURL == "" {
		return errors.New("database_url is required")
	}
	if err := platform.Migrate(ctx, cfg.DatabaseURL); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	pool, err := platform.OpenDB(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	ca, err := LoadCA(cfg)
	if err != nil {
		return err
	}
	fl := &fleet.Service{DB: pool, CA: ca, NodeLife: opt.NodeCertLife, HubLife: opt.HubCertLife}
	gw := &grpcgw.Server{Fleet: fl, Log: log, ReconcileQuiet: opt.ReconcileQuiet}
	ln, err := net.Listen("tcp", cfg.GRPCListen)
	if err != nil {
		return err
	}
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(gw.TLSConfig())),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}))
	backendv1.RegisterHubControlServer(srv, gw)
	go func() { _ = srv.Serve(ln) }()
	log.Info("backend started", "hub_control", ln.Addr().String(), "version", Version)
	if opt.OnListen != nil {
		opt.OnListen(ln.Addr().String())
	}
	if opt.OnReady != nil {
		opt.OnReady(&Backend{Fleet: fl})
	}
	<-ctx.Done()
	srv.Stop()
	return nil
}
