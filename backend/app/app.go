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

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"vortex/backend/internal/apply"
	"vortex/backend/internal/billing"
	"vortex/backend/internal/db"
	"vortex/backend/internal/fleet"
	"vortex/backend/internal/groups"
	"vortex/backend/internal/grpcgw"
	"vortex/backend/internal/leases"
	"vortex/backend/internal/metering"
	"vortex/backend/internal/pki"
	"vortex/backend/internal/platform"
	"vortex/backend/internal/rules"
	backendv1 "vortex/proto/gen/go/vortex/backend/v1"
)

// Version 由构建时注入。
var Version = "dev"

type Config = platform.Config

// RuleSpec、RuleSaved 是规则的内容与保存结果（供集成测试与运维工具使用）。
type (
	RuleSpec  = rules.Spec
	RuleSaved = rules.Saved
)

func LoadConfig(path string) (*Config, error) { return platform.LoadConfig(path) }

// Options 供测试使用。
type Options struct {
	OnListen       func(addr string)
	OnReady        func(*Backend)
	ReconcileQuiet time.Duration
	// 证书有效期覆盖（测试用）
	NodeCertLife, HubCertLife time.Duration
	// ReconcileEvery 是定期部署与推进编排的周期（成员变化等也由此收敛）；默认 10 秒
	ReconcileEvery time.Duration
	// LeaseEvery 是份额重算与限制快照广播的周期；默认 60 秒（LIM-009）
	LeaseEvery time.Duration
	// ReclaimAfter 覆盖失联实例额度的超时回收时长（HUB-015 ②，测试用）
	ReclaimAfter time.Duration
	// BillingEvery 是套餐定时任务的周期；默认 1 分钟
	BillingEvery time.Duration
}

// Backend 是运行中的后端，暴露运维与测试所需的操作。
type Backend struct {
	Fleet  *fleet.Service
	Rules  *rules.Service
	Apply  *apply.Service
	Leases *leases.Service
	Groups *groups.Service // 节点组、成员、组倍率、段许可（BE-T09）
}

// CreateRule 保存规则并部署（RUL-010 ①～②：校验、编译、下发准备）。
func (b *Backend) CreateRule(ctx context.Context, tenant [16]byte, spec rules.Spec) (*rules.Saved, error) {
	s, err := b.Rules.Create(ctx, tenant, spec)
	if err != nil {
		return nil, err
	}
	if _, err = b.Apply.Deploy(ctx); err != nil {
		return s, err
	}
	return s, b.Leases.Refresh(ctx, tenant)
}

// UpdateRule 修改规则并部署。
func (b *Backend) UpdateRule(ctx context.Context, tenant, id [16]byte, rowVersion int64, spec rules.Spec) (*rules.Saved, error) {
	s, err := b.Rules.Update(ctx, tenant, id, rowVersion, spec)
	if err != nil {
		return nil, err
	}
	if _, err = b.Apply.Deploy(ctx); err != nil {
		return s, err
	}
	return s, b.Leases.Refresh(ctx, tenant)
}

// DeleteRule 删除规则并部署。
func (b *Backend) DeleteRule(ctx context.Context, tenant, id [16]byte) error {
	if err := b.Rules.Delete(ctx, tenant, id); err != nil {
		return err
	}
	if _, err := b.Apply.Deploy(ctx); err != nil {
		return err
	}
	return b.Leases.Refresh(ctx, tenant)
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

// reconcileLoop 定期部署（节点组成员、节点状态等变化由此收敛）并推进编排。
func reconcileLoop(ctx context.Context, ap *apply.Service, every time.Duration, log *slog.Logger) {
	if every <= 0 {
		every = 10 * time.Second
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if _, err := ap.Deploy(ctx); err != nil && ctx.Err() == nil {
			log.Warn("deploy", "err", err)
		}
		if _, err := ap.Advance(ctx); err != nil && ctx.Err() == nil {
			log.Warn("advance", "err", err)
		}
	}
}

// disputeLoop 每分钟自动结算到期的争议（MTR-009）。
func disputeLoop(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if _, err := metering.ResolveDisputes(ctx, pool, log); err != nil && ctx.Err() == nil {
			log.Warn("disputes", "err", err)
		}
	}
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
	if err := db.New(pool).EnsureInitialBillingVersion(ctx, platform.NewUUIDv7().PG()); err != nil {
		return fmt.Errorf("initial billing version: %w", err)
	}
	fl := &fleet.Service{DB: pool, CA: ca, NodeLife: opt.NodeCertLife, HubLife: opt.HubCertLife}
	gw := &grpcgw.Server{Fleet: fl, Log: log, ReconcileQuiet: opt.ReconcileQuiet}
	ap := &apply.Service{DB: pool, Notify: gw.Wake, Log: log}
	gw.Apply = ap
	ls := &leases.Service{DB: pool, Hubs: gw, Log: log, ReclaimAfter: opt.ReclaimAfter}
	if err := ls.Startup(ctx); err != nil { // 上次运行时连接的实例视为失联，冻结其额度（HUB-015）
		return fmt.Errorf("startup: %w", err)
	}
	gw.Leases = ls
	gw.Metering = &metering.Service{Budgets: ls, Log: log}
	go ls.Run(ctx, opt.LeaseEvery)
	go disputeLoop(ctx, pool, log)
	go (&billing.Service{DB: pool, Budgets: ls, Log: log}).Run(ctx, opt.BillingEvery) // 月度重置、流量包作废、限制记录（BE-T16）
	go gw.RunDelivery(ctx)
	go reconcileLoop(ctx, ap, opt.ReconcileEvery, log)
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
		opt.OnReady(&Backend{Fleet: fl, Rules: &rules.Service{DB: pool}, Apply: ap, Leases: ls, Groups: &groups.Service{DB: pool,
			Changed: func(ctx context.Context) { // 成员与段许可变化：重新部署（PRT-007）并推进
				if _, err := ap.Deploy(ctx); err != nil {
					log.Warn("deploy after a group change", "err", err)
				}
				if _, err := ap.Advance(ctx); err != nil {
					log.Warn("advance after a group change", "err", err)
				}
			},
			BillingChanged: gw.BroadcastBillingVersions}})
	}
	<-ctx.Done()
	srv.Stop()
	return nil
}
