// Package app 组装并运行 vortex-node（02 方案 2；AGT-T01）：本地存储、Engine、探测、Agent、本地运维接口、
// 控制连接（gRPC + mTLS），由 supervisor 管理各模块。cmd/vortex-node 与集成测试都通过它启动节点。
package app

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"vortex/engine"
	"vortex/engine/types"
	"vortex/node/agent"
	"vortex/node/agent/control"
	"vortex/node/agent/grpcconn"
	"vortex/node/agent/localapi"
	"vortex/node/agent/probe"
	"vortex/node/agent/report"
	"vortex/node/agent/store"
	"vortex/node/internal/supervisor"
)

// Version 由构建时注入。
var Version = "dev"

const DefaultSocket = "/run/vortex/node.sock"

// Config 是节点配置文件。
type Config struct {
	NodeID        string     `json:"node_id"`
	DataDir       string     `json:"data_dir"`
	Socket        string     `json:"socket"`
	EdgeHubs      []HubEntry `json:"edge_hubs"`
	DiskLowBytes  uint64     `json:"disk_low_bytes"`  // 默认 1 GiB
	DiskHighBytes uint64     `json:"disk_high_bytes"` // 默认 2 GiB
	// 节点证书与平台 CA（注册时写入，AGT-001）。三者都设置时通过 gRPC 连接 Edge Hub。
	CertFile string `json:"cert_file"`
	KeyFile  string `json:"key_file"`
	CAFile   string `json:"ca_file"`
}

type HubEntry struct {
	HubID   string `json:"hub_id"`
	Address string `json:"address"`
}

func ParseUUID(s string) (types.UUID, error) {
	var u types.UUID
	b, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	if err != nil || len(b) != 16 {
		return u, fmt.Errorf("invalid uuid %q", s)
	}
	copy(u[:], b)
	return u, nil
}

func FormatUUID(u types.UUID) string {
	if u == (types.UUID{}) {
		return ""
	}
	h := hex.EncodeToString(u[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
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
		c.DataDir = "/var/lib/vortex"
	}
	if c.Socket == "" {
		c.Socket = DefaultSocket
	}
	if c.DiskLowBytes == 0 {
		c.DiskLowBytes = 1 << 30
	}
	if c.DiskHighBytes == 0 {
		c.DiskHighBytes = 2 << 30
	}
	return &c, nil
}

// Dialer 返回配置的控制连接传输：证书齐全时为 gRPC；都未设置时为 nil（不连接 Edge Hub）。
func Dialer(cfg *Config) (control.Dialer, error) {
	if cfg.CertFile == "" && cfg.KeyFile == "" && cfg.CAFile == "" {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("node certificate: %w", err)
	}
	pem, err := os.ReadFile(cfg.CAFile)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, errors.New("ca_file: no certificates")
	}
	return grpcconn.Dialer{Cert: cert, Roots: roots}, nil
}

// Run 组装并运行节点，直至 ctx 结束。dialer 为 nil 时不建立控制连接。
func Run(ctx context.Context, cfg *Config, dialer control.Dialer, log *slog.Logger) error {
	node, err := ParseUUID(cfg.NodeID)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(cfg.Socket), 0o755); err != nil {
		return err
	}
	// 本地存储：一个 SQLite（WAL）数据库，启动标识、计量、最后生效配置各一张表（AGT-003）
	var logs []*store.SQLite
	defer func() {
		for _, l := range logs {
			l.Close()
		}
	}()
	db := filepath.Join(cfg.DataDir, "agent.db")
	open := func(table string) (*store.SQLite, error) {
		l, err := store.OpenSQLite(db, table)
		if err == nil {
			logs = append(logs, l)
		}
		return l, err
	}
	bootLog, err := open("boot")
	if err != nil {
		return err
	}
	usageLog, err := open("usage")
	if err != nil {
		return err
	}
	configLog, err := open("config")
	if err != nil {
		return err
	}

	prober := probe.New()
	eng := engine.New(engine.Options{Healthy: prober.TargetHealthy})
	disk := &report.DiskGuard{Path: cfg.DataDir, Low: cfg.DiskLowBytes, High: cfg.DiskHighBytes,
		OnLow: func(free uint64) { log.Warn("disk low: new sessions should stop (AGT-006)", "free", free) },
		OnOK:  func(free uint64) { log.Info("disk recovered", "free", free) }}
	a, err := agent.New(ctx, agent.Options{Node: node, AgentVersion: Version, Engine: eng,
		BootLog: bootLog, UsageLog: usageLog, ConfigLog: configLog, Disk: disk,
		OnApplied: func(c *types.RuntimeConfig) { prober.SetSubjects(probe.Subjects(c)) }})
	if err != nil {
		eng.Close()
		return err
	}
	if c := a.Status(); c.AppliedVersion != 0 {
		log.Info("recovered applied config; waiting for run grants (AGT-010)", "version", c.AppliedVersion)
	}
	_, _ = disk.Check()

	api := &localapi.Server{
		Status: func() localapi.Status { return StatusView(a.Status(), disk) },
		Drain: func(ctx context.Context, req localapi.DrainRequest) error {
			var ids []types.UUID
			for _, s := range req.Rules {
				id, err := ParseUUID(s)
				if err != nil {
					return err
				}
				ids = append(ids, id)
			}
			return a.Drain(ctx, ids)
		},
	}
	if err := api.Listen(cfg.Socket); err != nil {
		eng.Close()
		return err
	}
	defer api.Close()

	sup := &supervisor.Supervisor{OnEvent: func(e supervisor.Event) {
		log.Error("module exited", "module", e.Module, "err", e.Err, "restart_in", e.Restart)
	}}
	sup.Go(ctx, "agent", func(ctx context.Context) error { a.Run(ctx); return nil })
	sup.Go(ctx, "probe", func(ctx context.Context) error { prober.Run(ctx, probe.Interval, a.ReportProbes); return nil })
	if dialer != nil && len(cfg.EdgeHubs) > 0 {
		var eps []control.Endpoint
		for _, h := range cfg.EdgeHubs {
			id, err := ParseUUID(h.HubID)
			if err != nil {
				return err
			}
			eps = append(eps, control.Endpoint{ID: id, Address: h.Address})
		}
		c := &control.Controller{Agent: a, Hubs: eps, Dialer: dialer}
		sup.Go(ctx, "control", func(ctx context.Context) error { c.Run(ctx); return nil })
	} else {
		log.Warn("no control transport: running without an Edge Hub connection")
	}
	log.Info("vortex-node started", "node", cfg.NodeID, "boot", FormatUUID(a.Status().Boot), "version", Version)
	<-ctx.Done()
	sup.Wait()
	return a.Shutdown(eng.Close)
}

func StatusView(s agent.Status, disk *report.DiskGuard) localapi.Status {
	mode := map[agent.Mode]string{agent.ModeUnspecified: "", agent.ModeOnline: "online", agent.ModeAutonomous: "autonomous"}[s.Mode]
	v := localapi.Status{NodeID: FormatUUID(s.Node), BootID: FormatUUID(s.Boot), PreviousBootID: FormatUUID(s.PreviousBoot),
		Hub: FormatUUID(s.Hub), HubMode: mode, Connected: s.Connected, Phase: s.Phase.String(), DesiredVersion: s.AppliedVersion,
		ActualVersion: s.ActualVersion, GrantRemainingS: map[string]int64{}, PendingBatches: s.PendingBatches, DiskLow: s.DiskLow}
	for r, d := range s.RunRemaining {
		v.GrantRemainingS[FormatUUID(r)] = int64(d / time.Second)
	}
	if disk != nil {
		v.DiskFreeBytes = disk.Last()
	}
	return v
}
