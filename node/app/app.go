// Package app 组装并运行 vortex-node（02 方案 2；AGT-T01）：本地存储、Engine、探测、Agent、本地运维接口、
// 控制连接（gRPC + mTLS），由 supervisor 管理各模块。cmd/vortex-node 与集成测试都通过它启动节点。
package app

import (
	"context"
	"crypto/ed25519"
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
	"time"

	"vortex/engine"
	"vortex/engine/types"
	"vortex/engine/vlink"
	"vortex/node/agent"
	"vortex/node/agent/boot"
	"vortex/node/agent/certs"
	"vortex/node/agent/control"
	"vortex/node/agent/grpcconn"
	"vortex/node/agent/localapi"
	"vortex/node/agent/probe"
	"vortex/node/agent/reconcile"
	"vortex/node/agent/report"
	"vortex/node/agent/store"
	"vortex/node/agent/upgrade"
	"vortex/node/agent/wire"
	"vortex/node/internal/supervisor"
	edgehubv1 "vortex/proto/gen/go/vortex/edgehub/v1"
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
	// LinkListen 是节点间链路（VLink）的监听地址，例如 "0.0.0.0:7443"。为空时本节点不接受入站链路
	// （仍可作为入口或反向建链方主动建链）；节点间链路需要节点证书（cert_file/key_file/ca_file）。
	LinkListen string `json:"link_listen"`
	// 升级（AGT-008）：平台发布公钥（Ed25519，PEM PUBLIC KEY，随安装包分发）。未设置时拒绝一切升级指令。
	ReleaseKeyFile string `json:"release_key_file"`
	// Binary 是 systemd 启动的程序路径，升级时原子替换它；为空时取当前程序（os.Executable）。
	Binary string `json:"binary"`
	// DisableSelfUpgrade：容器部署时由镜像升级，拒绝升级指令（报告失败）。
	DisableSelfUpgrade bool `json:"disable_self_upgrade"`

	renewEvery time.Duration // 证书检查周期（测试用；默认 1 小时）
}

// SetRenewEvery 设置证书续签的检查周期（测试用）。
func (c *Config) SetRenewEvery(d time.Duration) { c.renewEvery = d }

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

// EnvDisableSelfUpgrade 为 "1" 时禁用自升级（容器镜像中设置，deploy/node/Dockerfile）。
const EnvDisableSelfUpgrade = "VORTEX_DISABLE_SELF_UPGRADE"

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
	if os.Getenv(EnvDisableSelfUpgrade) == "1" { // 容器镜像设置：由镜像升级
		c.DisableSelfUpgrade = true
	}
	return &c, nil
}

// Dialer 返回配置的控制连接传输：证书齐全时为 gRPC；都未设置时为 nil（不连接 Edge Hub）。
func Dialer(cfg *Config) (control.Dialer, error) {
	if cfg.CertFile == "" && cfg.KeyFile == "" && cfg.CAFile == "" {
		return nil, nil
	}
	cert, err := certs.Load(cfg.CertFile, cfg.KeyFile)
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
	return grpcconn.Dialer{Holder: certs.NewHolder(cert), Roots: roots}, nil
}

// tunnelOptions 由节点证书构造 Engine 的节点间链路参数（ENG-015 起；身份即注册得到的节点证书）。
// 没有 gRPC 证书时返回 nil（只支持直接转发）。证书续签后新建的链路使用新证书。
func tunnelOptions(cfg *Config, node, bootID types.UUID, dialer control.Dialer, prober *probe.Prober) (*engine.TunnelOptions, error) {
	gd, ok := dialer.(grpcconn.Dialer)
	if !ok || gd.Holder == nil {
		if cfg.LinkListen != "" {
			return nil, errors.New("link_listen requires a node certificate (cert_file/key_file/ca_file)")
		}
		return nil, nil
	}
	ident := func() *vlink.Identity { return &vlink.Identity{NodeID: node, Cert: gd.Holder.Get()} }
	leaf, err := gd.Holder.Leaf()
	if err != nil {
		return nil, err
	}
	if id, err := vlink.NodeIDFromCert(leaf); err != nil || id != node {
		return nil, fmt.Errorf("node certificate identity does not match node_id %s", cfg.NodeID)
	}
	t := &engine.TunnelOptions{Identity: ident(), CurrentIdentity: ident, Roots: gd.Roots, BootID: bootID,
		PeerHealthy: prober.PeerHealthy}
	if cfg.LinkListen != "" {
		ln, err := net.Listen("tcp", cfg.LinkListen)
		if err != nil {
			return nil, fmt.Errorf("link_listen: %w", err)
		}
		t.LinkListener = ln
	}
	return t, nil
}

// faultCrash 是故障注入，只用于测试构建（-ldflags "-X vortex/node/app.faultCrash=…"），验证启动后崩溃的版本被回退。
var faultCrash string // "at-start"：Run 一开始即崩溃；"after-upgrade-check"：升级启动检查之后崩溃

// ErrRestart：Run 因升级或回退而结束，进程应退出并由 systemd 重新启动（AGT-008）。
var ErrRestart = errors.New("restart requested (upgrade)")

// Manifest 是本程序的版本与本地数据格式（`vortex-node version -json`，升级时的兼容矩阵）。
func Manifest() upgrade.Manifest {
	return upgrade.Manifest{Version: Version, StoreSchema: store.Schema, StoreMin: store.SchemaMin, StoreMax: store.SchemaMax}
}

// LoadReleaseKey 读取平台发布公钥。
func LoadReleaseKey(path string) (ed25519.PublicKey, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	blk, _ := pem.Decode(b)
	if blk == nil || blk.Type != "PUBLIC KEY" {
		return nil, fmt.Errorf("%s: no PEM PUBLIC KEY", path)
	}
	k, err := x509.ParsePKIXPublicKey(blk.Bytes)
	if err != nil {
		return nil, err
	}
	pub, ok := k.(ed25519.PublicKey)
	if !ok {
		return nil, fmt.Errorf("%s: not an Ed25519 key", path)
	}
	return pub, nil
}

// healthy：升级后的新版本已连接 Edge Hub，且配置已生效或没有配置（AGT-008 的健康判定）。
func healthy(s agent.Status) bool {
	return s.Connected && (s.Phase == reconcile.PhaseApplied || s.Phase == reconcile.PhaseIdle)
}

// Run 组装并运行节点，直至 ctx 结束；因升级或回退需要重启时返回 ErrRestart。dialer 为 nil 时不建立控制连接。
func Run(parent context.Context, cfg *Config, dialer control.Dialer, log *slog.Logger) error {
	if faultCrash == "at-start" {
		panic("fault injection: crash at start") // 仅测试构建
	}
	ctx, stopRun := context.WithCancelCause(parent)
	defer stopRun(nil)
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
	// 升级后的首次启动：先计数（启动后崩溃也算），超过上限即回退并重启，不打开本地数据（AGT-008）
	var a *agent.Agent
	disk := &report.DiskGuard{Path: cfg.DataDir, Low: cfg.DiskLowBytes, High: cfg.DiskHighBytes,
		OnLow: func(free uint64) { log.Warn("disk low: new sessions should stop (AGT-006)", "free", free) },
		OnOK:  func(free uint64) { log.Info("disk recovered", "free", free) }}
	up, err := upgradeManager(cfg, func() *agent.Agent { return a }, disk, log, func() { stopRun(ErrRestart) })
	if err != nil {
		return err
	}
	if up.Begin() {
		log.Warn("upgraded version failed to start repeatedly: rolled back, restarting")
		return ErrRestart
	}
	if faultCrash == "after-upgrade-check" {
		panic("fault injection: crash after the upgrade start check") // 仅测试构建（-ldflags -X）
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

	b, err := boot.Start(bootLog)
	if err != nil {
		return err
	}
	prober := probe.New()
	tun, err := tunnelOptions(cfg, node, b.Current, dialer, prober)
	if err != nil {
		return err
	}
	if tun == nil {
		log.Warn("no node certificate: tunnel (multi-hop) forwarding disabled, direct forwarding only")
	} else if tun.LinkListener != nil {
		log.Info("link listener", "addr", tun.LinkListener.Addr().String())
	}
	eng := engine.New(engine.Options{Healthy: prober.TargetHealthy, Tunnel: tun})
	a, err = agent.New(ctx, agent.Options{Node: node, AgentVersion: Version, Engine: eng,
		BootLog: bootLog, Boot: &b, UsageLog: usageLog, ConfigLog: configLog, Disk: disk,
		OnApplied: func(c *types.RuntimeConfig) { prober.SetSubjects(probe.Subjects(c)) },
		OnUpgrade: func(c upgrade.Command) {
			log.Info("upgrade command", "version", c.Version, "irreversible", c.Irreversible)
			if st := refuseUpgrade(cfg, c); st != nil {
				go a.ReportUpgrade(*st)
				return
			}
			if err := up.Start(ctx, c); err != nil {
				log.Warn("upgrade ignored", "version", c.Version, "err", err)
			}
		},
		OnClockSkew: func(hub types.UUID, off, unc time.Duration, alert bool) {
			if alert {
				log.Warn("clock skew with Edge Hub exceeds 5s; grants are unaffected, timestamps in logs and usage may be off (AGT-005)",
					"hub", FormatUUID(hub), "offset", off, "uncertainty", unc)
			}
		}})
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
	go func() {
		if up.Await(ctx) {
			log.Warn("upgraded version unhealthy: rolled back, restarting")
		}
	}()
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
		if gd, ok := dialer.(grpcconn.Dialer); ok && gd.Holder != nil {
			r := &certs.Renewer{Holder: gd.Holder, Roots: gd.Roots, CertFile: cfg.CertFile, KeyFile: cfg.KeyFile, Every: cfg.renewEvery,
				OnEvent: func(renewed bool, err error) {
					if err != nil {
						log.Error("certificate renewal failed (AGT-012)", "err", err)
					} else if renewed {
						log.Info("certificate renewed")
					}
				}}
			for _, ep := range eps {
				ep := ep
				r.Submit = append(r.Submit, func(ctx context.Context, csr []byte) ([][]byte, error) { return gd.RenewCert(ctx, ep, csr) })
			}
			sup.Go(ctx, "cert", func(ctx context.Context) error { r.Run(ctx); return nil })
		}
	} else {
		log.Warn("no control transport: running without an Edge Hub connection")
	}
	log.Info("vortex-node started", "node", cfg.NodeID, "boot", FormatUUID(a.Status().Boot), "version", Version)
	<-ctx.Done()
	sup.Wait()
	if err := a.Shutdown(eng.Close); err != nil {
		return err
	}
	if errors.Is(context.Cause(ctx), ErrRestart) && parent.Err() == nil {
		return ErrRestart
	}
	return nil
}

// refuseUpgrade：禁用自升级（容器部署）时返回要上报的失败。
func refuseUpgrade(cfg *Config, c upgrade.Command) *upgrade.Status {
	if !cfg.DisableSelfUpgrade {
		return nil
	}
	return &upgrade.Status{Target: c.Version, Phase: upgrade.PhaseFailed, Error: "self-upgrade disabled (container deployment): update the image"}
}

// binaryPath 是安装的程序路径：配置的 binary，否则为当前程序（作为 .prev 执行 Guard 时去掉后缀）。
func binaryPath(cfg *Config) (string, error) {
	if cfg.Binary != "" {
		return cfg.Binary, nil
	}
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}
	if exe, err = filepath.EvalSymlinks(exe); err != nil {
		return "", err
	}
	return installedPath(exe), nil
}

// installedPath：作为 .prev 运行时，安装路径是去掉后缀的那一个。
func installedPath(exe string) string { return strings.TrimSuffix(exe, ".prev") }

// UpgradeGuard 是 `vortex-node upgrade-guard`：由上一版本的程序（.prev）在每次启动之前执行（systemd ExecStartPre），
// 计数升级后新版本的启动次数，超过上限时恢复上一版本（AGT-008）。返回是否已回退。
func UpgradeGuard(cfg *Config) (bool, error) {
	bin, err := binaryPath(cfg)
	if err != nil {
		return false, err
	}
	m := &upgrade.Manager{Self: Manifest(), Binary: bin, Dir: filepath.Join(cfg.DataDir, "upgrade")}
	return m.Guard()
}

func upgradeManager(cfg *Config, agentOf func() *agent.Agent, disk *report.DiskGuard, log *slog.Logger, restart func()) (*upgrade.Manager, error) {
	bin, err := binaryPath(cfg)
	if err != nil {
		return nil, err
	}
	m := &upgrade.Manager{Self: Manifest(), Binary: bin, Dir: filepath.Join(cfg.DataDir, "upgrade"),
		Drain:   func(ctx context.Context) error { return agentOf().Drain(ctx, nil) },
		Restart: restart,
		Report: func(s upgrade.Status) {
			log.Info("upgrade", "target", s.Target, "phase", s.Phase.String(), "err", s.Error)
			agentOf().ReportUpgrade(s)
		},
		Healthy: func() bool { return healthy(agentOf().Status()) },
	}
	if cfg.ReleaseKeyFile != "" {
		k, err := LoadReleaseKey(cfg.ReleaseKeyFile)
		if err != nil {
			return nil, fmt.Errorf("release_key_file: %w", err)
		}
		m.ReleaseKey = k
	}
	if disk != nil {
		inner := m.Free
		m.Free = func(p string) (uint64, error) {
			if disk.Low > 0 && disk.Last() > 0 && disk.Last() < disk.Low {
				return 0, nil // 数据盘已在低水位（AGT-006）：不升级
			}
			if inner == nil {
				return upgrade.Free(p)
			}
			return inner(p)
		}
	}
	return m, nil
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
	if c := s.ClockSkew; c != nil {
		ms := c.Offset.Milliseconds()
		v.ClockSkewMs, v.ClockSkewAlert = &ms, c.Alert
	}
	return v
}

// ---------------------------------------------------------------- 注册（AGT-001）

// EnrollParams 是 vortex-node enroll 的参数。
type EnrollParams struct {
	Token      string // 一次性注册令牌
	Hub        string // Edge Hub 地址
	CAFile     string // 平台 CA（随安装包分发，用于校验 Edge Hub 与签发结果）
	DataDir    string
	ConfigPath string // 写入的节点配置文件
	Socket     string
	Caps       *edgehubv1.NodeCapabilities
	// 写入节点配置的部署项（安装脚本填写）
	LinkListen     string
	ReleaseKeyFile string
	Binary         string
}

// Enroll 本地生成私钥与 CSR，用令牌经 Edge Hub 换取节点证书，校验后写入数据目录（私钥 0600），并写节点配置。
// 令牌只用于注册，不写入任何文件（SEC-001）。
func Enroll(ctx context.Context, p EnrollParams) (*Config, error) {
	roots, err := certs.LoadPool(p.CAFile)
	if err != nil {
		return nil, err
	}
	key, err := certs.NewKey()
	if err != nil {
		return nil, err
	}
	csr, err := certs.CSR(key)
	if err != nil {
		return nil, err
	}
	resp, err := grpcconn.Enroll(ctx, p.Hub, roots, &edgehubv1.EnrollRequest{EnrollToken: p.Token, CsrDer: csr, AgentVersion: Version, Capabilities: p.Caps})
	if err != nil {
		return nil, fmt.Errorf("enroll: %w", err)
	}
	id, _, err := certs.Verify(resp.GetCertificateChainDer(), key, roots, types.UUID{})
	if err != nil {
		return nil, fmt.Errorf("enroll: issued certificate: %w", err)
	}
	if want, err := wire.FromUUID(resp.GetNodeId()); err != nil || want != id {
		return nil, fmt.Errorf("enroll: node_id in response does not match the certificate")
	}
	if err := os.MkdirAll(p.DataDir, 0o700); err != nil {
		return nil, err
	}
	cfg := &Config{NodeID: FormatUUID(id), DataDir: p.DataDir, Socket: p.Socket,
		LinkListen: p.LinkListen, ReleaseKeyFile: p.ReleaseKeyFile, Binary: p.Binary,
		CertFile: filepath.Join(p.DataDir, "node.crt"), KeyFile: filepath.Join(p.DataDir, "node.key"), CAFile: filepath.Join(p.DataDir, "ca.crt")}
	_, caPEM, err := certs.PoolFromDER(resp.GetCaBundleDer())
	if err != nil {
		return nil, err
	}
	if err := certs.WriteAtomic(cfg.CAFile, caPEM, 0o644); err != nil {
		return nil, err
	}
	if err := certs.Save(cfg.CertFile, cfg.KeyFile, resp.GetCertificateChainDer(), key); err != nil {
		return nil, err
	}
	for _, h := range resp.GetEdgeHubs() {
		hid, err := wire.FromUUID(h.GetHubId())
		if err != nil {
			return nil, err
		}
		cfg.EdgeHubs = append(cfg.EdgeHubs, HubEntry{HubID: FormatUUID(hid), Address: h.GetAddress()})
	}
	b, _ := json.MarshalIndent(cfg, "", "  ")
	if err := certs.WriteAtomic(p.ConfigPath, b, 0o600); err != nil {
		return nil, err
	}
	return cfg, nil
}
