// vortex-node 是节点端可执行文件：Agent + 嵌入的 Engine（GEN-002，02 方案 2；AGT-T01、AGT-T16）。
//
//	vortex-node run    -config /etc/vortex/node.json
//	vortex-node status [-socket /run/vortex/node.sock]
//	vortex-node drain  [-socket ...] [-timeout 300s] [-rules id,id]
//	vortex-node enroll -token ... -hub ...        （需要 gRPC，Go 模块代理放行后实现）
//	vortex-node version
//
// run 在一个 supervisor 下运行 Agent 事件循环、探测、本地运维接口与控制连接；任何模块异常退出都记录并
// 按退避重启，不影响 Engine 的转发。控制连接的传输（NodeControl.Connect gRPC 双向流）在 Go 模块代理放行后
// 接入；在此之前 run 以无控制连接的方式运行（恢复最后生效配置后保持“授权待确认”）。
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"vortex/engine"
	"vortex/engine/types"
	"vortex/node/agent"
	"vortex/node/agent/control"
	"vortex/node/agent/localapi"
	"vortex/node/agent/probe"
	"vortex/node/agent/report"
	"vortex/node/agent/store"
	"vortex/node/internal/supervisor"
)

var version = "dev"

const defaultSocket = "/run/vortex/node.sock"

// Config 是节点配置文件。
type Config struct {
	NodeID        string     `json:"node_id"`
	DataDir       string     `json:"data_dir"`
	Socket        string     `json:"socket"`
	EdgeHubs      []HubEntry `json:"edge_hubs"`
	DiskLowBytes  uint64     `json:"disk_low_bytes"`  // 默认 1 GiB
	DiskHighBytes uint64     `json:"disk_high_bytes"` // 默认 2 GiB
}

type HubEntry struct {
	HubID   string `json:"hub_id"`
	Address string `json:"address"`
}

func parseUUID(s string) (types.UUID, error) {
	var u types.UUID
	b, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	if err != nil || len(b) != 16 {
		return u, fmt.Errorf("invalid uuid %q", s)
	}
	copy(u[:], b)
	return u, nil
}

func fmtUUID(u types.UUID) string {
	if u == (types.UUID{}) {
		return ""
	}
	h := hex.EncodeToString(u[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

func loadConfig(path string) (*Config, error) {
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
		c.Socket = defaultSocket
	}
	if c.DiskLowBytes == 0 {
		c.DiskLowBytes = 1 << 30
	}
	if c.DiskHighBytes == 0 {
		c.DiskHighBytes = 2 << 30
	}
	return &c, nil
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: vortex-node run|status|drain|enroll|version")
		return 2
	}
	switch args[0] {
	case "version":
		fmt.Fprintln(stdout, version)
		return 0
	case "run":
		fs := flag.NewFlagSet("run", flag.ContinueOnError)
		fs.SetOutput(stderr)
		cfgPath := fs.String("config", "/etc/vortex/node.json", "node config file")
		if fs.Parse(args[1:]) != nil {
			return 2
		}
		cfg, err := loadConfig(*cfgPath)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		log := slog.New(slog.NewJSONHandler(stderr, nil))
		if err := runNode(ctx, cfg, nil, log); err != nil {
			log.Error("run", "err", err)
			return 1
		}
		return 0
	case "status":
		fs := flag.NewFlagSet("status", flag.ContinueOnError)
		fs.SetOutput(stderr)
		sock := fs.String("socket", defaultSocket, "local API socket")
		if fs.Parse(args[1:]) != nil {
			return 2
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		st, err := localapi.GetStatus(ctx, *sock)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(st)
		return 0
	case "drain":
		fs := flag.NewFlagSet("drain", flag.ContinueOnError)
		fs.SetOutput(stderr)
		sock := fs.String("socket", defaultSocket, "local API socket")
		timeout := fs.Duration("timeout", agent.DefaultDrainTimeout, "drain timeout (NOD-003)")
		rules := fs.String("rules", "", "comma-separated rule ids; empty = whole node")
		if fs.Parse(args[1:]) != nil {
			return 2
		}
		req := localapi.DrainRequest{TimeoutS: int(timeout.Seconds())}
		if *rules != "" {
			req.Rules = strings.Split(*rules, ",")
		}
		ctx, cancel := context.WithTimeout(context.Background(), *timeout+10*time.Second)
		defer cancel()
		if err := localapi.Drain(ctx, *sock, req); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		fmt.Fprintln(stdout, "drained")
		return 0
	case "enroll":
		fmt.Fprintln(stderr, "enroll: requires the NodeControl gRPC client, not available in this build")
		return 2
	}
	fmt.Fprintf(stderr, "unknown command %q\n", args[0])
	return 2
}

func openLog(dir, name string) (*store.File, error) { return store.OpenFile(filepath.Join(dir, name)) }

// runNode 组装并运行节点，直至 ctx 结束。dialer 为 nil 时不建立控制连接。
func runNode(ctx context.Context, cfg *Config, dialer control.Dialer, log *slog.Logger) error {
	node, err := parseUUID(cfg.NodeID)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(cfg.Socket), 0o755); err != nil {
		return err
	}
	var logs []*store.File
	defer func() {
		for _, l := range logs {
			l.Close()
		}
	}()
	open := func(name string) (*store.File, error) {
		l, err := openLog(cfg.DataDir, name)
		if err == nil {
			logs = append(logs, l)
		}
		return l, err
	}
	bootLog, err := open("boot.log")
	if err != nil {
		return err
	}
	usageLog, err := open("usage.log")
	if err != nil {
		return err
	}
	configLog, err := open("config.log")
	if err != nil {
		return err
	}

	prober := probe.New()
	eng := engine.New(engine.Options{Healthy: prober.TargetHealthy})
	disk := &report.DiskGuard{Path: cfg.DataDir, Low: cfg.DiskLowBytes, High: cfg.DiskHighBytes,
		OnLow: func(free uint64) { log.Warn("disk low: new sessions should stop (AGT-006)", "free", free) },
		OnOK:  func(free uint64) { log.Info("disk recovered", "free", free) }}
	a, err := agent.New(ctx, agent.Options{Node: node, AgentVersion: version, Engine: eng,
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
		Status: func() localapi.Status { return statusView(a.Status(), disk) },
		Drain: func(ctx context.Context, req localapi.DrainRequest) error {
			var ids []types.UUID
			for _, s := range req.Rules {
				id, err := parseUUID(s)
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
			id, err := parseUUID(h.HubID)
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
	log.Info("vortex-node started", "node", cfg.NodeID, "boot", fmtUUID(a.Status().Boot), "version", version)
	<-ctx.Done()
	sup.Wait()
	return a.Shutdown(eng.Close)
}

func statusView(s agent.Status, disk *report.DiskGuard) localapi.Status {
	mode := map[agent.Mode]string{agent.ModeUnspecified: "", agent.ModeOnline: "online", agent.ModeAutonomous: "autonomous"}[s.Mode]
	v := localapi.Status{NodeID: fmtUUID(s.Node), BootID: fmtUUID(s.Boot), PreviousBootID: fmtUUID(s.PreviousBoot),
		Hub: fmtUUID(s.Hub), HubMode: mode, Connected: s.Connected, Phase: s.Phase.String(), DesiredVersion: s.AppliedVersion,
		ActualVersion: s.ActualVersion, GrantRemainingS: map[string]int64{}, PendingBatches: s.PendingBatches, DiskLow: s.DiskLow}
	for r, d := range s.RunRemaining {
		v.GrantRemainingS[fmtUUID(r)] = int64(d / time.Second)
	}
	if disk != nil {
		v.DiskFreeBytes = disk.Last()
	}
	return v
}
