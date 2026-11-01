// vortex-node 是节点端可执行文件：Agent + 嵌入的 Engine（GEN-002，02 方案 2；AGT-T01、AGT-T16）。
//
//	vortex-node run    -config /etc/vortex/node.json
//	vortex-node status [-socket /run/vortex/node.sock]
//	vortex-node drain  [-socket ...] [-timeout 300s] [-rules id,id]
//	vortex-node enroll -token T -hub host:port [-ca /etc/vortex/ca.crt] [-data-dir ...] [-config ...]
//	vortex-node version [-json]
//
// run 的组装见 node/app：supervisor 下运行 Agent 事件循环、探测、本地运维接口与控制连接（配置了证书时
// 经 gRPC + mTLS 连接 Edge Hub；未配置时不连接，恢复最后生效配置后保持“授权待确认”）。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"vortex/node/agent"
	"vortex/node/agent/localapi"
	"vortex/node/app"
)

// ExitRestart 是升级或回退后请求 systemd 重启的退出码（单元配置 Restart=always）。
const ExitRestart = 75

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: vortex-node run|status|drain|enroll|upgrade-guard|version")
		return 2
	}
	switch args[0] {
	case "version":
		if len(args) > 1 && args[1] == "-json" { // 升级时由旧版本执行，读取兼容信息（AGT-008）
			b, _ := json.Marshal(app.Manifest())
			fmt.Fprintln(stdout, string(b))
			return 0
		}
		fmt.Fprintln(stdout, app.Version)
		return 0
	case "run":
		fs := flag.NewFlagSet("run", flag.ContinueOnError)
		fs.SetOutput(stderr)
		cfgPath := fs.String("config", "/etc/vortex/node.json", "node config file")
		if fs.Parse(args[1:]) != nil {
			return 2
		}
		cfg, err := app.LoadConfig(*cfgPath)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		log := slog.New(slog.NewJSONHandler(stderr, nil))
		dialer, err := app.Dialer(cfg)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		if err := app.Run(ctx, cfg, dialer, log); errors.Is(err, app.ErrRestart) {
			log.Info("exiting for restart (upgrade)")
			return ExitRestart
		} else if err != nil {
			log.Error("run", "err", err)
			return 1
		}
		return 0
	case "status":
		fs := flag.NewFlagSet("status", flag.ContinueOnError)
		fs.SetOutput(stderr)
		sock := fs.String("socket", app.DefaultSocket, "local API socket")
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
		sock := fs.String("socket", app.DefaultSocket, "local API socket")
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
	case "upgrade-guard": // systemd ExecStartPre，由上一版本执行（AGT-008）；失败不阻止启动
		fs := flag.NewFlagSet("upgrade-guard", flag.ContinueOnError)
		fs.SetOutput(stderr)
		cfgPath := fs.String("config", "/etc/vortex/node.json", "node config file")
		if fs.Parse(args[1:]) != nil {
			return 2
		}
		cfg, err := app.LoadConfig(*cfgPath)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		rolled, err := app.UpgradeGuard(cfg)
		if err != nil {
			fmt.Fprintln(stderr, "upgrade-guard:", err)
			return 1
		}
		if rolled {
			fmt.Fprintln(stderr, "upgrade-guard: new version failed to start repeatedly; previous version restored")
		}
		return 0
	case "enroll":
		fs := flag.NewFlagSet("enroll", flag.ContinueOnError)
		fs.SetOutput(stderr)
		token := fs.String("token", "", "one-time enrollment token")
		hub := fs.String("hub", "", "edge hub address host:port")
		ca := fs.String("ca", "/etc/vortex/ca.crt", "platform CA (PEM), shipped with the installer")
		dataDir := fs.String("data-dir", "/var/lib/vortex", "data directory for keys and state")
		cfgPath := fs.String("config", "/etc/vortex/node.json", "node config file to write")
		sock := fs.String("socket", app.DefaultSocket, "local API socket")
		link := fs.String("link-listen", "", "VLink listen address to write into the config, e.g. 0.0.0.0:7443")
		relKey := fs.String("release-key", "", "platform release public key (PEM) to write into the config")
		bin := fs.String("binary", "", "installed program path to write into the config (replaced on upgrade)")
		if fs.Parse(args[1:]) != nil || *token == "" || *hub == "" {
			fmt.Fprintln(stderr, "enroll: -token and -hub are required")
			return 2
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cfg, err := app.Enroll(ctx, app.EnrollParams{Token: *token, Hub: *hub, CAFile: *ca, DataDir: *dataDir, ConfigPath: *cfgPath, Socket: *sock,
			LinkListen: *link, ReleaseKeyFile: *relKey, Binary: *bin})
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		fmt.Fprintf(stdout, "enrolled as node %s; config written to %s\n", cfg.NodeID, *cfgPath)
		return 0
	}
	fmt.Fprintf(stderr, "unknown command %q\n", args[0])
	return 2
}
