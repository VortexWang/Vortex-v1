// vortex-node 是节点端可执行文件：Agent + 嵌入的 Engine（GEN-002，02 方案 2；AGT-T01、AGT-T16）。
//
//	vortex-node run    -config /etc/vortex/node.json
//	vortex-node status [-socket /run/vortex/node.sock]
//	vortex-node drain  [-socket ...] [-timeout 300s] [-rules id,id]
//	vortex-node enroll -token ... -hub ...        （AGT-T02，尚未实现）
//	vortex-node version
//
// run 的组装见 node/app：supervisor 下运行 Agent 事件循环、探测、本地运维接口与控制连接（配置了证书时
// 经 gRPC + mTLS 连接 Edge Hub；未配置时不连接，恢复最后生效配置后保持“授权待确认”）。
// enroll 在注册与证书续签实现后提供（AGT-T02）。
package main

import (
	"context"
	"encoding/json"
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

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: vortex-node run|status|drain|enroll|version")
		return 2
	}
	switch args[0] {
	case "version":
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
		if err := app.Run(ctx, cfg, dialer, log); err != nil {
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
	case "enroll":
		fmt.Fprintln(stderr, "enroll: not implemented yet (AGT-T02)")
		return 2
	}
	fmt.Fprintf(stderr, "unknown command %q\n", args[0])
	return 2
}
