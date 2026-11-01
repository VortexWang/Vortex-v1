// edge-hub 是 Edge Hub 可执行文件（03 方案 2；HUB-T01）。
//
//	edge-hub run -config /etc/vortex/edge-hub.json
//	edge-hub version
//
// run 的组装见 edge-hub/app：SQLite 账本重放恢复（包括自治中重启）、运行循环、NodeControl gRPC 服务、
// HubControl gRPC 客户端。
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"vortex/edgehub/app"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: edge-hub run|version")
		return 2
	}
	switch args[0] {
	case "version":
		fmt.Fprintln(stdout, app.Version)
		return 0
	case "run":
		path := "/etc/vortex/edge-hub.json"
		if len(args) == 3 && args[1] == "-config" {
			path = args[2]
		}
		cfg, err := app.LoadConfig(path)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		log := slog.New(slog.NewJSONHandler(stderr, nil))
		if err := app.Run(ctx, cfg, log, app.Options{}); err != nil {
			log.Error("run", "err", err)
			return 1
		}
		return 0
	}
	fmt.Fprintf(stderr, "unknown command %q\n", args[0])
	return 2
}
