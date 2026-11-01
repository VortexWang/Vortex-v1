// edge-hub 是 Edge Hub 可执行文件（03 方案 2；HUB-T01）。
//
//	edge-hub run -config /etc/vortex/edge-hub.json
//	edge-hub enroll -token T -backend host:port -region R [-ca ...] [-listen :7400] [-data-dir ...] [-config ...]
//	edge-hub version
//
// run 的组装见 edge-hub/app：SQLite 账本重放恢复（包括自治中重启）、运行循环、NodeControl gRPC 服务、
// HubControl gRPC 客户端。
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
	"vortex/edgehub/app"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: edge-hub run|enroll|version")
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
	if args[0] == "enroll" {
		fs := flag.NewFlagSet("enroll", flag.ContinueOnError)
		fs.SetOutput(stderr)
		token := fs.String("token", "", "one-time bootstrap token")
		backend := fs.String("backend", "", "backend HubControl address host:port")
		ca := fs.String("ca", "/etc/vortex/ca.crt", "platform CA (PEM), shipped with the installer")
		region := fs.String("region", "", "region of this instance")
		listen := fs.String("listen", ":7400", "NodeControl listen address to write into the config")
		advertise := fs.String("advertise", "", "address nodes use to reach this hub (host:port); default: -listen if it has a host")
		dataDir := fs.String("data-dir", "/var/lib/vortex-edge-hub", "data directory")
		cfgPath := fs.String("config", "/etc/vortex/edge-hub.json", "config file to write")
		if fs.Parse(args[1:]) != nil || *token == "" || *backend == "" || *region == "" {
			fmt.Fprintln(stderr, "enroll: -token, -backend and -region are required")
			return 2
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cfg, err := app.Enroll(ctx, app.EnrollParams{Token: *token, Backend: *backend, CAFile: *ca, Region: *region, Listen: *listen, Advertise: *advertise,
			DataDir: *dataDir, ConfigPath: *cfgPath})
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		fmt.Fprintf(stdout, "enrolled as hub %s; config written to %s\n", cfg.HubID, *cfgPath)
		return 0
	}
	fmt.Fprintf(stderr, "unknown command %q\n", args[0])
	return 2
}
