// backend 是 VORTEX 后端进程（BE-T01）。
//
//	backend run -config /etc/vortex/backend.json
//	backend version
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

	"vortex/backend/app"
	"vortex/backend/internal/platform"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: backend run|version")
		return 2
	}
	switch args[0] {
	case "version":
		fmt.Fprintln(stdout, app.Version)
		return 0
	case "run":
		fs := flag.NewFlagSet("run", flag.ContinueOnError)
		fs.SetOutput(stderr)
		cfgPath := fs.String("config", "/etc/vortex/backend.json", "config file")
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
		log := platform.NewLogger(stderr, slog.LevelInfo)
		if err := app.Run(ctx, cfg, log, app.Options{}); err != nil {
			log.Error("run", "err", err)
			return 1
		}
		return 0
	}
	fmt.Fprintf(stderr, "unknown command %q\n", args[0])
	return 2
}
