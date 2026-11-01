// edge-hub 是 Edge Hub 可执行文件（03 方案 2；HUB-T01 的非 gRPC 部分）。
//
//	edge-hub run -config /etc/vortex/edge-hub.json
//	edge-hub version
//
// run 打开数据目录下的账本（重放恢复，包括自治中重启），组装各模块，运行 runner 循环。面向 Agent 的
// NodeControl 服务与到后端的 HubControl 客户端（gRPC）在 Go 模块代理放行后接入；在此之前 run 不接受
// 连接，后端始终失联，30 秒后进入自治（与真实部署中后端不可达时的行为一致）。
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"vortex/edgehub/issuer"
	"vortex/edgehub/ledger"
	"vortex/edgehub/model"
	"vortex/edgehub/runner"
	"vortex/edgehub/session"
	"vortex/edgehub/upstream"
)

var version = "dev"

type Config struct {
	HubID   string `json:"hub_id"`
	Region  string `json:"region"`
	DataDir string `json:"data_dir"`
}

func parseUUID(s string) (model.UUID, error) {
	var u model.UUID
	b, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	if err != nil || len(b) != 16 {
		return u, fmt.Errorf("invalid uuid %q", s)
	}
	copy(u[:], b)
	return u, nil
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: edge-hub run|version")
		return 2
	}
	switch args[0] {
	case "version":
		fmt.Fprintln(stdout, version)
		return 0
	case "run":
		path := "/etc/vortex/edge-hub.json"
		if len(args) == 3 && args[1] == "-config" {
			path = args[2]
		}
		b, err := os.ReadFile(path)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		var cfg Config
		if err := json.Unmarshal(b, &cfg); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		log := slog.New(slog.NewJSONHandler(stderr, nil))
		if err := runHub(ctx, &cfg, log); err != nil {
			log.Error("run", "err", err)
			return 1
		}
		return 0
	}
	fmt.Fprintf(stderr, "unknown command %q\n", args[0])
	return 2
}

// runHub 组装并运行 Edge Hub 直至 ctx 结束。
func runHub(ctx context.Context, cfg *Config, log *slog.Logger) error {
	id, err := parseUUID(cfg.HubID)
	if err != nil {
		return err
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "/var/lib/vortex-edge-hub"
	}
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return err
	}
	st, err := ledger.OpenFile(filepath.Join(cfg.DataDir, "ledger.jsonl"))
	if err != nil {
		return err
	}
	defer st.Close()
	// 规则状态由配置分发维护（后端下发的期望配置）；接入 gRPC 前没有来源
	h, err := session.Open(session.Options{HubID: id, Store: st,
		Rules: session.RulesFunc(func(model.UUID) []issuer.RuleState { return nil })})
	if err != nil {
		return err
	}
	link := upstream.New(h)
	log.Info("edge-hub started", "hub", cfg.HubID, "region", cfg.Region, "mode", h.Mode.Mode().String(),
		"ledger_last_seq", h.Ledger.LastSeq(), "version", version)
	r := &runner.Runner{Hub: h, Link: link, OnError: func(step string, err error) { log.Error("loop", "step", step, "err", err) }}
	h.Mode.OnChange(func(m model.Mode) { log.Warn("mode changed", "mode", m.String()) })
	r.Run(ctx)
	return nil
}
