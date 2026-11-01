package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRunHubAndRestart(t *testing.T) {
	cfg := &Config{HubID: "4b000000-0000-4000-8000-000000000001", DataDir: filepath.Join(t.TempDir(), "data")}
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	for i := 0; i < 2; i++ { // 第二次：从账本重放启动
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		if err := runHub(ctx, cfg, log); err != nil {
			t.Fatal(err)
		}
		cancel()
	}
	if strings.Count(buf.String(), "edge-hub started") != 2 {
		t.Fatalf("log %s", buf.String())
	}
	if err := runHub(context.Background(), &Config{HubID: "bad"}, slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil {
		t.Fatal("bad hub id accepted")
	}
	var out, errb bytes.Buffer
	if run([]string{"version"}, &out, &errb) != 0 || run(nil, &out, &errb) != 2 || run([]string{"x"}, &out, &errb) != 2 ||
		run([]string{"run", "-config", "/nonexistent"}, &out, &errb) != 1 {
		t.Fatal("exit codes")
	}
}
