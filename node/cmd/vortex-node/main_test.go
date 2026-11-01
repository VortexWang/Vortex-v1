package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"vortex/node/agent/localapi"
	"vortex/node/app"
)

func TestRunStatusDrain(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "node.sock")
	cfgPath := filepath.Join(dir, "node.json")
	cfg := app.Config{NodeID: "0d000000-0000-4000-8000-000000000001", DataDir: filepath.Join(dir, "data"), Socket: sock, DiskLowBytes: 1}
	b, _ := json.Marshal(cfg)
	_ = os.WriteFile(cfgPath, b, 0o600)
	c, err := app.LoadConfig(cfgPath)
	if err != nil || c.DiskHighBytes != 2<<30 {
		t.Fatalf("config %+v %v", c, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx, c, nil, slog.New(slog.NewTextHandler(io.Discard, nil))) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(sock); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("socket not created")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if fi, _ := os.Stat(sock); fi.Mode().Perm() != 0o600 {
		t.Fatalf("socket mode %v", fi.Mode().Perm())
	}
	var out, errb bytes.Buffer
	if code := run([]string{"status", "-socket", sock}, &out, &errb); code != 0 {
		t.Fatalf("status exit %d: %s", code, errb.String())
	}
	var st localapi.Status
	if err := json.Unmarshal(out.Bytes(), &st); err != nil {
		t.Fatal(err)
	}
	if st.NodeID != cfg.NodeID || st.Phase != "idle" || st.Connected || st.BootID == "" || st.DiskFreeBytes == 0 {
		t.Fatalf("status %+v", st)
	}
	out.Reset()
	if code := run([]string{"drain", "-socket", sock, "-timeout", "2s"}, &out, &errb); code != 0 || !strings.Contains(out.String(), "drained") {
		t.Fatalf("drain exit %d: %s %s", code, out.String(), errb.String())
	}
	if code := run([]string{"drain", "-socket", sock, "-rules", "nope"}, &out, &errb); code != 1 {
		t.Fatal("invalid rule id accepted")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	// 再次启动：新的 boot_id，上一次的记为 previous
	ctx2, cancel2 := context.WithCancel(context.Background())
	go func() { done <- app.Run(ctx2, c, nil, slog.New(slog.NewTextHandler(io.Discard, nil))) }()
	deadline = time.Now().Add(5 * time.Second)
	var st2 localapi.Status
	for {
		s, err := localapi.GetStatus(context.Background(), sock)
		if err == nil && s.BootID != st.BootID {
			st2 = s
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second run not up")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if st2.PreviousBootID != st.BootID {
		t.Fatalf("previous boot %q, want %q", st2.PreviousBootID, st.BootID)
	}
	cancel2()
	<-done
}

func TestCommands(t *testing.T) {
	var out, errb bytes.Buffer
	if run(nil, &out, &errb) != 2 || run([]string{"bogus"}, &out, &errb) != 2 || run([]string{"enroll"}, &out, &errb) != 2 {
		t.Fatal("exit codes")
	}
	if run([]string{"version"}, &out, &errb) != 0 || strings.TrimSpace(out.String()) != app.Version {
		t.Fatal("version")
	}
	if run([]string{"status", "-socket", filepath.Join(t.TempDir(), "none.sock")}, &out, &errb) != 1 {
		t.Fatal("status without a running node")
	}
	if _, err := app.ParseUUID("xyz"); err == nil {
		t.Fatal("bad uuid accepted")
	}
	if u, _ := app.ParseUUID("0d000000-0000-4000-8000-000000000001"); app.FormatUUID(u) != "0d000000-0000-4000-8000-000000000001" {
		t.Fatal("uuid round trip")
	}
}
