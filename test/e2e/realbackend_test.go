package e2e

import (
	"context"
	"encoding/pem"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	backendapp "vortex/backend/app"
	"vortex/backend/apptest"
	hubapp "vortex/edgehub/app"
	nodeapp "vortex/node/app"
)

// 真实后端（M4 第一批）：backend（app.Run：迁移、平台 CA、HubControl）← edge-hub ← vortex-node。
// 实例用后端签发的引导令牌换取身份（带对外地址，CC-03），节点用注册令牌经 Edge Hub 注册（证书由平台中间 CA 签发，
// 实例列表来自后端），节点上线后后端记录其状态；实例证书经 RenewHubCert 续签（CC-02），原证书过期后
// 后端重启，Edge Hub 以新证书重新连接。
func TestRealBackendEnrollment(t *testing.T) {
	dsn := apptest.Database(t)
	dir := t.TempDir()
	caCfg, ca := apptest.CA(t, dir)
	caFile := filepath.Join(dir, "platform-ca.crt")
	_ = os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Root.Raw}), 0o644)
	admin := apptest.Admin(t, dsn)
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	if os.Getenv("STACK_DEBUG") != "" {
		quiet = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}

	beAddr := freePort(t).String()
	cfg := &backendapp.Config{DatabaseURL: dsn, GRPCListen: beAddr, CA: caCfg}
	startBackend := func() (*backendapp.Backend, context.CancelFunc, chan error) {
		ctx, cancel := context.WithCancel(context.Background())
		ready := make(chan *backendapp.Backend, 1)
		done := make(chan error, 1)
		go func() {
			done <- backendapp.Run(ctx, cfg, quiet, backendapp.Options{HubCertLife: 6 * time.Second, ReconcileQuiet: 200 * time.Millisecond,
				OnReady: func(b *backendapp.Backend) { ready <- b }})
		}()
		select {
		case b := <-ready:
			return b, cancel, done
		case err := <-done:
			t.Fatalf("backend: %v", err)
		case <-time.After(30 * time.Second):
			t.Fatal("backend did not start")
		}
		return nil, cancel, done
	}
	be, stopBackend, beDone := startBackend()

	// Edge Hub 引导
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hubListen := freePort(t).String()
	tok, err := be.CreateHubBootstrapToken(ctx, "test", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	hp := hubapp.EnrollParams{Token: "wrong", Backend: beAddr, CAFile: caFile, Region: "test", Listen: hubListen,
		DataDir: filepath.Join(dir, "hub"), ConfigPath: filepath.Join(dir, "hub.json")}
	if _, err := hubapp.Enroll(ctx, hp); err == nil {
		t.Fatal("bad bootstrap token accepted")
	}
	hp.Token = tok
	hubCfg, err := hubapp.Enroll(ctx, hp)
	if err != nil {
		t.Fatal(err)
	}
	firstHubSerial := serial(t, hubCfg.CertFile)
	hubCfg.SetRenewEvery(200 * time.Millisecond)
	hubDone := make(chan error, 1)
	go func() { hubDone <- hubapp.Run(ctx, hubCfg, quiet, hubapp.Options{}) }()
	start := time.Now()

	// 节点注册（经 Edge Hub 到后端）
	np := nodeapp.EnrollParams{Hub: hubListen, CAFile: caFile, DataDir: filepath.Join(dir, "node"),
		ConfigPath: filepath.Join(dir, "node.json"), Socket: filepath.Join(dir, "node.sock")}
	ntok, err := be.CreateEnrollToken(ctx, [16]byte{}, admin)
	if err != nil {
		t.Fatal(err)
	}
	np.Token = ntok
	var nodeCfg *nodeapp.Config
	eventually(t, "node enrolled through the hub", func() bool {
		var err error
		nodeCfg, err = nodeapp.Enroll(ctx, np) // Edge Hub 在线（收到后端消息）之前拒绝注册
		return err == nil
	})
	if len(nodeCfg.EdgeHubs) != 1 || nodeCfg.EdgeHubs[0].Address != hubListen || nodeCfg.EdgeHubs[0].HubID != hubCfg.HubID {
		t.Fatalf("hub list from the backend: %+v", nodeCfg.EdgeHubs)
	}
	if _, err := nodeapp.Enroll(ctx, np); err == nil {
		t.Fatal("enrollment token reused")
	}
	loaded, _ := nodeapp.LoadConfig(np.ConfigPath)
	dialer, err := nodeapp.Dialer(loaded)
	if err != nil {
		t.Fatal(err)
	}
	nodeDone := make(chan error, 1)
	go func() { nodeDone <- nodeapp.Run(ctx, loaded, dialer, quiet) }()
	defer func() {
		cancel()
		<-nodeDone
		<-hubDone
		stopBackend()
		<-beDone
	}()
	nodeID, _ := nodeapp.ParseUUID(nodeCfg.NodeID)
	waitDB := func(what, sql string, args []any, want string) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for {
			var got string
			apptest.QueryRow(t, dsn, sql, args, &got)
			if got == want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: got %q, want %q", what, got, want)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	waitDB("node online in the backend", "SELECT lifecycle || '/' || coalesce(agent_version, '') FROM nodes WHERE id = $1",
		[]any{nodeID[:]}, "online/"+nodeapp.Version)
	hubID, _ := hubapp.ParseUUID(hubCfg.HubID)
	waitDB("hub online in the backend", "SELECT mode FROM edge_hubs WHERE id = $1", []any{hubID[:]}, "online")

	// 实例证书续签（有效期 6 秒，剩余 1/3 时续签）
	eventually30s(t, "hub certificate renewed by the backend", func() bool { return serial(t, hubCfg.CertFile).Cmp(firstHubSerial) != 0 })
	var hubCerts int
	apptest.QueryRow(t, dsn, "SELECT count(*) FROM certificates WHERE subject_kind = 'hub' AND subject_id = $1", []any{hubID[:]}, &hubCerts)
	if hubCerts < 2 {
		t.Fatalf("%d hub certificates recorded", hubCerts)
	}
	time.Sleep(time.Until(start.Add(7 * time.Second))) // 原证书过期
	stopBackend()
	<-beDone
	waitDB("hub offline after the backend stopped", "SELECT mode FROM edge_hubs WHERE id = $1", []any{hubID[:]}, "offline")
	_, stopBackend, beDone = startBackend()
	waitDB("hub reconnected with the renewed certificate", "SELECT mode FROM edge_hubs WHERE id = $1", []any{hubID[:]}, "online")
	waitDB("node still online", "SELECT lifecycle FROM nodes WHERE id = $1", []any{nodeID[:]}, "online")
}
