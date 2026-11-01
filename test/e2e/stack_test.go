package e2e

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/durationpb"

	hubapp "vortex/edgehub/app"
	"vortex/edgehub/backendgrpc"
	"vortex/engine/types"
	"vortex/engine/vlink"
	"vortex/node/agent/localapi"
	nodewire "vortex/node/agent/wire"
	nodeapp "vortex/node/app"
	backendv1 "vortex/proto/gen/go/vortex/backend/v1"
	edgehubv1 "vortex/proto/gen/go/vortex/edgehub/v1"
	runtimev1 "vortex/proto/gen/go/vortex/runtime/v1"
)

// protoBackend 是线上协议层面的模拟后端（HubControl 服务端）：注册后委托额度、下发计费版本与期望配置；
// 节点接入后为其启动下发份额；Prepare 成功即 Commit；账本条目立即确认。
type protoBackend struct {
	backendv1.UnimplementedHubControlServer
	cfg  *types.RuntimeConfig
	more []*types.RuntimeConfig // 其他节点的期望配置（按 NodeID 下发）

	mu      sync.Mutex
	usage   int
	applied []uint64
	pushed  map[string]bool
	out     func(*backendv1.BackendMessage) // 当前流（push 用）
	version map[string]string               // 在线节点的程序版本（NodeStatus.agent_version），按 node_id
}

// push 经当前流下发一条消息。
func (b *protoBackend) push(m *backendv1.BackendMessage) bool {
	b.mu.Lock()
	out := b.out
	b.mu.Unlock()
	if out == nil {
		return false
	}
	out(m)
	return true
}

// versions 返回各在线节点的程序版本。
func (b *protoBackend) versions() map[string]string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := map[string]string{}
	for k, v := range b.version {
		out[k] = v
	}
	return out
}

func (b *protoBackend) Connect(s grpc.BidiStreamingServer[backendv1.HubUpMessage, backendv1.BackendMessage]) error {
	var smu sync.Mutex
	send := func(m *backendv1.BackendMessage) { smu.Lock(); _ = s.Send(m); smu.Unlock() }
	b.mu.Lock()
	b.out = send
	b.mu.Unlock()
	for {
		m, err := s.Recv()
		if err != nil {
			return err
		}
		switch x := m.Body.(type) {
		case *backendv1.HubUpMessage_Register:
			send(&backendv1.BackendMessage{Body: &backendv1.BackendMessage_Budget{Budget: &backendv1.BudgetUpdate{
				TenantId: nodewire.UUID(tenant), BudgetVersion: 1, Delegated: 1 << 30}}})
			send(&backendv1.BackendMessage{Body: &backendv1.BackendMessage_BillingVersions{BillingVersions: &backendv1.BillingVersionSync{
				Versions: []*backendv1.BillingVersion{{Version: 1}}}}})
			send(&backendv1.BackendMessage{Body: &backendv1.BackendMessage_Desired{Desired: &backendv1.NodeDesiredConfig{
				NodeId: nodewire.UUID(nodeID), Config: nodewire.Config(b.cfg)}}})
			for _, c := range b.more {
				send(&backendv1.BackendMessage{Body: &backendv1.BackendMessage_Desired{Desired: &backendv1.NodeDesiredConfig{
					NodeId: nodewire.UUID(c.NodeID), Config: nodewire.Config(c)}}})
			}
		case *backendv1.HubUpMessage_NodeStatus:
			for _, n := range x.NodeStatus.Nodes {
				key := string(n.GetBootId().GetValue())
				b.mu.Lock()
				if b.version == nil {
					b.version = map[string]string{}
				}
				if nk := string(n.GetNodeId().GetValue()); n.Connected {
					b.version[nk] = n.AgentVersion
				} else {
					delete(b.version, nk)
				}
				first := n.Connected && !b.pushed[key]
				b.pushed[key] = true
				b.mu.Unlock()
				if first {
					meta := func() *runtimev1.GrantMeta {
						return &runtimev1.GrantMeta{GrantId: nodewire.UUID(rid()), Ttl: durationpb.New(5 * time.Minute)}
					}
					send(&backendv1.BackendMessage{Body: &backendv1.BackendMessage_Leases{Leases: &backendv1.ShareLeaseUpdate{NodeId: n.NodeId,
						Leases: []*runtimev1.ShareLease{
							{Meta: meta(), TenantId: nodewire.UUID(tenant), Kind: runtimev1.ShareKind_SHARE_KIND_RATE, Amount: 10_000_000_000},
							{Meta: meta(), TenantId: nodewire.UUID(tenant), Kind: runtimev1.ShareKind_SHARE_KIND_CONN, Amount: 1000}}}}})
				}
			}
		case *backendv1.HubUpMessage_ApplyProgress:
			if p := x.ApplyProgress.GetPrepare(); p != nil && len(p.GetResult().GetErrors()) == 0 {
				send(&backendv1.BackendMessage{Body: &backendv1.BackendMessage_Command{Command: &backendv1.NodeApplyCommand{
					NodeId: x.ApplyProgress.NodeId, Command: &edgehubv1.ApplyCommand{Version: p.GetResult().GetVersion(),
						Action: edgehubv1.ApplyAction_APPLY_ACTION_COMMIT}}}})
			}
			if a := x.ApplyProgress.GetApply(); a != nil {
				b.mu.Lock()
				b.applied = append(b.applied, a.GetResult().GetVersion())
				b.mu.Unlock()
			}
		case *backendv1.HubUpMessage_Ledger:
			b.mu.Lock()
			for _, e := range x.Ledger.Entries {
				if e.GetNodeUsage() != nil {
					b.usage++
				}
			}
			b.mu.Unlock()
			send(&backendv1.BackendMessage{Body: &backendv1.BackendMessage_LedgerAck{LedgerAck: &backendv1.LedgerAck{
				AckedThroughSeq: x.Ledger.Entries[len(x.Ledger.Entries)-1].Seq}}})
		}
	}
}

var ridN byte

func rid() types.UUID {
	ridN++
	return types.UUID{0xee, ridN}
}

func writePEM(t *testing.T, dir, name string, c tls.Certificate) (string, string) {
	cert := filepath.Join(dir, name+".crt")
	key := filepath.Join(dir, name+".key")
	_ = os.WriteFile(cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: c.Certificate[0]}), 0o600)
	der, err := x509.MarshalPKCS8PrivateKey(c.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(key, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600)
	return cert, key
}

// 完整组装：模拟后端（gRPC）← edge-hub（app.Run：SQLite 账本、NodeControl 服务、HubControl 客户端）
// ← vortex-node（app.Run：SQLite 本地存储、gRPC 控制连接、Engine）。配置经后端下发、两阶段应用，授权与份额
// 经 Edge Hub 签发与转发，流量经节点转发，计量经 Edge Hub 落盘并由后端确认；status 反映这一切。
func TestFullStack(t *testing.T) {
	p := newPKI(t)
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.crt")
	_ = os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: p.ca.Raw}), 0o600)
	cfg := config(t)
	addr := cfg.Listeners[0].Bind

	// 后端
	be := &protoBackend{cfg: cfg, pushed: map[string]bool{}}
	bln, _ := net.Listen("tcp4", "127.0.0.1:0")
	bsrv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{p.issue(t, backendgrpc.BackendURI, x509.ExtKeyUsageServerAuth)},
		ClientCAs: p.roots, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13})))
	backendv1.RegisterHubControlServer(bsrv, be)
	go func() { _ = bsrv.Serve(bln) }()
	defer bsrv.Stop()

	// Edge Hub
	hc, hk := writePEM(t, dir, "hub", p.hub(t, hubA))
	hubCfg := &hubapp.Config{HubID: vlink.FormatUUID(hubA), Region: "test", DataDir: filepath.Join(dir, "hub"), Listen: "127.0.0.1:0",
		CertFile: hc, KeyFile: hk, CAFile: caFile, BackendAddress: bln.Addr().String()}
	listen := make(chan string, 1)
	ctx, cancel := context.WithCancel(context.Background())
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	if os.Getenv("STACK_DEBUG") != "" {
		quiet = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	hubDone := make(chan error, 1)
	go func() {
		hubDone <- hubapp.Run(ctx, hubCfg, quiet, hubapp.Options{OnListen: func(a string) { listen <- a }})
	}()
	hubAddr := <-listen

	// 节点
	nc, nk := writePEM(t, dir, "node", p.node(t, nodeID))
	nodeCfg := &nodeapp.Config{NodeID: vlink.FormatUUID(nodeID), DataDir: filepath.Join(dir, "node"), Socket: filepath.Join(dir, "node.sock"),
		EdgeHubs: []nodeapp.HubEntry{{HubID: vlink.FormatUUID(hubA), Address: hubAddr}}, DiskLowBytes: 1, DiskHighBytes: 1,
		CertFile: nc, KeyFile: nk, CAFile: caFile}
	b, _ := json.Marshal(nodeCfg)
	cfgPath := filepath.Join(dir, "node.json")
	_ = os.WriteFile(cfgPath, b, 0o600)
	loaded, err := nodeapp.LoadConfig(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	dialer, err := nodeapp.Dialer(loaded)
	if err != nil || dialer == nil {
		t.Fatalf("dialer %v", err)
	}
	nodeDone := make(chan error, 1)
	go func() { nodeDone <- nodeapp.Run(ctx, loaded, dialer, quiet) }()
	defer func() {
		cancel()
		if err := <-nodeDone; err != nil {
			t.Error(err)
		}
		if err := <-hubDone; err != nil {
			t.Error(err)
		}
	}()

	if os.Getenv("STACK_DEBUG") != "" {
		defer func() {
			s, e := localapi.GetStatus(context.Background(), nodeCfg.Socket)
			t.Logf("status %+v %v", s, e)
		}()
	}
	eventually(t, "forwarding through the full stack", func() bool { return ok(addr) })
	st, err := localapi.GetStatus(context.Background(), nodeCfg.Socket)
	if err != nil || !st.Connected || st.HubMode != "online" || st.Phase != "applied" || st.DesiredVersion != 1 || st.Hub != vlink.FormatUUID(hubA) {
		t.Fatalf("status %+v %v", st, err)
	}
	if !ok(addr) {
		t.Fatal("second session")
	}
	be.mu.Lock()
	applied := append([]uint64(nil), be.applied...)
	be.mu.Unlock()
	if len(applied) == 0 || applied[0] != 1 {
		t.Fatalf("backend saw apply results %v", applied)
	}
	// 计量经 Edge Hub 到后端：节点每 10 秒封批，这里直接看 Edge Hub 账本上报的结果（等一次封批）
	// 节点每 10 秒封批一次，再经 Edge Hub 落盘与上报：给 30 秒
	deadline := time.Now().Add(30 * time.Second)
	for {
		be.mu.Lock()
		n := be.usage
		be.mu.Unlock()
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timeout: usage reached the backend")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := os.Stat(filepath.Join(dir, "hub", "ledger.db")); err != nil {
		t.Fatal("hub ledger is not SQLite")
	}
	if _, err := os.Stat(filepath.Join(dir, "node", "agent.db")); err != nil {
		t.Fatal("agent store is not SQLite")
	}
}
