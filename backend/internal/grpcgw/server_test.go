package grpcgw

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"vortex/backend/apptest"
	"vortex/backend/internal/fleet"
	"vortex/backend/internal/pki"
	"vortex/backend/internal/platform"
	backendv1 "vortex/proto/gen/go/vortex/backend/v1"
	commonv1 "vortex/proto/gen/go/vortex/common/v1"
	edgehubv1 "vortex/proto/gen/go/vortex/edgehub/v1"
)

type env struct {
	t     *testing.T
	dsn   string
	ca    *pki.CA
	fl    *fleet.Service
	addr  string
	admin platform.UUID
	gw    *Server
}

func setup(t *testing.T) *env {
	dsn := apptest.Database(t)
	_, ca := apptest.CA(t, t.TempDir())
	pool, err := platform.OpenDB(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	fl := &fleet.Service{DB: pool, CA: ca}
	gw := &Server{Fleet: fl, ReconcileQuiet: 300 * time.Millisecond}
	ln, _ := net.Listen("tcp4", "127.0.0.1:0")
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(gw.TLSConfig())))
	backendv1.RegisterHubControlServer(srv, gw)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	return &env{t: t, dsn: dsn, ca: ca, fl: fl, addr: ln.Addr().String(), admin: apptest.Admin(t, dsn), gw: gw}
}

// client 连接后端；cert 为 nil 时不出示客户端证书。后端身份按平台根与 spiffe://vortex/backend 校验。
func (e *env) client(cert *tls.Certificate) backendv1.HubControlClient {
	tc := &tls.Config{MinVersion: tls.VersionTLS13, InsecureSkipVerify: true,
		VerifyConnection: func(cs tls.ConnectionState) error {
			leaf, err := e.ca.Verify(cs.PeerCertificates, x509.ExtKeyUsageServerAuth)
			if err != nil {
				return err
			}
			if len(leaf.URIs) != 1 || leaf.URIs[0].String() != pki.BackendURI {
				return errors.New("not the backend")
			}
			return nil
		}}
	if cert != nil {
		tc.Certificates = []tls.Certificate{*cert}
	}
	cc, err := grpc.NewClient(e.addr, grpc.WithTransportCredentials(credentials.NewTLS(tc)))
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { cc.Close() })
	return backendv1.NewHubControlClient(cc)
}

func keyCSR(t *testing.T) (*ecdsa.PrivateKey, []byte) {
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	der, _ := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, k)
	return k, der
}

// enrollHub 经 gRPC 引导一个实例，返回其证书。
func (e *env) enrollHub(region string) (platform.UUID, tls.Certificate) {
	tok, err := e.fl.CreateHubBootstrapToken(context.Background(), region, time.Hour)
	if err != nil {
		e.t.Fatal(err)
	}
	k, csr := keyCSR(e.t)
	r, err := e.client(nil).EnrollHub(context.Background(), &backendv1.EnrollHubRequest{BootstrapToken: tok, CsrDer: csr, Region: region,
		HubVersion: "1", AdvertiseAddress: "hub.example:7400"})
	if err != nil {
		e.t.Fatal(err)
	}
	if len(r.CaBundleDer) != 1 || string(r.CaBundleDer[0]) != string(e.ca.Root.Raw) {
		e.t.Fatal("ca bundle")
	}
	var id platform.UUID
	copy(id[:], r.HubId.Value)
	return id, tls.Certificate{Certificate: r.CertificateChainDer, PrivateKey: k}
}

func code(err error) codes.Code { return status.Code(err) }

func TestIdentityAndEnrollment(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	hub, hubCert := e.enrollHub("eu")
	tok, _ := e.fl.CreateEnrollToken(ctx, platform.UUID{}, e.admin)
	_, csr := keyCSR(t)
	req := &backendv1.EnrollNodeRequest{Request: &edgehubv1.EnrollRequest{EnrollToken: tok, CsrDer: csr, AgentVersion: "1.0",
		Capabilities: &edgehubv1.NodeCapabilities{CanExit: true}}, ObservedRemoteAddress: "198.51.100.9:5555"}
	// 无客户端证书：只允许 EnrollHub
	if _, err := e.client(nil).EnrollNode(ctx, req); code(err) != codes.Unauthenticated {
		t.Fatalf("EnrollNode without a certificate: %v", err)
	}
	if _, err := e.client(nil).EnrollHub(ctx, &backendv1.EnrollHubRequest{BootstrapToken: "x", Region: "eu", AdvertiseAddress: "a:1"}); code(err) != codes.PermissionDenied {
		t.Fatalf("bad bootstrap token: %v", err)
	}
	resp, err := e.client(&hubCert).EnrollNode(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.EdgeHubs) != 1 || resp.EdgeHubs[0].Address != "hub.example:7400" || string(resp.EdgeHubs[0].HubId.Value) != string(hub[:]) {
		t.Fatalf("hub list %+v", resp.EdgeHubs)
	}
	var node platform.UUID
	copy(node[:], resp.NodeId.Value)
	// 节点证书不能冒充实例
	nkey, ncsr2 := keyCSR(t)
	is2, err := e.fl.RenewNodeCert(ctx, hub, node, ncsr2)
	if err != nil {
		t.Fatal(err)
	}
	asNode := tls.Certificate{Certificate: is2.Chain, PrivateKey: nkey}
	if _, err := e.client(&asNode).RenewHubCert(ctx, &edgehubv1.RenewCertRequest{CsrDer: csr}); code(err) != codes.PermissionDenied {
		t.Fatalf("node certificate as a hub: %v", err)
	}
	// 他平台签发的“实例证书”：握手层拒绝
	g, _ := pki.Generate("rogue")
	rk, rcsr := keyCSR(t)
	rogue, _ := g.CA.Issue(rcsr, pki.HubURI(hub), time.Hour, x509.ExtKeyUsageClientAuth)
	rc := tls.Certificate{Certificate: rogue.Chain, PrivateKey: rk}
	if _, err := e.client(&rc).RenewHubCert(ctx, &edgehubv1.RenewCertRequest{CsrDer: csr}); err == nil {
		t.Fatal("certificate from another CA accepted")
	}
	// 本平台签发、但未登记在证书表中（例如私自签发）的实例证书
	uk, ucsr := keyCSR(t)
	unrec, _ := e.ca.Issue(ucsr, pki.HubURI(hub), time.Hour, x509.ExtKeyUsageClientAuth)
	uc := tls.Certificate{Certificate: unrec.Chain, PrivateKey: uk}
	if _, err := e.client(&uc).RenewHubCert(ctx, &edgehubv1.RenewCertRequest{CsrDer: csr}); code(err) != codes.PermissionDenied {
		t.Fatalf("unrecorded certificate: %v", err)
	}
	// 续签：节点证书（经实例代理）与实例证书
	_, c3 := keyCSR(t)
	if r, err := e.client(&hubCert).RenewNodeCert(ctx, &backendv1.RenewNodeCertRequest{NodeId: resp.NodeId, CsrDer: c3}); err != nil || len(r.CertificateChainDer) != 2 {
		t.Fatalf("renew node: %v", err)
	}
	if _, err := e.client(&hubCert).RenewNodeCert(ctx, &backendv1.RenewNodeCertRequest{NodeId: &commonv1.Uuid{Value: make([]byte, 16)}, CsrDer: c3}); code(err) != codes.NotFound {
		t.Fatalf("renew unknown node: %v", err)
	}
	hk, c4 := keyCSR(t)
	r, err := e.client(&hubCert).RenewHubCert(ctx, &edgehubv1.RenewCertRequest{CsrDer: c4})
	if err != nil {
		t.Fatal(err)
	}
	renewed := tls.Certificate{Certificate: r.CertificateChainDer, PrivateKey: hk}
	// 吊销旧实例证书后，旧证书不可用、新证书可用
	leaf, _ := x509.ParseCertificate(hubCert.Certificate[0])
	if err := e.fl.Revoke(ctx, leaf.SerialNumber.Bytes(), "compromised"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.client(&hubCert).RenewHubCert(ctx, &edgehubv1.RenewCertRequest{CsrDer: c4}); code(err) != codes.PermissionDenied {
		t.Fatalf("revoked certificate: %v", err)
	}
	if _, err := e.client(&renewed).RenewHubCert(ctx, &edgehubv1.RenewCertRequest{CsrDer: c4}); err != nil {
		t.Fatalf("renewed certificate: %v", err)
	}
	if _, err := e.client(&renewed).RenewHubCert(ctx, &edgehubv1.RenewCertRequest{CsrDer: []byte{1}}); code(err) != codes.InvalidArgument {
		t.Fatalf("bad csr: %v", err)
	}
}

type stream struct {
	t *testing.T
	s grpc.BidiStreamingClient[backendv1.HubUpMessage, backendv1.BackendMessage]
}

func (e *env) connect(cert tls.Certificate) *stream {
	ctx, cancel := context.WithCancel(context.Background())
	e.t.Cleanup(cancel)
	s, err := e.client(&cert).Connect(ctx)
	if err != nil {
		e.t.Fatal(err)
	}
	return &stream{t: e.t, s: s}
}

func (s *stream) send(m *backendv1.HubUpMessage) {
	if err := s.s.Send(m); err != nil {
		s.t.Fatal(err)
	}
}

// expect 读取消息直到 f 返回 true（最多 2 秒）。
func (s *stream) expect(what string, f func(*backendv1.BackendMessage) bool) *backendv1.BackendMessage {
	s.t.Helper()
	ch := make(chan *backendv1.BackendMessage, 1)
	errc := make(chan error, 1)
	go func() {
		for {
			m, err := s.s.Recv()
			if err != nil {
				errc <- err
				return
			}
			if f(m) {
				ch <- m
				return
			}
		}
	}()
	select {
	case m := <-ch:
		return m
	case err := <-errc:
		s.t.Fatalf("%s: %v", what, err)
	case <-time.After(3 * time.Second):
		s.t.Fatalf("timeout: %s", what)
	}
	return nil
}

func ledger(seqs ...uint64) *backendv1.HubUpMessage {
	l := &backendv1.LedgerEntries{}
	for _, s := range seqs {
		l.Entries = append(l.Entries, &backendv1.LedgerEntry{Seq: s, Body: &backendv1.LedgerEntry_GrantSettled{GrantSettled: &backendv1.GrantSettled{
			GrantId: &commonv1.Uuid{Value: make([]byte, 16)}, UsedRawBytes: s}}})
	}
	return &backendv1.HubUpMessage{Body: &backendv1.HubUpMessage_Ledger{Ledger: l}}
}

func TestConnect(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	hub, cert := e.enrollHub("eu")
	if _, err := e.client(nil).Connect(ctx); err != nil {
		t.Fatal(err) // 流在第一次收发时才报错
	}
	noCert, _ := e.client(nil).Connect(ctx)
	if _, err := noCert.Recv(); code(err) != codes.Unauthenticated {
		t.Fatalf("Connect without certificate: %v", err)
	}
	s := e.connect(cert)
	s.send(&backendv1.HubUpMessage{Body: &backendv1.HubUpMessage_Register{Register: &backendv1.HubRegister{Region: "eu", HubVersion: "1.1",
		Mode: commonv1.HubMode_HUB_MODE_ONLINE}}})
	s.expect("revocation list first", func(m *backendv1.BackendMessage) bool { return m.GetRevocations() != nil })
	s.expect("billing versions", func(m *backendv1.BackendMessage) bool { return m.GetBillingVersions() != nil })
	var mode, ver string
	apptest.QueryRow(t, e.dsn, "SELECT mode, hub_version FROM edge_hubs WHERE id = $1", []any{hub.PG()}, &mode, &ver)
	if mode != "online" || ver != "1.1" {
		t.Fatalf("hub row %s %s", mode, ver)
	}
	// 账本：按 (hub_id, seq) 幂等入库，累计确认（序号可有缺口，契约补充 L3）
	s.send(ledger(3, 5))
	if a := s.expect("ack", func(m *backendv1.BackendMessage) bool { return m.GetLedgerAck() != nil }); a.GetLedgerAck().AckedThroughSeq != 5 {
		t.Fatalf("ack %d", a.GetLedgerAck().AckedThroughSeq)
	}
	s.send(ledger(5, 6))
	s.expect("ack 6", func(m *backendv1.BackendMessage) bool { return m.GetLedgerAck().GetAckedThroughSeq() == 6 })
	var n int
	apptest.QueryRow(t, e.dsn, "SELECT count(*) FROM hub_ledger_entries WHERE hub_id = $1", []any{hub.PG()}, &n)
	if n != 3 {
		t.Fatalf("%d ledger rows, want 3", n)
	}
	// 节点状态
	tok, _ := e.fl.CreateEnrollToken(ctx, platform.UUID{}, e.admin)
	_, csr := keyCSR(t)
	ne, err := e.fl.EnrollNode(ctx, hub, tok, csr, fleet.Caps{})
	if err != nil {
		t.Fatal(err)
	}
	boot := platform.NewUUIDv7()
	s.send(&backendv1.HubUpMessage{Body: &backendv1.HubUpMessage_NodeStatus{NodeStatus: &backendv1.NodeStatusBatch{Nodes: []*backendv1.NodeStatus{
		{NodeId: uuidMsg(ne.NodeID), BootId: uuidMsg(boot), Connected: true, AgentVersion: "2.0"}}}}})
	deadline := time.Now().Add(3 * time.Second)
	for {
		var lc, av string
		var b platform.UUID
		apptest.QueryRow(t, e.dsn, "SELECT lifecycle, coalesce(agent_version,''), coalesce(current_boot_id, '00000000-0000-0000-0000-000000000000') FROM nodes WHERE id = $1",
			[]any{ne.NodeID.PG()}, &lc, &av, &b)
		if lc == "online" && av == "2.0" && b == boot {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("node row %s %s %x", lc, av, b)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// 重新连接：注册后告知已确认位置；对账中先限制、再 STARTED，账本静默后 COMPLETED
	s2 := e.connect(cert)
	s2.send(&backendv1.HubUpMessage{Body: &backendv1.HubUpMessage_Register{Register: &backendv1.HubRegister{Region: "eu",
		Mode: commonv1.HubMode_HUB_MODE_RECONCILING}}})
	if a := s2.expect("ack position", func(m *backendv1.BackendMessage) bool { return m.GetLedgerAck() != nil }); a.GetLedgerAck().AckedThroughSeq != 6 {
		t.Fatalf("ack position %d", a.GetLedgerAck().AckedThroughSeq)
	}
	s2.expect("restrictions before STARTED", func(m *backendv1.BackendMessage) bool {
		if m.GetReconcile() != nil {
			t.Fatal("STARTED before restrictions")
		}
		return m.GetRestrictions() != nil
	})
	s2.expect("STARTED", func(m *backendv1.BackendMessage) bool {
		return m.GetReconcile().GetPhase() == backendv1.ReconcilePhase_RECONCILE_PHASE_STARTED
	})
	s2.send(ledger(9))
	s2.expect("COMPLETED", func(m *backendv1.BackendMessage) bool {
		return m.GetReconcile().GetPhase() == backendv1.ReconcilePhase_RECONCILE_PHASE_COMPLETED
	})
	apptest.QueryRow(t, e.dsn, "SELECT mode FROM edge_hubs WHERE id = $1", []any{hub.PG()}, &mode)
	if mode != "online" {
		t.Fatalf("mode after reconcile %s", mode)
	}
	// 断开：实例标记离线
	_ = s2.s.CloseSend()
	_ = s.s.CloseSend()
	deadline = time.Now().Add(3 * time.Second)
	for {
		apptest.QueryRow(t, e.dsn, "SELECT mode FROM edge_hubs WHERE id = $1", []any{hub.PG()}, &mode)
		if mode == "offline" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("mode after disconnect %s", mode)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
