package backendgrpc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"

	"vortex/edgehub/issuer"
	"vortex/edgehub/ledger"
	"vortex/edgehub/model"
	"vortex/edgehub/session"
	"vortex/edgehub/upstream"
	"vortex/edgehub/wire"
	backendv1 "vortex/proto/gen/go/vortex/backend/v1"
	commonv1 "vortex/proto/gen/go/vortex/common/v1"
	runtimev1 "vortex/proto/gen/go/vortex/runtime/v1"
)

type ca struct {
	cert  *x509.Certificate
	key   *ecdsa.PrivateKey
	roots *x509.CertPool
	n     int64
}

func newCA(t *testing.T) *ca {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ca"}, NotBefore: time.Now().Add(-time.Hour),
		NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	c, _ := x509.ParseCertificate(der)
	p := x509.NewCertPool()
	p.AddCert(c)
	return &ca{cert: c, key: key, roots: p, n: 1}
}

func (c *ca) issue(t *testing.T, uri string, usage x509.ExtKeyUsage) tls.Certificate {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	c.n++
	u, _ := url.Parse(uri)
	tpl := &x509.Certificate{SerialNumber: big.NewInt(c.n), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		URIs: []*url.URL{u}, ExtKeyUsage: []x509.ExtKeyUsage{usage}, KeyUsage: x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, tpl, c.cert, &key.PublicKey, c.key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// fakeBackend：收到 HubRegister 后委托额度并下发计费版本；收到账本条目立即确认。
type fakeBackend struct {
	backendv1.UnimplementedHubControlServer
	mu       sync.Mutex
	regs     int
	entries  []*backendv1.LedgerEntry
	statuses int
	hubCN    []string
}

var tenant = model.UUID{0x7e}

func (f *fakeBackend) Connect(s grpc.BidiStreamingServer[backendv1.HubUpMessage, backendv1.BackendMessage]) error {
	for {
		m, err := s.Recv()
		if err != nil {
			return err
		}
		f.mu.Lock()
		switch b := m.Body.(type) {
		case *backendv1.HubUpMessage_Register:
			f.regs++
			f.mu.Unlock()
			_ = s.Send(&backendv1.BackendMessage{Body: &backendv1.BackendMessage_Budget{Budget: &backendv1.BudgetUpdate{
				TenantId: wire.UUID(tenant), BudgetVersion: uint64(f.regs), Delegated: 1 << 30}}})
			_ = s.Send(&backendv1.BackendMessage{Body: &backendv1.BackendMessage_BillingVersions{BillingVersions: &backendv1.BillingVersionSync{
				Versions: []*backendv1.BillingVersion{{Version: 1}}}}})
			continue
		case *backendv1.HubUpMessage_Ledger:
			f.entries = append(f.entries, b.Ledger.Entries...)
			last := b.Ledger.Entries[len(b.Ledger.Entries)-1].Seq
			f.mu.Unlock()
			_ = s.Send(&backendv1.BackendMessage{Body: &backendv1.BackendMessage_LedgerAck{LedgerAck: &backendv1.LedgerAck{AckedThroughSeq: last}}})
			continue
		case *backendv1.HubUpMessage_NodeStatus:
			f.statuses++
		}
		f.mu.Unlock()
	}
}

func serve(t *testing.T, fb *fakeBackend, cert tls.Certificate, roots *x509.CertPool, addr string) (*grpc.Server, string) {
	ln, err := net.Listen("tcp4", addr)
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, ClientCAs: roots,
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13})))
	backendv1.RegisterHubControlServer(srv, fb)
	go func() { _ = srv.Serve(ln) }()
	return srv, ln.Addr().String()
}

func eventually(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestClient(t *testing.T) {
	c := newCA(t)
	hubID := model.UUID{0x4b}
	node := model.UUID{0x0d}
	rule := model.UUID{0xaa}
	h, err := session.Open(session.Options{HubID: hubID, Store: &ledger.MemStore{}, Rules: session.RulesFunc(func(model.UUID) []issuer.RuleState {
		return []issuer.RuleState{{RuleID: rule, TenantID: tenant, EnforcementPoint: true, PathCoefMilli: 1000}}
	})})
	if err != nil {
		t.Fatal(err)
	}
	link := upstream.New(h)
	fb := &fakeBackend{}
	srv, addr := serve(t, fb, c.issue(t, BackendURI, x509.ExtKeyUsageServerAuth), c.roots, "127.0.0.1:0")
	cl := &Client{Link: link, Addr: addr, TLS: ClientTLS(c.issue(t, "spiffe://vortex/hub/"+hubID.String(), x509.ExtKeyUsageClientAuth), c.roots),
		Region: "r1", Version: "test", BackoffMin: 20 * time.Millisecond, BackoffMax: 100 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { cl.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	eventually(t, "budget from backend", func() bool { return h.Quota.View(tenant).Delegated == 1<<30 })
	s := h.NewSession(node, func(session.HubMsg) {})
	_ = s.Handle(session.Hello{Node: node, Boot: model.UUID{0xb1}})
	_ = s.Handle(session.GrantRequest{ID: 1, Kind: model.GrantBilling, Rule: rule})
	snap, _ := protoMarshal(&runtimev1.UsageSnapshot{GrantConsumptions: []*runtimev1.GrantConsumption{{GrantId: &commonv1.Uuid{Value: make([]byte, 16)}}}})
	_ = s.Handle(session.UsageBatch{Boot: model.UUID{0xb1}, Seq: 1, Snapshot: snap})
	_ = link.Tick()
	eventually(t, "ledger uploaded and acked", func() bool { es, _ := h.Ledger.Unacked(); return len(es) == 0 && h.Ledger.Acked() > 0 })
	fb.mu.Lock()
	var issued, usage int
	for _, e := range fb.entries {
		if gi := e.GetGrantIssued(); gi != nil && gi.Kind == 2 && gi.RawBytes > 0 {
			issued++
		}
		if nu := e.GetNodeUsage(); nu != nil && nu.Batch.Seq == 1 {
			usage++
		}
	}
	fb.mu.Unlock()
	if issued != 1 || usage != 1 {
		t.Fatalf("backend got issued=%d usage=%d", issued, usage)
	}

	// 后端重启：重连、重新注册
	srv.Stop()
	srv2, _ := serve(t, fb, c.issue(t, BackendURI, x509.ExtKeyUsageServerAuth), c.roots, addr)
	defer srv2.Stop()
	eventually(t, "re-registered", func() bool { fb.mu.Lock(); defer fb.mu.Unlock(); return fb.regs >= 2 })
}

// 后端证书不是平台 CA 签发或不带后端 URI：不连接。
func TestClientRejectsWrongBackend(t *testing.T) {
	c := newCA(t)
	for _, cert := range []tls.Certificate{
		c.issue(t, "spiffe://vortex/hub/x", x509.ExtKeyUsageServerAuth),
		newCA(t).issue(t, BackendURI, x509.ExtKeyUsageServerAuth),
	} {
		fb := &fakeBackend{}
		srv, addr := serve(t, fb, cert, c.roots, "127.0.0.1:0")
		h, _ := session.Open(session.Options{HubID: model.UUID{1}, Store: &ledger.MemStore{}})
		var mu sync.Mutex
		var errs []error
		cl := &Client{Link: upstream.New(h), Addr: addr, TLS: ClientTLS(c.issue(t, "spiffe://vortex/hub/y", x509.ExtKeyUsageClientAuth), c.roots),
			BackoffMin: 10 * time.Millisecond, BackoffMax: 20 * time.Millisecond, OnError: func(e error) { mu.Lock(); errs = append(errs, e); mu.Unlock() }}
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		cl.Run(ctx)
		cancel()
		srv.Stop()
		fb.mu.Lock()
		regs := fb.regs
		fb.mu.Unlock()
		mu.Lock()
		n := len(errs)
		mu.Unlock()
		if regs != 0 || n == 0 {
			t.Fatalf("connected to a backend with a wrong certificate (regs %d, errors %d)", regs, n)
		}
	}
}

func protoMarshal(m *runtimev1.UsageSnapshot) ([]byte, error) { return proto.Marshal(m) }
