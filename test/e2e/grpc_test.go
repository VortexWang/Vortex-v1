package e2e

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
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"vortex/edgehub/model"
	"vortex/edgehub/nodegrpc"
	"vortex/engine/types"
	"vortex/engine/vlink"
	"vortex/node/agent/control"
	"vortex/node/agent/grpcconn"
	"vortex/node/agent/reconcile"
	nodewire "vortex/node/agent/wire"
	commonv1 "vortex/proto/gen/go/vortex/common/v1"
	edgehubv1 "vortex/proto/gen/go/vortex/edgehub/v1"
)

// 测试 PKI：平台 CA 签发 Edge Hub 证书（SAN URI spiffe://vortex/hub/<id>）与节点证书（spiffe://vortex/node/<id>）。
type pki struct {
	ca    *x509.Certificate
	key   *ecdsa.PrivateKey
	roots *x509.CertPool
	n     int64
}

func newPKI(t *testing.T) *pki {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "vortex test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(der)
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	return &pki{ca: ca, key: key, roots: roots, n: 1}
}

func (p *pki) issue(t *testing.T, uri string, usage ...x509.ExtKeyUsage) tls.Certificate {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	p.n++
	u, _ := url.Parse(uri)
	tpl := &x509.Certificate{SerialNumber: big.NewInt(p.n), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		URIs: []*url.URL{u}, ExtKeyUsage: usage, KeyUsage: x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, tpl, p.ca, &key.PublicKey, p.key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, _ := x509.ParseCertificate(der)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

func (p *pki) node(t *testing.T, id types.UUID) tls.Certificate {
	return p.issue(t, vlink.NodeURI(id), x509.ExtKeyUsageClientAuth)
}

func (p *pki) hub(t *testing.T, id types.UUID) tls.Certificate {
	// Edge Hub 既是 Agent 的服务端，也是后端的客户端
	return p.issue(t, grpcconn.HubURI(id), x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth)
}

// grpcHub 在本机端口上提供某个 Edge Hub 的 NodeControl 服务。
type grpcHub struct {
	*Hub
	srv  *grpc.Server
	ns   *nodegrpc.Server
	addr string
}

func serveHub(t *testing.T, h *Hub, cert tls.Certificate, roots *x509.CertPool) *grpcHub {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(nodegrpc.ServerTLS(cert, roots))))
	ns := &nodegrpc.Server{Hub: h.Hub, Region: "test"}
	edgehubv1.RegisterNodeControlServer(srv, ns)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	return &grpcHub{Hub: h, srv: srv, ns: ns, addr: ln.Addr().String()}
}

// 真实 gRPC + mTLS：接入、配置、授权、转发、计量确认；实例宕机后切到另一实例；节点重启后恢复。
func TestGRPCEndToEnd(t *testing.T) {
	p := newPKI(t)
	cfg := config(t)
	addr := cfg.Listeners[0].Bind
	a := serveHub(t, newHub(t, hubA, t0(), cfg), p.hub(t, hubA), p.roots)
	b := serveHub(t, newHub(t, hubB, t0(), cfg), p.hub(t, hubB), p.roots)
	d := grpcconn.Dialer{Cert: p.node(t, nodeID), Roots: p.roots, DialTimeout: 2 * time.Second}
	eps := []control.Endpoint{{ID: hubA, Address: a.addr}, {ID: hubB, Address: b.addr}}
	l := newLogs()

	n1 := startNodeGRPC(t, l, d, eps)
	online(t, n1)
	if cur, _ := n1.c.Current(); cur != hubA {
		t.Fatalf("connected to %v", cur)
	}
	if !ok(addr) {
		t.Fatal("no forwarding over gRPC control")
	}
	n1.a.Tick(context.Background())
	n1.a.SealTick()
	eventually(t, "usage acked via gRPC", func() bool { return n1.a.Status().PendingBatches == 0 })
	if a.B.Batches(nodeID, n1.a.Status().Boot) == 0 {
		t.Fatal("backend got nothing")
	}
	if a.ns.Streams() != 1 {
		t.Fatalf("streams %d", a.ns.Streams())
	}

	// 实例 A 宕机（服务停止）：切到 B，转发不中断
	a.srv.Stop()
	eventually(t, "switched to B over gRPC", func() bool { cur, _ := n1.c.Current(); return cur == hubB })
	if !ok(addr) {
		t.Fatal("forwarding interrupted")
	}
	eventually(t, "B recorded A's grants", func() bool {
		for _, g := range b.Issuer.Held() {
			if g.IssuerHub == toModel(hubA) {
				return true
			}
		}
		return false
	})
	oldBoot := n1.a.Status().Boot
	n1.stop()

	n2 := startNodeGRPC(t, l, d, []control.Endpoint{{ID: hubB, Address: b.addr}})
	defer n2.stop()
	eventually(t, "recovered over gRPC", func() bool { return n2.a.Status().Phase == reconcile.PhaseApplied })
	if !ok(addr) {
		t.Fatal("no forwarding after restart")
	}
	if bt, _ := b.Issuer.Boot(toModel(nodeID)); bt == toModel(oldBoot) {
		t.Fatal("restart transfer not applied")
	}
	eventually(t, "previous boot's batches reported", func() bool { return n2.a.Status().PendingBatches == 0 })
}

func startNodeGRPC(t *testing.T, l logs, d grpcconn.Dialer, eps []control.Endpoint) *nodeProc {
	return startNodeOpt(t, l, d, eps, nodeOpts{})
}

// 身份：未出示证书、证书身份与 Hello 不符、Edge Hub 证书不是预期实例、已吊销的节点，都不能建立控制流。
func TestGRPCIdentity(t *testing.T) {
	p := newPKI(t)
	cfg := config(t)
	h := serveHub(t, newHub(t, hubA, t0(), cfg), p.hub(t, hubA), p.roots)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	try := func(cert *tls.Certificate, hub types.UUID, hello types.UUID) error {
		tc := grpcconn.ClientTLS(tls.Certificate{}, p.roots, hub)
		if cert != nil {
			tc.Certificates = []tls.Certificate{*cert}
		} else {
			tc.Certificates = nil
		}
		cc, err := grpc.NewClient(h.addr, grpc.WithTransportCredentials(credentials.NewTLS(tc)))
		if err != nil {
			return err
		}
		defer cc.Close()
		s, err := edgehubv1.NewNodeControlClient(cc).Connect(ctx)
		if err != nil {
			return err
		}
		if err := s.Send(&edgehubv1.AgentMessage{Body: &edgehubv1.AgentMessage_Hello{Hello: &edgehubv1.AgentHello{
			NodeId: nodewireUUID(hello), BootId: nodewireUUID(types.UUID{0xbb})}}}); err != nil {
			return err
		}
		_, err = s.Recv()
		return err
	}
	nodeCert := p.node(t, nodeID)
	if err := try(&nodeCert, hubA, nodeID); err != nil {
		t.Fatalf("valid node rejected: %v", err)
	}
	if err := try(nil, hubA, nodeID); err == nil {
		t.Fatal("stream without a client certificate accepted")
	}
	if err := try(&nodeCert, hubA, types.UUID{0x77}); err == nil {
		t.Fatal("hello for another node accepted")
	}
	if err := try(&nodeCert, hubB, nodeID); err == nil {
		t.Fatal("connected to a hub whose certificate names another hub_id")
	}
	other := newPKI(t).node(t, nodeID)
	if err := try(&other, hubA, nodeID); err == nil {
		t.Fatal("certificate from another CA accepted")
	}
	// Edge Hub 证书 URI 正确但不是平台 CA 签发：拒绝
	fake := serveHub(t, newHub(t, hubA, t0(), cfg), newPKI(t).hub(t, hubA), p.roots)
	fh := h
	h = fake
	if err := try(&nodeCert, hubA, nodeID); err == nil {
		t.Fatal("hub certificate from another CA accepted")
	}
	h = fh
	h.ns.Revoked = func(n model.UUID, _ []byte) bool { return n == toModel(nodeID) }
	if err := try(&nodeCert, hubA, nodeID); err == nil {
		t.Fatal("revoked node accepted")
	}
}

func nodewireUUID(u types.UUID) *commonv1.Uuid { return nodewire.UUID(u) }
