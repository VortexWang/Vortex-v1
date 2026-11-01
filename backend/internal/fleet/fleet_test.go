package fleet

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"errors"
	"testing"
	"time"

	"vortex/backend/apptest"
	"vortex/backend/internal/pki"
	"vortex/backend/internal/platform"
)

func csr(t *testing.T) []byte {
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	der, _ := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, k)
	return der
}

func setup(t *testing.T) (*Service, string) {
	dsn := apptest.Database(t)
	_, ca := apptest.CA(t, t.TempDir())
	pool, err := platform.OpenDB(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return &Service{DB: pool, CA: ca}, dsn
}

func TestHubBootstrap(t *testing.T) {
	s, dsn := setup(t)
	ctx := context.Background()
	tok, err := s.CreateHubBootstrapToken(ctx, "eu", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	// 令牌只存哈希
	var n int
	apptest.QueryRow(t, dsn, "SELECT count(*) FROM hub_bootstrap_tokens WHERE token_hash = $1", []any{[]byte(tok)}, &n)
	if n != 0 {
		t.Fatal("plaintext token stored")
	}
	if _, err := s.EnrollHub(ctx, "nope", csr(t), "eu", "1", "hub.example:7400"); !errors.Is(err, ErrToken) {
		t.Fatalf("bad token: %v", err)
	}
	if _, err := s.EnrollHub(ctx, tok, csr(t), "us", "1", "hub.example:7400"); !errors.Is(err, ErrRegion) {
		t.Fatalf("region: %v", err)
	}
	for _, adv := range []string{"", "hub.example", ":7400", "hub.example:0"} {
		if _, err := s.EnrollHub(ctx, tok, csr(t), "eu", "1", adv); !errors.Is(err, ErrAddress) {
			t.Fatalf("advertise %q: %v", adv, err)
		}
	}
	e, err := s.EnrollHub(ctx, tok, csr(t), "eu", "1.2", "hub.example:7400")
	if err != nil {
		t.Fatal(err)
	}
	if kind, id, _ := pki.ParseIdentity(e.Issued.Leaf); kind != "hub" || id != e.HubID {
		t.Fatal("certificate identity")
	}
	var addr, region string
	apptest.QueryRow(t, dsn, "SELECT address, region FROM edge_hubs WHERE id = $1", []any{e.HubID.PG()}, &addr, &region)
	if addr != "hub.example:7400" || region != "eu" {
		t.Fatalf("hub row %s %s", addr, region)
	}
	if !s.CertUsable(ctx, "hub", e.HubID, e.Issued.Serial) || s.CertUsable(ctx, "node", e.HubID, e.Issued.Serial) {
		t.Fatal("certificate record")
	}
	if _, err := s.EnrollHub(ctx, tok, csr(t), "eu", "1", "hub.example:7400"); !errors.Is(err, ErrToken) {
		t.Fatalf("token reused: %v", err)
	}
	// 过期
	s.Now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	tok2, _ := s.CreateHubBootstrapToken(ctx, "eu", time.Hour)
	s.Now = nil
	if _, err := s.EnrollHub(ctx, tok2, csr(t), "eu", "1", "hub.example:7400"); err != nil {
		t.Fatalf("token from the future: %v", err)
	}
	s.Now = func() time.Time { return time.Now().Add(-2 * time.Hour) }
	tok3, _ := s.CreateHubBootstrapToken(ctx, "eu", time.Hour)
	s.Now = nil
	if _, err := s.EnrollHub(ctx, tok3, csr(t), "eu", "1", "hub.example:7400"); !errors.Is(err, ErrToken) {
		t.Fatalf("expired token: %v", err)
	}
}

func enrolledHub(t *testing.T, s *Service, region, addr string) *HubEnrollment {
	tok, _ := s.CreateHubBootstrapToken(context.Background(), region, time.Hour)
	e, err := s.EnrollHub(context.Background(), tok, csr(t), region, "1", addr)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestNodeEnrollAndRenew(t *testing.T) {
	s, dsn := setup(t)
	ctx := context.Background()
	admin := apptest.Admin(t, dsn)
	tenant := apptest.Tenant(t, dsn)
	h1 := enrolledHub(t, s, "eu", "a.example:7400")
	h2 := enrolledHub(t, s, "eu", "b.example:7400")
	other := enrolledHub(t, s, "us", "c.example:7400")
	tok, err := s.CreateEnrollToken(ctx, tenant, admin)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnrollNode(ctx, h1.HubID, tok, csr(t), Caps{PublicAddresses: []string{"10.0.0.1"}}); !errors.Is(err, ErrAddress) {
		t.Fatalf("private public address: %v", err)
	}
	e, err := s.EnrollNode(ctx, h1.HubID, tok, csr(t), Caps{Entry: true, Exit: true, PublicAddresses: []string{"203.0.113.7"}, AgentVersion: "1.0"})
	if err != nil {
		t.Fatal(err)
	}
	if len(e.Hubs) != 2 {
		t.Fatalf("region hubs %d", len(e.Hubs))
	}
	var gotTenant platform.UUID
	var region, lifecycle string
	var entry, relay bool
	apptest.QueryRow(t, dsn, "SELECT tenant_id, region, lifecycle, can_entry, can_relay FROM nodes WHERE id = $1", []any{e.NodeID.PG()},
		&gotTenant, &region, &lifecycle, &entry, &relay)
	if gotTenant != tenant || region != "eu" || lifecycle != "pending" || !entry || relay {
		t.Fatalf("node row %x %s %s %v %v", gotTenant, region, lifecycle, entry, relay)
	}
	if _, err := s.EnrollNode(ctx, h2.HubID, tok, csr(t), Caps{}); !errors.Is(err, ErrToken) {
		t.Fatalf("token reused: %v", err)
	}
	// 平台节点令牌（无租户）
	ptok, _ := s.CreateEnrollToken(ctx, platform.UUID{}, admin)
	pe, err := s.EnrollNode(ctx, h2.HubID, ptok, csr(t), Caps{})
	if err != nil {
		t.Fatal(err)
	}
	var isNull bool
	apptest.QueryRow(t, dsn, "SELECT tenant_id IS NULL FROM nodes WHERE id = $1", []any{pe.NodeID.PG()}, &isNull)
	if !isNull {
		t.Fatal("platform node got a tenant")
	}
	// 续签：同区域任一实例可代理；他区域实例、已删除、已停用的节点被拒绝
	is, err := s.RenewNodeCert(ctx, h2.HubID, e.NodeID, csr(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, id, _ := pki.ParseIdentity(is.Leaf); id != e.NodeID {
		t.Fatal("renewal changed the node id")
	}
	if _, err := s.RenewNodeCert(ctx, other.HubID, e.NodeID, csr(t)); !errors.Is(err, ErrRegion) {
		t.Fatalf("other region: %v", err)
	}
	apptest.Exec(t, dsn, "UPDATE nodes SET lifecycle = 'disabled' WHERE id = $1", e.NodeID.PG())
	if _, err := s.RenewNodeCert(ctx, h1.HubID, e.NodeID, csr(t)); !errors.Is(err, ErrNotPermitted) {
		t.Fatalf("disabled: %v", err)
	}
	apptest.Exec(t, dsn, "UPDATE nodes SET deleted_at = now() WHERE id = $1", pe.NodeID.PG())
	if _, err := s.RenewNodeCert(ctx, h1.HubID, pe.NodeID, csr(t)); !errors.Is(err, ErrUnknown) {
		t.Fatalf("deleted: %v", err)
	}
	// 实例证书续签
	hs, err := s.RenewHubCert(ctx, h1.HubID, csr(t))
	if err != nil || !s.CertUsable(ctx, "hub", h1.HubID, hs.Serial) {
		t.Fatal("hub renewal", err)
	}
	// 吊销：吊销列表版本推进，序列号出现在列表中，证书不再可用
	v0, _, _ := s.Revocations(ctx)
	if err := s.Revoke(ctx, is.Serial, "test"); err != nil {
		t.Fatal(err)
	}
	if err := s.Revoke(ctx, is.Serial, "again"); !errors.Is(err, ErrUnknown) {
		t.Fatalf("double revoke: %v", err)
	}
	v1, serials, _ := s.Revocations(ctx)
	if v1 <= v0 || len(serials) != 1 || string(serials[0]) != string(is.Serial) || s.CertUsable(ctx, "node", e.NodeID, is.Serial) {
		t.Fatalf("revocations %d→%d %x", v0, v1, serials)
	}
}
