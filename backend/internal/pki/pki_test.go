package pki

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func csr(t *testing.T, key any) []byte {
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		t.Fatal(err)
	}
	return der
}

func TestIssueAndVerify(t *testing.T) {
	g, err := Generate("t")
	if err != nil {
		t.Fatal(err)
	}
	ca := g.CA
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	id := [16]byte{1, 2, 3}
	is, err := ca.Issue(csr(t, k), NodeURI(id), NodeLifetime, x509.ExtKeyUsageClientAuth)
	if err != nil {
		t.Fatal(err)
	}
	if len(is.Chain) != 2 || is.Leaf.NotAfter.Sub(is.Leaf.NotBefore) < NodeLifetime || len(is.Leaf.DNSNames) != 0 {
		t.Fatalf("leaf %+v", is.Leaf)
	}
	inter, _ := x509.ParseCertificate(is.Chain[1])
	if _, err := ca.Verify([]*x509.Certificate{is.Leaf, inter}, x509.ExtKeyUsageClientAuth); err != nil {
		t.Fatal(err)
	}
	if _, err := ca.Verify([]*x509.Certificate{is.Leaf, inter}, x509.ExtKeyUsageServerAuth); err == nil {
		t.Fatal("client-only certificate accepted for server auth")
	}
	kind, got, err := ParseIdentity(is.Leaf)
	if err != nil || kind != "node" || got != id {
		t.Fatalf("identity %s %x %v", kind, got, err)
	}
	other, _ := Generate("other")
	if _, err := other.CA.Verify([]*x509.Certificate{is.Leaf, inter}, x509.ExtKeyUsageClientAuth); err == nil {
		t.Fatal("certificate verified against another platform")
	}
	// CSR：只接受 ECDSA P-256 且签名有效
	rk, _ := rsa.GenerateKey(rand.Reader, 2048)
	p384, _ := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	bad := csr(t, k)
	bad[len(bad)-1] ^= 1
	for name, c := range map[string][]byte{"rsa": csr(t, rk), "p384": csr(t, p384), "bad signature": bad, "garbage": {1, 2, 3}} {
		if _, err := ca.Issue(c, NodeURI(id), time.Hour); !errors.Is(err, ErrCSR) {
			t.Errorf("%s: %v", name, err)
		}
	}
	// 有效期不超过中间 CA
	long, _ := ca.Issue(csr(t, k), HubURI(id), 100*365*24*time.Hour, x509.ExtKeyUsageServerAuth)
	if long.Leaf.NotAfter.After(ca.Inter.NotAfter) {
		t.Fatal("leaf outlives the intermediate")
	}
	sc, err := ca.ServerCert()
	if err != nil || sc.Leaf.URIs[0].String() != BackendURI {
		t.Fatal("server cert", err)
	}
}

func TestParseIdentityRejects(t *testing.T) {
	g, _ := Generate("t")
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	for _, uri := range []string{"spiffe://other/node/01020300-0000-0000-0000-000000000000", "spiffe://vortex/backend",
		"spiffe://vortex/user/01020300-0000-0000-0000-000000000000", "spiffe://vortex/node/xyz", "https://vortex/node/01020300-0000-0000-0000-000000000000"} {
		is, err := g.CA.Issue(csr(t, k), uri, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := ParseIdentity(is.Leaf); err == nil {
			t.Errorf("%s accepted", uri)
		}
	}
}

func TestEncryptedKeyFile(t *testing.T) {
	g, _ := Generate("t")
	pass := []byte("correct horse battery")
	if _, err := EncryptKey(g.CA.IntermediateKey(), []byte("short")); err == nil {
		t.Fatal("short passphrase accepted")
	}
	enc, err := EncryptKey(g.CA.IntermediateKey(), pass)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	r, i, kf := filepath.Join(dir, "r"), filepath.Join(dir, "i"), filepath.Join(dir, "k")
	_ = os.WriteFile(r, CertPEM(g.CA.Root), 0o644)
	_ = os.WriteFile(i, CertPEM(g.CA.Inter), 0o644)
	_ = os.WriteFile(kf, enc, 0o600)
	if _, err := Load(r, i, kf, []byte("wrong passphrase!!")); !errors.Is(err, ErrPassphrase) {
		t.Fatalf("wrong passphrase: %v", err)
	}
	ca, err := Load(r, i, kf, pass)
	if err != nil {
		t.Fatal(err)
	}
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if _, err := ca.Issue(csr(t, k), NodeURI([16]byte{9}), time.Hour); err != nil {
		t.Fatal(err)
	}
	// 中间 CA 私钥与证书不匹配、或中间 CA 不由该根签发
	g2, _ := Generate("t2")
	if _, err := New(g.CA.Root, g.CA.Inter, g2.CA.IntermediateKey()); err == nil {
		t.Fatal("mismatched key accepted")
	}
	if _, err := New(g2.CA.Root, g.CA.Inter, g.CA.IntermediateKey()); err == nil {
		t.Fatal("intermediate from another root accepted")
	}
	// 密文被篡改
	tampered := append([]byte(nil), enc...)
	tampered[len(tampered)/2] ^= 1
	if _, err := DecryptKey(tampered, pass); err == nil {
		t.Fatal("tampered key file accepted")
	}
}
