package certs

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"vortex/engine/types"
	"vortex/engine/vlink"
)

type ca struct {
	cert  *x509.Certificate
	key   *ecdsa.PrivateKey
	roots *x509.CertPool
}

func newCA(t *testing.T) *ca {
	key, _ := NewKey()
	tpl := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, _ := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	c, _ := x509.ParseCertificate(der)
	p := x509.NewCertPool()
	p.AddCert(c)
	return &ca{c, key, p}
}

func (c *ca) sign(t *testing.T, pub *ecdsa.PublicKey, id types.UUID, nb, na time.Time) []byte {
	u, _ := url.Parse(vlink.NodeURI(id))
	s, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{SerialNumber: s, NotBefore: nb, NotAfter: na, URIs: []*url.URL{u},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}, c.cert, pub, c.key)
	if err != nil {
		t.Fatal(err)
	}
	return der
}

var node = types.UUID{0xd}

func TestVerify(t *testing.T) {
	c := newCA(t)
	key, _ := NewKey()
	other, _ := NewKey()
	now := time.Now()
	good := c.sign(t, &key.PublicKey, node, now.Add(-time.Minute), now.Add(time.Hour))
	if id, _, err := Verify([][]byte{good}, key, c.roots, types.UUID{}); err != nil || id != node {
		t.Fatalf("good: %v %v", id, err)
	}
	if _, _, err := Verify([][]byte{good}, other, c.roots, types.UUID{}); !errors.Is(err, ErrMismatch) {
		t.Fatal("certificate for another key accepted")
	}
	if _, _, err := Verify([][]byte{good}, key, newCA(t).roots, types.UUID{}); err == nil {
		t.Fatal("certificate from another CA accepted")
	}
	if _, _, err := Verify([][]byte{good}, key, c.roots, types.UUID{9}); !errors.Is(err, ErrMismatch) {
		t.Fatal("node id change accepted on renewal")
	}
	if _, _, err := Verify(nil, key, c.roots, types.UUID{}); err == nil {
		t.Fatal("empty chain accepted")
	}
}

func TestHolderAndRenewDue(t *testing.T) {
	c := newCA(t)
	key, _ := NewKey()
	now := time.Now()
	leaf := func(nb, na time.Time) tls.Certificate {
		der := c.sign(t, &key.PublicKey, node, nb, na)
		l, _ := x509.ParseCertificate(der)
		return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: l}
	}
	h := NewHolder(leaf(now.Add(-time.Hour), now.Add(-time.Second)))
	if got, _ := h.GetClientCertificate(nil); len(got.Certificate) != 0 {
		t.Fatal("expired certificate offered (AGT-012: no new control connections)")
	}
	h.Set(leaf(now.Add(-time.Minute), now.Add(time.Hour)))
	if got, _ := h.GetClientCertificate(nil); len(got.Certificate) != 1 {
		t.Fatal("valid certificate not offered")
	}
	l, _ := h.Leaf()
	// 有效期 61 分钟：剩余 ≥ 1/3 不续签，剩余 < 1/3 续签
	if RenewDue(l, now) || !RenewDue(l, now.Add(41*time.Minute)) {
		t.Fatal("renewal threshold")
	}
}

// Save 在写入私钥之后、写入证书之前崩溃：Load 用 key.prev 与旧证书恢复。
func TestSaveCrashRecovery(t *testing.T) {
	c := newCA(t)
	dir := t.TempDir()
	cf, kf := filepath.Join(dir, "n.crt"), filepath.Join(dir, "n.key")
	k1, _ := NewKey()
	now := time.Now()
	if err := Save(cf, kf, [][]byte{c.sign(t, &k1.PublicKey, node, now, now.Add(time.Hour))}, k1); err != nil {
		t.Fatal(err)
	}
	k2, _ := NewKey()
	kp, _ := KeyPEM(k2)
	old, _ := os.ReadFile(kf)
	_ = WriteAtomic(kf+".prev", old, 0o600)
	_ = WriteAtomic(kf, kp, 0o600) // 新私钥已写，证书仍是旧的
	got, err := Load(cf, kf)
	if err != nil {
		t.Fatalf("recovery: %v", err)
	}
	if !got.PrivateKey.(*ecdsa.PrivateKey).Equal(k1) {
		t.Fatal("recovered the wrong key")
	}
	if fi, _ := os.Stat(kf); fi.Mode().Perm() != 0o600 {
		t.Fatal("key mode")
	}
}

func TestRenewer(t *testing.T) {
	c := newCA(t)
	dir := t.TempDir()
	cf, kf := filepath.Join(dir, "n.crt"), filepath.Join(dir, "n.key")
	k1, _ := NewKey()
	now := time.Now()
	_ = Save(cf, kf, [][]byte{c.sign(t, &k1.PublicKey, node, now.Add(-50*time.Minute), now.Add(10*time.Minute))}, k1)
	cur, _ := Load(cf, kf)
	h := NewHolder(cur)
	issue := func(id types.UUID) func(context.Context, []byte) ([][]byte, error) {
		return func(_ context.Context, csrDER []byte) ([][]byte, error) {
			csr, _ := x509.ParseCertificateRequest(csrDER)
			return [][]byte{c.sign(t, csr.PublicKey.(*ecdsa.PublicKey), id, time.Now(), time.Now().Add(time.Hour))}, nil
		}
	}
	failing := func(context.Context, []byte) ([][]byte, error) { return nil, errors.New("hub down") }
	r := &Renewer{Holder: h, Roots: c.roots, CertFile: cf, KeyFile: kf,
		Submit: []func(context.Context, []byte) ([][]byte, error){failing, issue(types.UUID{9}), issue(node)}}
	ok, err := r.Check(context.Background())
	if err != nil || !ok {
		t.Fatalf("renew %v %v (first hub down, second returns a certificate for another node)", ok, err)
	}
	l, _ := h.Leaf()
	if id, _ := vlink.NodeIDFromCert(l); id != node || RenewDue(l, time.Now()) {
		t.Fatal("holder not updated")
	}
	if ok, _ := r.Check(context.Background()); ok {
		t.Fatal("renewed a fresh certificate")
	}
	// 全部失败：报错，证书不变
	k3, _ := NewKey()
	_ = Save(cf, kf, [][]byte{c.sign(t, &k3.PublicKey, node, now.Add(-50*time.Minute), now.Add(10*time.Minute))}, k3)
	cur, _ = Load(cf, kf)
	h.Set(cur)
	r.Submit = r.Submit[:1]
	if ok, err := r.Check(context.Background()); ok || err == nil {
		t.Fatal("failure not reported")
	}
	if got, _ := Load(cf, kf); !got.PrivateKey.(*ecdsa.PrivateKey).Equal(k3) {
		t.Fatal("files changed on a failed renewal")
	}
}
