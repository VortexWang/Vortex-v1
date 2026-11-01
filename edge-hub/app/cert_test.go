package app

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"vortex/edgehub/model"
	"vortex/edgehub/nodegrpc"
)

type testCA struct {
	cert  *x509.Certificate
	key   *ecdsa.PrivateKey
	roots *x509.CertPool
	n     int64
}

func newTestCA(t *testing.T) *testCA {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ca"}, NotBefore: time.Now().Add(-time.Hour),
		NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	c, _ := x509.ParseCertificate(der)
	roots := x509.NewCertPool()
	roots.AddCert(c)
	return &testCA{cert: c, key: key, roots: roots, n: 1}
}

// sign 为 pub 签发证书：uri 与有效期可变，用于构造正确与错误的签发结果。
func (ca *testCA) sign(t *testing.T, pub any, uri string, notBefore time.Time, life time.Duration, ku ...x509.ExtKeyUsage) []byte {
	ca.n++
	u, _ := url.Parse(uri)
	tpl := &x509.Certificate{SerialNumber: big.NewInt(ca.n), NotBefore: notBefore, NotAfter: notBefore.Add(life), URIs: []*url.URL{u},
		ExtKeyUsage: ku, KeyUsage: x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, tpl, ca.cert, pub, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return der
}

var hubID = model.UUID{0xa1}

func both() []x509.ExtKeyUsage {
	return []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
}

// 初始证书：已过 2/3 有效期（需要续签）或刚签发。
func initial(t *testing.T, ca *testCA, dir string, due bool) (*certHolder, string, string) {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	nb := time.Now().Add(-time.Minute)
	if due {
		nb = time.Now().Add(-21 * 24 * time.Hour)
	}
	der := ca.sign(t, &key.PublicKey, nodegrpc.HubURI(hubID), nb, 30*24*time.Hour, both()...)
	cf, kf := filepath.Join(dir, "hub.crt"), filepath.Join(dir, "hub.key")
	if err := saveCert(cf, kf, [][]byte{der}, key); err != nil {
		t.Fatal(err)
	}
	c, err := loadCert(cf, kf)
	if err != nil {
		t.Fatal(err)
	}
	return &certHolder{cert: c}, cf, kf
}

func TestRenewDue(t *testing.T) {
	now := time.Now()
	c := &x509.Certificate{NotBefore: now.Add(-20 * 24 * time.Hour), NotAfter: now.Add(10*24*time.Hour + time.Minute)}
	if renewDue(c, now) {
		t.Fatal("renewed with more than 1/3 left")
	}
	c.NotAfter = now.Add(10*24*time.Hour - time.Minute)
	if !renewDue(c, now) {
		t.Fatal("not renewed with less than 1/3 left")
	}
}

// 续签：新私钥，身份不变，文件原子替换并保留旧私钥；holder 换成新证书。签发结果有误时拒绝且什么都不变。
func TestRenewer(t *testing.T) {
	ca := newTestCA(t)
	other := newTestCA(t)
	cases := []struct {
		name  string
		issue func(pub any) [][]byte
		ok    bool
	}{
		{"good", func(pub any) [][]byte {
			return [][]byte{ca.sign(t, pub, nodegrpc.HubURI(hubID), time.Now(), 30*24*time.Hour, both()...)}
		}, true},
		{"other hub", func(pub any) [][]byte {
			return [][]byte{ca.sign(t, pub, nodegrpc.HubURI(model.UUID{0xb2}), time.Now(), time.Hour, both()...)}
		}, false},
		{"other ca", func(pub any) [][]byte {
			return [][]byte{other.sign(t, pub, nodegrpc.HubURI(hubID), time.Now(), time.Hour, both()...)}
		}, false},
		{"server auth only", func(pub any) [][]byte {
			return [][]byte{ca.sign(t, pub, nodegrpc.HubURI(hubID), time.Now(), time.Hour, x509.ExtKeyUsageServerAuth)}
		}, false},
		{"other key", func(any) [][]byte {
			k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
			return [][]byte{ca.sign(t, &k.PublicKey, nodegrpc.HubURI(hubID), time.Now(), time.Hour, both()...)}
		}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			h, cf, kf := initial(t, ca, dir, true)
			before := h.get().Certificate[0]
			beforeKey, _ := os.ReadFile(kf)
			r := &renewer{hub: hubID, holder: h, roots: ca.roots, certFile: cf, keyFile: kf,
				submit: func(_ context.Context, csrDER []byte) ([][]byte, error) {
					csr, err := x509.ParseCertificateRequest(csrDER)
					if err != nil || csr.CheckSignature() != nil {
						return nil, errors.New("bad csr")
					}
					return c.issue(csr.PublicKey), nil
				}}
			renewed, err := r.check(context.Background())
			if renewed != c.ok || (err == nil) != c.ok {
				t.Fatalf("renewed %v err %v", renewed, err)
			}
			after := h.get().Certificate[0]
			if c.ok {
				if string(after) == string(before) {
					t.Fatal("holder not updated")
				}
				if prev, _ := os.ReadFile(kf + ".prev"); string(prev) != string(beforeKey) {
					t.Fatal("previous key not kept")
				}
				if fi, _ := os.Stat(kf); fi.Mode().Perm() != 0o600 {
					t.Fatal("key mode")
				}
				disk, err := loadCert(cf, kf)
				if err != nil || string(disk.Certificate[0]) != string(after) {
					t.Fatal("files do not hold the renewed certificate")
				}
				if again, _ := r.check(context.Background()); again {
					t.Fatal("renewed again right after renewal")
				}
				return
			}
			if string(after) != string(before) {
				t.Fatal("holder changed on a rejected renewal")
			}
			if k, _ := os.ReadFile(kf); string(k) != string(beforeKey) {
				t.Fatal("key file changed on a rejected renewal")
			}
		})
	}
}

func TestRenewerNotDue(t *testing.T) {
	ca := newTestCA(t)
	h, cf, kf := initial(t, ca, t.TempDir(), false)
	r := &renewer{hub: hubID, holder: h, roots: ca.roots, certFile: cf, keyFile: kf,
		submit: func(context.Context, []byte) ([][]byte, error) { t.Fatal("submitted early"); return nil, nil }}
	if renewed, err := r.check(context.Background()); renewed || err != nil {
		t.Fatal(renewed, err)
	}
}

// 写入中途崩溃（新私钥已写、证书仍是旧的）：加载时回退到 key.prev。
func TestLoadCertRecoversPreviousKey(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()
	h, cf, kf := initial(t, ca, dir, false)
	old, _ := os.ReadFile(kf)
	_ = os.WriteFile(kf+".prev", old, 0o600)
	k2, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	d, _ := x509.MarshalPKCS8PrivateKey(k2)
	_ = writeAtomic(kf, pemBlock("PRIVATE KEY", d), 0o600)
	c, err := loadCert(cf, kf)
	if err != nil || string(c.Certificate[0]) != string(h.get().Certificate[0]) {
		t.Fatalf("recovery: %v", err)
	}
}

// holder 的 TLS 配置：每次握手取当前证书。
func TestHolderTLS(t *testing.T) {
	ca := newTestCA(t)
	h, _, _ := initial(t, ca, t.TempDir(), false)
	s := h.serverTLS(&tls.Config{Certificates: []tls.Certificate{{}}})
	c := h.clientTLS(&tls.Config{Certificates: []tls.Certificate{{}}})
	if s.Certificates != nil || c.Certificates != nil {
		t.Fatal("static certificates kept")
	}
	h2, _, _ := initial(t, ca, t.TempDir(), false)
	h.set(h2.get())
	sc, _ := s.GetCertificate(nil)
	cc, _ := c.GetClientCertificate(nil)
	if string(sc.Certificate[0]) != string(h2.get().Certificate[0]) || string(cc.Certificate[0]) != string(h2.get().Certificate[0]) {
		t.Fatal("TLS does not use the current certificate")
	}
}

func pemBlock(typ string, der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der})
}
