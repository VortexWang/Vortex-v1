package enginetest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net/url"
	"sync"
	"time"

	"vortex/engine/types"
	"vortex/engine/vlink"
)

// PKI 是测试用的平台 CA：根 CA → 节点中间 CA → 节点证书（SAN URI spiffe://vortex/node/<id>）。
type PKI struct {
	Roots *x509.CertPool
	inter *x509.Certificate
	ikey  *ecdsa.PrivateKey

	mu     sync.Mutex
	serial int64
}

func NewPKI() *PKI {
	rk, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	now := time.Now()
	rt := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "vortex test root"}, IsCA: true,
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour), KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true}
	rder, err := x509.CreateCertificate(rand.Reader, rt, rt, &rk.PublicKey, rk)
	if err != nil {
		panic(err)
	}
	root, _ := x509.ParseCertificate(rder)
	ik, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	it := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "vortex test node CA"}, IsCA: true,
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour), KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true}
	ider, err := x509.CreateCertificate(rand.Reader, it, root, &ik.PublicKey, rk)
	if err != nil {
		panic(err)
	}
	inter, _ := x509.ParseCertificate(ider)
	p := &PKI{Roots: x509.NewCertPool(), inter: inter, ikey: ik, serial: 100}
	p.Roots.AddCert(root)
	return p
}

// Node 签发节点证书。
func (p *PKI) Node(id types.UUID) *vlink.Identity {
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	u, _ := url.Parse(vlink.NodeURI(id))
	p.mu.Lock()
	p.serial++
	sn := p.serial
	p.mu.Unlock()
	now := time.Now()
	tpl := &x509.Certificate{SerialNumber: big.NewInt(sn), Subject: pkix.Name{CommonName: "node"}, URIs: []*url.URL{u},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, tpl, p.inter, &k.PublicKey, p.ikey)
	if err != nil {
		panic(err)
	}
	return &vlink.Identity{NodeID: id, Cert: tls.Certificate{Certificate: [][]byte{der, p.inter.Raw}, PrivateKey: k}}
}
