package app

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"vortex/edgehub/model"
)

// 实例证书续签（C2 变更 CC-02；默认有效期 30 天，剩余 1/3 时续签，需求变更 CR-02）。

// certHolder 持有当前实例证书：NodeControl 的服务端证书与连接后端的客户端证书都从这里取，续签后新握手使用新证书。
type certHolder struct {
	mu   sync.RWMutex
	cert tls.Certificate
}

func (h *certHolder) get() tls.Certificate {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.cert
}

func (h *certHolder) set(c tls.Certificate) {
	h.mu.Lock()
	h.cert = c
	h.mu.Unlock()
}

func (h *certHolder) leaf() (*x509.Certificate, error) {
	c := h.get()
	if c.Leaf != nil {
		return c.Leaf, nil
	}
	if len(c.Certificate) == 0 {
		return nil, errors.New("no certificate")
	}
	return x509.ParseCertificate(c.Certificate[0])
}

func (h *certHolder) serverCert(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	c := h.get()
	return &c, nil
}

func (h *certHolder) clientCert(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	c := h.get()
	return &c, nil
}

// serverTLS / clientTLS 把固定证书换成从 holder 取（续签后无需重启）。
func (h *certHolder) serverTLS(c *tls.Config) *tls.Config {
	c.Certificates, c.GetCertificate = nil, h.serverCert
	return c
}

func (h *certHolder) clientTLS(c *tls.Config) *tls.Config {
	c.Certificates, c.GetClientCertificate = nil, h.clientCert
	return c
}

// loadCert 读取证书与私钥；不匹配时用 key.prev（saveCert 在两次替换之间崩溃的情形）。
func loadCert(certFile, keyFile string) (tls.Certificate, error) {
	c, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err == nil {
		return c, nil
	}
	if c2, err2 := tls.LoadX509KeyPair(certFile, keyFile+".prev"); err2 == nil {
		return c2, nil
	}
	return tls.Certificate{}, err
}

// saveCert 先保留旧私钥为 key.prev，再写新私钥，最后写证书（各自原子替换）。
func saveCert(certFile, keyFile string, chain [][]byte, key *ecdsa.PrivateKey) error {
	kd, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	if old, err := os.ReadFile(keyFile); err == nil {
		if err := writeAtomic(keyFile+".prev", old, 0o600); err != nil {
			return err
		}
	}
	if err := writeAtomic(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kd}), 0o600); err != nil {
		return err
	}
	var cp []byte
	for _, d := range chain {
		cp = append(cp, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: d})...)
	}
	return writeAtomic(certFile, cp, 0o644)
}

// renewDue：剩余有效期少于总有效期的 1/3。
func renewDue(leaf *x509.Certificate, now time.Time) bool {
	return leaf.NotAfter.Sub(now) < leaf.NotAfter.Sub(leaf.NotBefore)/3
}

// renewer 定期检查并续签实例证书；失败告警，1 分钟起翻倍退避（不超过检查周期）。
type renewer struct {
	hub      model.UUID
	holder   *certHolder
	roots    *x509.CertPool
	certFile string
	keyFile  string
	submit   func(ctx context.Context, csr []byte) ([][]byte, error)
	every    time.Duration
	onEvent  func(renewed bool, err error)
}

func (r *renewer) check(ctx context.Context) (bool, error) {
	leaf, err := r.holder.leaf()
	if err != nil {
		return false, err
	}
	if !renewDue(leaf, time.Now()) {
		return false, nil
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return false, err
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		return false, err
	}
	chain, err := r.submit(ctx, csr)
	if err != nil {
		return false, err
	}
	if err := verifyHubCert(chain, key, r.roots, r.hub); err != nil {
		return false, fmt.Errorf("renewed certificate rejected: %w", err)
	}
	if err := saveCert(r.certFile, r.keyFile, chain, key); err != nil {
		return false, err
	}
	c, err := loadCert(r.certFile, r.keyFile)
	if err != nil {
		return false, err
	}
	r.holder.set(c)
	return true, nil
}

func (r *renewer) run(ctx context.Context) {
	every := r.every
	if every <= 0 {
		every = time.Hour
	}
	retry := time.Minute
	if retry > every {
		retry = every
	}
	wait := time.Duration(0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		renewed, err := r.check(ctx)
		if r.onEvent != nil && (renewed || err != nil) {
			r.onEvent(renewed, err)
		}
		if err != nil {
			wait = retry
			retry = min(retry*2, every)
			continue
		}
		wait, retry = every, min(time.Minute, every)
	}
}
