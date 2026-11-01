// Package certs 是 Agent 的身份材料（AGT-001、AGT-012、SEC-001）：本地生成 ECDSA P-256 私钥与 CSR，
// 校验签发结果，原子写入文件（私钥 0600），持有当前证书供 TLS 使用，到期前续签。
//
// 私钥只在本地生成，不离开节点；注册令牌只用于注册，与运行身份分离。
package certs

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"vortex/engine/types"
	"vortex/engine/vlink"
)

var ErrMismatch = errors.New("certs: issued certificate does not match the request")

// NewKey 生成 ECDSA P-256 私钥。
func NewKey() (*ecdsa.PrivateKey, error) { return ecdsa.GenerateKey(elliptic.P256(), rand.Reader) }

// CSR 生成证书请求（DER）。身份（SAN URI）由平台 CA 按注册令牌或现有身份填写，请求中不带。
func CSR(key *ecdsa.PrivateKey) ([]byte, error) {
	return x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
}

// Verify 校验签发结果：叶证书公钥与本地私钥一致、链可校验到平台 CA、SAN 中恰好一个节点 URI。
// want 非零时节点 ID 必须一致（续签不得改变身份）。返回节点 ID。
func Verify(chain [][]byte, key *ecdsa.PrivateKey, roots *x509.CertPool, want types.UUID) (types.UUID, *x509.Certificate, error) {
	if len(chain) == 0 {
		return types.UUID{}, nil, fmt.Errorf("%w: empty chain", ErrMismatch)
	}
	certs := make([]*x509.Certificate, len(chain))
	for i, d := range chain {
		c, err := x509.ParseCertificate(d)
		if err != nil {
			return types.UUID{}, nil, err
		}
		certs[i] = c
	}
	leaf := certs[0]
	pub, ok := leaf.PublicKey.(*ecdsa.PublicKey)
	if !ok || !pub.Equal(&key.PublicKey) {
		return types.UUID{}, nil, fmt.Errorf("%w: public key", ErrMismatch)
	}
	inter := x509.NewCertPool()
	for _, c := range certs[1:] {
		inter.AddCert(c)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: inter, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		return types.UUID{}, nil, err
	}
	id, err := vlink.NodeIDFromCert(leaf)
	if err != nil {
		return types.UUID{}, nil, err
	}
	if want != (types.UUID{}) && id != want {
		return types.UUID{}, nil, fmt.Errorf("%w: node id changed", ErrMismatch)
	}
	return id, leaf, nil
}

// PoolFromDER 由 CA 证书（DER）构造证书池。
func PoolFromDER(ders [][]byte) (*x509.CertPool, []byte, error) {
	pool := x509.NewCertPool()
	var pemOut bytes.Buffer
	for _, d := range ders {
		c, err := x509.ParseCertificate(d)
		if err != nil {
			return nil, nil, err
		}
		pool.AddCert(c)
		_ = pem.Encode(&pemOut, &pem.Block{Type: "CERTIFICATE", Bytes: d})
	}
	if len(ders) == 0 {
		return nil, nil, errors.New("certs: empty CA bundle")
	}
	return pool, pemOut.Bytes(), nil
}

// LoadPool 读取 PEM 格式的 CA 文件。
func LoadPool(path string) (*x509.CertPool, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	p := x509.NewCertPool()
	if !p.AppendCertsFromPEM(b) {
		return nil, fmt.Errorf("%s: no certificates", path)
	}
	return p, nil
}

// WriteAtomic 写入文件：临时文件 + fsync + rename + 目录 fsync，中途崩溃时文件要么是旧内容要么是新内容。
func WriteAtomic(path string, data []byte, mode os.FileMode) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Chmod(tmp, mode)
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		os.Remove(tmp)
		return err
	}
	if d, err := os.Open(filepath.Dir(path)); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return nil
}

// ChainPEM / KeyPEM 编码证书链与私钥。
func ChainPEM(chain [][]byte) []byte {
	var b bytes.Buffer
	for _, d := range chain {
		_ = pem.Encode(&b, &pem.Block{Type: "CERTIFICATE", Bytes: d})
	}
	return b.Bytes()
}

func KeyPEM(key *ecdsa.PrivateKey) ([]byte, error) {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), nil
}

// Save 写入证书链与私钥（私钥 0600）。先写私钥再写证书：两者都只在完整写入后替换，崩溃后最坏情况是
// 新私钥配旧证书——加载时公钥不匹配会报错，此时用旧私钥的备份 key.prev 恢复（见 Load）。
func Save(certFile, keyFile string, chain [][]byte, key *ecdsa.PrivateKey) error {
	kp, err := KeyPEM(key)
	if err != nil {
		return err
	}
	if old, err := os.ReadFile(keyFile); err == nil {
		if err := WriteAtomic(keyFile+".prev", old, 0o600); err != nil {
			return err
		}
	}
	if err := WriteAtomic(keyFile, kp, 0o600); err != nil {
		return err
	}
	return WriteAtomic(certFile, ChainPEM(chain), 0o644)
}

// Load 读取证书与私钥；私钥与证书不匹配时尝试 key.prev（Save 在两次替换之间崩溃的情形）。
func Load(certFile, keyFile string) (tls.Certificate, error) {
	c, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err == nil {
		return c, nil
	}
	if c2, err2 := tls.LoadX509KeyPair(certFile, keyFile+".prev"); err2 == nil {
		return c2, nil
	}
	return tls.Certificate{}, err
}

// Holder 持有当前证书，供 TLS 的 GetClientCertificate 使用；续签后替换，之后建立的连接使用新证书。
type Holder struct {
	mu   sync.RWMutex
	cert tls.Certificate
}

func NewHolder(c tls.Certificate) *Holder { return &Holder{cert: c} }

func (h *Holder) Get() tls.Certificate {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.cert
}

func (h *Holder) Set(c tls.Certificate) {
	h.mu.Lock()
	h.cert = c
	h.mu.Unlock()
}

// Leaf 返回当前叶证书。
func (h *Holder) Leaf() (*x509.Certificate, error) {
	c := h.Get()
	if c.Leaf != nil {
		return c.Leaf, nil
	}
	if len(c.Certificate) == 0 {
		return nil, errors.New("certs: no certificate")
	}
	return x509.ParseCertificate(c.Certificate[0])
}

// GetClientCertificate 用于 tls.Config。证书已过期时返回空证书：不再建立新的控制连接（AGT-012），
// 握手会因缺少客户端证书被 Edge Hub 拒绝。
func (h *Holder) GetClientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	c := h.Get()
	if leaf, err := h.Leaf(); err == nil && time.Now().After(leaf.NotAfter) {
		return &tls.Certificate{}, nil
	}
	return &c, nil
}

// RenewDue：剩余有效期少于总有效期的 1/3 时续签（AGT-012）。
func RenewDue(leaf *x509.Certificate, now time.Time) bool {
	life := leaf.NotAfter.Sub(leaf.NotBefore)
	return leaf.NotAfter.Sub(now) < life/3
}
