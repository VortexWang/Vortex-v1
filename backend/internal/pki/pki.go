// Package pki 是平台 CA（SEC-001、SEC-012；BE-T10）：根 CA 只用于签发中间 CA，私钥离线保存；中间 CA 签发节点、
// Edge Hub 与后端证书，私钥以加密文件保存（VORTEX-CA-KEY：scrypt 派生密钥 + AES-256-GCM），口令取自环境变量，
// 不以明文进入数据库或日志。
//
// 证书身份只在 SAN URI 中（与 C1、契约补充 L5 一致）：节点 spiffe://vortex/node/<id>，Edge Hub
// spiffe://vortex/hub/<id>，后端 spiffe://vortex/backend。不写主机名。
package pki

import (
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"strings"
	"time"

	"golang.org/x/crypto/scrypt"
)

// 默认有效期：节点证书 30 天（AGT-012），Edge Hub 实例证书 30 天（CR-02），后端证书 30 天。
const (
	NodeLifetime    = 30 * 24 * time.Hour
	HubLifetime     = 30 * 24 * time.Hour
	BackendLifetime = 30 * 24 * time.Hour
)

const BackendURI = "spiffe://vortex/backend"

func NodeURI(id [16]byte) string { return "spiffe://vortex/node/" + formatUUID(id) }
func HubURI(id [16]byte) string  { return "spiffe://vortex/hub/" + formatUUID(id) }

func formatUUID(u [16]byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
}

var (
	ErrCSR        = errors.New("pki: invalid CSR")
	ErrPassphrase = errors.New("pki: wrong passphrase or corrupt key file")
)

// CA 是可签发的中间 CA 与信任根。
type CA struct {
	Root  *x509.Certificate
	Inter *x509.Certificate
	key   crypto.Signer
	Pool  *x509.CertPool // 只含根
}

// Bundle 是分发给组件的信任根（EnrollResponse.ca_bundle_der）。
func (c *CA) Bundle() [][]byte { return [][]byte{c.Root.Raw} }

// Issued 是一次签发的结果。
type Issued struct {
	Chain  [][]byte // 叶证书在前，随后是中间 CA
	Leaf   *x509.Certificate
	Serial []byte
}

// Issue 按 CSR 签发：只接受 ECDSA P-256 公钥与有效的自签名；身份由 uri 决定（CSR 中的主题与扩展一律忽略）。
func (c *CA) Issue(csrDER []byte, uri string, life time.Duration, usage ...x509.ExtKeyUsage) (*Issued, error) {
	csr, err := x509.ParseCertificateRequest(csrDER)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCSR, err)
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCSR, err)
	}
	pub, ok := csr.PublicKey.(*ecdsa.PublicKey)
	if !ok || pub.Curve != elliptic.P256() {
		return nil, fmt.Errorf("%w: key must be ECDSA P-256", ErrCSR)
	}
	return c.issue(pub, uri, life, usage...)
}

func (c *CA) issue(pub crypto.PublicKey, uri string, life time.Duration, usage ...x509.ExtKeyUsage) (*Issued, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		return nil, err
	}
	now := time.Now()
	notAfter := now.Add(life)
	if notAfter.After(c.Inter.NotAfter) {
		notAfter = c.Inter.NotAfter
	}
	tpl := &x509.Certificate{SerialNumber: serial, NotBefore: now.Add(-time.Minute), NotAfter: notAfter, URIs: []*url.URL{u},
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: usage}
	der, err := x509.CreateCertificate(rand.Reader, tpl, c.Inter, pub, c.key)
	if err != nil {
		return nil, err
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &Issued{Chain: [][]byte{der, c.Inter.Raw}, Leaf: leaf, Serial: leaf.SerialNumber.Bytes()}, nil
}

// ServerCert 为后端自己签发一张服务端证书（spiffe://vortex/backend，私钥只在内存中）。
func (c *CA) ServerCert() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	is, err := c.issue(&key.PublicKey, BackendURI, BackendLifetime, x509.ExtKeyUsageServerAuth)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: is.Chain, PrivateKey: key, Leaf: is.Leaf}, nil
}

// Verify 校验对端证书链到本平台根，返回叶证书。
func (c *CA) Verify(chain []*x509.Certificate, usage x509.ExtKeyUsage) (*x509.Certificate, error) {
	if len(chain) == 0 {
		return nil, errors.New("pki: no certificate")
	}
	inter := x509.NewCertPool()
	for _, x := range chain[1:] {
		inter.AddCert(x)
	}
	if _, err := chain[0].Verify(x509.VerifyOptions{Roots: c.Pool, Intermediates: inter, KeyUsages: []x509.ExtKeyUsage{usage}}); err != nil {
		return nil, err
	}
	return chain[0], nil
}

// ParseIdentity 从叶证书的唯一 SAN URI 取出 (kind, id)，kind 为 "node" / "hub"。
func ParseIdentity(leaf *x509.Certificate) (string, [16]byte, error) {
	var id [16]byte
	if len(leaf.URIs) != 1 {
		return "", id, errors.New("pki: certificate must carry exactly one URI")
	}
	u := leaf.URIs[0]
	if u.Scheme != "spiffe" || u.Host != "vortex" {
		return "", id, errors.New("pki: not a platform identity")
	}
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(parts) != 2 || (parts[0] != "node" && parts[0] != "hub") {
		return "", id, errors.New("pki: not a node or hub identity")
	}
	b, err := hex.DecodeString(strings.ReplaceAll(parts[1], "-", ""))
	if err != nil || len(b) != 16 {
		return "", id, errors.New("pki: bad id")
	}
	copy(id[:], b)
	return parts[0], id, nil
}

// ---------------------------------------------------------------- 加载与初始化

// Load 读取根证书、中间 CA 证书与加密的中间 CA 私钥。
func Load(rootFile, interFile, keyFile string, passphrase []byte) (*CA, error) {
	root, err := readCert(rootFile)
	if err != nil {
		return nil, err
	}
	inter, err := readCert(interFile)
	if err != nil {
		return nil, err
	}
	kb, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, err
	}
	key, err := DecryptKey(kb, passphrase)
	if err != nil {
		return nil, err
	}
	return New(root, inter, key)
}

// New 组装 CA，并检查中间 CA 由根签发、私钥与中间 CA 证书匹配。
func New(root, inter *x509.Certificate, key crypto.Signer) (*CA, error) {
	if err := inter.CheckSignatureFrom(root); err != nil {
		return nil, fmt.Errorf("pki: intermediate not signed by root: %w", err)
	}
	pub, ok := key.Public().(*ecdsa.PublicKey)
	ip, ok2 := inter.PublicKey.(*ecdsa.PublicKey)
	if !ok || !ok2 || !pub.Equal(ip) {
		return nil, errors.New("pki: intermediate key does not match its certificate")
	}
	pool := x509.NewCertPool()
	pool.AddCert(root)
	return &CA{Root: root, Inter: inter, key: key, Pool: pool}, nil
}

func readCert(path string) (*x509.Certificate, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	blk, _ := pem.Decode(b)
	if blk == nil || blk.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("%s: no PEM certificate", path)
	}
	return x509.ParseCertificate(blk.Bytes)
}

// Generated 是 Generate 的结果：根私钥只用于签发中间 CA，之后应离线保存（SEC-012）。
type Generated struct {
	CA      *CA
	RootKey *ecdsa.PrivateKey
}

// Generate 生成根 CA（10 年）与中间 CA（3 年）。
func Generate(org string) (*Generated, error) {
	rk, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	rootTpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{Organization: []string{org}, CommonName: org + " Root CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.AddDate(10, 0, 0), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
	rd, err := x509.CreateCertificate(rand.Reader, rootTpl, rootTpl, &rk.PublicKey, rk)
	if err != nil {
		return nil, err
	}
	root, _ := x509.ParseCertificate(rd)
	ik, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	interTpl := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{Organization: []string{org}, CommonName: org + " Intermediate CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.AddDate(3, 0, 0), IsCA: true, BasicConstraintsValid: true, MaxPathLenZero: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
	id, err := x509.CreateCertificate(rand.Reader, interTpl, root, &ik.PublicKey, rk)
	if err != nil {
		return nil, err
	}
	inter, _ := x509.ParseCertificate(id)
	ca, err := New(root, inter, ik)
	if err != nil {
		return nil, err
	}
	return &Generated{CA: ca, RootKey: rk}, nil
}

// CertPEM 编码证书。
func CertPEM(c *x509.Certificate) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: c.Raw})
}

// IntermediateKey 返回中间 CA 私钥（只供 vortexctl ca init 加密写盘）。
func (c *CA) IntermediateKey() crypto.Signer { return c.key }

// ---------------------------------------------------------------- 加密私钥文件

const (
	keyBlockType = "VORTEX-CA-KEY"
	scryptN      = 1 << 15
	scryptR      = 8
	scryptP      = 1
)

// EncryptKey 以口令加密私钥（PKCS#8）：scrypt(N=2^15, r=8, p=1) 派生 256 位密钥，AES-256-GCM。
func EncryptKey(key crypto.Signer, passphrase []byte) ([]byte, error) {
	if len(passphrase) < 12 {
		return nil, errors.New("pki: passphrase must be at least 12 bytes")
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}
	aead, err := keyAEAD(passphrase, salt)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	body := append(append(append([]byte{}, salt...), nonce...), aead.Seal(nil, nonce, der, []byte(keyBlockType))...)
	return pem.EncodeToMemory(&pem.Block{Type: keyBlockType, Headers: map[string]string{"KDF": "scrypt-32768-8-1", "Cipher": "AES-256-GCM"},
		Bytes: body}), nil
}

// DecryptKey 解密 EncryptKey 的输出。
func DecryptKey(pemBytes, passphrase []byte) (crypto.Signer, error) {
	blk, _ := pem.Decode(pemBytes)
	if blk == nil || blk.Type != keyBlockType {
		return nil, fmt.Errorf("pki: not a %s file", keyBlockType)
	}
	if len(blk.Bytes) < 16+12+16 {
		return nil, ErrPassphrase
	}
	salt, rest := blk.Bytes[:16], blk.Bytes[16:]
	aead, err := keyAEAD(passphrase, salt)
	if err != nil {
		return nil, err
	}
	nonce, ct := rest[:aead.NonceSize()], rest[aead.NonceSize():]
	der, err := aead.Open(nil, nonce, ct, []byte(keyBlockType))
	if err != nil {
		return nil, ErrPassphrase
	}
	k, err := x509.ParsePKCS8PrivateKey(der)
	if err != nil {
		return nil, err
	}
	s, ok := k.(crypto.Signer)
	if !ok {
		return nil, errors.New("pki: key is not a signer")
	}
	return s, nil
}

func keyAEAD(passphrase, salt []byte) (cipher.AEAD, error) {
	k, err := scrypt.Key(passphrase, salt, scryptN, scryptR, scryptP, 32)
	if err != nil {
		return nil, err
	}
	b, err := aes.NewCipher(k)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(b)
}
