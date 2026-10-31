package vlink

import (
	"bytes"
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"vortex/engine/types"
)

// 承载层（C1 第 1 节、ENG-010、SEC-002）：TLS 1.3 双向认证，身份只取自证书 SAN URI
// spiffe://vortex/node/<node_id> 与平台 CA 链，不做主机名校验（R11）。

var (
	ErrNoNodeURI     = errors.New("vlink: certificate has no vortex node URI")
	ErrRevoked       = errors.New("vlink: peer certificate revoked")
	ErrUnexpectedID  = errors.New("vlink: peer node_id is not the expected one")
	ErrCipherSuite   = errors.New("vlink: cipher suite not allowed")
	ErrALPN          = errors.New("vlink: ALPN vortex-vlink/1 not negotiated")
	ErrTLSVersion    = errors.New("vlink: TLS 1.3 required")
	ErrNoCertificate = errors.New("vlink: peer presented no certificate")
)

// NodeURI 返回证书 SAN 中的节点 URI。
func NodeURI(id types.UUID) string { return "spiffe://vortex/node/" + FormatUUID(id) }

// NodeIDFromCert 从证书 SAN URI 提取 node_id。存在多个节点 URI 时视为无效证书。
func NodeIDFromCert(c *x509.Certificate) (types.UUID, error) {
	var (
		id    types.UUID
		found int
	)
	for _, u := range c.URIs {
		if u.Scheme != "spiffe" || u.Host != "vortex" || !strings.HasPrefix(u.Path, "/node/") {
			continue
		}
		v, err := ParseUUID(strings.TrimPrefix(u.Path, "/node/"))
		if err != nil {
			return types.UUID{}, fmt.Errorf("%w: %v", ErrNoNodeURI, err)
		}
		id = v
		found++
	}
	if found != 1 {
		return types.UUID{}, ErrNoNodeURI
	}
	return id, nil
}

// Revocations 是可原子替换的吊销列表（随运行配置下发，ENG-010）。
type Revocations struct{ p atomic.Pointer[revSet] }

type revSet struct {
	serials map[string]bool
	nodes   map[types.UUID]bool
}

// Set 替换吊销列表；已建立的链路由调用方根据新列表决定是否关闭（RevokedLink）。
func (r *Revocations) Set(l types.RevocationList) {
	s := &revSet{serials: map[string]bool{}, nodes: map[types.UUID]bool{}}
	for _, sn := range l.CertSerials {
		s.serials[string(bytes.TrimLeft(sn, "\x00"))] = true
	}
	for _, n := range l.NodeIDs {
		s.nodes[n] = true
	}
	r.p.Store(s)
}

// Revoked 判断证书或节点是否已吊销。序列号按无符号大端比较，忽略前导零。
func (r *Revocations) Revoked(c *x509.Certificate, node types.UUID) bool {
	s := r.p.Load()
	if s == nil {
		return false
	}
	return s.nodes[node] || s.serials[string(c.SerialNumber.Bytes())]
}

// Identity 是本节点的证书与私钥（ECDSA P-256，签发自平台中间 CA）。
type Identity struct {
	NodeID types.UUID
	Cert   tls.Certificate // Certificate[0] 为叶子，其后为中间 CA；PrivateKey 必须实现 crypto.Signer
}

// Signer 返回私钥。
func (id *Identity) Signer() (crypto.Signer, error) {
	s, ok := id.Cert.PrivateKey.(crypto.Signer)
	if !ok {
		return nil, errors.New("vlink: identity key is not a crypto.Signer")
	}
	return s, nil
}

// Trust 是校验对端身份所需的信任材料。
type Trust struct {
	Roots       *x509.CertPool // 平台 CA
	Revocations *Revocations   // nil 表示没有吊销列表
}

var allowedSuites = map[uint16]bool{tls.TLS_AES_128_GCM_SHA256: true, tls.TLS_AES_256_GCM_SHA384: true}

// VerifyPeer 校验已完成握手的连接状态：TLS 1.3、允许的套件、ALPN、证书链、节点 URI、吊销，
// 以及（expect 非零时）对端是否为预期节点。返回对端 node_id。
func (t *Trust) VerifyPeer(cs *tls.ConnectionState, expect types.UUID) (types.UUID, error) {
	if cs.Version != tls.VersionTLS13 {
		return types.UUID{}, ErrTLSVersion
	}
	// Go 不允许配置 TLS 1.3 套件；只能在握手后拒绝 ChaCha20（ENG-044 依赖 AES-GCM 硬件加速）
	if !allowedSuites[cs.CipherSuite] {
		return types.UUID{}, fmt.Errorf("%w: %s", ErrCipherSuite, tls.CipherSuiteName(cs.CipherSuite))
	}
	if cs.NegotiatedProtocol != ALPN {
		return types.UUID{}, ErrALPN
	}
	if len(cs.PeerCertificates) == 0 {
		return types.UUID{}, ErrNoCertificate
	}
	leaf := cs.PeerCertificates[0]
	inter := x509.NewCertPool()
	for _, c := range cs.PeerCertificates[1:] {
		inter.AddCert(c)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: t.Roots, Intermediates: inter,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}}); err != nil {
		return types.UUID{}, err
	}
	id, err := NodeIDFromCert(leaf)
	if err != nil {
		return types.UUID{}, err
	}
	if t.Revocations != nil && t.Revocations.Revoked(leaf, id) {
		return id, ErrRevoked
	}
	if expect != (types.UUID{}) && id != expect {
		return id, fmt.Errorf("%w: got %s, want %s", ErrUnexpectedID, FormatUUID(id), FormatUUID(expect))
	}
	return id, nil
}

func baseConfig(id *Identity) *tls.Config {
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		MaxVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{id.Cert},
		NextProtos:   []string{ALPN},
		// 链路是长连接、以吞吐为主：始终使用最大记录（16 KiB），不在空闲后退回小记录（ENG-044）
		DynamicRecordSizingDisabled: true,
	}
}

// DefaultTLSRecordSizing 报告链路的 TLS 配置是否关闭了动态记录大小（测试用）。
func DefaultTLSRecordSizing() bool {
	return baseConfig(&Identity{}).DynamicRecordSizingDisabled
}

// ServerConfig 是接受方配置：要求客户端证书，链与身份在 VerifyConnection 中按 Trust 校验。
func ServerConfig(id *Identity, t *Trust) *tls.Config {
	c := baseConfig(id)
	c.ClientAuth = tls.RequireAnyClientCert // 链校验在 VerifyConnection 中进行，以便同时检查节点 URI 与吊销
	c.VerifyConnection = func(cs tls.ConnectionState) error {
		_, err := t.VerifyPeer(&cs, types.UUID{})
		return err
	}
	return c
}

// ClientConfig 是发起方配置；expect 为要连接的节点（零值表示不限定）。
func ClientConfig(id *Identity, t *Trust, expect types.UUID) *tls.Config {
	c := baseConfig(id)
	c.ServerName = "vortex-node" // 仅作 SNI 占位；身份不取自主机名（R11）
	c.InsecureSkipVerify = true  // 默认的主机名校验不适用；链与身份在 VerifyConnection 中校验
	c.VerifyConnection = func(cs tls.ConnectionState) error {
		_, err := t.VerifyPeer(&cs, expect)
		return err
	}
	return c
}

// FormatUUID 以 8-4-4-4-12 形式输出。
func FormatUUID(u types.UUID) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
}

// ParseUUID 解析 8-4-4-4-12 形式（大小写均可）。
func ParseUUID(s string) (types.UUID, error) {
	var u types.UUID
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return u, fmt.Errorf("bad uuid %q", s)
	}
	hex := strings.ReplaceAll(s, "-", "")
	for i := 0; i < 16; i++ {
		hi, ok1 := unhex(hex[2*i])
		lo, ok2 := unhex(hex[2*i+1])
		if !ok1 || !ok2 {
			return u, fmt.Errorf("bad uuid %q", s)
		}
		u[i] = hi<<4 | lo
	}
	return u, nil
}

func unhex(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}
