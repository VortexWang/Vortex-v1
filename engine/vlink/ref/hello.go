package ref

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
)

const ExporterLabel = "EXPORTER-vortex-vlink-hello"

// HELLO TLV tag（C1 第 4 节，含评审决定 R1：0x07 改为摘要）。
const (
	helloVersion      = 0x01
	helloNodeID       = 0x02
	helloBootID       = 0x03
	helloGeneration   = 0x04
	helloCapabilities = 0x05
	helloExporterSig  = 0x06
	helloServedDigest = 0x07
	helloLinkMode     = 0x08
)

const (
	CapPool  uint64 = 1 << 0
	CapDgram uint64 = 1 << 1
)

type UUID [16]byte

type ServedRule struct {
	RuleID  UUID
	Version uint64
}

type Hello struct {
	Version      uint16
	NodeID       UUID
	BootID       UUID
	Generation   uint64
	Capabilities uint64
	ExporterSig  []byte
	// 仅反向建链发起方填写（ENG-018）：所服务规则的数量与摘要。
	ServedCount  uint32
	ServedDigest [32]byte
	HasServed    bool
	LinkMode     uint8 // 0 = mux，1 = pool
}

// ServedRulesDigest：按 (rule_id 字节序, rule_version) 排序后，SHA-256( rule_id(16) ‖ rule_version(8, 大端) ‖ … )。
func ServedRulesDigest(rules []ServedRule) (uint32, [32]byte) {
	rs := append([]ServedRule(nil), rules...)
	sort.Slice(rs, func(i, j int) bool {
		if c := string(rs[i].RuleID[:]); c != string(rs[j].RuleID[:]) {
			return c < string(rs[j].RuleID[:])
		}
		return rs[i].Version < rs[j].Version
	})
	h := sha256.New()
	var v [8]byte
	for _, r := range rs {
		h.Write(r.RuleID[:])
		binary.BigEndian.PutUint64(v[:], r.Version)
		h.Write(v[:])
	}
	var d [32]byte
	copy(d[:], h.Sum(nil))
	return uint32(len(rs)), d
}

// exporterContext = node_id(16) ‖ boot_id(16) ‖ generation(8, 大端)
func exporterContext(node, boot UUID, gen uint64) []byte {
	c := make([]byte, 40)
	copy(c[0:16], node[:])
	copy(c[16:32], boot[:])
	binary.BigEndian.PutUint64(c[32:40], gen)
	return c
}

func ekm(cs tls.ConnectionState, node, boot UUID, gen uint64) ([]byte, error) {
	if cs.Version != tls.VersionTLS13 {
		return nil, errors.New("vlink: TLS 1.3 required")
	}
	return cs.ExportKeyingMaterial(ExporterLabel, exporterContext(node, boot, gen), 32)
}

// Sign 计算本端 HELLO 的 exporter 签名（ECDSA P-256 / SHA-256，ASN.1 DER）。
func (h *Hello) Sign(cs tls.ConnectionState, key *ecdsa.PrivateKey) error {
	m, err := ekm(cs, h.NodeID, h.BootID, h.Generation)
	if err != nil {
		return err
	}
	d := sha256.Sum256(m)
	sig, err := key.Sign(rand.Reader, d[:], crypto.SHA256)
	if err != nil {
		return err
	}
	h.ExporterSig = sig
	return nil
}

// NodeIDFromCert 从证书 SAN URI spiffe://vortex/node/<uuid> 提取 node_id（C1 第 1 节）。
func NodeIDFromCert(c *x509.Certificate) (string, error) {
	for _, u := range c.URIs {
		if u.Scheme == "spiffe" && u.Host == "vortex" && strings.HasPrefix(u.Path, "/node/") {
			return strings.TrimPrefix(u.Path, "/node/"), nil
		}
	}
	return "", errors.New("vlink: certificate has no vortex node URI")
}

func NodeURI(id string) *url.URL {
	return &url.URL{Scheme: "spiffe", Host: "vortex", Path: "/node/" + id}
}

// VerifyPeer 校验对端 HELLO：node_id 与证书一致、签名绑定本 TLS 连接、代次不低于本地已知值。
func (h *Hello) VerifyPeer(cs tls.ConnectionState, knownGeneration uint64) error {
	if len(cs.PeerCertificates) == 0 {
		return errors.New("vlink: no peer certificate")
	}
	cert := cs.PeerCertificates[0]
	id, err := NodeIDFromCert(cert)
	if err != nil {
		return err
	}
	if id != FormatUUID(h.NodeID) {
		return fmt.Errorf("vlink: HELLO node_id %s != certificate %s", FormatUUID(h.NodeID), id)
	}
	if h.Generation < knownGeneration {
		return errors.New("vlink: stale generation")
	}
	pub, ok := cert.PublicKey.(*ecdsa.PublicKey)
	if !ok {
		return errors.New("vlink: peer key is not ECDSA")
	}
	m, err := ekm(cs, h.NodeID, h.BootID, h.Generation)
	if err != nil {
		return err
	}
	d := sha256.Sum256(m)
	if !ecdsa.VerifyASN1(pub, d[:], h.ExporterSig) {
		return errors.New("vlink: exporter signature invalid (replayed or forged HELLO)")
	}
	return nil
}

func (h *Hello) MarshalPayload() []byte {
	u16 := func(v uint16) []byte { return binary.BigEndian.AppendUint16(nil, v) }
	u64 := func(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }
	items := []TLV{
		{helloVersion, u16(h.Version)},
		{helloNodeID, h.NodeID[:]},
		{helloBootID, h.BootID[:]},
		{helloGeneration, u64(h.Generation)},
		{helloCapabilities, u64(h.Capabilities)},
		{helloExporterSig, h.ExporterSig},
	}
	if h.HasServed {
		v := binary.BigEndian.AppendUint32(nil, h.ServedCount)
		items = append(items, TLV{helloServedDigest, append(v, h.ServedDigest[:]...)})
	}
	items = append(items, TLV{helloLinkMode, []byte{h.LinkMode}})
	return EncodeTLV(items)
}

func UnmarshalHello(p []byte) (*Hello, error) {
	m, err := DecodeTLV(p)
	if err != nil {
		return nil, err
	}
	need := func(tag uint8, n int) ([]byte, error) {
		v, ok := m[tag]
		if !ok || (n >= 0 && len(v) != n) {
			return nil, fmt.Errorf("vlink: HELLO tag 0x%02x missing or wrong length", tag)
		}
		return v, nil
	}
	h := &Hello{}
	var v []byte
	if v, err = need(helloVersion, 2); err != nil {
		return nil, err
	}
	h.Version = binary.BigEndian.Uint16(v)
	if v, err = need(helloNodeID, 16); err != nil {
		return nil, err
	}
	copy(h.NodeID[:], v)
	if v, err = need(helloBootID, 16); err != nil {
		return nil, err
	}
	copy(h.BootID[:], v)
	if v, err = need(helloGeneration, 8); err != nil {
		return nil, err
	}
	h.Generation = binary.BigEndian.Uint64(v)
	if v, err = need(helloCapabilities, 8); err != nil {
		return nil, err
	}
	h.Capabilities = binary.BigEndian.Uint64(v)
	if v, err = need(helloExporterSig, -1); err != nil {
		return nil, err
	}
	h.ExporterSig = v
	if v, ok := m[helloServedDigest]; ok {
		if len(v) != 36 {
			return nil, errors.New("vlink: served digest length")
		}
		h.HasServed = true
		h.ServedCount = binary.BigEndian.Uint32(v[0:4])
		copy(h.ServedDigest[:], v[4:])
	}
	if v, ok := m[helloLinkMode]; ok && len(v) == 1 {
		h.LinkMode = v[0]
	}
	return h, nil
}

func FormatUUID(u UUID) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
}
