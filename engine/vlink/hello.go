package vlink

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
	"sort"

	"vortex/engine/types"
)

const ExporterLabel = "EXPORTER-vortex-vlink-hello"

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

// 能力位（C1 第 4 节）。
const (
	CapPool  uint64 = 1 << 0
	CapDgram uint64 = 1 << 1
)

type LinkMode uint8

const (
	ModeMux  LinkMode = 0
	ModePool LinkMode = 1
)

// ServedRule 是反向建链方所服务的规则（R1：HELLO 只携带其摘要）。
type ServedRule struct {
	RuleID  types.UUID
	Version uint64
}

// Hello 是 HELLO 帧载荷。
type Hello struct {
	Version      uint16
	NodeID       types.UUID
	BootID       types.UUID
	Generation   uint64
	Capabilities uint64
	ExporterSig  []byte
	HasServed    bool // 仅反向建链发起方
	ServedCount  uint32
	ServedDigest [32]byte
	LinkMode     LinkMode
}

// ServedDigest：按 (rule_id 字节序, rule_version) 排序后 SHA-256(rule_id(16) ‖ rule_version(8, 大端) ‖ …)。
func ServedDigest(rules []ServedRule) (uint32, [32]byte) {
	rs := append([]ServedRule(nil), rules...)
	sort.Slice(rs, func(i, j int) bool {
		if rs[i].RuleID != rs[j].RuleID {
			return string(rs[i].RuleID[:]) < string(rs[j].RuleID[:])
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
	h.Sum(d[:0])
	return uint32(len(rs)), d
}

// exporterDigest = SHA-256(TLS-Exporter(label, node_id ‖ boot_id ‖ generation(8, 大端), 32))（R8）。
func exporterDigest(cs *tls.ConnectionState, node, boot types.UUID, gen uint64) ([32]byte, error) {
	if cs.Version != tls.VersionTLS13 {
		return [32]byte{}, errors.New("vlink: TLS 1.3 required")
	}
	var ctx [40]byte
	copy(ctx[0:16], node[:])
	copy(ctx[16:32], boot[:])
	binary.BigEndian.PutUint64(ctx[32:40], gen)
	m, err := cs.ExportKeyingMaterial(ExporterLabel, ctx[:], 32)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(m), nil
}

// Sign 计算 exporter 签名：ECDSA P-256 / SHA-256，ASN.1 DER。
func (h *Hello) Sign(cs *tls.ConnectionState, key crypto.Signer) error {
	d, err := exporterDigest(cs, h.NodeID, h.BootID, h.Generation)
	if err != nil {
		return err
	}
	sig, err := key.Sign(rand.Reader, d[:], crypto.SHA256)
	if err != nil {
		return err
	}
	h.ExporterSig = sig
	return nil
}

// verifySig 用对端证书公钥验证签名绑定本 TLS 连接与对端本次启动（防重放）。
func (h *Hello) verifySig(cs *tls.ConnectionState) error {
	if len(cs.PeerCertificates) == 0 {
		return errors.New("vlink: no peer certificate")
	}
	pub, ok := cs.PeerCertificates[0].PublicKey.(*ecdsa.PublicKey)
	if !ok {
		return errors.New("vlink: peer key is not ECDSA")
	}
	d, err := exporterDigest(cs, h.NodeID, h.BootID, h.Generation)
	if err != nil {
		return err
	}
	if !ecdsa.VerifyASN1(pub, d[:], h.ExporterSig) {
		return errors.New("vlink: exporter signature invalid (replayed or forged HELLO)")
	}
	return nil
}

// AppendPayload 编码 HELLO 载荷，tag 顺序与参考实现相同（测试向量逐字节一致）。
func (h *Hello) AppendPayload(dst []byte) []byte {
	var b8 [8]byte
	binary.BigEndian.PutUint16(b8[:2], h.Version)
	dst = appendTLV(dst, helloVersion, b8[:2])
	dst = appendTLV(dst, helloNodeID, h.NodeID[:])
	dst = appendTLV(dst, helloBootID, h.BootID[:])
	binary.BigEndian.PutUint64(b8[:], h.Generation)
	dst = appendTLV(dst, helloGeneration, b8[:])
	binary.BigEndian.PutUint64(b8[:], h.Capabilities)
	dst = appendTLV(dst, helloCapabilities, b8[:])
	dst = appendTLV(dst, helloExporterSig, h.ExporterSig)
	if h.HasServed {
		var sd [36]byte
		binary.BigEndian.PutUint32(sd[:4], h.ServedCount)
		copy(sd[4:], h.ServedDigest[:])
		dst = appendTLV(dst, helloServedDigest, sd[:])
	}
	return appendTLV(dst, helloLinkMode, []byte{byte(h.LinkMode)})
}

// ParseHello 解码 HELLO 载荷。必需 tag：version、node_id、boot_id、generation、capabilities、exporter_sig；
// link_mode 缺省为 mux；served_digest 仅反向建链方发送。长度不符、重复 tag 为协议错误，未知 tag 忽略。
func ParseHello(p []byte) (*Hello, error) {
	var s tlvSet
	if err := decodeTLV(p, &s); err != nil {
		return nil, err
	}
	h := &Hello{}
	v, err := s.need("HELLO", helloVersion, 2)
	if err != nil {
		return nil, err
	}
	h.Version = binary.BigEndian.Uint16(v)
	if v, err = s.need("HELLO", helloNodeID, 16); err != nil {
		return nil, err
	}
	h.NodeID = types.UUID(v)
	if v, err = s.need("HELLO", helloBootID, 16); err != nil {
		return nil, err
	}
	h.BootID = types.UUID(v)
	if v, err = s.need("HELLO", helloGeneration, 8); err != nil {
		return nil, err
	}
	h.Generation = binary.BigEndian.Uint64(v)
	if v, err = s.need("HELLO", helloCapabilities, 8); err != nil {
		return nil, err
	}
	h.Capabilities = binary.BigEndian.Uint64(v)
	if v, err = s.need("HELLO", helloExporterSig, -1); err != nil {
		return nil, err
	}
	h.ExporterSig = append([]byte(nil), v...)
	if v, ok := s.get(helloServedDigest); ok {
		if len(v) != 36 {
			return nil, fmt.Errorf("%w: HELLO served_digest length %d", ErrProtocol, len(v))
		}
		h.HasServed = true
		h.ServedCount = binary.BigEndian.Uint32(v[:4])
		copy(h.ServedDigest[:], v[4:])
	}
	if v, ok := s.get(helloLinkMode); ok {
		if len(v) != 1 || v[0] > byte(ModePool) {
			return nil, fmt.Errorf("%w: HELLO link_mode", ErrProtocol)
		}
		h.LinkMode = LinkMode(v[0])
	}
	return h, nil
}

// ---------------------------------------------------------------- OPEN（C1 第 5 节）

const (
	openRuleID     = 0x01
	openRuleVer    = 0x02
	openPathGen    = 0x03
	openHopIndex   = 0x04
	openNetwork    = 0x05
	openVisited    = 0x06
	openClientAddr = 0x07
)

// MaxVisited：入口加 3 个中继（R10）。
const MaxVisited = 4

// ErrBadOpen 表示 OPEN 载荷不合规，接收方回 OPEN_ERR(unauthorized)（R10），不关闭链路。
var ErrBadOpen = errors.New("vlink: malformed OPEN")

// Open 是 OPEN 帧载荷。不含下游地址：下一跳与目标只取接收方本地配置（ENG-021）。
type Open struct {
	RuleID         types.UUID
	RuleVersion    uint64
	PathGeneration uint64
	HopIndex       uint8
	Network        types.Network
	Visited        []types.UUID
	Client         netip.AddrPort
}

func (o *Open) AppendPayload(dst []byte) []byte {
	var b8 [8]byte
	dst = appendTLV(dst, openRuleID, o.RuleID[:])
	binary.BigEndian.PutUint64(b8[:], o.RuleVersion)
	dst = appendTLV(dst, openRuleVer, b8[:])
	binary.BigEndian.PutUint64(b8[:], o.PathGeneration)
	dst = appendTLV(dst, openPathGen, b8[:])
	dst = appendTLV(dst, openHopIndex, []byte{o.HopIndex})
	dst = appendTLV(dst, openNetwork, []byte{byte(o.Network)})
	dst = append(dst, openVisited)
	dst = binary.BigEndian.AppendUint16(dst, uint16(16*len(o.Visited)))
	for _, v := range o.Visited {
		dst = append(dst, v[:]...)
	}
	a := o.Client.Addr().Unmap()
	if a.Is4() {
		dst = append(dst, openClientAddr, 0, 7, 4)
	} else {
		dst = append(dst, openClientAddr, 0, 19, 6)
	}
	dst = append(dst, a.AsSlice()...)
	return binary.BigEndian.AppendUint16(dst, o.Client.Port())
}

// ParseOpen 解码 OPEN 载荷；任何不合规都返回包装了 ErrBadOpen 的错误。
func ParseOpen(p []byte) (*Open, error) {
	o, err := parseOpen(p)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadOpen, err)
	}
	return o, nil
}

func parseOpen(p []byte) (*Open, error) {
	var s tlvSet
	if err := decodeTLV(p, &s); err != nil {
		return nil, err
	}
	o := &Open{}
	v, err := s.need("OPEN", openRuleID, 16)
	if err != nil {
		return nil, err
	}
	o.RuleID = types.UUID(v)
	if v, err = s.need("OPEN", openRuleVer, 8); err != nil {
		return nil, err
	}
	o.RuleVersion = binary.BigEndian.Uint64(v)
	if v, err = s.need("OPEN", openPathGen, 8); err != nil {
		return nil, err
	}
	o.PathGeneration = binary.BigEndian.Uint64(v)
	if v, err = s.need("OPEN", openHopIndex, 1); err != nil {
		return nil, err
	}
	o.HopIndex = v[0]
	if v, err = s.need("OPEN", openNetwork, 1); err != nil {
		return nil, err
	}
	switch v[0] {
	case 1:
		o.Network = types.NetworkTCP
	case 2:
		o.Network = types.NetworkUDP
	default:
		return nil, fmt.Errorf("network %d", v[0])
	}
	if v, err = s.need("OPEN", openVisited, -1); err != nil {
		return nil, err
	}
	if len(v)%16 != 0 || len(v)/16 > MaxVisited {
		return nil, fmt.Errorf("visited length %d", len(v))
	}
	for i := 0; i < len(v); i += 16 {
		o.Visited = append(o.Visited, types.UUID(v[i:i+16]))
	}
	if v, err = s.need("OPEN", openClientAddr, -1); err != nil {
		return nil, err
	}
	switch {
	case len(v) == 7 && v[0] == 4:
		o.Client = netip.AddrPortFrom(netip.AddrFrom4([4]byte(v[1:5])), binary.BigEndian.Uint16(v[5:7]))
	case len(v) == 19 && v[0] == 6:
		o.Client = netip.AddrPortFrom(netip.AddrFrom16([16]byte(v[1:17])), binary.BigEndian.Uint16(v[17:19]))
	default:
		return nil, fmt.Errorf("client_addr length %d family %v", len(v), v[:min(len(v), 1)])
	}
	return o, nil
}
