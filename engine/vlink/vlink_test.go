package vlink

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net"
	"net/netip"
	"net/url"
	"os"
	"reflect"
	"testing"
	"time"

	"vortex/engine/types"
	"vortex/engine/vlink/ref"
)

// ---------------------------------------------------------------- 测试向量与参考实现

type vector struct {
	Name string `json:"name"`
	Hex  string `json:"hex"`
}

func loadVectors(t *testing.T) []vector {
	b, err := os.ReadFile("ref/testdata/vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vs []vector
	if err := json.Unmarshal(b, &vs); err != nil {
		t.Fatal(err)
	}
	if len(vs) != 15 {
		t.Fatalf("expected 15 vectors, got %d", len(vs))
	}
	return vs
}

// 每个向量：生产实现读出 → 载荷解码再编码 → 整帧重新编码，必须与向量逐字节一致（ENG-T21 验收）。
func TestVectors(t *testing.T) {
	for _, v := range loadVectors(t) {
		raw, _ := hex.DecodeString(v.Hex)
		r := NewReader(bytes.NewReader(raw))
		h, p, err := r.Next()
		if err != nil {
			t.Fatalf("%s: %v", v.Name, err)
		}
		payload := append([]byte(nil), p...)
		switch h.Type {
		case TypeHello:
			hl, err := ParseHello(p)
			if err != nil {
				t.Fatalf("%s: %v", v.Name, err)
			}
			payload = hl.AppendPayload(nil)
		case TypeOpen:
			o, err := ParseOpen(p)
			if err != nil {
				t.Fatalf("%s: %v", v.Name, err)
			}
			payload = o.AppendPayload(nil)
		case TypeOpenErr, TypeRst:
			c, txt, err := ParseErrPayload(p)
			if err != nil {
				t.Fatalf("%s: %v", v.Name, err)
			}
			payload = AppendErrPayload(nil, c, txt)
		case TypeWindow:
			inc, err := ParseWindowPayload(p)
			if err != nil {
				t.Fatal(err)
			}
			payload = AppendWindowPayload(nil, inc)
		case TypeGoaway:
			last, c, err := ParseGoawayPayload(p)
			if err != nil {
				t.Fatal(err)
			}
			if v.Name == "goaway_draining" && (last != 41 || c != GoawayDraining) {
				t.Fatalf("goaway decoded %d %d", last, c)
			}
			payload = AppendGoawayPayload(nil, last, c)
		}
		if got := AppendFrame(nil, h.Type, h.StreamID, payload); !bytes.Equal(got, raw) {
			t.Fatalf("%s: re-encoded\n%x\nwant\n%x", v.Name, got, raw)
		}
		if _, _, err := r.Next(); err != io.EOF {
			t.Fatalf("%s: trailing data: %v", v.Name, err)
		}
	}
}

func b16(u types.UUID) []byte { return u[:] }

func uuidN(n byte) types.UUID {
	var u types.UUID
	for i := range u {
		u[i] = n
	}
	u[6] = 0x70 | (n & 0x0f)
	return u
}

func TestCrossCodecWithReference(t *testing.T) {
	opens := []Open{
		{RuleID: uuidN(3), RuleVersion: 9, PathGeneration: 2, HopIndex: 2, Network: types.NetworkTCP,
			Visited: []types.UUID{uuidN(1), uuidN(2)}, Client: netip.MustParseAddrPort("198.51.100.7:51000")},
		{RuleID: uuidN(4), RuleVersion: 1, Network: types.NetworkUDP, Client: netip.MustParseAddrPort("[2001:db8::1]:443")},
		{RuleID: uuidN(5), Network: types.NetworkTCP, Visited: []types.UUID{uuidN(1), uuidN(2), uuidN(3), uuidN(4)},
			Client: netip.MustParseAddrPort("[::ffff:10.0.0.1]:9")}, // v4 映射地址按 IPv4 编码
	}
	for _, o := range opens {
		p := o.AppendPayload(nil)
		ro, err := ref.UnmarshalOpen(p)
		if err != nil {
			t.Fatal(err)
		}
		if ro.RuleID != ref.UUID(o.RuleID) || ro.RuleVersion != o.RuleVersion || ro.PathGeneration != o.PathGeneration ||
			ro.HopIndex != o.HopIndex || ro.Network != uint8(o.Network) || len(ro.Visited) != len(o.Visited) ||
			ro.Client != netip.AddrPortFrom(o.Client.Addr().Unmap(), o.Client.Port()) {
			t.Fatalf("reference decoded %+v from %+v", ro, o)
		}
		if !bytes.Equal(ro.MarshalPayload(), p) {
			t.Fatal("reference re-encoding differs")
		}
		back, err := ParseOpen(ro.MarshalPayload())
		if err != nil {
			t.Fatal(err)
		}
		if back.RuleID != o.RuleID || back.Client.Addr() != o.Client.Addr().Unmap() || len(back.Visited) != len(o.Visited) {
			t.Fatalf("round trip %+v", back)
		}
	}
	rules := []ServedRule{{uuidN(9), 3}, {uuidN(2), 7}, {uuidN(2), 5}}
	var rr []ref.ServedRule
	for _, r := range rules {
		rr = append(rr, ref.ServedRule{RuleID: ref.UUID(r.RuleID), Version: r.Version})
	}
	n1, d1 := ServedDigest(rules)
	n2, d2 := ref.ServedRulesDigest(rr)
	if n1 != n2 || d1 != d2 {
		t.Fatal("served digest differs from reference")
	}
	h := Hello{Version: 1, NodeID: uuidN(1), BootID: uuidN(2), Generation: 5, Capabilities: CapPool | CapDgram,
		ExporterSig: []byte{1, 2, 3}, HasServed: true, ServedCount: n1, ServedDigest: d1, LinkMode: ModePool}
	rh, err := ref.UnmarshalHello(h.AppendPayload(nil))
	if err != nil {
		t.Fatal(err)
	}
	if rh.NodeID != ref.UUID(h.NodeID) || rh.Generation != 5 || rh.ServedDigest != d1 || rh.LinkMode != 1 || !bytes.Equal(rh.MarshalPayload(), h.AppendPayload(nil)) {
		t.Fatalf("reference hello %+v", rh)
	}
}

// ---------------------------------------------------------------- 帧与 TLV 规则

func hdr(t FrameType, length uint16, sid uint32) []byte {
	b := make([]byte, HeaderLen)
	b[0] = byte(t)
	b[2], b[3] = byte(length>>8), byte(length)
	b[4], b[5], b[6], b[7] = byte(sid>>24), byte(sid>>16), byte(sid>>8), byte(sid)
	return b
}

func TestFrameRules(t *testing.T) {
	// 超长：只有帧头、没有载荷，读取方必须立即拒绝而不是等待载荷
	r := NewReader(bytes.NewReader(hdr(TypeData, MaxPayload+1, 1)))
	if _, _, err := r.Next(); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("oversized: %v", err)
	}
	if _, _, err := NewReader(bytes.NewReader(append(hdr(TypeData, MaxPayload, 1), make([]byte, MaxPayload)...))).Next(); err != nil {
		t.Fatalf("max payload rejected: %v", err)
	}
	for _, c := range []struct {
		t   FrameType
		sid uint32
		ok  bool
	}{
		{TypeHello, 0, true}, {TypeHello, 1, false}, {TypePing, 3, false}, {TypePong, 0, true}, {TypeGoaway, 2, false},
		{TypeOpen, 0, false}, {TypeData, 0, false}, {TypeDgram, 0, false}, {TypeFin, 7, true}, {TypeRst, 0, false},
		{TypeOpenOK, 0, false}, {TypeOpenErr, 0, false},
		{TypeWindow, 0, true}, {TypeWindow, 5, true}, {0x0D, 1, false}, {0x00, 0, false}, {0x7E, 1, false},
	} {
		err := Header{Type: c.t, StreamID: c.sid}.Check(false)
		if (err == nil) != c.ok || (err != nil && !errors.Is(err, ErrProtocol)) {
			t.Errorf("%v stream %d: %v", c.t, c.sid, err)
		}
	}
	if (Header{Type: 0x80, StreamID: 9}).Check(false) == nil || (Header{Type: 0x80, StreamID: 9}).Check(true) != nil {
		t.Error("extension frames must require negotiation")
	}
	// PADDING 被跳过，flags 被忽略
	raw := append(hdr(TypePadding, 3, 0), 0, 0, 0)
	d := hdr(TypeData, 2, 5)
	d[1] = 0xff
	raw = append(append(raw, d...), 'h', 'i')
	h, p, err := NewReader(bytes.NewReader(raw)).Next()
	if err != nil || h.Type != TypeData || string(p) != "hi" {
		t.Fatalf("padding/flags: %v %v %q", err, h, p)
	}
	// 截断的载荷
	if _, _, err := NewReader(bytes.NewReader(append(hdr(TypeData, 10, 1), 1, 2))).Next(); err != io.ErrUnexpectedEOF {
		t.Fatalf("truncated: %v", err)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("AppendFrame accepted DATA on stream 0")
			}
		}()
		AppendFrame(nil, TypeData, 0, nil)
	}()
}

func TestReaderZeroAlloc(t *testing.T) {
	frame := AppendFrame(nil, TypeData, 1, make([]byte, 16384))
	src := bytes.NewReader(nil)
	r := NewReader(src)
	allocs := testing.AllocsPerRun(200, func() {
		src.Reset(frame)
		if _, _, err := r.Next(); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("Next allocates %.1f per frame", allocs)
	}
}

func TestTLVRules(t *testing.T) {
	good := (&Hello{Version: 1, NodeID: uuidN(1), BootID: uuidN(2), ExporterSig: []byte{9}}).AppendPayload(nil)
	if _, err := ParseHello(good); err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{
		"duplicate tag":   append(append([]byte{}, good...), 0x01, 0, 2, 0, 1),
		"truncated":       good[:len(good)-1],
		"missing node_id": appendTLV(appendTLV(nil, helloVersion, []byte{0, 1}), helloBootID, make([]byte, 16)),
		"short boot_id":   bytes.Replace(good, append([]byte{helloBootID, 0, 16}, b16(uuidN(2))...), append([]byte{helloBootID, 0, 15}, make([]byte, 15)...), 1),
		"bad link_mode":   append(append([]byte{}, good[:len(good)-4]...), helloLinkMode, 0, 1, 7),
		"served length":   append(append([]byte{}, good...), helloServedDigest, 0, 3, 1, 2, 3),
	}
	for name, p := range cases {
		if _, err := ParseHello(p); !errors.Is(err, ErrProtocol) {
			t.Errorf("%s: %v", name, err)
		}
	}
	// 未知 tag 忽略；link_mode 缺省为 mux
	withUnknown := appendTLV(append([]byte{}, good[:len(good)-4]...), 0x42, []byte("future"))
	h, err := ParseHello(withUnknown)
	if err != nil || h.LinkMode != ModeMux {
		t.Fatalf("unknown tag: %v %+v", err, h)
	}
}

func TestOpenConstraints(t *testing.T) {
	base := Open{RuleID: uuidN(3), Network: types.NetworkTCP, Client: netip.MustParseAddrPort("192.0.2.1:1")}
	five := base
	five.Visited = []types.UUID{uuidN(1), uuidN(2), uuidN(3), uuidN(4), uuidN(5)}
	badNet := base.AppendPayload(nil)
	i := bytes.Index(badNet, []byte{openNetwork, 0, 1, 1})
	badNet[i+3] = 3
	badAddr := bytes.Replace(base.AppendPayload(nil), []byte{openClientAddr, 0, 7, 4}, []byte{openClientAddr, 0, 7, 6}, 1)
	noAddr := base.AppendPayload(nil)
	noAddr = noAddr[:bytes.LastIndex(noAddr, []byte{openClientAddr, 0, 7})]
	for name, p := range map[string][]byte{"5 visited": five.AppendPayload(nil), "network 3": badNet,
		"family mismatch": badAddr, "no client_addr": noAddr, "duplicate": append(base.AppendPayload(nil), openHopIndex, 0, 1, 0)} {
		if _, err := ParseOpen(p); !errors.Is(err, ErrBadOpen) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// ---------------------------------------------------------------- PKI：根 CA → 中间 CA → 节点证书

type pki struct {
	roots  *x509.CertPool
	inter  *x509.Certificate
	ikey   *ecdsa.PrivateKey
	serial int64
}

func newPKI(t *testing.T) *pki {
	t.Helper()
	rk, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	rt := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "vortex root"}, IsCA: true,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true}
	rder, _ := x509.CreateCertificate(rand.Reader, rt, rt, &rk.PublicKey, rk)
	root, _ := x509.ParseCertificate(rder)
	ik, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	it := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "vortex node CA"}, IsCA: true,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true}
	ider, err := x509.CreateCertificate(rand.Reader, it, root, &ik.PublicKey, rk)
	if err != nil {
		t.Fatal(err)
	}
	inter, _ := x509.ParseCertificate(ider)
	p := &pki{roots: x509.NewCertPool(), inter: inter, ikey: ik, serial: 100}
	p.roots.AddCert(root)
	return p
}

// node 签发节点证书；uris 为 nil 时使用 spiffe://vortex/node/<id>。
func (p *pki) node(t *testing.T, id types.UUID, uris ...string) *Identity {
	t.Helper()
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if uris == nil {
		uris = []string{NodeURI(id)}
	}
	var us []*url.URL
	for _, s := range uris {
		u, _ := url.Parse(s)
		us = append(us, u)
	}
	p.serial++
	tpl := &x509.Certificate{SerialNumber: big.NewInt(p.serial), Subject: pkix.Name{CommonName: "node"}, URIs: us,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, tpl, p.inter, &k.PublicKey, p.ikey)
	if err != nil {
		t.Fatal(err)
	}
	return &Identity{NodeID: id, Cert: tls.Certificate{Certificate: [][]byte{der, p.inter.Raw}, PrivateKey: k}}
}

func local(id *Identity, tr *Trust, gen uint64) *Local {
	return &Local{Identity: id, Trust: tr, BootID: uuidN(0x30 + id.NodeID[0]), Generation: gen, Capabilities: CapPool | CapDgram}
}

type side struct {
	est *Established
	err error
}

// pair 在真实 TCP 回环上分别以发起方与接受方完成握手。
func pair(t *testing.T, cliL, srvL *Local, cliEx, srvEx Expect) (cli, srv side) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	done := make(chan side, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			done <- side{err: err}
			return
		}
		e, err := Handshake(context.Background(), tls.Server(c, ServerConfig(srvL.Identity, srvL.Trust)), srvL, srvEx)
		done <- side{e, err}
	}()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	e, err := Handshake(context.Background(), tls.Client(c, ClientConfig(cliL.Identity, cliL.Trust, cliEx.NodeID)), cliL, cliEx)
	cli = side{e, err}
	srv = <-done
	for _, s := range []side{cli, srv} {
		if s.est != nil {
			t.Cleanup(func() { s.est.TLS.Close() })
		}
	}
	return
}

func TestHandshakeAndFirstFrames(t *testing.T) {
	p := newPKI(t)
	tr := &Trust{Roots: p.roots, Revocations: &Revocations{}}
	a, b := p.node(t, uuidN(1)), p.node(t, uuidN(2))
	la, lb := local(a, tr, 3), local(b, tr, 7)
	lb.Capabilities = CapDgram
	lb.Mode = ModePool
	cli, srv := pair(t, la, lb, Expect{NodeID: b.NodeID}, Expect{})
	if cli.err != nil || srv.err != nil {
		t.Fatal(cli.err, srv.err)
	}
	if cli.est.Peer.NodeID != b.NodeID || srv.est.Peer.NodeID != a.NodeID {
		t.Fatal("peer ids")
	}
	if cli.est.Peer.Capabilities != CapDgram || srv.est.Peer.Capabilities != CapDgram {
		t.Fatalf("capability intersection %b %b", cli.est.Peer.Capabilities, srv.est.Peer.Capabilities)
	}
	if cli.est.Peer.Hello.Generation != 7 || cli.est.Peer.Hello.LinkMode != ModePool || cli.est.Peer.Version != 1 {
		t.Fatalf("peer hello %+v", cli.est.Peer.Hello)
	}
	if cs := cli.est.TLS.ConnectionState(); !allowedSuites[cs.CipherSuite] {
		t.Fatalf("suite %s", tls.CipherSuiteName(cs.CipherSuite))
	}
	// HELLO 之后的帧继续经同一个 Reader 读取
	go func() { _, _ = cli.est.TLS.Write(AppendFrame(nil, TypeData, 1, []byte("after hello"))) }()
	h, pl, err := srv.est.Reader.Next()
	if err != nil || h.Type != TypeData || string(pl) != "after hello" {
		t.Fatalf("frame after hello: %v %v %q", err, h, pl)
	}
}

func TestHandshakeRejections(t *testing.T) {
	p := newPKI(t)
	rev := &Revocations{}
	tr := &Trust{Roots: p.roots, Revocations: rev}
	a, b := p.node(t, uuidN(1)), p.node(t, uuidN(2))

	other := newPKI(t)
	foreign := other.node(t, uuidN(1))
	if c, s := pair(t, local(foreign, tr, 1), local(b, tr, 1), Expect{}, Expect{}); c.err == nil || s.err == nil {
		t.Error("certificate from a foreign CA accepted")
	}
	noURI := p.node(t, uuidN(4), "https://example.com/node")
	if c, s := pair(t, local(noURI, tr, 1), local(b, tr, 1), Expect{}, Expect{}); c.err == nil || !errors.Is(s.err, ErrNoNodeURI) {
		t.Errorf("certificate without node URI: %v / %v", c.err, s.err)
	}
	twoURI := p.node(t, uuidN(4), NodeURI(uuidN(4)), NodeURI(uuidN(5)))
	if _, s := pair(t, local(twoURI, tr, 1), local(b, tr, 1), Expect{}, Expect{}); !errors.Is(s.err, ErrNoNodeURI) {
		t.Errorf("certificate with two node URIs: %v", s.err)
	}
	if c, _ := pair(t, local(a, tr, 1), local(b, tr, 1), Expect{NodeID: uuidN(9)}, Expect{}); !errors.Is(c.err, ErrUnexpectedID) {
		t.Errorf("dialed the wrong node: %v", c.err)
	}

	leaf, _ := x509.ParseCertificate(a.Cert.Certificate[0])
	rev.Set(types.RevocationList{CertSerials: [][]byte{append([]byte{0}, leaf.SerialNumber.Bytes()...)}})
	if _, s := pair(t, local(a, tr, 1), local(b, tr, 1), Expect{}, Expect{}); !errors.Is(s.err, ErrRevoked) {
		t.Errorf("revoked serial accepted: %v", s.err)
	}
	rev.Set(types.RevocationList{NodeIDs: []types.UUID{b.NodeID}})
	if c, _ := pair(t, local(a, tr, 1), local(b, tr, 1), Expect{}, Expect{}); !errors.Is(c.err, ErrRevoked) {
		t.Errorf("revoked node accepted: %v", c.err)
	}
	rev.Set(types.RevocationList{})

	// 代次低于本地已知值：接受方拒绝并发送 GOAWAY(protocol_error)
	lb := local(b, tr, 1)
	lb.KnownGeneration = func(types.UUID) uint64 { return 5 }
	c, s := pair(t, local(a, tr, 4), lb, Expect{}, Expect{})
	var he *HandshakeError
	if !errors.As(s.err, &he) || !errors.Is(s.err, ErrStaleGeneration) || !he.Sent || he.Code != GoawayProtocolError {
		t.Errorf("stale generation: %v", s.err)
	}
	_ = c

	// 反向链路：摘要一致与不一致
	served := []ServedRule{{uuidN(7), 2}, {uuidN(8), 1}}
	c, s = pair(t, local(a, tr, 1), local(b, tr, 1), Expect{Reverse: true, SendServed: served}, Expect{Served: served})
	if c.err != nil || s.err != nil || s.est.Peer.ServedMismatch {
		t.Errorf("matching served digest: %v %v", c.err, s.err)
	}
	c, s = pair(t, local(a, tr, 1), local(b, tr, 1), Expect{Reverse: true, SendServed: served[:1]}, Expect{Served: served})
	if c.err != nil || s.err != nil || !s.est.Peer.ServedMismatch {
		t.Errorf("mismatching served digest must still connect and be flagged: %v %v", c.err, s.err)
	}
}

// rawPeer 以发起方身份完成 TLS（不经 Handshake），把连接交给 f，用于构造异常对端。
func rawPeer(t *testing.T, id *Identity, tr *Trust, srvL *Local, f func(*tls.Conn)) error {
	t.Helper()
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	res := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			res <- err
			return
		}
		e, err := Handshake(context.Background(), tls.Server(c, ServerConfig(srvL.Identity, srvL.Trust)), srvL, Expect{})
		if e != nil {
			e.TLS.Close()
		}
		res <- err
	}()
	nc, _ := net.Dial("tcp", ln.Addr().String())
	tc := tls.Client(nc, ClientConfig(id, tr, types.UUID{}))
	defer tc.Close()
	if err := tc.Handshake(); err != nil {
		t.Fatal(err)
	}
	f(tc)
	return <-res
}

// goawayFrom 读出对端发来的 HELLO 之后的 GOAWAY。
func goawayFrom(tc *tls.Conn) (GoawayCode, bool) {
	_ = tc.SetReadDeadline(time.Now().Add(3 * time.Second))
	r := NewReader(tc)
	for {
		h, p, err := r.Next()
		if err != nil {
			return 0, false
		}
		if h.Type == TypeGoaway {
			_, c, err := ParseGoawayPayload(p)
			return c, err == nil
		}
	}
}

func signedHello(t *testing.T, tc *tls.Conn, id *Identity, mut func(*Hello)) []byte {
	h := &Hello{Version: 1, NodeID: id.NodeID, BootID: uuidN(0x55), Generation: 1}
	if mut != nil {
		mut(h)
	}
	cs := tc.ConnectionState()
	s, _ := id.Signer()
	if err := h.Sign(&cs, s); err != nil {
		t.Fatal(err)
	}
	return AppendFrame(nil, TypeHello, 0, h.AppendPayload(nil))
}

func TestHandshakeProtocolViolations(t *testing.T) {
	p := newPKI(t)
	tr := &Trust{Roots: p.roots}
	a, b := p.node(t, uuidN(1)), p.node(t, uuidN(2))
	lb := local(b, tr, 1)

	expectGoaway := func(name string, want GoawayCode, wantErr error, f func(*tls.Conn)) {
		var code GoawayCode
		var ok bool
		err := rawPeer(t, a, tr, lb, func(tc *tls.Conn) {
			f(tc)
			code, ok = goawayFrom(tc)
		})
		if err == nil || (wantErr != nil && !errors.Is(err, wantErr)) {
			t.Errorf("%s: handshake error %v", name, err)
		}
		if !ok || code != want {
			t.Errorf("%s: GOAWAY %v %d, want %d", name, ok, code, want)
		}
	}
	expectGoaway("first frame not HELLO", GoawayProtocolError, ErrProtocol, func(tc *tls.Conn) {
		_, _ = tc.Write(AppendFrame(nil, TypeData, 1, []byte("x")))
	})
	expectGoaway("valid HELLO payload in a non-HELLO frame", GoawayProtocolError, ErrProtocol, func(tc *tls.Conn) {
		f := signedHello(t, tc, a, nil)
		f[0] = byte(TypePong)
		_, _ = tc.Write(f)
	})
	expectGoaway("node_id differs from certificate", GoawayProtocolError, ErrProtocol, func(tc *tls.Conn) {
		_, _ = tc.Write(signedHello(t, tc, a, func(h *Hello) { h.NodeID = uuidN(3) }))
	})
	expectGoaway("version 0", GoawayVersionUnsupported, ErrVersion, func(tc *tls.Conn) {
		_, _ = tc.Write(signedHello(t, tc, a, func(h *Hello) { h.Version = 0 }))
	})
	expectGoaway("tampered generation", GoawayProtocolError, nil, func(tc *tls.Conn) {
		f := signedHello(t, tc, a, nil)
		raw := bytes.Replace(f, []byte{helloGeneration, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1}, []byte{helloGeneration, 0, 8, 0, 0, 0, 0, 0, 0, 0, 9}, 1)
		_, _ = tc.Write(raw)
	})

	// 重放：把另一条 TLS 连接上签名的 HELLO 原样发送
	var replay []byte
	_ = rawPeer(t, a, tr, lb, func(tc *tls.Conn) {
		replay = signedHello(t, tc, a, nil)
		_, _ = tc.Write(replay)
	})
	expectGoaway("replayed HELLO", GoawayProtocolError, nil, func(tc *tls.Conn) { _, _ = tc.Write(replay) })

	// 版本更高的对端：取 min，成功
	if err := rawPeer(t, a, tr, lb, func(tc *tls.Conn) {
		_, _ = tc.Write(signedHello(t, tc, a, func(h *Hello) { h.Version = 2 }))
	}); err != nil {
		t.Errorf("newer peer version must negotiate down: %v", err)
	}

	// 超时：对端不发送 HELLO
	lt := local(b, tr, 1)
	lt.Timeout = 300 * time.Millisecond
	var waited time.Duration
	err := rawPeer(t, a, tr, lt, func(tc *tls.Conn) {
		start := time.Now()
		goawayFrom(tc) // 读到 HELLO 之后一直等到接受方关闭（最多 3 秒）
		waited = time.Since(start)
	})
	if err == nil || waited > 1500*time.Millisecond {
		t.Errorf("hello timeout: %v, peer closed after %v", err, waited)
	}
}

func TestALPNRequired(t *testing.T) {
	p := newPKI(t)
	tr := &Trust{Roots: p.roots}
	a, b := p.node(t, uuidN(1)), p.node(t, uuidN(2))
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	res := make(chan error, 1)
	go func() {
		c, _ := ln.Accept()
		_, err := Handshake(context.Background(), tls.Server(c, ServerConfig(b, tr)), local(b, tr, 1), Expect{})
		res <- err
	}()
	cfg := ClientConfig(a, tr, types.UUID{})
	cfg.NextProtos = nil
	cfg.VerifyConnection = nil
	nc, _ := net.Dial("tcp", ln.Addr().String())
	tc := tls.Client(nc, cfg)
	_ = tc.Handshake()
	defer tc.Close()
	if err := <-res; !errors.Is(err, ErrALPN) {
		t.Fatalf("peer without ALPN: %v", err)
	}
}

func TestVerifyPeerRejectsChaCha(t *testing.T) {
	cs := &tls.ConnectionState{Version: tls.VersionTLS13, CipherSuite: tls.TLS_CHACHA20_POLY1305_SHA256, NegotiatedProtocol: ALPN}
	if _, err := (&Trust{}).VerifyPeer(cs, types.UUID{}); !errors.Is(err, ErrCipherSuite) {
		t.Fatal(err)
	}
}

// 与参考实现互连（ENG-T21 验收）：参考实现作为发起方，用它自己的签名与解码；生产实现作为接受方。
func TestInteropWithReference(t *testing.T) {
	p := newPKI(t)
	tr := &Trust{Roots: p.roots}
	a, b := p.node(t, uuidN(1)), p.node(t, uuidN(2))
	var peerHello *ref.Hello
	err := rawPeer(t, a, tr, local(b, tr, 4), func(tc *tls.Conn) {
		h := &ref.Hello{Version: ref.ProtocolVersion, NodeID: ref.UUID(a.NodeID), BootID: ref.UUID(uuidN(0x44)), Generation: 2,
			Capabilities: ref.CapPool | ref.CapDgram}
		if err := h.Sign(tc.ConnectionState(), a.Cert.PrivateKey.(*ecdsa.PrivateKey)); err != nil {
			t.Fatal(err)
		}
		f, _ := ref.Frame{Type: ref.TypeHello, Payload: h.MarshalPayload()}.MarshalBinary()
		if _, err := tc.Write(f); err != nil {
			t.Fatal(err)
		}
		rf, err := ref.ReadFrame(tc, false)
		if err != nil || rf.Type != ref.TypeHello {
			t.Fatalf("reference read: %v %v", err, rf.Type)
		}
		if peerHello, err = ref.UnmarshalHello(rf.Payload); err != nil {
			t.Fatal(err)
		}
		if err := peerHello.VerifyPeer(tc.ConnectionState(), 4); err != nil {
			t.Fatalf("reference rejected production HELLO: %v", err)
		}
	})
	if err != nil {
		t.Fatalf("production rejected reference HELLO: %v", err)
	}
	if peerHello.NodeID != ref.UUID(b.NodeID) || peerHello.Generation != 4 {
		t.Fatalf("reference saw %+v", peerHello)
	}
}

func TestUUIDText(t *testing.T) {
	u := uuidN(0xab)
	s := FormatUUID(u)
	if s != ref.FormatUUID(ref.UUID(u)) {
		t.Fatal("format differs from reference")
	}
	back, err := ParseUUID(s)
	if err != nil || back != u {
		t.Fatal(err)
	}
	for _, bad := range []string{"", "abababab-abab-7bab-abab-abababababa", "zbababab-abab-7bab-abab-abababababab", "abababababab-7bab-abab-abababababab0"} {
		if _, err := ParseUUID(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
	if !reflect.DeepEqual(uuidN(1), uuidN(1)) {
		t.Fatal()
	}
}
