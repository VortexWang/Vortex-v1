package ref

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"math/big"
	"net"
	"net/netip"
	"net/url"
	"os"
	"testing"
	"time"
)

var update = flag.Bool("update", false, "rewrite testdata/vectors.json")

func uuidN(n byte) UUID {
	var u UUID
	for i := range u {
		u[i] = n
	}
	u[6] = 0x70 | (n & 0x0f) // UUIDv7 版本位
	return u
}

// ---------- 帧规则 ----------

func TestFrameRules(t *testing.T) {
	cases := []struct {
		name string
		f    Frame
		want error
	}{
		{"data ok", Frame{Type: TypeData, StreamID: 1, Payload: []byte("x")}, nil},
		{"max payload ok", Frame{Type: TypeData, StreamID: 1, Payload: make([]byte, MaxPayload)}, nil},
		{"payload too large", Frame{Type: TypeData, StreamID: 1, Payload: make([]byte, MaxPayload+1)}, ErrPayloadTooLarge},
		{"hello on stream", Frame{Type: TypeHello, StreamID: 3}, ErrStreamID},
		{"data on link", Frame{Type: TypeData, StreamID: 0}, ErrStreamID},
		{"window link ok", Frame{Type: TypeWindow, StreamID: 0, Payload: WindowPayload(1)}, nil},
		{"window stream ok", Frame{Type: TypeWindow, StreamID: 5, Payload: WindowPayload(1)}, nil},
		{"undefined 0x0d", Frame{Type: 0x0D, StreamID: 1}, ErrUnknownType},
	}
	for _, c := range cases {
		if err := c.f.Validate(false); !errors.Is(err, c.want) {
			t.Errorf("%s: got %v want %v", c.name, err, c.want)
		}
	}
	// 扩展帧：未协商拒绝，协商后接受
	ext := Frame{Type: 0x80, StreamID: 0}
	if ext.Validate(false) == nil || ext.Validate(true) != nil {
		t.Error("extension frame negotiation rule broken")
	}
	// 读取端：长度字段超限直接拒绝，不分配缓冲
	hdr := []byte{byte(TypeData), 0, 0x40, 0x01, 0, 0, 0, 1} // length = 16385
	if _, err := ReadFrame(bytes.NewReader(hdr), false); !errors.Is(err, ErrPayloadTooLarge) {
		t.Errorf("oversized length not rejected: %v", err)
	}
}

func TestTLVDuplicateRejected(t *testing.T) {
	b := EncodeTLV([]TLV{{1, []byte{1}}, {1, []byte{2}}})
	if _, err := DecodeTLV(b); !errors.Is(err, ErrTLVDuplicate) {
		t.Fatalf("duplicate tag accepted: %v", err)
	}
	if _, err := DecodeTLV([]byte{1, 0, 5, 1}); !errors.Is(err, ErrTLVTruncated) {
		t.Fatalf("truncated accepted: %v", err)
	}
}

// ---------- 评审发现 R1：HELLO 列出全部规则会超出单帧上限 ----------

func TestHelloServedRulesCapacity(t *testing.T) {
	h := Hello{Version: 1, NodeID: uuidN(1), BootID: uuidN(2), Generation: 1, ExporterSig: make([]byte, 72), LinkMode: 0}
	base := len(h.MarshalPayload())
	perRule := 16 + 8                               // 草案：rule_id + rule_version
	maxRules := (0xFFFF - 0) / perRule              // TLV len 字段上限
	maxByFrame := (MaxPayload - base - 3) / perRule // 单帧上限
	if maxByFrame < maxRules {
		maxRules = maxByFrame
	}
	t.Logf("草案方式单个 HELLO 最多可列 %d 条规则；需求 13.1 节规模基线为 1,000 条", maxRules)
	if maxRules >= 1000 {
		t.Fatal("expected draft design to be unable to carry 1,000 rules")
	}
	// 摘要方式：固定 36 字节，与规则数无关
	rules := make([]ServedRule, 5000)
	for i := range rules {
		var id UUID
		id[0], id[1], id[6] = byte(i>>8), byte(i), 0x70 // 唯一 ID
		rules[i] = ServedRule{RuleID: id, Version: uint64(i)}
	}
	h.HasServed = true
	h.ServedCount, h.ServedDigest = ServedRulesDigest(rules)
	if got := len(h.MarshalPayload()); got != base+3+36 {
		t.Fatalf("digest HELLO size %d", got)
	}
	// 摘要与顺序无关
	rev := append([]ServedRule(nil), rules...)
	for i, j := 0, len(rev)-1; i < j; i, j = i+1, j-1 {
		rev[i], rev[j] = rev[j], rev[i]
	}
	if _, d2 := ServedRulesDigest(rev); d2 != h.ServedDigest {
		t.Fatal("digest depends on order")
	}
}

// ---------- OPEN ----------

func TestOpenRoundTrip(t *testing.T) {
	for _, ap := range []string{"198.51.100.7:51234", "[2001:db8::7]:443"} {
		o := &Open{RuleID: uuidN(3), RuleVersion: 7, PathGeneration: 2, HopIndex: 1, Network: NetworkUDP,
			Visited: []UUID{uuidN(1)}, Client: netip.MustParseAddrPort(ap)}
		back, err := UnmarshalOpen(o.MarshalPayload())
		if err != nil {
			t.Fatal(err)
		}
		if back.Client != o.Client || back.HopIndex != 1 || back.Visited[0] != uuidN(1) || back.Network != NetworkUDP {
			t.Fatalf("mismatch %+v", back)
		}
	}
	bad := &Open{RuleID: uuidN(3), Network: NetworkTCP, Visited: []UUID{uuidN(1), uuidN(2), uuidN(3), uuidN(4), uuidN(5)},
		Client: netip.MustParseAddrPort("1.2.3.4:5")}
	if _, err := UnmarshalOpen(bad.MarshalPayload()); err == nil {
		t.Fatal("visited list longer than path accepted")
	}
}

// ---------- TLS 1.3 + HELLO exporter 绑定 ----------

type nodeID struct {
	uuid UUID
	key  *ecdsa.PrivateKey
	cert tls.Certificate
}

func newPKI(t *testing.T, ids ...UUID) (*x509.CertPool, []nodeID) {
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "vortex test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true,
		KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true}
	caDER, err := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(caDER)
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	var out []nodeID
	for i, id := range ids {
		k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		tpl := &x509.Certificate{SerialNumber: big.NewInt(int64(10 + i)), Subject: pkix.Name{CommonName: "node"},
			NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
			URIs:        []*url.URL{NodeURI(FormatUUID(id))},
			KeyUsage:    x509.KeyUsageDigitalSignature,
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}}
		der, err := x509.CreateCertificate(rand.Reader, tpl, ca, &k.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, nodeID{uuid: id, key: k, cert: tls.Certificate{Certificate: [][]byte{der}, PrivateKey: k}})
	}
	return pool, out
}

func tlsPair(t *testing.T, pool *x509.CertPool, a, b nodeID) (*tls.Conn, *tls.Conn) {
	cfg := func(n nodeID) *tls.Config {
		return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{n.cert}, RootCAs: pool,
			ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert, NextProtos: []string{ALPN},
			ServerName: "vortex-node", InsecureSkipVerify: true, // 主机名不用于 VLink 身份；改由下方回调校验 CA 链
			VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
				c, err := x509.ParseCertificate(raw[0])
				if err != nil {
					return err
				}
				_, err = c.Verify(x509.VerifyOptions{Roots: pool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}})
				return err
			}}
	}
	c1, c2 := net.Pipe()
	cli, srv := tls.Client(c1, cfg(a)), tls.Server(c2, cfg(b))
	errc := make(chan error, 1)
	go func() { errc <- srv.Handshake() }()
	if err := cli.Handshake(); err != nil {
		t.Fatal(err)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	if cli.ConnectionState().NegotiatedProtocol != ALPN || cli.ConnectionState().Version != tls.VersionTLS13 {
		t.Fatal("ALPN / TLS 1.3 not negotiated")
	}
	return cli, srv
}

func exchangeHello(t *testing.T, c *tls.Conn, me nodeID, gen uint64) chan *Hello {
	h := &Hello{Version: 1, NodeID: me.uuid, BootID: uuidN(0x20 + me.uuid[0]), Generation: gen, Capabilities: CapPool | CapDgram}
	if err := h.Sign(c.ConnectionState(), me.key); err != nil {
		t.Fatal(err)
	}
	b, _ := Frame{Type: TypeHello, Payload: h.MarshalPayload()}.MarshalBinary()
	got := make(chan *Hello, 1)
	go func() {
		f, err := ReadFrame(c, false)
		if err != nil || f.Type != TypeHello {
			got <- nil
			return
		}
		p, _ := UnmarshalHello(f.Payload)
		got <- p
	}()
	// net.Pipe 无缓冲：写入放到独立 goroutine，双方同时发送 HELLO 互不等待（评审决定 R6）
	go func() { _, _ = c.Write(b) }()
	return got
}

func TestHelloBindingOverTLS13(t *testing.T) {
	a, b, c := uuidN(1), uuidN(2), uuidN(3)
	pool, n := newPKI(t, a, b, c)

	// 正常：双方同时发送 HELLO，互相校验通过（评审决定 R6：双方不等待对方）
	cli, srv := tlsPair(t, pool, n[0], n[1])
	recvOnCli := exchangeHello(t, cli, n[0], 5) // 客户端（A）收到的是 B 的 HELLO
	recvOnSrv := exchangeHello(t, srv, n[1], 5) // 服务端（B）收到的是 A 的 HELLO
	hB, hA := <-recvOnCli, <-recvOnSrv
	if hA == nil || hB == nil {
		t.Fatal("HELLO not received")
	}
	if err := hB.VerifyPeer(cli.ConnectionState(), 5); err != nil {
		t.Fatalf("client verify: %v", err)
	}
	if err := hA.VerifyPeer(srv.ConnectionState(), 5); err != nil {
		t.Fatalf("server verify: %v", err)
	}

	// 代次过旧被拒
	if err := hA.VerifyPeer(srv.ConnectionState(), 6); err == nil {
		t.Fatal("stale generation accepted")
	}

	// 重放：把连接 1 上 A 的 HELLO 拿到 A 与 C 的另一条连接上使用，exporter 不同，签名失效
	cli2, srv2 := tlsPair(t, pool, n[0], n[2])
	if err := hA.VerifyPeer(srv2.ConnectionState(), 5); err == nil {
		t.Fatal("replayed HELLO accepted on a different TLS connection")
	}

	// 冒名：C 的连接上声称自己是 A
	forged := &Hello{Version: 1, NodeID: a, BootID: uuidN(9), Generation: 5}
	_ = forged.Sign(cli2.ConnectionState(), n[2].key)
	_ = srv2
	if err := forged.VerifyPeer(cli2.ConnectionState(), 5); err == nil {
		t.Fatal("node_id not matching certificate accepted")
	}
	// 测试用 net.Pipe 无人读取 close_notify，直接关闭底层连接避免 tls.Conn.Close 等待写超时
	for _, c := range []*tls.Conn{cli, srv, cli2, srv2} {
		_ = c.NetConn().Close()
	}
}

// ---------- 互操作测试向量 ----------

type vector struct {
	Name string `json:"name"`
	Note string `json:"note"`
	Hex  string `json:"hex"`
}

func buildVectors(t *testing.T) []vector {
	mk := func(name, note string, f Frame) vector {
		b, err := f.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		// 解码回来必须一致
		back, err := ReadFrame(bytes.NewReader(b), false)
		if err != nil || back.Type != f.Type || back.StreamID != f.StreamID || !bytes.Equal(back.Payload, f.Payload) {
			t.Fatalf("%s: round trip failed: %v", name, err)
		}
		return vector{name, note, hex.EncodeToString(b)}
	}
	sig := bytes.Repeat([]byte{0xAB}, 8) // 占位：真实签名随连接变化，向量只验证编码结构
	hello := &Hello{Version: 1, NodeID: uuidN(1), BootID: uuidN(2), Generation: 5, Capabilities: CapPool | CapDgram, ExporterSig: sig}
	helloRev := *hello
	helloRev.HasServed = true
	helloRev.ServedCount, helloRev.ServedDigest = ServedRulesDigest([]ServedRule{{uuidN(3), 7}, {uuidN(4), 1}})
	open := &Open{RuleID: uuidN(3), RuleVersion: 7, PathGeneration: 2, HopIndex: 1, Network: NetworkTCP,
		Visited: []UUID{uuidN(1)}, Client: netip.MustParseAddrPort("198.51.100.7:51234")}
	open6 := *open
	open6.Network, open6.Client = NetworkUDP, netip.MustParseAddrPort("[2001:db8::7]:443")
	return []vector{
		mk("hello_forward", "正向建链 HELLO；exporter_sig 为 8 字节占位", Frame{Type: TypeHello, Payload: hello.MarshalPayload()}),
		mk("hello_reverse", "反向建链 HELLO，含 served 摘要（2 条规则）", Frame{Type: TypeHello, Payload: helloRev.MarshalPayload()}),
		mk("open_tcp_v4", "OPEN：中继跳位 1，TCP，IPv4 客户端", Frame{Type: TypeOpen, StreamID: 1, Payload: open.MarshalPayload()}),
		mk("open_udp_v6", "OPEN：UDP 会话，IPv6 客户端（ENG-047）", Frame{Type: TypeOpen, StreamID: 3, Payload: open6.MarshalPayload()}),
		mk("open_ok", "OPEN_OK", Frame{Type: TypeOpenOK, StreamID: 1}),
		mk("open_err_quota", "OPEN_ERR quota_exhausted", Frame{Type: TypeOpenErr, StreamID: 1, Payload: ErrPayload(CodeQuotaExhausted)}),
		mk("data", "DATA 'hello'", Frame{Type: TypeData, StreamID: 1, Payload: []byte("hello")}),
		mk("fin", "FIN 半关闭", Frame{Type: TypeFin, StreamID: 1}),
		mk("rst_unauthorized", "RST unauthorized", Frame{Type: TypeRst, StreamID: 1, Payload: ErrPayload(CodeUnauthorized)}),
		mk("dgram", "DGRAM 4 字节", Frame{Type: TypeDgram, StreamID: 3, Payload: []byte{0xde, 0xad, 0xbe, 0xef}}),
		mk("window_link", "链路级 WINDOW +262144", Frame{Type: TypeWindow, StreamID: 0, Payload: WindowPayload(262144)}),
		mk("window_stream", "流级 WINDOW +65536", Frame{Type: TypeWindow, StreamID: 1, Payload: WindowPayload(65536)}),
		mk("ping", "PING 8 字节", Frame{Type: TypePing, Payload: []byte{1, 2, 3, 4, 5, 6, 7, 8}}),
		mk("pong", "PONG 回显", Frame{Type: TypePong, Payload: []byte{1, 2, 3, 4, 5, 6, 7, 8}}),
		mk("goaway_draining", "GOAWAY last_stream_id=41 draining", Frame{Type: TypeGoaway, Payload: GoawayPayload(41, GoawayDraining)}),
	}
}

func TestVectors(t *testing.T) {
	vs := buildVectors(t)
	path := "testdata/vectors.json"
	if *update {
		_ = os.MkdirAll("testdata", 0o755)
		b, _ := json.MarshalIndent(vs, "", "  ")
		if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var want []vector
	if err := json.Unmarshal(b, &want); err != nil {
		t.Fatal(err)
	}
	if len(want) != len(vs) {
		t.Fatalf("vector count %d != %d", len(vs), len(want))
	}
	for i := range vs {
		if vs[i] != want[i] {
			t.Errorf("%s changed:\n got %s\nwant %s", vs[i].Name, vs[i].Hex, want[i].Hex)
		}
	}
}
