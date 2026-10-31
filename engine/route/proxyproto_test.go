package route

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"vortex/engine/types"
)

// parseV2 是按 HAProxy PROXY protocol v2 规范独立编写的解析器，用于验证生成的头部。
func parseV2(b []byte) (src, dst netip.AddrPort, dgram bool, rest []byte, err error) {
	if len(b) < 16 || !bytes.Equal(b[:12], proxyV2Sig[:]) {
		return src, dst, false, nil, fmt.Errorf("bad signature")
	}
	if b[12] != 0x21 {
		return src, dst, false, nil, fmt.Errorf("ver/cmd %#x", b[12])
	}
	fam, proto := b[13]>>4, b[13]&0x0f
	n := int(binary.BigEndian.Uint16(b[14:16]))
	body := b[16 : 16+n]
	switch fam {
	case 1:
		src = netip.AddrPortFrom(netip.AddrFrom4([4]byte(body[0:4])), binary.BigEndian.Uint16(body[8:10]))
		dst = netip.AddrPortFrom(netip.AddrFrom4([4]byte(body[4:8])), binary.BigEndian.Uint16(body[10:12]))
	case 2:
		src = netip.AddrPortFrom(netip.AddrFrom16([16]byte(body[0:16])), binary.BigEndian.Uint16(body[32:34]))
		dst = netip.AddrPortFrom(netip.AddrFrom16([16]byte(body[16:32])), binary.BigEndian.Uint16(body[34:36]))
	default:
		return src, dst, false, nil, fmt.Errorf("family %d", fam)
	}
	return src, dst, proto == 2, b[16+n:], nil
}

func TestProxyHeaderFormats(t *testing.T) {
	src, dst := netip.MustParseAddrPort("198.51.100.7:51234"), netip.MustParseAddrPort("203.0.113.1:30001")
	v1, _ := ProxyHeader(types.ProxyV1, false, src, dst)
	if string(v1) != "PROXY TCP4 198.51.100.7 203.0.113.1 51234 30001\r\n" {
		t.Fatalf("v1 %q", v1)
	}
	v16, _ := ProxyHeader(types.ProxyV1, false, netip.MustParseAddrPort("[2001:db8::7]:443"), netip.MustParseAddrPort("[2001:db8::1]:80"))
	if string(v16) != "PROXY TCP6 2001:db8::7 2001:db8::1 443 80\r\n" {
		t.Fatalf("v1 ipv6 %q", v16)
	}
	if _, err := ProxyHeader(types.ProxyV1, true, src, dst); err != ErrProxyV1UDP {
		t.Fatal("v1 over UDP must be rejected (RUL-001)")
	}
	for _, udp := range []bool{false, true} {
		h, _ := ProxyHeader(types.ProxyV2, udp, src, dst)
		gs, gd, dg, rest, err := parseV2(h)
		if err != nil || gs != src || gd != dst || dg != udp || len(rest) != 0 || len(h) != 28 {
			t.Fatalf("v2 udp=%v: %v %v %v %v len=%d", udp, gs, gd, dg, err, len(h))
		}
	}
	// IPv4 映射地址按 IPv4 编码；地址族不一致时两端都用 IPv6
	h, _ := ProxyHeader(types.ProxyV2, false, netip.MustParseAddrPort("[::ffff:198.51.100.7]:1"), dst)
	if gs, _, _, _, _ := parseV2(h); !gs.Addr().Is4() {
		t.Fatal("mapped IPv4 should be encoded as IPv4")
	}
	h, _ = ProxyHeader(types.ProxyV2, false, src, netip.MustParseAddrPort("[2001:db8::1]:80"))
	if gs, gd, _, _, err := parseV2(h); err != nil || gs.Port() != 51234 || !gd.Addr().Is6() {
		t.Fatalf("mixed family: %v %v %v", gs, gd, err)
	}
	if h, _ := ProxyHeader(types.ProxyOff, false, src, dst); h != nil {
		t.Fatal("off must produce no header")
	}
}

// headerTarget 记录每条连接收到的完整字节（头部 + 数据）。
func headerTarget(t *testing.T) (types.Target, chan []byte) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	got := make(chan []byte, 4)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				b, _ := io.ReadAll(c)
				got <- b
			}()
		}
	}()
	ap := netip.MustParseAddrPort(ln.Addr().String())
	return types.Target{Host: ap.Addr().String(), Port: ap.Port(), Weight: 1}, got
}

func TestProxyProtocolTCPEndToEnd(t *testing.T) {
	for _, v := range []types.ProxyProtocol{types.ProxyV1, types.ProxyV2} {
		tgt, got := headerTarget(t)
		adm := &testAdmission{hook: &countingHook{}}
		f := newEntry(t, []types.Target{tgt}, types.ScheduleWeightedRoundRobin, adm)
		f.tables.Swap(NewTable(2, []types.Rule{{RuleID: f.ruleID, RuleVersion: 2, Targets: []types.Target{tgt}, ProxyProtocol: v}}))
		c, err := net.Dial("tcp", f.addr)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = c.Write([]byte("payload"))
		_ = c.(*net.TCPConn).CloseWrite()
		_, _ = io.ReadAll(c)
		c.Close()
		var b []byte
		select {
		case b = <-got:
		case <-time.After(2 * time.Second):
			t.Fatal("target got nothing")
		}
		client := netip.MustParseAddrPort(c.LocalAddr().String())
		entry := netip.MustParseAddrPort(f.addr)
		var body []byte
		if v == types.ProxyV1 {
			line, err := bufio.NewReader(bytes.NewReader(b)).ReadString('\n')
			want := fmt.Sprintf("PROXY TCP4 %s %s %d %d\r\n", client.Addr(), entry.Addr(), client.Port(), entry.Port())
			if err != nil || line != want {
				t.Fatalf("v1 header %q, want %q", line, want)
			}
			body = b[len(line):]
		} else {
			src, dst, dg, rest, err := parseV2(b)
			if err != nil || src != client || dst != entry || dg {
				t.Fatalf("v2 header: %v %v %v %v", src, dst, dg, err)
			}
			body = rest
		}
		if string(body) != "payload" {
			t.Fatalf("v%d body %q", v-1, body)
		}
		// 头部由平台生成，不计入业务载荷（ENG-035）
		deadline := time.Now().Add(time.Second)
		for adm.hook.up.Load() == 0 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if adm.hook.up.Load() != int64(len("payload")) {
			t.Fatalf("metered %d bytes, want %d (header must not be metered)", adm.hook.up.Load(), len("payload"))
		}
	}
}

func TestProxyProtocolUDPPerDatagram(t *testing.T) {
	echo := newUDPEcho(t)
	h := &packetHook{limit: 1 << 16}
	f := newUDPEntry(t, types.Rule{Targets: []types.Target{echo.tgt(1, 0)}, ProxyProtocol: types.ProxyV2}, &hookAdmission{hook: h})
	c := udpClient(t, f.addr)
	client := netip.MustParseAddrPort(c.LocalAddr().String())
	for _, msg := range []string{"one", "two", "three"} {
		_ = c.SetDeadline(time.Now().Add(time.Second))
		_, _ = c.Write([]byte(msg))
		buf := make([]byte, 256)
		n, err := c.Read(buf) // 回显目标把收到的整个数据报（头部 + 数据）发回
		if err != nil {
			t.Fatal(err)
		}
		src, dst, dg, rest, err := parseV2(buf[:n])
		if err != nil || src != client || dst.String() != f.addr || !dg || string(rest) != msg {
			t.Fatalf("datagram %q: %v %v dgram=%v rest=%q %v", msg, src, dst, dg, rest, err)
		}
	}
	if want := int64(len("one") + len("two") + len("three")); h.up.Load() != want {
		t.Fatalf("metered up %d, want %d (header excluded)", h.up.Load(), want)
	}
	if !strings.HasPrefix(f.addr, "127.0.0.1:") {
		t.Fatal("unexpected listener address")
	}
}
