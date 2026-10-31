package vlink

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"vortex/engine/pipe"
	"vortex/engine/types"
)

func established(t *testing.T) (cli, srv *Established) {
	t.Helper()
	p := newPKI(t)
	tr := &Trust{Roots: p.roots}
	a, b := p.node(t, uuidN(1)), p.node(t, uuidN(2))
	c, s := pair(t, local(a, tr, 1), local(b, tr, 1), Expect{NodeID: b.NodeID}, Expect{})
	if c.err != nil || s.err != nil {
		t.Fatal(c.err, s.err)
	}
	return c.est, s.est
}

func links(t *testing.T, opt LinkOptions) (cli, srv *Link) {
	t.Helper()
	c, s := established(t)
	co, so := opt, opt
	co.Initiator = true
	cli, srv = NewLink(c, co), NewLink(s, so)
	t.Cleanup(func() { cli.Close(); srv.Close() })
	return
}

func testOpen(rule byte) *Open {
	return &Open{RuleID: uuidN(rule), RuleVersion: 1, Network: types.NetworkTCP, Client: netip.MustParseAddrPort("192.0.2.9:4000")}
}

// echoServer 接受全部入站流并回显，直到链路关闭。
func echoServer(t *testing.T, l *Link) {
	go func() {
		for {
			s, err := l.Accept(context.Background())
			if err != nil {
				return
			}
			s.Accept()
			go func() {
				defer s.Close()
				if _, err := io.Copy(s, s); err == nil {
					_ = s.CloseWrite()
				}
			}()
		}
	}()
}

func TestEchoLargeTransfer(t *testing.T) {
	cli, srv := links(t, LinkOptions{})
	echoServer(t, srv)
	s, err := cli.Open(testOpen(1))
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 24<<20) // 超过链路窗口，确认额度被归还
	_, _ = rand.Read(data)
	go func() {
		_, _ = s.Write(data)
		_ = s.CloseWrite()
	}()
	got, err := io.ReadAll(s)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(got) != sha256.Sum256(data) {
		t.Fatalf("echo mismatch: %d bytes", len(got))
	}
	_ = s.Close()
	if s.ID()%2 != 1 {
		t.Fatalf("initiator stream id %d not odd", s.ID())
	}
	if s.OpenRTT() == 0 {
		t.Error("OPEN_OK not recorded")
	}
	waitStreams(t, cli, 0)
	waitStreams(t, srv, 0)
}

func waitStreams(t *testing.T, l *Link, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for l.NumStreams() != n && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := l.NumStreams(); got != n {
		t.Fatalf("link still has %d streams, want %d", got, n)
	}
}

func TestManyStreamsBothDirections(t *testing.T) {
	cli, srv := links(t, LinkOptions{})
	echoServer(t, srv)
	echoServer(t, cli)
	var wg sync.WaitGroup
	errs := make(chan error, 200)
	for i := 0; i < 100; i++ {
		for _, l := range []*Link{cli, srv} {
			wg.Add(1)
			go func(l *Link, i int) {
				defer wg.Done()
				s, err := l.Open(testOpen(byte(i)))
				if err != nil {
					errs <- err
					return
				}
				msg := bytes.Repeat([]byte{byte(i)}, 1000+i*517)
				go func() { _, _ = s.Write(msg); _ = s.CloseWrite() }()
				got, err := io.ReadAll(s)
				_ = s.Close()
				if err != nil || !bytes.Equal(got, msg) {
					errs <- errors.New("stream content mismatch")
				}
				if (s.ID()%2 == 1) != (l == cli) {
					errs <- errors.New("stream id parity")
				}
			}(l, i)
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	waitStreams(t, cli, 0)
	waitStreams(t, srv, 0)
}

func TestZeroRTTDataBufferedUntilAccept(t *testing.T) {
	cli, srv := links(t, LinkOptions{})
	s, _ := cli.Open(testOpen(1))
	if _, err := s.Write([]byte("early data")); err != nil { // 未收到 OPEN_OK 就发送
		t.Fatal(err)
	}
	in, err := srv.Accept(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if in.OpenInfo().RuleID != uuidN(1) || in.OpenInfo().Client.Port() != 4000 {
		t.Fatalf("open info %+v", in.OpenInfo())
	}
	time.Sleep(50 * time.Millisecond) // 开流检查期间数据已到达并缓存
	in.Accept()
	buf := make([]byte, 64)
	n, err := in.Read(buf)
	if err != nil || string(buf[:n]) != "early data" {
		t.Fatalf("read %q %v", buf[:n], err)
	}
}

func TestRejectReturnsReasonAndDiscards(t *testing.T) {
	cli, srv := links(t, LinkOptions{})
	go func() {
		for i := 0; i < 80; i++ {
			s, err := srv.Accept(context.Background())
			if err != nil {
				return
			}
			time.Sleep(20 * time.Millisecond) // 让 0-RTT 数据先到（缓存在流中，拒绝时丢弃并归还额度）
			s.Reject(CodeQuotaExhausted, "no grant")
		}
	}()
	// 被拒绝的流携带的数据总量超过链路窗口：若丢弃的数据不归还链路额度，后面的流会卡住
	for i := 0; i < 80; i++ {
		s, _ := cli.Open(testOpen(1))
		go func() { _, _ = s.Write(make([]byte, DefaultStreamWindow)) }()
		_, err := s.Read(make([]byte, 1))
		var se *StreamError
		if !errors.As(err, &se) || se.Code != CodeQuotaExhausted || !se.Open || !se.Remote || se.Text != "no grant" {
			t.Fatalf("stream %d: %v", i, err)
		}
		if _, err := s.Write([]byte("x")); !errors.As(err, &se) {
			t.Fatalf("write after OPEN_ERR: %v", err)
		}
	}
	waitStreams(t, srv, 0)
	if srv.Err() != nil || cli.Err() != nil {
		t.Fatal(srv.Err(), cli.Err())
	}
	// 链路额度没有泄漏：新流仍能传输
	ok := make(chan error, 1)
	go func() {
		in, err := srv.Accept(context.Background())
		if err == nil {
			in.Accept()
			_, err = io.Copy(io.Discard, in)
		}
		ok <- err
	}()
	s, _ := cli.Open(testOpen(9))
	_ = s.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if _, err := s.Write(make([]byte, 1<<20)); err != nil {
		t.Fatalf("link credit leaked by rejected streams: %v", err)
	}
}

func TestHalfCloseAndReset(t *testing.T) {
	cli, srv := links(t, LinkOptions{})
	s, _ := cli.Open(testOpen(1))
	_, _ = s.Write([]byte("req"))
	_ = s.CloseWrite()
	in, _ := srv.Accept(context.Background())
	in.Accept()
	req, err := io.ReadAll(in)
	if err != nil || string(req) != "req" {
		t.Fatalf("server read %q %v", req, err)
	}
	// 客户端已半关闭，服务端仍可写回（ENG-005）
	if _, err := in.Write([]byte("resp")); err != nil {
		t.Fatal(err)
	}
	_ = in.CloseWrite()
	resp, err := io.ReadAll(s)
	if err != nil || string(resp) != "resp" {
		t.Fatalf("client read %q %v", resp, err)
	}
	if _, err := s.Write([]byte("x")); !errors.Is(err, ErrWriteAfterFin) {
		t.Fatalf("write after FIN: %v", err)
	}
	waitStreams(t, cli, 0)

	s2, _ := cli.Open(testOpen(2))
	in2, _ := srv.Accept(context.Background())
	in2.Reset(CodeTargetUnreachable, "dial failed")
	_, err = s2.Read(make([]byte, 1))
	var se *StreamError
	if !errors.As(err, &se) || se.Code != CodeTargetUnreachable || se.Open {
		t.Fatalf("reset: %v", err)
	}
	// Close 在对端未结束时发送 RST(no_route)
	s3, _ := cli.Open(testOpen(3))
	in3, _ := srv.Accept(context.Background())
	_ = s3.Close()
	if _, err := in3.Read(make([]byte, 1)); !errors.As(err, &se) || se.Code != CodeNoRoute {
		t.Fatalf("close without fin: %v", err)
	}
}

func TestStreamWindowBackpressureIsPerStream(t *testing.T) {
	cli, srv := links(t, LinkOptions{})
	slow, _ := cli.Open(testOpen(1))
	inSlow, _ := srv.Accept(context.Background())
	inSlow.Accept()
	var sent int64
	var mu sync.Mutex
	go func() {
		buf := make([]byte, 16384)
		for {
			if _, err := slow.Write(buf); err != nil {
				return
			}
			mu.Lock()
			sent += int64(len(buf))
			mu.Unlock()
		}
	}()
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	if sent != DefaultStreamWindow {
		t.Fatalf("sent %d bytes to a stream nobody reads, want exactly the window %d", sent, DefaultStreamWindow)
	}
	mu.Unlock()
	// 另一条流不受影响（链路窗口远大于单流窗口）
	echoServer(t, srv)
	fast, _ := cli.Open(testOpen(2))
	go func() { _, _ = fast.Write(make([]byte, 2<<20)); _ = fast.CloseWrite() }()
	n, err := io.Copy(io.Discard, fast)
	if err != nil || n != 2<<20 {
		t.Fatalf("other stream: %d %v", n, err)
	}
	// 读一些之后发送方继续
	_, _ = io.ReadFull(inSlow, make([]byte, DefaultStreamWindow))
	time.Sleep(100 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if sent <= DefaultStreamWindow {
		t.Fatal("window update did not resume the writer")
	}
}

func TestWindowGrowsWhenItLimitsThroughput(t *testing.T) {
	cli, srv := links(t, LinkOptions{RTT: time.Second}) // 让“2 个 RTT 内用完窗口”必然成立
	echoServer(t, srv)
	s, _ := cli.Open(testOpen(1))
	go func() { _, _ = s.Write(make([]byte, 32<<20)); _ = s.CloseWrite() }()
	// 实测 RTT 会替换初始估计；回环 RTT 很小，增长与否取决于消费速度，这里只要求不超过上限且能增长
	if _, err := io.Copy(io.Discard, s); err != nil {
		t.Fatal(err)
	}
	if w := s.RecvWindow(); w <= DefaultStreamWindow || w > DefaultMaxStreamWindow {
		t.Fatalf("receive window %d, want growth within (%d, %d]", w, DefaultStreamWindow, DefaultMaxStreamWindow)
	}
}

func TestReadDeadline(t *testing.T) {
	cli, srv := links(t, LinkOptions{})
	s, _ := cli.Open(testOpen(1))
	in, _ := srv.Accept(context.Background())
	in.Accept()
	_ = s.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	start := time.Now()
	_, err := s.Read(make([]byte, 1))
	if !errors.Is(err, os.ErrDeadlineExceeded) || time.Since(start) > time.Second {
		t.Fatalf("deadline: %v after %v", err, time.Since(start))
	}
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatal("deadline error is not a net.Error timeout")
	}
	// 截止时间延后后读取恢复
	_ = s.SetReadDeadline(time.Now().Add(time.Second))
	_, _ = in.Write([]byte("z"))
	if n, err := s.Read(make([]byte, 1)); n != 1 || err != nil {
		t.Fatalf("read after extending deadline: %d %v", n, err)
	}
	// 写截止时间：对端不读，窗口用尽后写入在截止时间返回
	_ = s.SetWriteDeadline(time.Now().Add(150 * time.Millisecond))
	_, err = s.Write(make([]byte, 2*DefaultStreamWindow))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("write deadline: %v", err)
	}
}

func TestDrainAndGoaway(t *testing.T) {
	cli, srv := links(t, LinkOptions{})
	old, _ := cli.Open(testOpen(1))
	inOld, _ := srv.Accept(context.Background())
	inOld.Accept()
	srv.Drain(GoawayDraining)
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, err := cli.Open(testOpen(2))
		if errors.Is(err, ErrGoaway) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("GOAWAY not observed by peer")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// 已有流继续
	_, _ = old.Write([]byte("still"))
	buf := make([]byte, 5)
	if _, err := io.ReadFull(inOld, buf); err != nil || string(buf) != "still" {
		t.Fatalf("existing stream after drain: %q %v", buf, err)
	}
}

func TestLinkCloseEndsStreams(t *testing.T) {
	before := runtime.NumGoroutine()
	c, s := established(t)
	cli, srv := NewLink(c, LinkOptions{Initiator: true}), NewLink(s, LinkOptions{})
	st, _ := cli.Open(testOpen(1))
	in, _ := srv.Accept(context.Background())
	done := make(chan error, 1)
	go func() { _, err := in.Read(make([]byte, 1)); done <- err }()
	_ = cli.Close()
	if err := <-done; !errors.Is(err, ErrLinkClosed) {
		t.Fatalf("peer stream after link close: %v", err)
	}
	if _, err := st.Write([]byte("x")); !errors.Is(err, ErrLinkClosed) {
		t.Fatalf("local stream after close: %v", err)
	}
	<-srv.Done()
	_ = srv.Close()
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before {
		t.Fatalf("goroutines: before %d after %d", before, n)
	}
}

func TestPing(t *testing.T) {
	cli, _ := links(t, LinkOptions{})
	d, err := cli.Ping(context.Background())
	if err != nil || d <= 0 || d > time.Second {
		t.Fatalf("ping %v %v", d, err)
	}
}

func TestStreamIDExhaustion(t *testing.T) {
	cli, srv := links(t, LinkOptions{MaxStreamID: 5})
	for i := 0; i < 3; i++ { // 1, 3, 5
		if _, err := cli.Open(testOpen(1)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := cli.Open(testOpen(1)); !errors.Is(err, ErrStreamIDs) {
		t.Fatalf("want exhaustion, got %v", err)
	}
	// 对端收到 GOAWAY(stream_id_exhausted) 后也不再在这条链路上开流
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := srv.Open(testOpen(1)); errors.Is(err, ErrGoaway) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("peer did not observe GOAWAY")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ---------------------------------------------------------------- 异常对端（直接写原始帧）

// rawAgainst 返回一个 Link（接受方）与对端的原始连接。
func rawAgainst(t *testing.T) (*Link, *Established) {
	c, s := established(t)
	l := NewLink(s, LinkOptions{})
	t.Cleanup(func() { l.Close() })
	return l, c
}

func expectProtocolGoaway(t *testing.T, name string, l *Link, raw *Established, frames ...[]byte) {
	t.Helper()
	for _, f := range frames {
		if _, err := raw.TLS.Write(f); err != nil {
			t.Fatal(err)
		}
	}
	_ = raw.TLS.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		h, p, err := raw.Reader.Next()
		if err != nil {
			t.Fatalf("%s: no GOAWAY before close: %v", name, err)
		}
		if h.Type == TypeGoaway {
			if _, code, _ := ParseGoawayPayload(p); code != GoawayProtocolError {
				t.Fatalf("%s: GOAWAY code %d", name, code)
			}
			break
		}
	}
	select {
	case <-l.Done():
		if !errors.Is(l.Err(), ErrProtocol) {
			t.Fatalf("%s: link error %v", name, l.Err())
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: link not closed", name)
	}
}

func openFrame(id uint32) []byte {
	return AppendFrame(nil, TypeOpen, id, testOpen(1).AppendPayload(nil))
}

func TestProtocolViolationsCloseLink(t *testing.T) {
	cases := map[string][][]byte{
		"DATA on never-opened stream": {AppendFrame(nil, TypeData, 7, []byte("x"))},
		"OPEN with acceptor parity":   {openFrame(2)},
		"OPEN id not increasing":      {openFrame(5), openFrame(3)},
		"stream window exceeded":      {openFrame(1), AppendFrame(nil, TypeData, 1, make([]byte, 16384))},
		"DATA after FIN":              {openFrame(1), AppendFrame(nil, TypeFin, 1, nil), AppendFrame(nil, TypeData, 1, []byte("x"))},
		"second HELLO":                {AppendFrame(nil, TypeHello, 0, nil)},
		"PING wrong length":           {AppendFrame(nil, TypePing, 0, []byte{1})},
		"unknown frame type":          {hdr(0x0D, 0, 1)},
	}
	// 超出流窗口：先合法发满 262,144 字节（16 帧），再多发 1 字节
	over := [][]byte{openFrame(1)}
	for i := 0; i < 16; i++ {
		over = append(over, AppendFrame(nil, TypeData, 1, make([]byte, 16384)))
	}
	cases["stream window exceeded"] = append(over, AppendFrame(nil, TypeData, 1, []byte{1}))
	// 链路窗口：65 条流各发满 256 KiB 流窗口，总量超过 16 MiB 链路窗口
	var link [][]byte
	for i := uint32(0); i < 65; i++ {
		link = append(link, openFrame(2*i+1))
		for j := 0; j < 16; j++ {
			link = append(link, AppendFrame(nil, TypeData, 2*i+1, make([]byte, 16384)))
		}
	}
	cases["link window exceeded"] = link
	for name, frames := range cases {
		t.Run(name, func(t *testing.T) {
			l, raw := rawAgainst(t)
			expectProtocolGoaway(t, name, l, raw, frames...)
		})
	}
}

func TestMalformedOpenRejectedLinkSurvives(t *testing.T) {
	l, raw := rawAgainst(t)
	bad := testOpen(1).AppendPayload(nil)
	bad = append(bad, openHopIndex, 0, 1, 0) // 重复 tag
	_, _ = raw.TLS.Write(AppendFrame(nil, TypeOpen, 1, bad))
	_ = raw.TLS.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		h, p, err := raw.Reader.Next()
		if err != nil {
			t.Fatal(err)
		}
		if h.Type == TypeOpenErr && h.StreamID == 1 {
			if c, _, _ := ParseErrPayload(p); c != CodeUnauthorized {
				t.Fatalf("code %v", c)
			}
			break
		}
	}
	// 被拒绝的流的迟到数据被丢弃，链路继续；下一条合法流正常接受
	_, _ = raw.TLS.Write(AppendFrame(nil, TypeData, 1, []byte("late")))
	_, _ = raw.TLS.Write(openFrame(3))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := l.Accept(ctx)
	if err != nil || s.ID() != 3 {
		t.Fatalf("accept after malformed open: %v", err)
	}
	if l.Err() != nil {
		t.Fatal(l.Err())
	}
}

func TestDrainRejectsNewPeerStreams(t *testing.T) {
	l, raw := rawAgainst(t)
	_, _ = raw.TLS.Write(openFrame(1))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := l.Accept(ctx); err != nil {
		t.Fatal(err)
	}
	l.Drain(GoawayDraining)
	_, _ = raw.TLS.Write(openFrame(3))
	_ = raw.TLS.SetReadDeadline(time.Now().Add(2 * time.Second))
	sawGoaway := false
	for {
		h, p, err := raw.Reader.Next()
		if err != nil {
			t.Fatal(err)
		}
		if h.Type == TypeGoaway {
			last, code, _ := ParseGoawayPayload(p)
			if last != 1 || code != GoawayDraining {
				t.Fatalf("goaway %d %d", last, code)
			}
			sawGoaway = true
		}
		if h.Type == TypeOpenErr && h.StreamID == 3 {
			if c, _, _ := ParseErrPayload(p); c != CodeNoRoute || !sawGoaway {
				t.Fatalf("drain reject code %v goaway %v", c, sawGoaway)
			}
			return
		}
	}
}

// 流可直接交给 pipe：入口 TCP 连接 ↔ 流（隧道入口的数据路径，ENG-T25 的基础）。
func TestPipeOverStream(t *testing.T) {
	cli, srv := links(t, LinkOptions{})
	echoServer(t, srv)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		s, _ := cli.Open(testOpen(1))
		pipe.Run(context.Background(), c, s, pipe.Options{IdleTimeout: 5 * time.Second})
		_ = c.Close()
		_ = s.Close()
	}()
	c, _ := net.Dial("tcp", ln.Addr().String())
	defer c.Close()
	data := make([]byte, 5<<20)
	_, _ = rand.Read(data)
	go func() { _, _ = c.Write(data); _ = c.(*net.TCPConn).CloseWrite() }()
	got, err := io.ReadAll(c)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("pipe over stream: %d bytes %v", len(got), err)
	}
	waitStreams(t, cli, 0)
}

// 单流吞吐（参考值，记录在日志中；性能验收见 NFR-002 基准）。
func TestThroughputReport(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	cli, srv := links(t, LinkOptions{})
	got := make(chan int64, 1)
	go func() {
		in, err := srv.Accept(context.Background())
		if err != nil {
			got <- 0
			return
		}
		in.Accept()
		n, _ := io.Copy(io.Discard, in)
		got <- n
	}()
	st, _ := cli.Open(testOpen(1))
	buf := make([]byte, 256<<10)
	const total = 512 << 20
	start := time.Now()
	for n := 0; n < total; n += len(buf) {
		if _, err := st.Write(buf); err != nil {
			t.Fatal(err)
		}
	}
	_ = st.CloseWrite()
	if n := <-got; n != total {
		t.Fatalf("received %d", n)
	}
	t.Logf("single stream over TLS loopback: %.0f Mbit/s (window %d)", float64(total)*8/time.Since(start).Seconds()/1e6, DefaultStreamWindow)
}

// 已结束的流的迟到 DATA 被丢弃，并且必须归还链路额度（否则链路最终会停住）。
func TestLateDataReturnsLinkCredit(t *testing.T) {
	l, raw := rawAgainst(t)
	bad := append(testOpen(1).AppendPayload(nil), openHopIndex, 0, 1, 0)
	_, _ = raw.TLS.Write(AppendFrame(nil, TypeOpen, 1, bad)) // 被拒绝：流 1 已结束
	for i := 0; i < DefaultLinkWindow/2/16384+1; i++ {       // 超过链路窗口一半
		_, _ = raw.TLS.Write(AppendFrame(nil, TypeData, 1, make([]byte, 16384)))
	}
	_ = raw.TLS.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		h, p, err := raw.Reader.Next()
		if err != nil {
			t.Fatalf("no link WINDOW for discarded data: %v", err)
		}
		if h.Type == TypeGoaway {
			t.Fatal("link closed")
		}
		if h.Type == TypeWindow && h.StreamID == 0 {
			if inc, _ := ParseWindowPayload(p); inc < DefaultLinkWindow/2 {
				t.Fatalf("increment %d", inc)
			}
			break
		}
	}
	if l.Err() != nil {
		t.Fatal(l.Err())
	}
}
