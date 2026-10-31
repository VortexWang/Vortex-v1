package vlink

import (
	"context"
	"errors"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"vortex/engine/types"
)

func udpOpen() *Open {
	return &Open{RuleID: uuidN(1), RuleVersion: 1, Network: types.NetworkUDP, Client: netip.MustParseAddrPort("192.0.2.9:4000")}
}

func waitCount(t *testing.T, c *atomic.Uint64, want uint64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for c.Load() < want && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := c.Load(); got != want {
		t.Fatalf("counter %d, want %d", got, want)
	}
}

func TestDgramZeroRTTOrderAndBothDirections(t *testing.T) {
	cli, srv := links(t, LinkOptions{})
	s, _ := cli.Open(udpOpen())
	for i := 0; i < 5; i++ { // OPEN 之后立即发送，接受方尚未检查（ENG-047 0-RTT）
		if err := s.SendDgram([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	in, _ := srv.Accept(context.Background())
	time.Sleep(50 * time.Millisecond)
	in.Accept()
	got := make(chan byte, 10)
	in.OnDgram(func(p []byte) { got <- p[0] })
	_ = s.SendDgram([]byte{5})
	for i := 0; i < 6; i++ {
		select {
		case b := <-got:
			if b != byte(i) {
				t.Fatalf("datagram %d arrived as %d (order lost while switching to callback)", i, b)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("datagram %d missing", i)
		}
	}
	// 反方向：ReadDgram
	_ = in.SendDgram(make([]byte, 1200))
	_ = s.SetReadDeadline(time.Now().Add(2 * time.Second))
	if n, err := s.ReadDgram(make([]byte, 2000)); n != 1200 || err != nil {
		t.Fatalf("read dgram %d %v", n, err)
	}
	if cli.Dgrams().Sent.Load() != 6 || srv.Dgrams().Received.Load() != 6 {
		t.Fatalf("counters sent %d received %d", cli.Dgrams().Sent.Load(), srv.Dgrams().Received.Load())
	}
}

func TestDgramDrops(t *testing.T) {
	l, raw := rawAgainst(t)
	// 未开流的会话、TCP 流上的 DGRAM：丢弃并计数，链路不关闭
	_, _ = raw.TLS.Write(AppendFrame(nil, TypeDgram, 9, []byte("x")))
	_, _ = raw.TLS.Write(openFrame(1)) // TCP 流
	_, _ = raw.TLS.Write(AppendFrame(nil, TypeDgram, 1, []byte("x")))
	waitCount(t, &l.Dgrams().DropUnknown, 1)
	waitCount(t, &l.Dgrams().DropNotDatagram, 1)
	if l.Err() != nil {
		t.Fatal(l.Err())
	}

	cli, srv := links(t, LinkOptions{DgramRecvQueue: 4, DgramQueue: 1100})
	s, _ := cli.Open(udpOpen())
	if err := s.SendDgram(make([]byte, MaxPayload+1)); err != ErrDgramTooLarge {
		t.Fatalf("oversize: %v", err)
	}
	if err := s.SendDgram(make([]byte, 1200)); err != ErrDgramDropped { // 大于整个发送队列
		t.Fatalf("queue full: %v", err)
	}
	for i := 0; i < 10; i++ {
		_ = s.SendDgram(make([]byte, 100))
	}
	in, _ := srv.Accept(context.Background())
	waitCount(t, &srv.Dgrams().DropRecvQueue, 6) // 未处理的入站数据报最多排 4 个
	if cli.Dgrams().DropOversize.Load() != 1 || cli.Dgrams().DropSendQueue.Load() < 1 {
		t.Fatal("send-side drop counters")
	}
	// 对端终止会话后，本端发送返回原因（关闭后的会话收到的 DGRAM 计入 DropUnknown，见上面的原始帧用例）
	in.Reset(CodeNoRoute, "")
	deadline := time.Now().Add(2 * time.Second)
	for s.SendDgram([]byte("late")) == nil && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	var se *StreamError
	if err := s.SendDgram([]byte("late")); !errors.As(err, &se) || se.Code != CodeNoRoute {
		t.Fatalf("send after peer reset: %v", err)
	}
}

func TestDgramBatchShare(t *testing.T) {
	l := &Link{}
	mk := func(n int) wframe { return wframe{b: make([]byte, n)} }
	for i := 0; i < 400; i++ {
		l.dgram = append(l.dgram, mk(1208))
		l.dgramBytes += 1208
		l.data = append(l.data, mk(16392))
	}
	l.ctrl = []wframe{mk(12)}
	batch := l.takeBatchLocked(nil)
	var dg, dt int
	for _, f := range batch {
		switch len(f.b) {
		case 1208:
			dg += len(f.b)
		case 16392:
			dt += len(f.b)
		}
	}
	if len(batch) == 0 || len(batch[0].b) != 12 {
		t.Fatal("control frame must come first")
	}
	if dt == 0 || dg > (dg+dt)/2+1208 {
		t.Fatalf("with DATA waiting DGRAM took %d of %d bytes", dg, dg+dt)
	}
	if l.dgramBytes != 400*1208-dg {
		t.Fatal("queue byte accounting")
	}
	// 没有 DATA 时 DGRAM 可以占满整批
	l.data = nil
	batch = l.takeBatchLocked(nil)
	dg = 0
	for _, f := range batch {
		dg += len(f.b)
	}
	if dg < writeBatch {
		t.Fatalf("without DATA, DGRAM got only %d bytes", dg)
	}
}

// 混合负载（ENG-048 / NFR-005）：同一链路上一条 TCP 流满速传输的同时，UDP 1,200 字节、100 Mbps 丢包 ≤ 0.1%。
func TestDgramUnderTCPLoad(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	cli, srv := links(t, LinkOptions{})
	bulk, _ := cli.Open(testOpen(1))
	inBulk, _ := srv.Accept(context.Background())
	inBulk.Accept()
	stopBulk := make(chan struct{})
	go func() {
		buf := make([]byte, 256<<10)
		for {
			select {
			case <-stopBulk:
				return
			default:
			}
			if _, err := bulk.Write(buf); err != nil {
				return
			}
		}
	}()
	go func() {
		buf := make([]byte, 256<<10)
		for {
			if _, err := inBulk.Read(buf); err != nil {
				return
			}
		}
	}()

	u, _ := cli.Open(udpOpen())
	in, _ := srv.Accept(context.Background())
	in.Accept()
	var got atomic.Uint64
	in.OnDgram(func(p []byte) { got.Add(1) })
	const pps, secs = 10417, 2 // 1200 B × 10,417/s ≈ 100 Mbit/s
	payload := make([]byte, 1200)
	start := time.Now()
	sent := 0
	for time.Since(start) < secs*time.Second {
		due := int(time.Since(start).Seconds() * pps)
		for ; sent < due; sent++ {
			_ = u.SendDgram(payload)
		}
		time.Sleep(200 * time.Microsecond)
	}
	time.Sleep(300 * time.Millisecond)
	close(stopBulk)
	loss := 1 - float64(got.Load())/float64(sent)
	t.Logf("udp under tcp load: sent %d received %d loss %.4f%%, send-queue drops %d", sent, got.Load(), loss*100, cli.Dgrams().DropSendQueue.Load())
	if loss > 0.001 {
		t.Fatalf("loss %.4f%% > 0.1%%", loss*100)
	}
}
