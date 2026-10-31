package vlink

import (
	"context"
	"errors"
	"io"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

var fastKA = LinkOptions{KeepaliveIdle: 100 * time.Millisecond, PingWait: 100 * time.Millisecond, DeadAfter: 400 * time.Millisecond}

// rawWith 返回使用给定参数的链路（接受方）与对端的原始连接。
func rawWith(t *testing.T, opt LinkOptions) (*Link, *Established) {
	c, s := established(t)
	l := NewLink(s, opt)
	t.Cleanup(func() { l.Close() })
	return l, c
}

// rawPeerLoop 读取对端发来的帧；pong 为真时回应 PING。返回收到的 PING 计数。
func rawPeerLoop(raw *Established, pong bool) *atomic.Int64 {
	var pings atomic.Int64
	go func() {
		for {
			h, p, err := raw.Reader.Next()
			if err != nil {
				return
			}
			if h.Type == TypePing {
				pings.Add(1)
				if pong {
					_, _ = raw.TLS.Write(AppendFrame(nil, TypePong, 0, p))
				}
			}
		}
	}()
	return &pings
}

func TestKeepalivePingsWhenIdleAndStaysAlive(t *testing.T) {
	l, raw := rawWith(t, fastKA)
	pings := rawPeerLoop(raw, true)
	time.Sleep(time.Second)
	if l.Err() != nil {
		t.Fatalf("link died although the peer answers PINGs: %v", l.Err())
	}
	// 建链时 1 次，之后每空闲 100 ms 一次：约 10 次
	if n := pings.Load(); n < 5 || n > 15 {
		t.Fatalf("%d keepalive PINGs in 1 s", n)
	}
}

func TestSilentPeerDetectedDead(t *testing.T) {
	l, raw := rawWith(t, fastKA)
	pings := rawPeerLoop(raw, false) // 读取但从不回应
	start := time.Now()
	select {
	case <-l.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("silent peer not detected")
	}
	took := time.Since(start)
	if !errors.Is(l.Err(), ErrLinkDead) || took < 350*time.Millisecond || took > 800*time.Millisecond {
		t.Fatalf("dead after %v: %v", took, l.Err())
	}
	if pings.Load() < 3 { // 建链 1 次 + 空闲 100 ms + 等待 100 ms 后重发……
		t.Fatalf("only %d PINGs before declaring dead", pings.Load())
	}
}

func TestAnyFrameRefreshesLiveness(t *testing.T) {
	l, raw := rawWith(t, fastKA)
	rawPeerLoop(raw, false)
	stop := make(chan struct{})
	defer close(stop)
	go func() { // 不回应 PING，但持续发送其他帧（链路级 WINDOW 0）
		for {
			select {
			case <-stop:
				return
			case <-time.After(50 * time.Millisecond):
				_, _ = raw.TLS.Write(AppendFrame(nil, TypeWindow, 0, AppendWindowPayload(nil, 0)))
			}
		}
	}()
	time.Sleep(time.Second)
	if l.Err() != nil {
		t.Fatalf("link died although frames keep arriving: %v", l.Err())
	}
}

func TestRTTMeasuredByKeepalive(t *testing.T) {
	cli, _ := links(t, LinkOptions{RTT: 5 * time.Second})
	deadline := time.Now().Add(2 * time.Second)
	for cli.RTT() == 5*time.Second && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if r := cli.RTT(); r >= time.Second {
		t.Fatalf("RTT not measured: %v", r)
	}
}

// ENG-045：每条链路只有读、写各 1 个常驻 goroutine（保活使用定时器）。
func TestTwoGoroutinesPerLink(t *testing.T) {
	var pairs [][2]*Established
	for i := 0; i < 5; i++ {
		c, s := established(t)
		pairs = append(pairs, [2]*Established{c, s})
	}
	time.Sleep(100 * time.Millisecond)
	before := runtime.NumGoroutine()
	var ls []*Link
	for _, p := range pairs {
		ls = append(ls, NewLink(p[0], LinkOptions{Initiator: true}), NewLink(p[1], LinkOptions{}))
	}
	time.Sleep(200 * time.Millisecond)
	if d := runtime.NumGoroutine() - before; d != 2*len(ls) {
		t.Fatalf("%d goroutines for %d links, want %d", d, len(ls), 2*len(ls))
	}
	for _, l := range ls {
		l.Close()
	}
}

func TestCloseGracefulWaitsForStreams(t *testing.T) {
	cli, srv := links(t, LinkOptions{})
	s, _ := cli.Open(testOpen(1))
	in, _ := srv.Accept(context.Background())
	in.Accept()
	done := make(chan error, 1)
	go func() { done <- srv.CloseGraceful(context.Background(), GoawayDraining) }()
	// 对端收到 GOAWAY 后不能再开流
	deadline := time.Now().Add(2 * time.Second)
	for {
		if c, ok := cli.GoawayReceived(); ok && c == GoawayDraining {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("GOAWAY(draining) not received")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := cli.Open(testOpen(2)); !errors.Is(err, ErrGoaway) {
		t.Fatalf("open after GOAWAY: %v", err)
	}
	select {
	case <-done:
		t.Fatal("graceful close did not wait for the open stream")
	case <-time.After(100 * time.Millisecond):
	}
	// 已有流照常完成
	_, _ = s.Write([]byte("bye"))
	_ = s.CloseWrite()
	b, _ := io.ReadAll(in)
	_ = in.CloseWrite()
	_, _ = io.ReadAll(s)
	_ = in.Close()
	_ = s.Close()
	if string(b) != "bye" {
		t.Fatalf("existing stream got %q", b)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("graceful close did not finish after the last stream ended")
	}
}
