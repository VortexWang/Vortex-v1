package vlink

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
)

// pipeConn 让链路运行在内存管道上（模糊测试不做 TLS 握手）。
type pipeConn struct{ net.Conn }

func (pipeConn) ConnectionState() tls.ConnectionState { return tls.ConnectionState{} }

// FuzzLinkFrames 把任意字节作为对端在握手之后发来的帧序列（ENG-T37）。性质：不 panic、不死锁；
// 链路要么继续运行，要么以错误关闭；Close 在限时内返回且 goroutine 全部退出。
func FuzzLinkFrames(f *testing.F) {
	seeds := [][]byte{
		openFrame(1),
		append(openFrame(1), AppendFrame(nil, TypeData, 1, []byte("hello"))...),
		append(append(openFrame(1), AppendFrame(nil, TypeFin, 1, nil)...), AppendFrame(nil, TypeRst, 1, AppendErrPayload(nil, CodeNoRoute, "x"))...),
		append(AppendFrame(nil, TypeOpen, 1, udpOpen().AppendPayload(nil)), AppendFrame(nil, TypeDgram, 1, []byte{1, 2, 3})...),
		AppendFrame(nil, TypeWindow, 0, AppendWindowPayload(nil, 1<<20)),
		AppendFrame(nil, TypePing, 0, make([]byte, 8)),
		AppendFrame(nil, TypeGoaway, 0, AppendGoawayPayload(nil, 0, GoawayDraining)),
		append(openFrame(3), AppendFrame(nil, TypeWindow, 3, AppendWindowPayload(nil, 7))...),
		AppendFrame(nil, TypeDgram, 99, []byte("forged")),
		append(AppendFrame(nil, TypeOpen, 1, []byte{1, 0, 2, 9}), AppendFrame(nil, TypeData, 1, []byte("x"))...),
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		a, b := net.Pipe()
		l := newLink(pipeConn{a}, NewReader(a), PeerInfo{Hello: &Hello{}}, LinkOptions{})
		var wg sync.WaitGroup // 本测试启动的 goroutine；链路自身的读写 goroutine 由 Close 等待
		wg.Add(2)
		go func() { defer wg.Done(); _, _ = io.Copy(io.Discard, b) }() // 读走链路写出的一切
		go func() {
			defer wg.Done()
			for {
				s, err := l.Accept(context.Background())
				if err != nil {
					return
				}
				s.Accept()
				_ = s.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
				_, _ = s.Read(make([]byte, 64))
				s.Close()
			}
		}()
		_ = b.SetWriteDeadline(time.Now().Add(time.Second))
		_, _ = b.Write(data)
		time.Sleep(time.Millisecond)
		done := make(chan struct{})
		go func() { l.Close(); close(done) }()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("Close did not return (deadlock)")
		}
		b.Close()
		finished := make(chan struct{})
		go func() { wg.Wait(); close(finished) }()
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			buf := make([]byte, 1<<16)
			t.Fatalf("goroutines did not exit after link close\n%s", buf[:runtime.Stack(buf, true)])
		}
		if l.NumStreams() != 0 {
			t.Fatalf("%d streams left after close", l.NumStreams())
		}
	})
}
