package pipe

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"vortex/engine/types"
)

// ---------- 测试拓扑：客户端 → [代理 Run] → 目标 ----------

type topo struct {
	client, proxyA, proxyB, target net.Conn
}

// tcpPair 返回一对已连接的 TCP 连接。
func tcpPair(t testing.TB) (net.Conn, net.Conn) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	ch := make(chan net.Conn, 1)
	go func() { c, _ := ln.Accept(); ch <- c }()
	c1, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	return c1, <-ch
}

func newTopo(t testing.TB, tcp bool) topo {
	if tcp {
		c, pa := tcpPair(t)
		pb, tg := tcpPair(t)
		return topo{c, pa, pb, tg}
	}
	// net.Pipe 不是 *net.TCPConn，必然走缓冲路径；不支持 CloseWrite，因此只用于不依赖半关闭的用例
	c, pa := net.Pipe()
	pb, tg := net.Pipe()
	return topo{c, pa, pb, tg}
}

func (tp topo) closeAll() {
	for _, c := range []net.Conn{tp.client, tp.proxyA, tp.proxyB, tp.target} {
		_ = c.Close()
	}
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// ---------- 完整性与半关闭（ENG-005、NFR-006 大文件校验） ----------

func TestIntegrityAndHalfClose(t *testing.T) {
	for _, mode := range []struct {
		name   string
		splice bool
	}{{"splice", true}, {"buffered", false}} {
		t.Run(mode.name, func(t *testing.T) {
			tp := newTopo(t, true)
			defer tp.closeAll()
			// 半关闭未传递时目标永远等不到 EOF：用截止时间让测试明确失败而不是挂起
			dl := time.Now().Add(15 * time.Second)
			_ = tp.client.SetDeadline(dl)
			_ = tp.target.SetDeadline(dl)
			upData, downData := randBytes(48<<20), randBytes(16<<20)

			resCh := make(chan Result, 1)
			go func() { resCh <- Run(context.Background(), tp.proxyA, tp.proxyB, Options{DisableSplice: !mode.splice}) }()

			// 目标：读完全部上行数据（看到 EOF 说明半关闭已传递），之后仍能发送下行数据
			targetSum := make(chan [32]byte, 1)
			go func() {
				h := sha256.New()
				_, _ = io.Copy(h, tp.target)
				var s [32]byte
				copy(s[:], h.Sum(nil))
				targetSum <- s
				_, _ = tp.target.Write(downData)
				_ = tp.target.(*net.TCPConn).CloseWrite()
			}()
			go func() {
				_, _ = tp.client.Write(upData)
				_ = tp.client.(*net.TCPConn).CloseWrite() // 客户端只关闭写方向
			}()
			h := sha256.New()
			if _, err := io.Copy(h, tp.client); err != nil {
				t.Fatalf("half-close not propagated or transfer stalled: %v", err)
			}
			if got := <-targetSum; got != sha256.Sum256(upData) {
				t.Fatal("upstream data corrupted")
			}
			var down [32]byte
			copy(down[:], h.Sum(nil))
			if down != sha256.Sum256(downData) {
				t.Fatal("downstream data corrupted")
			}
			res := <-resCh
			if res.Err != nil || res.Up != int64(len(upData)) || res.Down != int64(len(downData)) {
				t.Fatalf("result %+v", res)
			}
			if res.Spliced != mode.splice {
				t.Fatalf("spliced=%v want %v", res.Spliced, mode.splice)
			}
		})
	}
}

func TestSpliceActuallyUsed(t *testing.T) {
	before := SplicedBytes()
	tp := newTopo(t, true)
	defer tp.closeAll()
	go Run(context.Background(), tp.proxyA, tp.proxyB, Options{})
	data := randBytes(4 << 20)
	go func() { _, _ = tp.client.Write(data); _ = tp.client.(*net.TCPConn).CloseWrite() }()
	n, _ := io.CopyN(io.Discard, tp.target, int64(len(data)))
	if n != int64(len(data)) {
		t.Fatalf("got %d", n)
	}
	if SplicedBytes()-before < int64(len(data)) {
		t.Fatalf("splice path not used: %d bytes spliced", SplicedBytes()-before)
	}
}

func TestNonTCPFallsBackToBuffered(t *testing.T) {
	tp := newTopo(t, false)
	defer tp.closeAll()
	resCh := make(chan Result, 1)
	go func() { resCh <- Run(context.Background(), tp.proxyA, tp.proxyB, Options{}) }()
	go func() { _, _ = tp.client.Write([]byte("ping")); _ = tp.client.Close() }()
	buf := make([]byte, 4)
	_, _ = io.ReadFull(tp.target, buf)
	_ = tp.target.Close()
	if r := <-resCh; r.Spliced || string(buf) != "ping" {
		t.Fatalf("spliced=%v buf=%q", r.Spliced, buf)
	}
}

// ---------- Hook 约定（ENG-036、ENG-037） ----------

type recHook struct {
	limit    int
	mu       sync.Mutex
	acquired map[types.Direction]int
	released map[types.Direction]int
	written  map[types.Direction]int
	pending  map[types.Direction]int // 已 Acquire 未 Release 的次数
	violated atomic.Bool
	stopAt   int64 // 上行累计写成功超过该值后返回 0；0 = 不停止
}

func newRecHook(limit int) *recHook {
	return &recHook{limit: limit, acquired: map[types.Direction]int{}, released: map[types.Direction]int{},
		written: map[types.Direction]int{}, pending: map[types.Direction]int{}}
}

func (h *recHook) Acquire(_ context.Context, d types.Direction, want int) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.pending[d] != 0 {
		h.violated.Store(true) // 上一次 Acquire 尚未 Release
	}
	if h.stopAt > 0 && d == types.DirUp && int64(h.written[d]) >= h.stopAt {
		h.pending[d]++ // 返回 0 也是一次 Acquire，但不得再期待 Release
		h.pending[d]--
		return 0, nil
	}
	h.pending[d]++
	h.acquired[d]++
	if want > h.limit {
		want = h.limit
	}
	return want, nil
}

func (h *recHook) Release(d types.Direction, granted, written int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if written > granted || written < 0 || granted > h.limit || h.pending[d] != 1 {
		h.violated.Store(true)
	}
	h.pending[d]--
	h.released[d]++
	h.written[d] += written
}

func TestHookContract(t *testing.T) {
	for _, splice := range []bool{true, false} {
		tp := newTopo(t, true)
		h := newRecHook(1000) // 每次最多 1000 字节，验证拷贝量从不超过授予量
		resCh := make(chan Result, 1)
		go func() {
			resCh <- Run(context.Background(), tp.proxyA, tp.proxyB, Options{Hook: h, DisableSplice: !splice})
		}()
		data := randBytes(1 << 20)
		go func() { _, _ = tp.client.Write(data); _ = tp.client.(*net.TCPConn).CloseWrite() }()
		go func() { _, _ = io.Copy(io.Discard, tp.target); _ = tp.target.(*net.TCPConn).CloseWrite() }()
		_, _ = io.Copy(io.Discard, tp.client)
		res := <-resCh
		tp.closeAll()
		h.mu.Lock()
		if h.violated.Load() {
			t.Fatalf("splice=%v: hook contract violated", splice)
		}
		if h.acquired[types.DirUp] != h.released[types.DirUp] || h.written[types.DirUp] != len(data) || res.Up != int64(len(data)) {
			t.Fatalf("splice=%v: acquire %d release %d written %d", splice, h.acquired[types.DirUp], h.released[types.DirUp], h.written[types.DirUp])
		}
		if h.acquired[types.DirUp] < len(data)/1000 {
			t.Fatalf("splice=%v: grants not respected (%d acquires)", splice, h.acquired[types.DirUp])
		}
		h.mu.Unlock()
	}
}

func TestHookStopEndsSession(t *testing.T) {
	for _, splice := range []bool{true, false} {
		tp := newTopo(t, true)
		h := newRecHook(64 << 10)
		h.stopAt = 256 << 10 // 额度耗尽（ENG-028）
		resCh := make(chan Result, 1)
		go func() {
			resCh <- Run(context.Background(), tp.proxyA, tp.proxyB, Options{Hook: h, DisableSplice: !splice})
		}()
		go func() { _, _ = tp.client.Write(randBytes(4 << 20)) }()
		go func() { _, _ = io.Copy(io.Discard, tp.target) }()
		select {
		case res := <-resCh:
			if !errors.Is(res.Err, ErrStopped) {
				t.Fatalf("splice=%v: err %v", splice, res.Err)
			}
			if res.Up < 256<<10 || res.Up > 256<<10+64<<10 {
				t.Fatalf("splice=%v: up %d exceeds stop point by more than one grant", splice, res.Up)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("splice=%v: session did not stop", splice)
		}
		tp.closeAll()
	}
}

// ---------- 空闲超时（ENG-006） ----------

func TestIdleTimeout(t *testing.T) {
	for _, splice := range []bool{true, false} {
		tp := newTopo(t, true)
		resCh := make(chan Result, 1)
		start := time.Now()
		go func() {
			resCh <- Run(context.Background(), tp.proxyA, tp.proxyB, Options{IdleTimeout: 200 * time.Millisecond, DisableSplice: !splice})
		}()
		select {
		case res := <-resCh:
			el := time.Since(start)
			if !errors.Is(res.Err, ErrIdleTimeout) || el < 190*time.Millisecond || el > time.Second {
				t.Fatalf("splice=%v: err=%v after %v", splice, res.Err, el)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("splice=%v: no idle timeout", splice)
		}
		tp.closeAll()
	}
}

func TestOneActiveDirectionKeepsSessionAlive(t *testing.T) {
	for _, splice := range []bool{true, false} {
		tp := newTopo(t, true)
		resCh := make(chan Result, 1)
		go func() {
			resCh <- Run(context.Background(), tp.proxyA, tp.proxyB, Options{IdleTimeout: 150 * time.Millisecond, DisableSplice: !splice})
		}()
		// 只有下行持续有少量数据，上行完全静默：整个会话不得超时
		go func() { _, _ = io.Copy(io.Discard, tp.client) }()
		for i := 0; i < 10; i++ {
			time.Sleep(60 * time.Millisecond)
			if _, err := tp.target.Write([]byte("x")); err != nil {
				t.Fatal(err)
			}
		}
		select {
		case res := <-resCh:
			t.Fatalf("splice=%v: session ended early: %v", splice, res.Err)
		default:
		}
		tp.closeAll()
		<-resCh
	}
}

func TestContextCancelStops(t *testing.T) {
	tp := newTopo(t, true)
	defer tp.closeAll()
	ctx, cancel := context.WithCancel(context.Background())
	resCh := make(chan Result, 1)
	go func() { resCh <- Run(ctx, tp.proxyA, tp.proxyB, Options{}) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case res := <-resCh:
		if !errors.Is(res.Err, context.Canceled) {
			t.Fatalf("err %v", res.Err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancel did not stop session")
	}
}

// ---------- 泄漏（ENG-008、NFR-010、NFR-011） ----------

func fdCount() int {
	es, _ := os.ReadDir("/proc/self/fd")
	return len(es)
}

func TestLeakFree(t *testing.T) {
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	g0, f0 := runtime.NumGoroutine(), fdCount()
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tp := newTopo(t, true)
			done := make(chan struct{})
			go func() {
				Run(context.Background(), tp.proxyA, tp.proxyB, Options{IdleTimeout: time.Second})
				close(done)
			}()
			go func() { _, _ = tp.client.Write(randBytes(64 << 10)); _ = tp.client.(*net.TCPConn).CloseWrite() }()
			go func() { _, _ = io.Copy(io.Discard, tp.target); _ = tp.target.(*net.TCPConn).CloseWrite() }()
			_, _ = io.Copy(io.Discard, tp.client)
			<-done
			tp.closeAll()
		}(i)
	}
	wg.Wait()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= g0 && fdCount() <= f0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("leak: goroutines %d→%d, fds %d→%d", g0, runtime.NumGoroutine(), f0, fdCount())
}

// ---------- 基准（开发机，仅作回归对比；非 13.2 节验收环境） ----------

func benchThroughput(b *testing.B, splice bool) {
	tp := newTopo(b, true)
	defer tp.closeAll()
	go Run(context.Background(), tp.proxyA, tp.proxyB, Options{DisableSplice: !splice})
	go func() { _, _ = io.Copy(io.Discard, tp.target) }()
	chunk := randBytes(1 << 20)
	b.SetBytes(int64(len(chunk)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := tp.client.Write(chunk); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSplice(b *testing.B)   { benchThroughput(b, true) }
func BenchmarkBuffered(b *testing.B) { benchThroughput(b, false) }
