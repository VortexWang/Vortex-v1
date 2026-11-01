package vlink

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"vortex/engine/types"
)

// 流复用与流控（C1 第 2、3、5～9 节；ENG-014～ENG-016）。
//
// 每条链路固定两个 goroutine：读循环把帧分发到各流的接收缓冲，写循环把排队的帧合并后写入 TLS。
// 流本身没有 goroutine：Read / Write 在调用方的 goroutine 中完成，因此一条隧道会话的 goroutine
// 数与直接转发相同（ENG-045）。

// 契约默认值（C1 第 7 节）。两端必须一致；LinkOptions 只为测试提供覆盖。
const (
	DefaultStreamWindow    = 262_144
	DefaultMaxStreamWindow = 4_194_304
	DefaultLinkWindow      = 16_777_216
)

var (
	ErrLinkClosed      = errors.New("vlink: link closed")
	ErrGoaway          = errors.New("vlink: peer sent GOAWAY, no new streams on this link")
	ErrStreamIDs       = errors.New("vlink: stream ids exhausted")
	ErrStreamClosed    = errors.New("vlink: stream closed")
	ErrWriteAfterFin   = errors.New("vlink: write after CloseWrite")
	ErrAcceptQueueFull = errors.New("vlink: accept queue full")
)

// StreamError 是对端用 OPEN_ERR 或 RST 终止流时的原因（C1 第 6 节）。
type StreamError struct {
	Code   ErrorCode
	Text   string
	Open   bool // 来自 OPEN_ERR（开流失败）
	Remote bool // 由对端发出；false 表示本端 Reset
}

func (e *StreamError) Error() string {
	kind := "RST"
	if e.Open {
		kind = "OPEN_ERR"
	}
	side := "local"
	if e.Remote {
		side = "remote"
	}
	return fmt.Sprintf("vlink: %s %s %v %s", side, kind, e.Code, e.Text)
}

// LinkOptions 是链路参数；零值取契约默认值。
type LinkOptions struct {
	Initiator       bool // 本端是 TLS 客户端（建链发起方）：使用奇数流 ID
	StreamWindow    uint32
	MaxStreamWindow uint32
	LinkWindow      uint32
	AcceptBacklog   int           // 等待 Accept 的入站流上限，默认 1024；超出回 OPEN_ERR(no_route)
	MaxStreamID     uint32        // 测试用；默认 2^32 − 1
	RTT             time.Duration // 初始 RTT 估计（用于接收窗口自适应），默认 50 ms，建链后由 PING 实测替换
	DgramQueue      int           // DGRAM 发送队列字节上限，默认 1 MiB（ENG-048）
	DgramRecvQueue  int           // 每会话待处理的入站数据报上限，默认 512
	Mode            LinkMode      // 本端发起时在 HELLO 中声明的模式（仅记录，供 Mode 返回）
	KeepaliveIdle   time.Duration // 空闲多久发 PING，默认 3 秒（ENG-019）
	PingWait        time.Duration // 单次 PING 等待，默认 3 秒
	DeadAfter       time.Duration // 自最后一次收到帧起多久判定失效，默认 12 秒（NFR-016）
}

func (o *LinkOptions) fill() {
	if o.StreamWindow == 0 {
		o.StreamWindow = DefaultStreamWindow
	}
	if o.MaxStreamWindow == 0 {
		o.MaxStreamWindow = DefaultMaxStreamWindow
	}
	if o.LinkWindow == 0 {
		o.LinkWindow = DefaultLinkWindow
	}
	if o.AcceptBacklog == 0 {
		o.AcceptBacklog = 1024
	}
	if o.MaxStreamID == 0 {
		o.MaxStreamID = math.MaxUint32
	}
	if o.RTT == 0 {
		o.RTT = 50 * time.Millisecond
	}
	if o.KeepaliveIdle == 0 {
		o.KeepaliveIdle = 3 * time.Second
	}
	if o.PingWait == 0 {
		o.PingWait = 3 * time.Second
	}
	if o.DeadAfter == 0 {
		o.DeadAfter = 12 * time.Second
	}
	if o.DgramQueue == 0 {
		o.DgramQueue = 1 << 20
	}
	if o.DgramRecvQueue == 0 {
		o.DgramRecvQueue = 512
	}
}

// broadcaster 是可广播的唤醒点：等待者取当前通道，广播时关闭并替换。
type broadcaster struct{ ch chan struct{} }

func (b *broadcaster) wait() <-chan struct{} {
	if b.ch == nil {
		b.ch = make(chan struct{})
	}
	return b.ch
}

func (b *broadcaster) broadcast() {
	if b.ch != nil {
		close(b.ch)
		b.ch = nil
	}
}

// DgramStats 是 DGRAM 丢弃计数（指标 vortex_engine_dgram_dropped_total{reason}）。
type DgramStats struct {
	Sent, Received  atomic.Uint64
	DropUnknown     atomic.Uint64 // 未开流或已关闭会话的 DGRAM（ENG-047）
	DropOversize    atomic.Uint64 // 超过单帧上限（ENG-048）
	DropSendQueue   atomic.Uint64 // 链路发送队列满
	DropRecvQueue   atomic.Uint64 // 会话入站队列满（处理跟不上）
	DropNotDatagram atomic.Uint64 // 发到 TCP 流上的 DGRAM
}

// Dgrams 返回本链路的 DGRAM 计数。
func (l *Link) Dgrams() *DgramStats { return &l.dstats }

type wframe struct {
	b    []byte
	pool *[]byte // 非 nil 时写完归还 framePool
}

var framePool = sync.Pool{New: func() any { b := make([]byte, 0, HeaderLen+MaxPayload); return &b }}

// Link 是一条已完成握手的 VLink 链路上的流复用。
type Link struct {
	tc   transport
	r    *Reader
	peer PeerInfo
	opt  LinkOptions

	mu        sync.Mutex
	streams   map[uint32]*Stream
	nextID    uint32 // 本端下一个流 ID
	lastPeer  uint32 // 对端已使用的最大流 ID
	goawayOut bool   // 已发送 GOAWAY：不再接受 ID 大于 goawayAt 的新流
	goawayAt  uint32
	goawayIn  bool // 已收到 GOAWAY
	exhausted bool

	// 链路级发送额度（对端授予）与接收额度（本端授予）
	cmu       sync.Mutex
	sendWin   int64
	sendWake  broadcaster
	recvAvail int64 // 对端还可发送的字节
	recvUnack int64 // 已消费、尚未归还给对端的字节

	acceptCh chan *Stream

	wmu        sync.Mutex
	ctrl       []wframe
	data       []wframe
	dgram      []wframe
	dgramBytes int
	wsig       chan struct{}
	wbusy      bool

	pmu   sync.Mutex
	pings map[[8]byte]chan struct{}

	kaMu    sync.Mutex
	kaTimer *time.Timer
	kaKey   [8]byte
	kaSent  time.Time
	kaOut   bool // 保活 PING 已发出、尚未收到 PONG

	goawayCode atomic.Int32 // 收到的 GOAWAY 码；-1 表示未收到

	dstats DgramStats

	rtt      atomic.Int64 // 纳秒
	lastRecv atomic.Int64 // 最近一次收到帧（单调时钟，纳秒）
	start    time.Time

	done      chan struct{}
	closeOnce sync.Once
	err       error
	wg        sync.WaitGroup
}

// transport 是链路使用的连接能力；生产环境为 *tls.Conn，测试可用内存管道代替。
type transport interface {
	Write(b []byte) (int, error)
	Close() error
	SetWriteDeadline(t time.Time) error
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
	ConnectionState() tls.ConnectionState
}

// NewLink 在握手完成的连接上启动流复用（读、写各一个 goroutine）。
func NewLink(est *Established, opt LinkOptions) *Link {
	opt.fill()
	return newLink(est.TLS, est.Reader, est.Peer, opt)
}

func newLink(tc transport, r *Reader, peer PeerInfo, opt LinkOptions) *Link {
	opt.fill()
	l := &Link{tc: tc, r: r, peer: peer, opt: opt, streams: map[uint32]*Stream{},
		sendWin: int64(opt.LinkWindow), recvAvail: int64(opt.LinkWindow),
		acceptCh: make(chan *Stream, opt.AcceptBacklog), wsig: make(chan struct{}, 1),
		pings: map[[8]byte]chan struct{}{}, done: make(chan struct{}), start: time.Now()}
	if opt.Initiator {
		l.nextID = 1
	} else {
		l.nextID = 2
	}
	l.rtt.Store(int64(opt.RTT))
	l.goawayCode.Store(-1)
	l.lastRecv.Store(int64(time.Since(l.start)))
	l.wg.Add(2)
	go l.readLoop()
	go l.writeLoop()
	// 保活不占常驻 goroutine（ENG-045：每条链路只有读、写各 1 个）：自我重排的定时器。
	// 建链后立即发一次 PING，用 PONG 实测 RTT（接收窗口自适应用）。
	l.sendKeepalive()
	l.kaMu.Lock()
	l.kaTimer = time.AfterFunc(l.kaNext(), l.keepalive)
	l.kaMu.Unlock()
	return l
}

// ErrLinkDead：自最后一次收到对端帧起超过失效时间（ENG-019）。
var ErrLinkDead = errors.New("vlink: link dead (no frame from peer within the failure timeout)")

// sendKeepalive 发送保活 PING（固定的 8 字节随机值，记下发送时刻用于测 RTT）。
func (l *Link) sendKeepalive() {
	l.kaMu.Lock()
	_, _ = rand.Read(l.kaKey[:])
	l.kaSent = time.Now()
	l.kaOut = true
	key := l.kaKey
	l.kaMu.Unlock()
	l.sendCtrl(TypePing, 0, key[:])
}

func (l *Link) kaNext() time.Duration {
	return max(min(l.opt.KeepaliveIdle/3, time.Second), 10*time.Millisecond)
}

// keepalive：任何入站帧都刷新存活时间；空闲达到 KeepaliveIdle 发 PING，单次等待 PingWait 后未收到回应
// （且期间没有任何帧）再发；自最后一次收到帧起 DeadAfter 未收到任何帧即判定失效并关闭链路。
func (l *Link) keepalive() {
	if l.isClosed() {
		return
	}
	since := l.SinceLastFrame()
	if since >= l.opt.DeadAfter {
		l.fail(fmt.Errorf("%w: %v since last frame", ErrLinkDead, since.Round(time.Millisecond)), false)
		return
	}
	if since >= l.opt.KeepaliveIdle {
		l.kaMu.Lock()
		due := !l.kaOut || time.Since(l.kaSent) >= l.opt.PingWait
		l.kaMu.Unlock()
		if due {
			l.sendKeepalive()
		}
	}
	l.kaMu.Lock()
	if l.kaTimer != nil {
		l.kaTimer.Reset(l.kaNext())
	}
	l.kaMu.Unlock()
}

func (l *Link) Peer() PeerInfo                { return l.peer }
func (l *Link) Done() <-chan struct{}         { return l.done }
func (l *Link) RTT() time.Duration            { return time.Duration(l.rtt.Load()) }
func (l *Link) mono() int64                   { return int64(time.Since(l.start)) }
func (l *Link) SinceLastFrame() time.Duration { return time.Duration(l.mono() - l.lastRecv.Load()) }

// Err 返回链路关闭的原因；链路仍在运行时为 nil。
func (l *Link) Err() error {
	select {
	case <-l.done:
		return l.err
	default:
		return nil
	}
}

// NumStreams 返回当前未结束的流数。
func (l *Link) NumStreams() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.streams)
}

// ---------------------------------------------------------------- 写

func (l *Link) enqueue(ctrl bool, f wframe) {
	l.wmu.Lock()
	select {
	case <-l.done:
		l.wmu.Unlock()
		if f.pool != nil {
			framePool.Put(f.pool)
		}
		return
	default:
	}
	if ctrl {
		l.ctrl = append(l.ctrl, f)
	} else {
		l.data = append(l.data, f)
	}
	l.wmu.Unlock()
	select {
	case l.wsig <- struct{}{}:
	default:
	}
}

// enqueueDgram 把 DGRAM 放入独立队列；队列满时丢弃（ENG-048）。
func (l *Link) enqueueDgram(f wframe) bool {
	l.wmu.Lock()
	select {
	case <-l.done:
		l.wmu.Unlock()
		framePool.Put(f.pool)
		return false
	default:
	}
	if l.dgramBytes+len(f.b) > l.opt.DgramQueue {
		l.wmu.Unlock()
		framePool.Put(f.pool)
		l.dstats.DropSendQueue.Add(1)
		return false
	}
	l.dgram = append(l.dgram, f)
	l.dgramBytes += len(f.b)
	l.wmu.Unlock()
	select {
	case l.wsig <- struct{}{}:
	default:
	}
	return true
}

// sendCtrl 发送控制帧。控制帧可以越过排队的 DATA；FIN 必须在本流 DATA 之后，因此走数据队列。
func (l *Link) sendCtrl(t FrameType, sid uint32, payload []byte) {
	l.enqueue(true, wframe{b: AppendFrame(make([]byte, 0, HeaderLen+len(payload)), t, sid, payload)})
}

const writeBatch = 256 << 10

// takeBatchLocked 取出下一批要写的帧：控制帧全部在前；DGRAM 优先于 DATA，但有 DATA 等待时
// DGRAM 不超过本批剩余字节的一半（ENG-048），没有 DATA 时不受此限。
func (l *Link) takeBatchLocked(batch []wframe) []wframe {
	n := 0
	i := 0
	for ; i < len(l.ctrl) && n < writeBatch; i++ {
		batch = append(batch, l.ctrl[i])
		n += len(l.ctrl[i].b)
	}
	l.ctrl = append(l.ctrl[:0], l.ctrl[i:]...)
	limit := writeBatch
	if len(l.data) > 0 {
		limit = n + (writeBatch-n)/2
	}
	for i = 0; i < len(l.dgram) && n < limit; i++ {
		batch = append(batch, l.dgram[i])
		n += len(l.dgram[i].b)
		l.dgramBytes -= len(l.dgram[i].b)
	}
	l.dgram = append(l.dgram[:0], l.dgram[i:]...)
	for i = 0; i < len(l.data) && n < writeBatch; i++ {
		batch = append(batch, l.data[i])
		n += len(l.data[i].b)
	}
	l.data = append(l.data[:0], l.data[i:]...)
	return batch
}

// writeLoop 合并排队的帧，每批一次 TLS 写入（ENG-044 写合并）。控制帧先于数据帧。
func (l *Link) writeLoop() {
	defer l.wg.Done()
	buf := make([]byte, 0, writeBatch+HeaderLen+MaxPayload)
	var batch []wframe
	for {
		select {
		case <-l.wsig:
		case <-l.done:
			return
		}
		for {
			l.wmu.Lock()
			batch = l.takeBatchLocked(batch[:0])
			l.wmu.Unlock()
			if len(batch) == 0 {
				break
			}
			buf = buf[:0]
			for _, f := range batch {
				buf = append(buf, f.b...)
				if f.pool != nil {
					framePool.Put(f.pool)
				}
			}
			if _, err := l.tc.Write(buf); err != nil {
				l.fail(fmt.Errorf("%w: write: %v", ErrLinkClosed, err), false)
				return
			}
		}
	}
}

// ---------------------------------------------------------------- 链路级额度

// takeSend 取得至多 n 字节的链路发送额度；没有额度时阻塞直到 WINDOW、截止时间或链路关闭。
func (l *Link) takeSend(n int64, deadline <-chan struct{}) (int64, error) {
	for {
		l.cmu.Lock()
		if l.sendWin > 0 {
			m := min(n, l.sendWin)
			l.sendWin -= m
			l.cmu.Unlock()
			return m, nil
		}
		w := l.sendWake.wait()
		l.cmu.Unlock()
		select {
		case <-w:
		case <-deadline:
			return 0, errDeadline
		case <-l.done:
			return 0, l.err
		}
	}
}

func (l *Link) returnSend(n int64) {
	if n <= 0 {
		return
	}
	l.cmu.Lock()
	l.sendWin += n
	l.sendWake.broadcast()
	l.cmu.Unlock()
}

// consumed 记录本端消费（或丢弃）了 n 字节，累计到链路窗口一半时归还给对端。
func (l *Link) consumed(n int) {
	if n == 0 {
		return
	}
	l.cmu.Lock()
	l.recvUnack += int64(n)
	var inc int64
	if l.recvUnack >= int64(l.opt.LinkWindow)/2 {
		inc, l.recvUnack = l.recvUnack, 0
		l.recvAvail += inc
	}
	l.cmu.Unlock()
	if inc > 0 {
		l.sendCtrl(TypeWindow, 0, AppendWindowPayload(nil, uint32(inc)))
	}
}

// ---------------------------------------------------------------- 开流与接受

// Open 开流并立即返回（0-RTT，ENG-015）：OPEN 帧已排队，调用方可以马上写数据。
// 开流失败（OPEN_ERR）稍后经 Read / Write 以 *StreamError 返回。
func (l *Link) Open(o *Open) (*Stream, error) {
	payload := o.AppendPayload(nil)
	l.mu.Lock()
	switch {
	case l.isClosed():
		l.mu.Unlock()
		return nil, l.err
	case l.goawayIn:
		l.mu.Unlock()
		return nil, ErrGoaway
	case l.exhausted || l.nextID > l.opt.MaxStreamID || l.nextID < 1:
		first := !l.exhausted
		l.exhausted = true
		last := l.lastPeer
		l.mu.Unlock()
		if first { // 耗尽时发送 GOAWAY，由上层新建链路（C1 第 2 节）
			l.sendCtrl(TypeGoaway, 0, AppendGoawayPayload(nil, last, GoawayStreamIDExhausted))
		}
		return nil, ErrStreamIDs
	}
	id := l.nextID
	if l.nextID > math.MaxUint32-2 {
		l.nextID = 0 // 下一次即判定耗尽
	} else {
		l.nextID += 2
	}
	s := l.newStream(id, true)
	s.open = o
	s.openedAt = time.Now()
	l.streams[id] = s
	// 在持有 l.mu 时排队，保证 OPEN 按 ID 顺序发出（对端要求单调递增）
	l.enqueue(true, wframe{b: AppendFrame(make([]byte, 0, HeaderLen+len(payload)), TypeOpen, id, payload)})
	l.mu.Unlock()
	return s, nil
}

// Accept 返回下一条入站流。调用方完成开流检查后必须调用 Stream.Accept 或 Stream.Reject；
// 在此之前对端的 0-RTT 数据缓存在流窗口之内。
func (l *Link) Accept(ctx context.Context) (*Stream, error) {
	select {
	case s := <-l.acceptCh:
		return s, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-l.done:
		return nil, l.err
	}
}

// Drain 发送 GOAWAY：此后对端新开的流被拒绝（OPEN_ERR no_route），已有流继续直到结束（C1 第 8 节）。
func (l *Link) Drain(code GoawayCode) {
	l.mu.Lock()
	if l.goawayOut || l.isClosed() {
		l.mu.Unlock()
		return
	}
	l.goawayOut, l.goawayAt = true, l.lastPeer
	last := l.lastPeer
	l.mu.Unlock()
	l.sendCtrl(TypeGoaway, 0, AppendGoawayPayload(nil, last, code))
}

// Ping 发送 PING 并等待 PONG，返回往返时间。
func (l *Link) Ping(ctx context.Context) (time.Duration, error) {
	var key [8]byte
	_, _ = rand.Read(key[:])
	ch := make(chan struct{})
	l.pmu.Lock()
	l.pings[key] = ch
	l.pmu.Unlock()
	defer func() {
		l.pmu.Lock()
		delete(l.pings, key)
		l.pmu.Unlock()
	}()
	start := time.Now()
	l.sendCtrl(TypePing, 0, key[:])
	select {
	case <-ch:
		return time.Since(start), nil
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-l.done:
		return 0, l.err
	}
}

// Close 发送 GOAWAY(normal) 后关闭链路，所有流以 ErrLinkClosed 结束。
func (l *Link) Close() error { return l.Shutdown(GoawayNormal) }

// Shutdown 发送 GOAWAY(code) 后立即关闭链路（例如对端证书被吊销时用 GoawayRevoked）。
func (l *Link) Shutdown(code GoawayCode) error {
	if !l.isClosed() {
		_ = l.tc.SetWriteDeadline(time.Now().Add(time.Second))
		l.mu.Lock()
		last := l.lastPeer
		l.mu.Unlock()
		_, _ = l.tc.Write(AppendFrame(nil, TypeGoaway, 0, AppendGoawayPayload(nil, last, code)))
	}
	l.fail(ErrLinkClosed, false)
	l.wg.Wait()
	return nil
}

// InFlight 返回已发出、尚未被对端以 WINDOW 归还的字节（链路级），用于在多条 mux 链路间均衡。
func (l *Link) InFlight() int64 {
	l.cmu.Lock()
	defer l.cmu.Unlock()
	return int64(l.opt.LinkWindow) - l.sendWin
}

// Mode 返回发起方在 HELLO 中声明的链路模式（本端发起时为本端的模式）。
func (l *Link) Mode() LinkMode {
	if l.opt.Initiator {
		return l.opt.Mode
	}
	return l.peer.Hello.LinkMode
}

// GoawayReceived 返回对端发来的 GOAWAY 码（未收到时 ok 为 false）。
func (l *Link) GoawayReceived() (GoawayCode, bool) {
	c := l.goawayCode.Load()
	return GoawayCode(c), c >= 0
}

// CloseGraceful 发送 GOAWAY(code) 后等待已有流结束（或 ctx 结束），再关闭链路（ENG-019 优雅关闭）。
func (l *Link) CloseGraceful(ctx context.Context, code GoawayCode) error {
	l.Drain(code)
	t := time.NewTicker(20 * time.Millisecond)
	defer t.Stop()
	for l.NumStreams() > 0 {
		select {
		case <-ctx.Done():
			return l.Shutdown(code)
		case <-l.done:
			return nil
		case <-t.C:
		}
	}
	return l.Shutdown(code)
}

// Usable 表示还能在这条链路上开新流（未关闭、未收到 GOAWAY、流 ID 未耗尽）。
func (l *Link) Usable() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return !l.isClosed() && !l.goawayIn && !l.exhausted
}

// ConnectionState 返回 TLS 连接状态（对端证书用于吊销检查）。
func (l *Link) ConnectionState() tls.ConnectionState { return l.tc.ConnectionState() }

func (l *Link) isClosed() bool {
	select {
	case <-l.done:
		return true
	default:
		return false
	}
}

// fail 关闭链路；protocol 为真时先发送 GOAWAY(protocol_error)。
func (l *Link) fail(err error, protocol bool) {
	l.closeOnce.Do(func() {
		if protocol {
			_ = l.tc.SetWriteDeadline(time.Now().Add(time.Second))
			l.mu.Lock()
			last := l.lastPeer
			l.mu.Unlock()
			_, _ = l.tc.Write(AppendFrame(nil, TypeGoaway, 0, AppendGoawayPayload(nil, last, GoawayProtocolError)))
		}
		l.kaMu.Lock()
		if l.kaTimer != nil {
			l.kaTimer.Stop()
			l.kaTimer = nil
		}
		l.kaMu.Unlock()
		l.wmu.Lock()
		l.err = err
		close(l.done)
		for _, q := range [][]wframe{l.ctrl, l.data} {
			for _, f := range q {
				if f.pool != nil {
					framePool.Put(f.pool)
				}
			}
		}
		for _, f := range l.dgram {
			framePool.Put(f.pool)
		}
		l.ctrl, l.data, l.dgram, l.dgramBytes = nil, nil, nil, 0
		l.wmu.Unlock()
		_ = l.tc.Close()
		l.mu.Lock()
		ss := make([]*Stream, 0, len(l.streams))
		for _, s := range l.streams {
			ss = append(ss, s)
		}
		l.streams = map[uint32]*Stream{}
		l.mu.Unlock()
		for _, s := range ss {
			s.terminate(fmt.Errorf("%w: %v", ErrLinkClosed, err), false)
		}
		l.cmu.Lock()
		l.sendWake.broadcast()
		l.cmu.Unlock()
	})
}

// ---------------------------------------------------------------- 读

func (l *Link) protoErr(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrProtocol}, args...)...)
}

// seen 判断流 ID 是否属于已经使用过（现已结束）的流：这些流的迟到帧被丢弃而不是协议错误。
func (l *Link) seenLocked(id uint32) bool {
	if (id%2 == 1) == l.opt.Initiator { // 本端奇偶
		return id < l.nextID || l.nextID == 0
	}
	return id <= l.lastPeer
}

func (l *Link) readLoop() {
	defer l.wg.Done()
	for {
		h, p, err := l.r.Next()
		if err != nil {
			if errors.Is(err, ErrProtocol) {
				l.fail(err, true)
			} else if errors.Is(err, io.EOF) {
				l.fail(fmt.Errorf("%w: peer closed", ErrLinkClosed), false)
			} else {
				l.fail(fmt.Errorf("%w: read: %v", ErrLinkClosed, err), false)
			}
			return
		}
		l.lastRecv.Store(l.mono())
		if err := l.dispatch(h, p); err != nil {
			l.fail(err, errors.Is(err, ErrProtocol))
			return
		}
	}
}

func (l *Link) dispatch(h Header, p []byte) error {
	switch h.Type {
	case TypeHello:
		return l.protoErr("second HELLO")
	case TypePing:
		if len(p) != 8 {
			return l.protoErr("PING payload %d bytes", len(p))
		}
		l.sendCtrl(TypePong, 0, p)
		return nil
	case TypePong:
		if len(p) != 8 {
			return l.protoErr("PONG payload %d bytes", len(p))
		}
		l.pmu.Lock()
		if ch := l.pings[[8]byte(p)]; ch != nil {
			close(ch)
			delete(l.pings, [8]byte(p))
		}
		l.pmu.Unlock()
		l.kaMu.Lock()
		if l.kaOut && [8]byte(p) == l.kaKey {
			l.kaOut = false
			l.rtt.Store(int64(max(time.Since(l.kaSent), 100*time.Microsecond)))
		}
		l.kaMu.Unlock()
		return nil
	case TypeGoaway:
		_, code, err := ParseGoawayPayload(p)
		if err != nil {
			return err
		}
		l.mu.Lock()
		l.goawayIn = true
		l.mu.Unlock()
		l.goawayCode.Store(int32(code))
		return nil
	case TypeWindow:
		inc, err := ParseWindowPayload(p)
		if err != nil {
			return err
		}
		if h.StreamID == 0 {
			l.cmu.Lock()
			l.sendWin += int64(inc)
			if l.sendWin > math.MaxInt32 {
				l.cmu.Unlock()
				return l.protoErr("link window overflow")
			}
			l.sendWake.broadcast()
			l.cmu.Unlock()
			return nil
		}
	case TypeOpen:
		return l.onOpen(h.StreamID, p)
	case TypeDgram:
		l.mu.Lock()
		s := l.streams[h.StreamID]
		l.mu.Unlock()
		switch {
		case s == nil:
			l.dstats.DropUnknown.Add(1)
		case s.open == nil || s.open.Network != types.NetworkUDP:
			l.dstats.DropNotDatagram.Add(1)
		default:
			l.dstats.Received.Add(1)
			s.onDgram(p)
		}
		return nil
	}

	l.mu.Lock()
	s := l.streams[h.StreamID]
	if s == nil {
		seen := l.seenLocked(h.StreamID)
		l.mu.Unlock()
		if !seen {
			return l.protoErr("%v for unknown stream %d", h.Type, h.StreamID)
		}
		if h.Type == TypeData { // 已结束的流的迟到数据：丢弃并归还链路额度
			if err := l.chargeRecv(len(p)); err != nil {
				return err
			}
			l.consumed(len(p))
		}
		return nil
	}
	l.mu.Unlock()

	switch h.Type {
	case TypeData:
		if err := l.chargeRecv(len(p)); err != nil {
			return err
		}
		return s.onData(p)
	case TypeFin:
		return s.onFin()
	case TypeRst, TypeOpenErr:
		code, text, err := ParseErrPayload(p)
		if err != nil {
			return err
		}
		if h.Type == TypeOpenErr && !s.local {
			return l.protoErr("OPEN_ERR for inbound stream %d", h.StreamID)
		}
		s.terminate(&StreamError{Code: code, Text: text, Open: h.Type == TypeOpenErr, Remote: true}, false)
	case TypeOpenOK:
		if !s.local {
			return l.protoErr("OPEN_OK for inbound stream %d", h.StreamID)
		}
		s.mu.Lock()
		if !s.openedAt.IsZero() {
			s.openRTT = time.Since(s.openedAt)
		}
		s.mu.Unlock()
	case TypeWindow:
		inc, _ := ParseWindowPayload(p)
		return s.onWindow(inc)
	}
	return nil
}

// chargeRecv 按链路级窗口记账；超出即协议错误（对端违反流控）。
func (l *Link) chargeRecv(n int) error {
	l.cmu.Lock()
	defer l.cmu.Unlock()
	if int64(n) > l.recvAvail {
		return l.protoErr("link window exceeded")
	}
	l.recvAvail -= int64(n)
	return nil
}

func (l *Link) onOpen(id uint32, p []byte) error {
	if (id%2 == 1) == l.opt.Initiator {
		return l.protoErr("OPEN with local stream id %d", id)
	}
	l.mu.Lock()
	if id <= l.lastPeer {
		l.mu.Unlock()
		return l.protoErr("OPEN stream id %d not increasing (last %d)", id, l.lastPeer)
	}
	l.lastPeer = id
	reject := l.goawayOut && id > l.goawayAt
	l.mu.Unlock()
	if reject {
		l.sendCtrl(TypeOpenErr, id, AppendErrPayload(nil, CodeNoRoute, "draining"))
		return nil
	}
	o, err := ParseOpen(p)
	if err != nil {
		l.sendCtrl(TypeOpenErr, id, AppendErrPayload(nil, CodeUnauthorized, ""))
		return nil
	}
	s := l.newStream(id, false)
	s.open = o
	l.mu.Lock()
	if l.isClosed() { // 链路已关闭并清空了流表（fail 先关闭 done、再在 l.mu 下清空）：不再登记
		l.mu.Unlock()
		s.terminate(ErrLinkClosed, false)
		return nil
	}
	l.streams[id] = s
	l.mu.Unlock()
	select {
	case l.acceptCh <- s:
	default:
		s.Reject(CodeNoRoute, "accept queue full")
	}
	return nil
}

func (l *Link) remove(s *Stream) {
	l.mu.Lock()
	if l.streams[s.id] == s {
		delete(l.streams, s.id)
	}
	l.mu.Unlock()
}
