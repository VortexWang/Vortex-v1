package vlink

import (
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

var errDeadline = os.ErrDeadlineExceeded

// Stream 是链路上的一条 TCP 流，实现 net.Conn 与 CloseWrite（半关闭，ENG-005）。
// 同一方向不支持并发调用（与 pipe 的使用方式一致：每个方向一个 goroutine）。
type Stream struct {
	l     *Link
	id    uint32
	local bool // 本端开的流
	open  *Open

	mu sync.Mutex
	// 接收
	rbuf      [][]byte // 每块来自 framePool
	rpool     []*[]byte
	roff      int // rbuf[0] 中已读取的位置
	rlen      int
	rwake     broadcaster
	remoteFin bool
	recvAvail int64  // 对端还可在本流发送的字节
	recvWin   uint32 // 当前接收窗口（自适应增长）
	recvUnack int64
	lastGrant time.Time
	// UDP 会话（OPEN 的网络类型为 udp）的入站数据报
	dq       [][]byte
	dqPool   []*[]byte
	dhandler func([]byte)
	onClose  []func()
	// 发送
	sendWin  int64
	wwake    broadcaster
	localFin bool
	// 结束
	err      error // 非 nil：流已终止（RST、OPEN_ERR 或链路关闭）
	accepted bool
	// 截止时间
	rdl, wdl time.Time
	rdlWake  chan struct{} // 截止时间到达时关闭并替换
	wdlWake  chan struct{}
	rTimer   *time.Timer
	wTimer   *time.Timer
	openedAt time.Time
	openRTT  time.Duration
}

func (l *Link) newStream(id uint32, local bool) *Stream {
	return &Stream{l: l, id: id, local: local, recvAvail: int64(l.opt.StreamWindow), recvWin: l.opt.StreamWindow,
		sendWin: int64(l.opt.StreamWindow), lastGrant: time.Now(), rdlWake: make(chan struct{}), wdlWake: make(chan struct{})}
}

func (s *Stream) ID() uint32 { return s.id }

// OpenInfo 返回开流载荷（入站流为对端发来的 OPEN，出站流为本端发送的 OPEN）。
func (s *Stream) OpenInfo() *Open { return s.open }

// OpenRTT 返回 OPEN → OPEN_OK 的时间（仅出站流，收到 OPEN_OK 之前为 0）。
func (s *Stream) OpenRTT() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.openRTT
}

// RecvWindow 返回当前接收窗口（测试与指标用）。
func (s *Stream) RecvWindow() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recvWin
}

// Accept 确认入站流（发送 OPEN_OK，仅用于统计与时延测量，ENG-015）。
func (s *Stream) Accept() {
	s.mu.Lock()
	if s.local || s.accepted || s.err != nil {
		s.mu.Unlock()
		return
	}
	s.accepted = true
	s.mu.Unlock()
	s.l.sendCtrl(TypeOpenOK, s.id, nil)
}

// Reject 拒绝入站流（OPEN_ERR），丢弃已缓存的 0-RTT 数据。
func (s *Stream) Reject(code ErrorCode, text string) {
	s.mu.Lock()
	if s.local || s.accepted || s.err != nil {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	s.l.sendCtrl(TypeOpenErr, s.id, AppendErrPayload(nil, code, text))
	s.terminate(&StreamError{Code: code, Text: text, Open: true}, true)
}

// Reset 立即终止流（RST），原因码随帧发给对端。
func (s *Stream) Reset(code ErrorCode, text string) {
	s.mu.Lock()
	if s.err != nil {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	s.l.sendCtrl(TypeRst, s.id, AppendErrPayload(nil, code, text))
	s.terminate(&StreamError{Code: code, Text: text}, true)
}

// terminate 结束流：唤醒等待者，归还未读数据占用的链路额度，从链路摘除。
func (s *Stream) terminate(err error, local bool) {
	s.mu.Lock()
	if s.err != nil {
		s.mu.Unlock()
		return
	}
	s.err = err
	unread := s.rlen
	s.dropBuf()
	s.rwake.broadcast()
	s.wwake.broadcast()
	cbs := s.onClose
	s.onClose = nil
	s.mu.Unlock()
	s.l.consumed(unread)
	s.l.remove(s) // 先摘除再回调：回调中看到的流数已不含本流（pool 链路据此归还）
	for _, f := range cbs {
		f()
	}
}

func (s *Stream) dropBuf() {
	for _, p := range s.rpool {
		framePool.Put(p)
	}
	s.rbuf, s.rpool, s.roff, s.rlen = nil, nil, 0, 0
	for _, p := range s.dqPool {
		framePool.Put(p)
	}
	s.dq, s.dqPool = nil, nil
}

// finishedLocked：两个方向都已结束，可以从链路摘除。
func (s *Stream) finishedLocked() bool { return s.localFin && s.remoteFin }

// ---------------------------------------------------------------- 读循环回调（链路读 goroutine）

func (s *Stream) onData(p []byte) error {
	s.mu.Lock()
	if s.remoteFin {
		s.mu.Unlock()
		return s.l.protoErr("DATA after FIN on stream %d", s.id)
	}
	if int64(len(p)) > s.recvAvail {
		s.mu.Unlock()
		return s.l.protoErr("stream %d window exceeded", s.id)
	}
	s.recvAvail -= int64(len(p))
	if s.err != nil { // 本端已终止：丢弃
		s.mu.Unlock()
		s.l.consumed(len(p))
		return nil
	}
	if len(p) > 0 {
		bp := framePool.Get().(*[]byte)
		b := append((*bp)[:0], p...)
		s.rbuf = append(s.rbuf, b)
		s.rpool = append(s.rpool, bp)
		s.rlen += len(b)
		s.rwake.broadcast()
	}
	s.mu.Unlock()
	return nil
}

func (s *Stream) onFin() error {
	s.mu.Lock()
	if s.remoteFin {
		s.mu.Unlock()
		return s.l.protoErr("duplicate FIN on stream %d", s.id)
	}
	s.remoteFin = true
	s.rwake.broadcast()
	done := s.finishedLocked()
	s.mu.Unlock()
	if done {
		s.l.remove(s)
	}
	return nil
}

func (s *Stream) onWindow(inc uint32) error {
	s.mu.Lock()
	s.sendWin += int64(inc)
	over := s.sendWin > 1<<31-1
	s.wwake.broadcast()
	s.mu.Unlock()
	if over {
		return s.l.protoErr("stream %d window overflow", s.id)
	}
	return nil
}

// ---------------------------------------------------------------- net.Conn

// Read 读取对端数据；对端 FIN 且缓冲读空后返回 io.EOF。消费的字节归还流窗口与链路窗口。
func (s *Stream) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	for {
		s.mu.Lock()
		if s.rlen > 0 {
			n := 0
			for n < len(b) && len(s.rbuf) > 0 {
				c := copy(b[n:], s.rbuf[0][s.roff:])
				n += c
				s.roff += c
				if s.roff == len(s.rbuf[0]) {
					framePool.Put(s.rpool[0])
					s.rbuf[0], s.rpool[0] = nil, nil
					s.rbuf, s.rpool, s.roff = s.rbuf[1:], s.rpool[1:], 0
				}
			}
			s.rlen -= n
			inc := s.grantLocked(n)
			s.mu.Unlock()
			if inc > 0 {
				s.l.sendCtrl(TypeWindow, s.id, AppendWindowPayload(nil, inc))
			}
			s.l.consumed(n)
			return n, nil
		}
		if s.err != nil {
			err := s.err
			s.mu.Unlock()
			return 0, err
		}
		if s.remoteFin {
			s.mu.Unlock()
			return 0, io.EOF
		}
		if !s.rdl.IsZero() && !time.Now().Before(s.rdl) {
			s.mu.Unlock()
			return 0, errDeadline
		}
		w, dl := s.rwake.wait(), s.rdlWake
		s.mu.Unlock()
		select {
		case <-w:
		case <-dl:
		}
	}
}

// grantLocked 记录本流消费了 n 字节；累计到窗口一半时归还。若一个窗口在 2 个 RTT 内就被用完，
// 说明窗口限制了吞吐，把窗口翻倍直到上限（C1 第 7 节自适应窗口）。
func (s *Stream) grantLocked(n int) uint32 {
	if s.remoteFin {
		return 0
	}
	s.recvUnack += int64(n)
	if s.recvUnack < int64(s.recvWin)/2 {
		return 0
	}
	grow := uint32(0)
	now := time.Now()
	if now.Sub(s.lastGrant) < 2*s.l.RTT() && s.recvWin < s.l.opt.MaxStreamWindow {
		nw := min(s.recvWin*2, s.l.opt.MaxStreamWindow)
		grow, s.recvWin = nw-s.recvWin, nw
	}
	inc := uint32(s.recvUnack) + grow
	s.recvAvail += int64(inc)
	s.recvUnack = 0
	s.lastGrant = now
	return inc
}

// Write 按流窗口与链路窗口发送 DATA，单帧不超过 16 KiB。数据复制进帧缓冲后即返回，不等待写出。
func (s *Stream) Write(b []byte) (int, error) {
	written := 0
	for written < len(b) {
		s.mu.Lock()
		switch {
		case s.err != nil:
			err := s.err
			s.mu.Unlock()
			return written, err
		case s.localFin:
			s.mu.Unlock()
			return written, ErrWriteAfterFin
		case !s.wdl.IsZero() && !time.Now().Before(s.wdl):
			s.mu.Unlock()
			return written, errDeadline
		}
		if s.sendWin <= 0 {
			w, dl := s.wwake.wait(), s.wdlWake
			s.mu.Unlock()
			select {
			case <-w:
			case <-dl:
			case <-s.l.done:
			}
			continue
		}
		want := min(int64(len(b)-written), int64(MaxPayload), s.sendWin)
		dl := s.wdlWake
		s.mu.Unlock()

		got, err := s.l.takeSend(want, dl)
		if err != nil {
			if err == errDeadline {
				continue // 回到循环顶部判定截止时间
			}
			return written, err
		}
		s.mu.Lock()
		if s.err != nil || s.localFin {
			s.mu.Unlock()
			s.l.returnSend(got)
			continue
		}
		s.sendWin -= got
		bp := framePool.Get().(*[]byte)
		*bp = AppendFrame((*bp)[:0], TypeData, s.id, b[written:written+int(got)])
		// 在持有流锁时排队：同一流的 DATA 与 FIN 按调用顺序进入数据队列
		s.l.enqueue(false, wframe{b: *bp, pool: bp})
		s.mu.Unlock()
		written += int(got)
	}
	return written, nil
}

// CloseWrite 发送 FIN（本方向写结束）。
func (s *Stream) CloseWrite() error {
	s.mu.Lock()
	if s.err != nil {
		err := s.err
		s.mu.Unlock()
		return err
	}
	if s.localFin {
		s.mu.Unlock()
		return nil
	}
	s.localFin = true
	s.l.enqueue(false, wframe{b: AppendFrame(make([]byte, 0, HeaderLen), TypeFin, s.id, nil)})
	s.wwake.broadcast()
	done := s.finishedLocked()
	s.mu.Unlock()
	if done {
		s.l.remove(s)
	}
	return nil
}

// Close 结束两个方向：两个方向都已正常结束时只做清理；对端已 FIN 时补发 FIN；
// 否则发送 RST(no_route)（契约不设 internal_error，R4），让对端停止发送。
func (s *Stream) Close() error {
	s.mu.Lock()
	if s.err != nil {
		s.mu.Unlock()
		s.l.remove(s)
		return nil
	}
	remoteFin, localFin := s.remoteFin, s.localFin
	s.mu.Unlock()
	switch {
	case remoteFin && localFin:
	case remoteFin:
		_ = s.CloseWrite()
	default:
		s.Reset(CodeNoRoute, "")
		return nil
	}
	s.terminate(ErrStreamClosed, true)
	return nil
}

func (s *Stream) LocalAddr() net.Addr  { return s.l.tc.LocalAddr() }
func (s *Stream) RemoteAddr() net.Addr { return s.l.tc.RemoteAddr() }

func (s *Stream) SetDeadline(t time.Time) error {
	_ = s.SetReadDeadline(t)
	return s.SetWriteDeadline(t)
}

// SetReadDeadline：到期唤醒阻塞的 Read。pipe 每次读取前都会设置，定时器只创建一次并复用。
func (s *Stream) SetReadDeadline(t time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rdl = t
	s.rTimer = s.armLocked(s.rTimer, t, s.fireRead)
	return nil
}

func (s *Stream) SetWriteDeadline(t time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.wdl = t
	s.wTimer = s.armLocked(s.wTimer, t, s.fireWrite)
	return nil
}

// fire 只在截止时间确实已到时唤醒（定时器重置前已触发的旧回调不会误唤醒）。
func (s *Stream) fireRead() {
	s.mu.Lock()
	if !s.rdl.IsZero() && !time.Now().Before(s.rdl) {
		close(s.rdlWake)
		s.rdlWake = make(chan struct{})
	}
	s.mu.Unlock()
}

func (s *Stream) fireWrite() {
	s.mu.Lock()
	if !s.wdl.IsZero() && !time.Now().Before(s.wdl) {
		close(s.wdlWake)
		s.wdlWake = make(chan struct{})
	}
	s.mu.Unlock()
}

func (s *Stream) armLocked(tm *time.Timer, t time.Time, fire func()) *time.Timer {
	if t.IsZero() {
		if tm != nil {
			tm.Stop()
		}
		return tm
	}
	d := max(time.Until(t), 0)
	if tm == nil {
		return time.AfterFunc(d, fire)
	}
	tm.Reset(d)
	return tm
}

// Err 返回流的终止原因（RST、OPEN_ERR、链路关闭、本端关闭）；流仍在进行时为 nil。
func (s *Stream) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// ---------------------------------------------------------------- UDP 会话（ENG-041、ENG-047、ENG-048）

var (
	ErrDgramTooLarge = errors.New("vlink: datagram exceeds single frame payload")
	ErrDgramDropped  = errors.New("vlink: datagram dropped (link send queue full)")
)

// onDgram 在链路读 goroutine 中调用。设置了处理函数时直接交给它（p 只在调用期间有效）；
// 否则排队（例如开流检查尚未完成时的 0-RTT 数据报），队列满时丢弃。
func (s *Stream) onDgram(p []byte) {
	s.mu.Lock()
	if s.err != nil {
		s.mu.Unlock()
		s.l.dstats.DropUnknown.Add(1)
		return
	}
	if h := s.dhandler; h != nil {
		s.mu.Unlock()
		h(p)
		return
	}
	if len(s.dq) >= s.l.opt.DgramRecvQueue {
		s.mu.Unlock()
		s.l.dstats.DropRecvQueue.Add(1)
		return
	}
	bp := framePool.Get().(*[]byte)
	*bp = append((*bp)[:0], p...)
	s.dq = append(s.dq, *bp)
	s.dqPool = append(s.dqPool, bp)
	s.rwake.broadcast()
	s.mu.Unlock()
}

// OnDgram 改为回调方式接收：先按顺序交付已排队的数据报，之后在链路读 goroutine 中直接调用 h。
// h 不得阻塞（会阻塞整条链路的读取）；p 只在调用期间有效。
func (s *Stream) OnDgram(h func(p []byte)) {
	for {
		s.mu.Lock()
		q, qp := s.dq, s.dqPool
		s.dq, s.dqPool = nil, nil
		if len(q) == 0 {
			if s.err == nil {
				s.dhandler = h
			}
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
		for i, p := range q {
			h(p)
			framePool.Put(qp[i])
		}
	}
}

// ReadDgram 读取一个数据报（阻塞，受读截止时间约束）；数据报大于 b 时截断。
func (s *Stream) ReadDgram(b []byte) (int, error) {
	for {
		s.mu.Lock()
		if len(s.dq) > 0 {
			n := copy(b, s.dq[0])
			framePool.Put(s.dqPool[0])
			s.dq[0], s.dqPool[0] = nil, nil
			s.dq, s.dqPool = s.dq[1:], s.dqPool[1:]
			s.mu.Unlock()
			return n, nil
		}
		if s.err != nil {
			err := s.err
			s.mu.Unlock()
			return 0, err
		}
		if !s.rdl.IsZero() && !time.Now().Before(s.rdl) {
			s.mu.Unlock()
			return 0, errDeadline
		}
		w, dl := s.rwake.wait(), s.rdlWake
		s.mu.Unlock()
		select {
		case <-w:
		case <-dl:
		}
	}
}

// SendDgram 发送一个完整数据报。不受流控窗口约束，不阻塞；超过单帧上限或链路发送队列满时丢弃并计数。
func (s *Stream) SendDgram(p []byte) error {
	if len(p) > MaxPayload {
		s.l.dstats.DropOversize.Add(1)
		return ErrDgramTooLarge
	}
	s.mu.Lock()
	err := s.err
	s.mu.Unlock()
	if err != nil {
		return err
	}
	bp := framePool.Get().(*[]byte)
	*bp = AppendFrame((*bp)[:0], TypeDgram, s.id, p)
	if !s.l.enqueueDgram(wframe{b: *bp, pool: bp}) {
		if e := s.l.Err(); e != nil {
			return e
		}
		return ErrDgramDropped
	}
	s.l.dstats.Sent.Add(1)
	return nil
}

// DgramConn 把 UDP 会话流包装成与已连接 UDP socket 相同的接口（Read / Write 一次一个数据报）。
type DgramConn struct{ S *Stream }

func (c DgramConn) Read(b []byte) (int, error) { return c.S.ReadDgram(b) }
func (c DgramConn) Write(b []byte) (int, error) {
	if err := c.S.SendDgram(b); err != nil {
		return 0, err
	}
	return len(b), nil
}
func (c DgramConn) SetReadDeadline(t time.Time) error { return c.S.SetReadDeadline(t) }
func (c DgramConn) Close() error                      { return c.S.Close() }

// OnClose 登记流终止时调用的函数（流已终止时立即调用）。
func (s *Stream) OnClose(f func()) {
	s.mu.Lock()
	if s.err != nil {
		s.mu.Unlock()
		f()
		return
	}
	s.onClose = append(s.onClose, f)
	s.mu.Unlock()
}

// Link 返回流所在的链路。
func (s *Stream) Link() *Link { return s.l }
