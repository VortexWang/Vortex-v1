// Package pipe 实现会话的双向拷贝（需求 8.1、8.3 节）：
//   - 两端都是 *net.TCPConn 时走 splice 零拷贝（ENG-004），否则走分级缓冲池（ENG-007）；
//   - 半关闭正确传递（ENG-005）；会话级空闲超时（ENG-006）；
//   - 计量与限速在拷贝点通过 Hook 执行，不包装连接（ENG-004、ENG-026、ENG-037）；
//   - 每个会话最多 2 个 goroutine：调用方 goroutine 负责下行，另起 1 个负责上行（ENG-045）。
package pipe

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"vortex/engine/types"
)

// Hook 是拷贝点的门控与计量接口，由 gate / meter 实现。
type Hook interface {
	// Acquire 在每次拷贝前调用，返回本次最多允许拷贝的字节数（令牌与预扣块共同决定）。
	// 可以阻塞（例如等待令牌）；返回 0 表示该方向必须停止（额度耗尽、限制生效），会话随之结束。
	Acquire(ctx context.Context, d types.Direction, want int) (int, error)
	// Release 报告与上一次 Acquire 对应的实际写成功字节数（计量点在“写成功”处，ENG-036）；余量退回。
	// 每次 Acquire 恰好对应一次 Release（ENG-037：每笔预扣只能提交或退回一次）。
	Release(d types.Direction, granted, written int)
}

// NopHook 不限速、不计量。
type NopHook struct{}

func (NopHook) Acquire(_ context.Context, _ types.Direction, want int) (int, error) { return want, nil }
func (NopHook) Release(types.Direction, int, int)                                   {}

var (
	ErrIdleTimeout = errors.New("pipe: idle timeout")
	ErrStopped     = errors.New("pipe: stopped by hook")
)

type Options struct {
	Hook        Hook
	IdleTimeout time.Duration // 0 = 不超时（TCP 默认）
	// Go 用于启动上行 goroutine；生产中传入 lifecycle.Scope.Go 以满足 ENG-008。nil 时使用 go 语句。
	Go func(func())
	// DisableSplice 强制走缓冲路径（测试与对比用）。
	DisableSplice bool
}

type Result struct {
	Up, Down int64 // 写成功字节数
	Spliced  bool
	Err      error // nil 表示两个方向都正常结束（EOF）
}

// 统计：供指标与测试使用。
var splicedBytes atomic.Int64

// SplicedBytes 返回进程内经 splice 路径传输的累计字节数。
func SplicedBytes() int64 { return splicedBytes.Load() }

type halfCloser interface{ CloseWrite() error }

// session 保存两个方向共享的状态。
type session struct {
	a, b      net.Conn // a：客户端侧（上行读端）；b：目标或下一跳侧（上行写端）
	hook      Hook
	idle      time.Duration
	lastNano  atomic.Int64 // 最近一次任一方向成功读到数据的单调时间
	start     time.Time
	abortOnce sync.Once
	abortErr  atomic.Pointer[error] // 两个方向并发读取，必须原子访问
}

func (s *session) aborted() error {
	if p := s.abortErr.Load(); p != nil {
		return *p
	}
	return nil
}

func (s *session) touch() { s.lastNano.Store(int64(time.Since(s.start))) }

// idleExpired 判断整个会话是否空闲超时（任一方向有流量即视为活跃）。
func (s *session) idleExpired() bool {
	return time.Since(s.start)-time.Duration(s.lastNano.Load()) >= s.idle
}

func (s *session) armRead(c net.Conn) {
	if s.idle > 0 {
		_ = c.SetReadDeadline(time.Now().Add(s.idle))
	}
}

// armWrite 防止对端不再读取时写入永久阻塞：写入超过空闲时长即视为空闲超时。
func (s *session) armWrite(c net.Conn) {
	if s.idle > 0 {
		_ = c.SetWriteDeadline(time.Now().Add(s.idle))
	}
}

// writeErr 把写截止时间到达映射为空闲超时（未被中止时）。
func (s *session) writeErr(err error) error {
	if s.idle > 0 && errors.Is(err, os.ErrDeadlineExceeded) && s.aborted() == nil {
		return ErrIdleTimeout
	}
	return err
}

// abort 让另一方向立即退出：设置过去的截止时间而不关闭连接，关闭由会话所有者负责。
func (s *session) abort(err error) {
	s.abortOnce.Do(func() {
		s.abortErr.Store(&err)
		past := time.Unix(1, 0)
		_ = s.a.SetDeadline(past)
		_ = s.b.SetDeadline(past)
	})
}

// Run 在 a 与 b 之间双向拷贝，直到两个方向都结束或出错。返回时不关闭连接。
func Run(ctx context.Context, a, b net.Conn, opt Options) Result {
	if opt.Hook == nil {
		opt.Hook = NopHook{}
	}
	s := &session{a: a, b: b, hook: opt.Hook, idle: opt.IdleTimeout, start: time.Now()}
	s.touch()

	ta, okA := a.(*net.TCPConn)
	tb, okB := b.(*net.TCPConn)
	useSplice := okA && okB && !opt.DisableSplice

	stop := context.AfterFunc(ctx, func() { s.abort(ctx.Err()) })
	defer stop()

	var up int64
	var upErr error
	done := make(chan struct{})
	spawn := opt.Go
	if spawn == nil {
		spawn = func(f func()) { go f() }
	}
	spawn(func() {
		defer close(done)
		if useSplice {
			up, upErr = s.spliceDir(ctx, ta, tb, types.DirUp)
		} else {
			up, upErr = s.copyDir(ctx, a, b, types.DirUp)
		}
		if upErr != nil {
			s.abort(upErr)
		}
	})
	var down int64
	var downErr error
	if useSplice {
		down, downErr = s.spliceDir(ctx, tb, ta, types.DirDown)
	} else {
		down, downErr = s.copyDir(ctx, b, a, types.DirDown)
	}
	if downErr != nil {
		s.abort(downErr)
	}
	<-done

	res := Result{Up: up, Down: down, Spliced: useSplice}
	s.abortOnceRead(&res, upErr, downErr)
	return res
}

func (s *session) abortOnceRead(res *Result, upErr, downErr error) {
	// 优先报告首先触发中止的原因；被中止方向产生的截止时间错误只是连带结果。
	if err := s.aborted(); err != nil {
		res.Err = err
		return
	}
	if upErr != nil {
		res.Err = upErr
	} else if downErr != nil {
		res.Err = downErr
	}
}

// readErr 处理读错误：截止时间到达时，若会话整体仍活跃则续期，否则空闲超时。
// 返回 (继续读, 最终错误)。
func (s *session) readErr(c net.Conn, err error) (bool, error) {
	if s.idle > 0 && errors.Is(err, os.ErrDeadlineExceeded) && s.aborted() == nil {
		if !s.idleExpired() {
			s.armRead(c)
			return true, nil
		}
		return false, ErrIdleTimeout
	}
	return false, err
}

func closeWrite(c net.Conn) {
	if hc, ok := c.(halfCloser); ok {
		_ = hc.CloseWrite() // ENG-005：只关闭对应写方向
	}
}

// copyDir 是缓冲路径：分级缓冲池，稳态无每包分配（ENG-007）。
func (s *session) copyDir(ctx context.Context, src, dst net.Conn, d types.Direction) (int64, error) {
	var total int64
	tier := 0
	buf := getBuf(tier)
	defer func() { putBuf(buf) }()
	full := 0
	s.armRead(src)
	for {
		granted, err := s.hook.Acquire(ctx, d, len(*buf))
		if err != nil {
			return total, err
		}
		if granted <= 0 {
			return total, ErrStopped
		}
		if granted > len(*buf) {
			granted = len(*buf)
		}
		n, rerr := src.Read((*buf)[:granted])
		written := 0
		if n > 0 {
			s.touch()
			s.armRead(src)
			s.armWrite(dst)
			for written < n {
				m, werr := dst.Write((*buf)[written:n])
				written += m
				if werr != nil {
					s.hook.Release(d, granted, written)
					return total + int64(written), s.writeErr(werr)
				}
			}
			total += int64(n)
			// 自适应升级缓冲档位：连续读满则换更大的缓冲（16 KiB → 64 KiB → 256 KiB）
			if n == len(*buf) && tier < len(tiers)-1 {
				if full++; full >= 4 {
					putBuf(buf)
					tier++
					buf = getBuf(tier)
					full = 0
				}
			} else {
				full = 0
			}
		}
		s.hook.Release(d, granted, written)
		if rerr != nil {
			if rerr == io.EOF {
				closeWrite(dst)
				return total, nil
			}
			if again, ferr := s.readErr(src, rerr); again {
				continue
			} else {
				return total, ferr
			}
		}
	}
}

// ---- 分级缓冲池 ----

var tiers = [...]int{16 << 10, 64 << 10, 256 << 10}

var pools = func() (p [len(tiers)]sync.Pool) {
	for i := range p {
		size := tiers[i]
		p[i].New = func() any { b := make([]byte, size); return &b }
	}
	return
}()

func getBuf(tier int) *[]byte { return pools[tier].Get().(*[]byte) }

func putBuf(b *[]byte) {
	for i, size := range tiers {
		if len(*b) == size {
			pools[i].Put(b)
			return
		}
	}
}
