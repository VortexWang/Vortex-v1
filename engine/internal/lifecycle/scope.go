// Package lifecycle 提供引擎内统一的资源归属框架（ENG-008）：
// 每个 goroutine 与需要关闭的资源都必须归属于某个 Scope（监听、链路、会话）；
// 关闭操作幂等且级联，会话结束后不得残留 goroutine 与文件描述符。
package lifecycle

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
)

// 全局计数，供指标 vortex_engine_goroutines 与泄漏测试使用（NFR-011）。
var (
	liveGoroutines atomic.Int64
	liveClosers    atomic.Int64
)

// LiveGoroutines 返回所有 Scope 中仍在运行的 goroutine 数。
func LiveGoroutines() int64 { return liveGoroutines.Load() }

// LiveClosers 返回所有 Scope 中尚未关闭的资源数（通常对应文件描述符）。
func LiveClosers() int64 { return liveClosers.Load() }

// ErrClosed 表示 Scope 已关闭，不能再登记新的 goroutine 或资源。
var ErrClosed = errors.New("lifecycle: scope closed")

// Scope 是一组 goroutine、资源与子 Scope 的所有者。
type Scope struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.Mutex
	closed   bool
	closers  []io.Closer
	children map[*Scope]struct{}
	parent   *Scope
	closeErr error

	wg       sync.WaitGroup
	shutOnce sync.Once
}

// NewRoot 创建根 Scope。
func NewRoot(ctx context.Context) *Scope {
	c, cancel := context.WithCancel(ctx)
	return &Scope{ctx: c, cancel: cancel, children: map[*Scope]struct{}{}}
}

// Context 在 Scope 关闭时被取消。
func (s *Scope) Context() context.Context { return s.ctx }

// Child 创建子 Scope；父 Scope 关闭时子 Scope 一并关闭。父已关闭时返回一个已关闭的子 Scope。
func (s *Scope) Child() *Scope {
	c, cancel := context.WithCancel(s.ctx)
	ch := &Scope{ctx: c, cancel: cancel, children: map[*Scope]struct{}{}, parent: s}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		ch.Shutdown()
		return ch
	}
	s.children[ch] = struct{}{}
	s.mu.Unlock()
	return ch
}

// Go 在 Scope 内启动 goroutine。fn 必须在 ctx 取消后尽快返回。
func (s *Scope) Go(fn func(ctx context.Context)) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	// goroutine 同时计入本 Scope 与全部祖先，Wait 任一 Scope 即覆盖其整棵子树，
	// 即使子 Scope 已先行关闭并从父 Scope 中摘除。
	for p := s; p != nil; p = p.parent {
		p.wg.Add(1)
	}
	s.mu.Unlock()
	liveGoroutines.Add(1)
	go func() {
		defer func() {
			liveGoroutines.Add(-1)
			for p := s; p != nil; p = p.parent {
				p.wg.Done()
			}
		}()
		fn(s.ctx)
	}()
	return nil
}

// Own 登记一个资源；Scope 关闭时按登记的逆序关闭。Scope 已关闭时立即关闭该资源并返回 ErrClosed。
func (s *Scope) Own(c io.Closer) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = c.Close()
		return ErrClosed
	}
	s.closers = append(s.closers, c)
	s.mu.Unlock()
	liveClosers.Add(1)
	return nil
}

// Shutdown 取消 Context、关闭全部子 Scope 与资源，不等待 goroutine 退出。
// 幂等；可在 Scope 自己的 goroutine 内调用而不会死锁。
func (s *Scope) Shutdown() {
	s.shutOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		children := make([]*Scope, 0, len(s.children))
		for c := range s.children {
			children = append(children, c)
		}
		closers := s.closers
		s.closers = nil
		s.mu.Unlock()

		s.cancel()
		for _, c := range children {
			c.Shutdown()
		}
		var errs []error
		for i := len(closers) - 1; i >= 0; i-- {
			if err := closers[i].Close(); err != nil {
				errs = append(errs, err)
			}
			liveClosers.Add(-1)
		}
		s.mu.Lock()
		s.closeErr = errors.Join(errs...)
		s.mu.Unlock()
		if s.parent != nil {
			s.parent.mu.Lock()
			delete(s.parent.children, s)
			s.parent.mu.Unlock()
		}
	})
}

// Wait 等待本 Scope 及全部后代 Scope 的 goroutine 退出。不得在本 Scope 或其后代的 goroutine 内调用。
func (s *Scope) Wait() { s.wg.Wait() }

// Close = Shutdown + Wait，返回关闭资源时的错误。幂等。
func (s *Scope) Close() error {
	s.Shutdown()
	s.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeErr
}

// Closed 报告 Scope 是否已开始关闭。
func (s *Scope) Closed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}
