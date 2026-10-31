// Package supervisor 管理 Agent 各模块的 goroutine（02 方案 2）：模块异常退出（返回错误或 panic）时记录事件
// 并按退避重启该模块，不影响其他模块与 Engine 的转发。ctx 结束时等待全部模块退出。
package supervisor

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"
)

// Event 是一次模块异常退出。
type Event struct {
	Module  string
	Err     error
	Restart time.Duration // 距下次重启的等待
}

type Supervisor struct {
	OnEvent    func(Event)
	BackoffMin time.Duration // 默认 1 秒
	BackoffMax time.Duration // 默认 30 秒
	StableFor  time.Duration // 运行超过它再退出时退避复位，默认 60 秒

	wg sync.WaitGroup
}

// Go 启动一个模块。f 正常返回 nil 且 ctx 已结束时视为退出；ctx 未结束时返回（含 nil）都视为异常并重启。
func (s *Supervisor) Go(ctx context.Context, name string, f func(ctx context.Context) error) {
	lo, hi, stable := s.BackoffMin, s.BackoffMax, s.StableFor
	if lo == 0 {
		lo = time.Second
	}
	if hi == 0 {
		hi = 30 * time.Second
	}
	if stable == 0 {
		stable = 60 * time.Second
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		backoff := lo
		for {
			start := time.Now()
			err := runSafe(ctx, f)
			if ctx.Err() != nil {
				return
			}
			if err == nil {
				err = fmt.Errorf("module returned")
			}
			if time.Since(start) >= stable {
				backoff = lo
			}
			if s.OnEvent != nil {
				s.OnEvent(Event{Module: name, Err: err, Restart: backoff})
			}
			t := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
			backoff = min(backoff*2, hi)
		}
	}()
}

func runSafe(ctx context.Context, f func(context.Context) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
		}
	}()
	return f(ctx)
}

// Wait 等待全部模块退出（ctx 结束后）。
func (s *Supervisor) Wait() { s.wg.Wait() }
