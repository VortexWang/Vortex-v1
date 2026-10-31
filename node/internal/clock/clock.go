// Package clock 提供可注入的时钟。Agent 的授权有效期按单调时钟计算（AGT-005）：time.Now 返回的值带单调读数，
// 两个这样的值相减不受墙钟调整影响。
package clock

import (
	"sync"
	"time"
)

type Clock interface{ Now() time.Time }

type System struct{}

func (System) Now() time.Time { return time.Now() }

// Fake 是测试用的手动时钟。
type Fake struct {
	mu sync.Mutex
	t  time.Time
}

func NewFake(t time.Time) *Fake { return &Fake{t: t} }

func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.t
}

func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	f.t = f.t.Add(d)
	f.mu.Unlock()
}
