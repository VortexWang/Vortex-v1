package gate

import (
	"context"
	"sync"
	"time"
)

// Bucket 是字节令牌桶（ENG-026）：速率为份额（bit/s 换算为字节/秒），容量为 1 秒份额（突发不超过 1 秒）。
// 同时统计“令牌耗尽时间”——至少有一个拷贝在等待令牌的时间，供 LIM-009 的需求信号使用。
type Bucket struct {
	mu       sync.Mutex
	rate     float64 // 字节/秒
	cap      float64
	tokens   float64
	last     time.Time
	waiters  int
	starveAt time.Time
	starved  time.Duration // 自上次 TakeStarved 以来累计
}

// NewBucket 创建满桶。bps 为 bit/s。
func NewBucket(bps uint64) *Bucket {
	b := &Bucket{last: time.Now()}
	b.setRateLocked(bps)
	b.tokens = b.cap
	return b
}

func (b *Bucket) setRateLocked(bps uint64) {
	b.rate = float64(bps) / 8
	b.cap = b.rate // 1 秒份额
	if b.tokens > b.cap {
		b.tokens = b.cap
	}
}

// SetRate 调整速率（份额租约更新）；超出新容量的令牌立即作废。
func (b *Bucket) SetRate(bps uint64) {
	b.mu.Lock()
	b.refill(time.Now())
	b.setRateLocked(bps)
	b.mu.Unlock()
}

func (b *Bucket) refill(now time.Time) {
	if d := now.Sub(b.last); d > 0 {
		b.tokens += d.Seconds() * b.rate
		if b.tokens > b.cap {
			b.tokens = b.cap
		}
		b.last = now
	}
}

// Take 取走最多 want 个令牌。可用令牌少于 min(want, minChunk) 时等待，避免大量极小的拷贝。
// 速率为 0 时一直等待到 ctx 结束。
func (b *Bucket) Take(ctx context.Context, want, minChunk int) (int, error) {
	if want <= 0 {
		return 0, nil
	}
	need := want
	if minChunk > 0 && minChunk < need {
		need = minChunk
	}
	waiting := false
	defer func() {
		if waiting {
			b.mu.Lock()
			b.endWait(time.Now())
			b.mu.Unlock()
		}
	}()
	for {
		b.mu.Lock()
		now := time.Now()
		b.refill(now)
		if float64(need) > b.cap && b.cap >= 1 {
			need = int(b.cap) // 一次需求超过桶容量时，按容量取
		}
		if b.tokens >= float64(need) && b.cap > 0 {
			n := want
			if float64(n) > b.tokens {
				n = int(b.tokens)
			}
			b.tokens -= float64(n)
			b.mu.Unlock()
			return n, nil
		}
		if !waiting {
			waiting = true
			if b.waiters == 0 {
				b.starveAt = now
			}
			b.waiters++
		}
		var wait time.Duration
		if b.rate > 0 {
			wait = time.Duration((float64(need) - b.tokens) / b.rate * float64(time.Second))
		} else {
			wait = 50 * time.Millisecond
		}
		b.mu.Unlock()
		if wait < time.Millisecond {
			wait = time.Millisecond
		}
		if wait > 50*time.Millisecond {
			wait = 50 * time.Millisecond // 定期醒来，及时响应速率调整与取消
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return 0, ctx.Err()
		case <-t.C:
		}
	}
}

func (b *Bucket) endWait(now time.Time) {
	b.waiters--
	if b.waiters == 0 {
		b.starved += now.Sub(b.starveAt)
	}
}

// Refund 退回未使用的令牌（拷贝写成功字节少于授予量时）。
func (b *Bucket) Refund(n int) {
	if n <= 0 {
		return
	}
	b.mu.Lock()
	b.tokens += float64(n)
	if b.tokens > b.cap {
		b.tokens = b.cap
	}
	b.mu.Unlock()
}

// TakeStarved 返回自上次调用以来的累计耗尽时间，并清零。
func (b *Bucket) TakeStarved() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	s := b.starved
	if b.waiters > 0 { // 正在等待：计入到此刻为止的部分
		s += now.Sub(b.starveAt)
		b.starveAt = now
	}
	b.starved = 0
	return s
}
