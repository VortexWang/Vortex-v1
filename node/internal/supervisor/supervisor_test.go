package supervisor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRestartsOnErrorAndPanic(t *testing.T) {
	var mu sync.Mutex
	var evs []Event
	s := &Supervisor{BackoffMin: time.Millisecond, BackoffMax: 4 * time.Millisecond, StableFor: time.Hour,
		OnEvent: func(e Event) { mu.Lock(); evs = append(evs, e); mu.Unlock() }}
	ctx, cancel := context.WithCancel(context.Background())
	var runs atomic.Int32
	s.Go(ctx, "flaky", func(ctx context.Context) error {
		switch runs.Add(1) {
		case 1:
			return errors.New("boom")
		case 2:
			panic("bad")
		case 3:
			return nil // ctx 未结束时正常返回也算异常
		}
		<-ctx.Done()
		return nil
	})
	var other atomic.Int32
	s.Go(ctx, "steady", func(ctx context.Context) error { other.Add(1); <-ctx.Done(); return nil })
	deadline := time.Now().Add(2 * time.Second)
	for runs.Load() < 4 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	s.Wait()
	mu.Lock()
	defer mu.Unlock()
	if runs.Load() != 4 || len(evs) != 3 || other.Load() != 1 {
		t.Fatalf("runs %d events %d other %d", runs.Load(), len(evs), other.Load())
	}
	if evs[0].Restart != time.Millisecond || evs[1].Restart != 2*time.Millisecond || evs[2].Restart != 4*time.Millisecond {
		t.Fatalf("backoff %v %v %v", evs[0].Restart, evs[1].Restart, evs[2].Restart)
	}
	for _, e := range evs {
		if e.Err == nil || e.Module != "flaky" {
			t.Fatalf("event %+v (every abnormal exit carries an error)", e)
		}
	}
}

func TestBackoffResetsAfterStableRun(t *testing.T) {
	var evs []Event
	var mu sync.Mutex
	s := &Supervisor{BackoffMin: time.Millisecond, BackoffMax: 100 * time.Millisecond, StableFor: 20 * time.Millisecond,
		OnEvent: func(e Event) { mu.Lock(); evs = append(evs, e); mu.Unlock() }}
	ctx, cancel := context.WithCancel(context.Background())
	var runs atomic.Int32
	s.Go(ctx, "m", func(ctx context.Context) error {
		n := runs.Add(1)
		if n == 3 {
			time.Sleep(30 * time.Millisecond) // 稳定运行后再退出
		}
		if n >= 4 {
			<-ctx.Done()
		}
		return errors.New("x")
	})
	for runs.Load() < 4 {
		time.Sleep(time.Millisecond)
	}
	cancel()
	s.Wait()
	mu.Lock()
	defer mu.Unlock()
	if len(evs) != 3 || evs[1].Restart != 2*time.Millisecond || evs[2].Restart != time.Millisecond {
		t.Fatalf("events %+v", evs)
	}
}
