package lifecycle

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type closer struct {
	mu    *sync.Mutex
	order *[]int
	id    int
	err   error
}

func (c closer) Close() error {
	c.mu.Lock()
	*c.order = append(*c.order, c.id)
	c.mu.Unlock()
	return c.err
}

func TestCloseIsIdempotentAndCascades(t *testing.T) {
	base := LiveGoroutines()
	root := NewRoot(context.Background())
	var mu sync.Mutex
	var order []int
	_ = root.Own(closer{&mu, &order, 1, nil})
	_ = root.Own(closer{&mu, &order, 2, errors.New("boom")})
	child := root.Child()
	grand := child.Child()
	for _, s := range []*Scope{root, child, grand} {
		for i := 0; i < 3; i++ {
			if err := s.Go(func(ctx context.Context) { <-ctx.Done() }); err != nil {
				t.Fatal(err)
			}
		}
	}
	if got := LiveGoroutines() - base; got != 9 {
		t.Fatalf("live goroutines %d, want 9", got)
	}
	err := root.Close()
	if err == nil || err.Error() != "boom" {
		t.Fatalf("close error %v", err)
	}
	if err2 := root.Close(); err2 == nil {
		t.Fatal("second Close should report the same error")
	}
	if len(order) != 2 || order[0] != 2 || order[1] != 1 {
		t.Fatalf("closers must run once in reverse order, got %v", order)
	}
	if !child.Closed() || !grand.Closed() {
		t.Fatal("children not closed")
	}
	if got := LiveGoroutines() - base; got != 0 {
		t.Fatalf("goroutines leaked: %d", got)
	}
}

func TestRegisterAfterClose(t *testing.T) {
	s := NewRoot(context.Background())
	_ = s.Close()
	if s.Go(func(context.Context) {}) != ErrClosed {
		t.Fatal("Go after close must fail")
	}
	var mu sync.Mutex
	var order []int
	if s.Own(closer{&mu, &order, 7, nil}) != ErrClosed || len(order) != 1 {
		t.Fatal("Own after close must close the resource immediately")
	}
	if c := s.Child(); !c.Closed() {
		t.Fatal("child of closed scope must be closed")
	}
}

func TestShutdownFromOwnGoroutineDoesNotDeadlock(t *testing.T) {
	s := NewRoot(context.Background())
	done := make(chan struct{})
	_ = s.Go(func(ctx context.Context) {
		s.Shutdown() // 例如会话 goroutine 读到 EOF 后关闭自己的会话
		<-ctx.Done()
		close(done)
	})
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("deadlock")
	}
	s.Wait()
}

func TestChildRemovedFromParentAfterClose(t *testing.T) {
	root := NewRoot(context.Background())
	for i := 0; i < 1000; i++ {
		c := root.Child()
		_ = c.Go(func(ctx context.Context) {})
		_ = c.Close()
	}
	root.mu.Lock()
	n := len(root.children)
	root.mu.Unlock()
	if n != 0 {
		t.Fatalf("closed children retained by parent: %d（长时间运行会泄漏内存）", n)
	}
	_ = root.Close()
}

func TestConcurrentCloseRace(t *testing.T) {
	root := NewRoot(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		c := root.Child()
		wg.Add(2)
		go func() { defer wg.Done(); _ = c.Go(func(ctx context.Context) { <-ctx.Done() }) }()
		go func() { defer wg.Done(); c.Shutdown() }()
	}
	wg.Wait()
	_ = root.Close()
	if LiveGoroutines() != 0 {
		t.Fatalf("leak %d", LiveGoroutines())
	}
}
