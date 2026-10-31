package tunnel

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync/atomic"
	"time"

	"vortex/engine/internal/lifecycle"
	"vortex/engine/pipe"
	"vortex/engine/route"
	"vortex/engine/types"
	"vortex/engine/vlink"
)

// 非阻塞计量：数据报在链路读 goroutine 中转发，不能等待令牌或计费授权；额度不足即丢弃该包。
var noWait = func() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}()

// meter 对一个数据报做整包计量（ENG-038）：取得完整额度才发送，只计业务载荷。
func meter(hook pipe.Hook, d types.Direction, n int, send func() error) bool {
	granted, err := hook.Acquire(noWait, d, n)
	if err != nil || granted < n {
		if granted > 0 {
			hook.Release(d, granted, 0)
		}
		return false
	}
	if send() != nil {
		hook.Release(d, granted, 0)
		return false
	}
	hook.Release(d, granted, n)
	return true
}

const maxDatagram = 65535

// handleUDP 处理已通过开流检查与门控的 UDP 会话（中继或出口）。上行在链路读 goroutine 中直接转发，
// 下行：出口由本 goroutine 读取目标（ENG-045：出口每 UDP 会话 1 个 goroutine），中继同样以回调转发。
func (r *Router) handleUDP(ctx context.Context, sess *lifecycle.Scope, rule *types.Rule, s *vlink.Stream, hook pipe.Hook, downRef *atomic.Value) {
	idle := rule.UDPIdle
	if idle <= 0 {
		idle = 60 * time.Second
	}
	start := time.Now()
	var last atomic.Int64
	touch := func() { last.Store(int64(time.Since(start))) }
	touch()
	idleFor := func() time.Duration { return time.Since(start) - time.Duration(last.Load()) }
	ended := make(chan struct{})
	var endOnce atomic.Bool
	end := func() {
		if endOnce.CompareAndSwap(false, true) {
			close(ended)
		}
	}
	s.OnClose(end)
	stop := context.AfterFunc(ctx, end)
	defer stop()

	if rule.NextHop != nil { // 中继
		down, err := r.OpenUDP(ctx, rule, s.OpenInfo().Client, s.OpenInfo().Visited)
		if err != nil {
			s.Reset(vlink.CodeNoRoute, "next group has no available member")
			return
		}
		dp := down.(udpPath)
		downRef.Store(dp)
		if sess.Own(dp) != nil {
			return
		}
		dp.Stream.OnClose(end)
		s.OnDgram(func(p []byte) {
			if meter(hook, types.DirUp, len(p), func() error { return dp.Stream.SendDgram(p) }) {
				touch()
			}
		})
		dp.Stream.OnDgram(func(p []byte) {
			if meter(hook, types.DirDown, len(p), func() error { return s.SendDgram(p) }) {
				touch()
			}
		})
		waitIdle(ended, idle, idleFor)
		propagate(s, dp, hook)
		return
	}

	uc, err := r.Targets.DialUDP(ctx, rule) // 出口：每会话独立的已连接 socket（ENG-042）
	if err != nil {
		s.Reset(vlink.CodeTargetUnreachable, "")
		return
	}
	if sess.Own(uc) != nil {
		return
	}
	var header []byte
	if rule.ProxyProtocol == types.ProxyV2 {
		dst, _ := netip.ParseAddrPort(uc.LocalAddr().String())
		header, _ = route.ProxyHeader(types.ProxyV2, true, s.OpenInfo().Client, dst)
	}
	s.OnDgram(func(p []byte) {
		out := p
		if len(header) > 0 {
			out = append(append(make([]byte, 0, len(header)+len(p)), header...), p...)
		}
		if meter(hook, types.DirUp, len(p), func() error { _, err := uc.Write(out); return err }) {
			touch()
		}
	})
	s.OnClose(func() { _ = uc.Close() })
	buf := make([]byte, maxDatagram)
	for {
		_ = uc.SetReadDeadline(time.Now().Add(idle))
		n, err := uc.Read(buf)
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() && idleFor() < idle {
				continue
			}
			if errors.As(err, &ne) && ne.Timeout() {
				s.Reset(vlink.CodeNoRoute, "idle") // 空闲超时：通知上游回收会话
			}
			return
		}
		if meter(hook, types.DirDown, n, func() error { return s.SendDgram(buf[:n]) }) {
			touch()
		}
	}
}

// waitIdle 等待会话结束或空闲超时。
func waitIdle(ended <-chan struct{}, idle time.Duration, idleFor func() time.Duration) {
	t := time.NewTimer(idle)
	defer t.Stop()
	for {
		select {
		case <-ended:
			return
		case <-t.C:
			if d := idleFor(); d >= idle {
				return
			} else {
				t.Reset(idle - d)
			}
		}
	}
}
