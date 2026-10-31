package route

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync/atomic"
	"time"

	"vortex/engine/internal/lifecycle"
	"vortex/engine/pipe"
	"vortex/engine/types"
)

// Admission 是新会话的准入门控（ENG-024），由 gate 实现：检查运行授权、份额、连接数与计费授权，
// 通过时返回本会话拷贝点使用的 Hook（计量与限速）以及会话结束时必须调用一次的 release。
// kill 由门控在限制生效（授权到期、份额失效、连接数降额）时调用，立即关闭该会话（ENG-028），reason 为原因
// （gate.ErrRunGrant 等，或本包的 ErrRuleRevoked / ErrDrained）；隧道会话据此向路径上其他节点发送带原因码的 RST。
// 阻塞在读取上的会话不会调用 Hook，因此必须能被主动关闭。
type Admission interface {
	Admit(ctx context.Context, rule *types.Rule, client netip.AddrPort, kill func(reason error)) (hook pipe.Hook, release func(), err error)
}

// 运行时关闭会话的原因（门控之外）。
var (
	ErrRuleRevoked = errors.New("unauthorized: rule removed or permissions tightened")
	ErrDrained     = errors.New("no_route: drain deadline reached")
)

// Resetter 由隧道上游实现：以原因终止（向下游发送对应原因码的 RST），而不是普通关闭。
type Resetter interface{ ResetWith(reason error) }

// EndReasoner 由门控的 Hook 实现：会话因额度耗尽等原因自然结束时返回原因（否则 nil）。
type EndReasoner interface{ EndReason() error }

// resetTarget 在关闭会话前用原因终止上游（若上游支持）。
func resetTarget(t any, reason error) {
	if r, ok := t.(Resetter); ok && reason != nil {
		r.ResetWith(reason)
	}
}

// AllowAll 不做门控（测试与 M0 早期使用）。
type AllowAll struct{}

func (AllowAll) Admit(context.Context, *types.Rule, netip.AddrPort, func(error)) (pipe.Hook, func(), error) {
	return pipe.NopHook{}, func() {}, nil
}

var (
	ErrNoRoute       = errors.New("no_route")
	ErrRuleNotActive = errors.New("no_route: rule not active")
)

// DirectEntry 是直接转发规则在入口节点上的一个 TCP 监听（7.1 节“直接转发”）。
type DirectEntry struct {
	RuleID      types.UUID
	Tables      *Tables
	Listener    net.Listener
	Scope       *lifecycle.Scope // 拥有监听与全部会话；关闭即停止接入并关闭所有会话
	Admission   Admission
	Resolver    *Resolver
	Guard       *Guard
	Healthy     func(key string) bool  // 目标探测结果（AGT-007）；nil 表示全部健康
	Events      func(types.Event)      // nil 表示丢弃
	DialTimeout time.Duration          // 默认 5 秒
	GuardRef    *atomic.Pointer[Guard] // 非 nil 时优先于 Guard：目标策略可随配置热更新

	ruleRef atomic.Pointer[types.UUID] // SetRule 设置后优先于 RuleID：监听改绑规则时不重建 socket

	// NextHop 为下一跳规则（隧道）打开到下一组的流；nil 表示本入口只支持直接转发
	NextHop func(ctx context.Context, rule *types.Rule, client netip.AddrPort) (net.Conn, error)

	targets TargetDialer

	Active   atomic.Int64 // 当前会话数
	Accepted atomic.Int64
}

// Start 在 Scope 中启动接入循环（每个监听 1 个 goroutine，ENG-045）。
func (e *DirectEntry) Start() error {
	if e.Admission == nil {
		e.Admission = AllowAll{}
	}
	if e.DialTimeout == 0 {
		e.DialTimeout = 5 * time.Second
	}
	e.targets = TargetDialer{Resolver: e.Resolver, Guard: e.guard, Healthy: e.Healthy, Events: e.Events, Timeout: e.DialTimeout}
	if err := e.Scope.Own(e.Listener); err != nil {
		return err
	}
	return e.Scope.Go(e.acceptLoop)
}

// SetRule 把监听改绑到另一条规则；只影响之后接入的会话。
func (e *DirectEntry) SetRule(id types.UUID) { e.ruleRef.Store(&id) }

func (e *DirectEntry) ruleID() types.UUID {
	if p := e.ruleRef.Load(); p != nil {
		return *p
	}
	return e.RuleID
}

func (e *DirectEntry) guard() *Guard {
	if e.GuardRef != nil {
		if g := e.GuardRef.Load(); g != nil {
			return g
		}
	}
	return e.Guard
}

func (e *DirectEntry) emit(ev types.Event) {
	if e.Events != nil {
		e.Events(ev)
	}
}

func (e *DirectEntry) acceptLoop(ctx context.Context) {
	backoff := 5 * time.Millisecond
	for {
		c, err := e.Listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			// 例如 EMFILE：退避后重试，不让接入循环退出
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}
			if backoff < time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = 5 * time.Millisecond
		e.Accepted.Add(1)
		sess := e.Scope.Child()
		if sess.Own(c) != nil {
			continue
		}
		e.Active.Add(1)
		if err := sess.Go(func(ctx context.Context) {
			defer e.Active.Add(-1)
			defer sess.Shutdown() // 在自己的 goroutine 内关闭，不会死锁（lifecycle 约定）
			e.handle(ctx, sess, c)
		}); err != nil {
			e.Active.Add(-1)
		}
	}
}

func (e *DirectEntry) handle(ctx context.Context, sess *lifecycle.Scope, client net.Conn) {
	// 会话使用接入时刻的规则快照；之后的切换不影响本会话（ENG-032）
	rule := e.Tables.Load().Rule(e.ruleID())
	if rule == nil {
		return
	}
	clientAP, _ := netip.ParseAddrPort(client.RemoteAddr().String())
	var up atomic.Value // 上游连接建立后才有；kill 时用原因终止它（ENG-028）
	kill := func(reason error) {
		if v := up.Load(); v != nil {
			resetTarget(v.(*upConn).c, reason)
		}
		sess.Shutdown()
	}
	hook, release, err := e.Admission.Admit(ctx, rule, clientAP, kill)
	if err != nil {
		return // 门控拒绝：直接关闭连接，不连接目标（ENG-024）
	}
	defer release()

	var target net.Conn
	if rule.NextHop != nil {
		if e.NextHop == nil {
			return
		}
		target, err = e.NextHop(ctx, rule, clientAP)
	} else {
		target, err = e.dialTarget(ctx, rule)
	}
	if err != nil {
		return
	}
	up.Store(&upConn{target})
	if sess.Own(target) != nil {
		return
	}
	if rule.NextHop == nil && (rule.ProxyProtocol == types.ProxyV1 || rule.ProxyProtocol == types.ProxyV2) { // 隧道由出口添加
		local, _ := netip.ParseAddrPort(client.LocalAddr().String())
		h, err := ProxyHeader(rule.ProxyProtocol, false, clientAP, local)
		if err != nil {
			return
		}
		_ = target.SetWriteDeadline(time.Now().Add(e.DialTimeout))
		if _, err := target.Write(h); err != nil { // 平台生成的头部，不经过计量钩子（ENG-035）
			return
		}
		_ = target.SetWriteDeadline(time.Time{})
	}
	pipe.Run(ctx, client, target, pipe.Options{
		Hook:        hook,
		IdleTimeout: rule.TCPIdle,
		Go: func(f func()) {
			if sess.Go(func(context.Context) { f() }) != nil {
				f() // Scope 已关闭：同步执行，pipe 会因截止时间立即返回
			}
		},
	})
	if er, ok := hook.(EndReasoner); ok { // 额度耗尽等：把原因带给路径上其他节点
		resetTarget(target, er.EndReason())
	}
}

func (e *DirectEntry) dialTarget(ctx context.Context, r *types.Rule) (net.Conn, error) {
	return e.targets.Dial(ctx, r)
}

type upConn struct{ c any }
