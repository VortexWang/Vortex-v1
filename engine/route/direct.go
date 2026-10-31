package route

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"vortex/engine/internal/lifecycle"
	"vortex/engine/pipe"
	"vortex/engine/types"
)

// Admission 是新会话的准入门控（ENG-024），由 gate 实现：检查运行授权、份额、连接数与计费授权，
// 通过时返回本会话拷贝点使用的 Hook（计量与限速）以及会话结束时必须调用一次的 release。
type Admission interface {
	Admit(ctx context.Context, rule *types.Rule, client netip.AddrPort) (hook pipe.Hook, release func(), err error)
}

// AllowAll 不做门控（测试与 M0 早期使用）。
type AllowAll struct{}

func (AllowAll) Admit(context.Context, *types.Rule, netip.AddrPort) (pipe.Hook, func(), error) {
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
	Healthy     func(key string) bool // 目标探测结果（AGT-007）；nil 表示全部健康
	Events      func(types.Event)     // nil 表示丢弃
	DialTimeout time.Duration         // 默认 5 秒

	// 只保留当前规则快照的选择器（目标只在会话开始时选择）；按快照缓存会随每次配置切换无限增长
	selMu   sync.Mutex
	selRule *types.Rule
	sel     *Selector

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
	if err := e.Scope.Own(e.Listener); err != nil {
		return err
	}
	return e.Scope.Go(e.acceptLoop)
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

func (e *DirectEntry) selector(r *types.Rule) *Selector {
	e.selMu.Lock()
	defer e.selMu.Unlock()
	if e.selRule == r {
		return e.sel
	}
	cands := make([]Candidate, len(r.Targets))
	for i, t := range r.Targets {
		cands[i] = Candidate{Key: net.JoinHostPort(t.Host, fmt.Sprint(t.Port)), Weight: t.Weight, Priority: t.Priority}
	}
	s := NewSelector(r.TargetSchedule, cands)
	e.selRule, e.sel = r, s
	return s
}

func (e *DirectEntry) handle(ctx context.Context, sess *lifecycle.Scope, client net.Conn) {
	// 会话使用接入时刻的规则快照；之后的切换不影响本会话（ENG-032）
	rule := e.Tables.Load().Rule(e.RuleID)
	if rule == nil {
		return
	}
	clientAP, _ := netip.ParseAddrPort(client.RemoteAddr().String())
	hook, release, err := e.Admission.Admit(ctx, rule, clientAP)
	if err != nil {
		return // 门控拒绝：直接关闭连接，不连接目标（ENG-024）
	}
	defer release()

	target, err := e.dialTarget(ctx, rule)
	if err != nil {
		return
	}
	if sess.Own(target) != nil {
		return
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
}

// dialTarget 按调度方式依次尝试目标：解析 → 逐地址检查（SEC-011）→ 连接检查过的 IP（不按域名连接，防止 DNS 重绑定）。
func (e *DirectEntry) dialTarget(ctx context.Context, r *types.Rule) (net.Conn, error) {
	sel := e.selector(r)
	tried := map[string]bool{}
	d := net.Dialer{Timeout: e.DialTimeout}
	for len(tried) < sel.Len() {
		i := sel.Pick(e.Healthy, func(k string) bool { return tried[k] })
		if i < 0 {
			break
		}
		cand := sel.At(i)
		tried[cand.Key] = true
		t := r.Targets[i]
		aps, rejected, err := AllowedAddrs(ctx, e.Resolver, e.Guard, t.Host, t.Port)
		if errors.Is(err, ErrTargetForbidden) {
			e.emit(types.Event{Kind: types.EventTargetRejected, RuleID: r.RuleID, TenantID: r.TenantID,
				Detail: fmt.Sprintf("%s -> %s", cand.Key, rejected)})
			continue
		}
		if err != nil {
			continue
		}
		for _, ap := range aps {
			c, err := d.DialContext(ctx, "tcp", ap.String())
			if err == nil {
				return c, nil
			}
		}
	}
	return nil, ErrNoRoute
}
