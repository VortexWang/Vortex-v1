package route

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"vortex/engine/types"
)

// TargetDialer 按规则的目标调度连接目标（直接转发入口与隧道出口共用）：
// 解析 → 逐地址检查（SEC-011）→ 连接检查过的 IP（不按域名连接，防止 DNS 重绑定）。
type TargetDialer struct {
	Resolver *Resolver
	Guard    func() *Guard
	Healthy  func(key string) bool // nil 表示全部健康
	Events   func(types.Event)
	Timeout  time.Duration // 默认 5 秒

	mu   sync.Mutex
	sels map[types.UUID]ruleSelector // 每条规则只保留当前快照的选择器
}

type ruleSelector struct {
	rule *types.Rule
	sel  *Selector
}

// TargetKey 是目标的健康状态与已尝试判定键。
func TargetKey(t types.Target) string { return net.JoinHostPort(t.Host, fmt.Sprint(t.Port)) }

func (d *TargetDialer) selector(r *types.Rule) *Selector {
	d.mu.Lock()
	defer d.mu.Unlock()
	if rs, ok := d.sels[r.RuleID]; ok && rs.rule == r {
		return rs.sel
	}
	cands := make([]Candidate, len(r.Targets))
	for i, t := range r.Targets {
		cands[i] = Candidate{Key: TargetKey(t), Weight: t.Weight, Priority: t.Priority}
	}
	s := NewSelector(r.TargetSchedule, cands)
	if d.sels == nil {
		d.sels = map[types.UUID]ruleSelector{}
	}
	d.sels[r.RuleID] = ruleSelector{r, s}
	return s
}

// Forget 删除不再存在的规则的选择器。
func (d *TargetDialer) Forget(keep func(types.UUID) bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for id := range d.sels {
		if !keep(id) {
			delete(d.sels, id)
		}
	}
}

// Dial 依次尝试目标，全部失败返回 ErrNoRoute；全部因目标策略被拒时返回 ErrTargetForbidden。
func (d *TargetDialer) Dial(ctx context.Context, r *types.Rule) (net.Conn, error) {
	sel := d.selector(r)
	tried := map[string]bool{}
	timeout := d.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	nd := net.Dialer{Timeout: timeout}
	forbidden := 0
	for len(tried) < sel.Len() {
		i := sel.Pick(d.Healthy, func(k string) bool { return tried[k] })
		if i < 0 {
			break
		}
		cand := sel.At(i)
		tried[cand.Key] = true
		t := r.Targets[i]
		aps, rejected, err := AllowedAddrs(ctx, d.Resolver, d.Guard(), t.Host, t.Port)
		if errors.Is(err, ErrTargetForbidden) {
			forbidden++
			if d.Events != nil {
				d.Events(types.Event{Kind: types.EventTargetRejected, RuleID: r.RuleID, TenantID: r.TenantID,
					Detail: fmt.Sprintf("%s -> %s", cand.Key, rejected)})
			}
			continue
		}
		if err != nil {
			continue
		}
		for _, ap := range aps {
			c, err := nd.DialContext(ctx, "tcp", ap.String())
			if err == nil {
				return c, nil
			}
		}
	}
	if forbidden > 0 && forbidden == len(tried) {
		return nil, ErrTargetForbidden
	}
	return nil, ErrNoRoute
}

// DialUDP 选目标并建立已连接 UDP socket（隧道出口，ENG-042）。UDP 没有握手，存活以探测结果为准，
// 取第一个通过检查的地址。
func (d *TargetDialer) DialUDP(ctx context.Context, r *types.Rule) (*net.UDPConn, error) {
	sel := d.selector(r)
	tried := map[string]bool{}
	forbidden := 0
	for len(tried) < sel.Len() {
		i := sel.Pick(d.Healthy, func(k string) bool { return tried[k] })
		if i < 0 {
			break
		}
		cand := sel.At(i)
		tried[cand.Key] = true
		t := r.Targets[i]
		aps, rejected, err := AllowedAddrs(ctx, d.Resolver, d.Guard(), t.Host, t.Port)
		if errors.Is(err, ErrTargetForbidden) {
			forbidden++
			if d.Events != nil {
				d.Events(types.Event{Kind: types.EventTargetRejected, RuleID: r.RuleID, TenantID: r.TenantID,
					Detail: fmt.Sprintf("%s -> %s", cand.Key, rejected)})
			}
			continue
		}
		if err != nil || len(aps) == 0 {
			continue
		}
		c, err := net.DialUDP("udp", nil, net.UDPAddrFromAddrPort(aps[0]))
		if err == nil {
			_ = c.SetReadBuffer(udpSockBuffer)
			_ = c.SetWriteBuffer(udpSockBuffer)
			return c, nil
		}
	}
	if forbidden > 0 && forbidden == len(tried) {
		return nil, ErrTargetForbidden
	}
	return nil, ErrNoRoute
}
