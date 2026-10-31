// Package probe 是 Agent 的探测（AGT-007，02 方案 3.7）。
//
// 每 3 秒探测运行配置中的下一组成员与目标：
//   - 成员：对其 VLink 端口做 TCP 握手。应用层 PING 由已建立链路的保活完成（VLink keepalive，ENG-019），
//     这里不再单独建 TLS 连接；由对端反向建链的成员（无地址）不探测，以链路状态为准；
//   - 目标：TCP 规则的目标做 TCP 握手；只用于 UDP 的目标无法握手，不探测、视为可用（被动统计由引擎完成）；
//
// 连续 3 次失败标记不可用，连续 2 次成功恢复；初始为可用。结果供引擎选路（engine.Options.Healthy、
// TunnelOptions.PeerHealthy），并汇总上报（ProbeReport），RTT 未测得时为 0（前端显示“—”，FE-006）。
package probe

import (
	"context"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"

	"vortex/engine/route"
	"vortex/engine/types"
)

const (
	Interval     = 3 * time.Second
	FailAfter    = 3
	RecoverAfter = 2
)

// DialFunc 做一次 TCP 握手，返回耗时。
type DialFunc func(ctx context.Context, addr string) (time.Duration, error)

func tcpDial(ctx context.Context, addr string) (time.Duration, error) {
	var d net.Dialer
	start := time.Now()
	c, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return 0, err
	}
	rtt := time.Since(start)
	c.Close()
	return rtt, nil
}

// Subject 是一个探测对象：成员（Node 非零）或目标（Target = "host:port"）。
type Subject struct {
	Node   types.UUID
	Target string
	addr   string
}

func (s Subject) key() string {
	if s.Target != "" {
		return "t/" + s.Target
	}
	return "n/" + string(s.Node[:])
}

// Result 对应 ProbeResult。
type Result struct {
	Subject
	Reachable bool
	RTT       time.Duration // 0：未测得
}

type state struct {
	sub     Subject
	healthy bool
	fails   int
	oks     int
	rtt     time.Duration
	probed  bool
}

type Prober struct {
	Dial    DialFunc      // 默认 TCP
	Timeout time.Duration // 单次探测超时，默认 Interval

	mu     sync.Mutex
	states map[string]*state
}

func New() *Prober { return &Prober{states: map[string]*state{}} }

// Subjects 从运行配置得出探测对象。
func Subjects(cfg *types.RuntimeConfig) []Subject {
	peers := map[types.UUID]types.Peer{}
	for _, p := range cfg.Peers {
		peers[p.NodeID] = p
	}
	seen := map[string]bool{}
	var out []Subject
	add := func(s Subject) {
		if !seen[s.key()] {
			seen[s.key()] = true
			out = append(out, s)
		}
	}
	for _, r := range cfg.Rules {
		if r.NextHop != nil {
			for _, m := range r.NextHop.Members {
				p, ok := peers[m.NodeID]
				if !ok || p.Address == "" || !p.Outbound {
					continue // 反向建链的成员：以链路状态为准
				}
				add(Subject{Node: m.NodeID, addr: net.JoinHostPort(p.Address, strconv.Itoa(int(p.LinkPort)))})
			}
			continue
		}
		tcp := len(r.Networks) == 0
		for _, n := range r.Networks {
			tcp = tcp || n == types.NetworkTCP
		}
		if !tcp {
			continue
		}
		for _, t := range r.Targets {
			k := route.TargetKey(t)
			add(Subject{Target: k, addr: k})
		}
	}
	return out
}

// SetSubjects 替换探测对象（配置生效后调用）。已有对象保留状态，移除的丢弃。
func (p *Prober) SetSubjects(subs []Subject) {
	p.mu.Lock()
	defer p.mu.Unlock()
	next := map[string]*state{}
	for _, s := range subs {
		if st := p.states[s.key()]; st != nil {
			st.sub = s
			next[s.key()] = st
		} else {
			next[s.key()] = &state{sub: s, healthy: true}
		}
	}
	p.states = next
}

// Round 并发探测一轮，更新状态并返回结果（按对象排序）。
func (p *Prober) Round(ctx context.Context) []Result {
	dial := p.Dial
	if dial == nil {
		dial = tcpDial
	}
	timeout := p.Timeout
	if timeout == 0 {
		timeout = Interval
	}
	p.mu.Lock()
	subs := make([]Subject, 0, len(p.states))
	for _, st := range p.states {
		subs = append(subs, st.sub)
	}
	p.mu.Unlock()

	type res struct {
		s   Subject
		rtt time.Duration
		err error
	}
	ch := make(chan res, len(subs))
	for _, s := range subs {
		go func(s Subject) {
			cctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			rtt, err := dial(cctx, s.addr)
			ch <- res{s, rtt, err}
		}(s)
	}
	for range subs {
		r := <-ch
		p.mu.Lock()
		if st := p.states[r.s.key()]; st != nil {
			st.probed = true
			if r.err != nil {
				st.fails, st.oks, st.rtt = st.fails+1, 0, 0
				if st.fails >= FailAfter {
					st.healthy = false
				}
			} else {
				st.oks, st.fails, st.rtt = st.oks+1, 0, r.rtt
				if !st.healthy && st.oks >= RecoverAfter {
					st.healthy = true
				}
			}
		}
		p.mu.Unlock()
	}
	return p.Results()
}

// Results 返回当前结果。
func (p *Prober) Results() []Result {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]Result, 0, len(p.states))
	for _, st := range p.states {
		out = append(out, Result{Subject: Subject{Node: st.sub.Node, Target: st.sub.Target}, Reachable: st.healthy, RTT: st.rtt})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key() < out[j].key() })
	return out
}

// TargetHealthy 供 engine.Options.Healthy 使用（键为 route.TargetKey）。未探测的对象视为可用。
func (p *Prober) TargetHealthy(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	st := p.states["t/"+key]
	return st == nil || st.healthy
}

// PeerHealthy 供 engine.TunnelOptions.PeerHealthy 使用。
func (p *Prober) PeerHealthy(id types.UUID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	st := p.states["n/"+string(id[:])]
	return st == nil || st.healthy
}

// Run 每 Interval 探测一轮，把结果交给 report（例如经 Agent 上报 ProbeReport）。
func (p *Prober) Run(ctx context.Context, every time.Duration, report func([]Result)) {
	if every == 0 {
		every = Interval
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r := p.Round(ctx)
			if report != nil {
				report(r)
			}
		}
	}
}
