// Package control 是 Agent 的控制连接管理（AGT-002、HUB-014，02 方案 3.2；AGT-T04、T05 的非 gRPC 部分）。
//
//   - 按配置的同区域 Edge Hub 列表连接；断线后指数退避（1 秒起，上限 30 秒，±20% 抖动）并轮换实例；
//     收到 HubHello 即视为连接建立，退避复位；
//   - 当前实例声明自治时，每 ProbeEvery 探测列表中的其他实例（建立流、发送 Hello、读 HubHello）；
//     发现在线实例即切换：探测流转正，原连接关闭（HUB-014 要求 60 秒内，默认 15 秒一轮）；
//     非在线的探测流立即关闭。
//
// 传输抽象为 Dialer / Conn；gRPC 接入后由 Connect 双向流实现。
package control

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"vortex/engine/types"
	"vortex/node/agent"
)

// Endpoint 是一个 Edge Hub 实例（EnrollResponse.edge_hubs）。
type Endpoint struct {
	ID      types.UUID
	Address string
}

// Conn 是一条控制流。Recv 在流结束时返回错误。
type Conn interface {
	Send(agent.Out) error
	Recv(ctx context.Context) (agent.In, error)
	Close() error
}

type Dialer interface {
	Dial(ctx context.Context, ep Endpoint) (Conn, error)
}

// Handler 是 Controller 驱动的 Agent（*agent.Agent 满足）。
type Handler interface {
	Hello() agent.Hello
	Connected(agent.Session)
	Adopt(agent.Session)
	Disconnected()
	HandleHub(context.Context, agent.In)
}

type Controller struct {
	Agent        Handler
	Hubs         []Endpoint
	Dialer       Dialer
	BackoffMin   time.Duration // 默认 1 秒
	BackoffMax   time.Duration // 默认 30 秒
	ProbeEvery   time.Duration // 默认 15 秒
	HelloTimeout time.Duration // 等待 HubHello，默认 5 秒

	mu      sync.Mutex
	current types.UUID
	mode    agent.Mode
	rng     *rand.Rand
}

var errSwitch = errors.New("control: switching hub")

func (c *Controller) defaults() {
	if c.BackoffMin == 0 {
		c.BackoffMin = time.Second
	}
	if c.BackoffMax == 0 {
		c.BackoffMax = 30 * time.Second
	}
	if c.ProbeEvery == 0 {
		c.ProbeEvery = 15 * time.Second
	}
	if c.HelloTimeout == 0 {
		c.HelloTimeout = 5 * time.Second
	}
	if c.rng == nil {
		c.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
}

// Current 返回当前所连实例与其声明的模式。
func (c *Controller) Current() (types.UUID, agent.Mode) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current, c.mode
}

func (c *Controller) jitter(d time.Duration) time.Duration {
	c.mu.Lock()
	f := 0.8 + 0.4*c.rng.Float64()
	c.mu.Unlock()
	return time.Duration(float64(d) * f)
}

// Run 维持控制连接直至 ctx 结束。
func (c *Controller) Run(ctx context.Context) {
	c.defaults()
	backoff := c.BackoffMin
	idx := 0
	var adopted Conn
	var adoptedHello agent.In
	for ctx.Err() == nil {
		var conn Conn
		var first agent.In
		if adopted != nil {
			conn, first, adopted, adoptedHello = adopted, adoptedHello, nil, nil
		} else {
			ep := c.Hubs[idx%len(c.Hubs)]
			var err error
			conn, err = c.Dialer.Dial(ctx, ep)
			if err != nil {
				idx++
				c.sleep(ctx, backoff)
				backoff = min(backoff*2, c.BackoffMax)
				continue
			}
		}
		next, nextHello, established := c.serve(ctx, conn, first)
		if established {
			backoff = c.BackoffMin
		}
		if next != nil {
			adopted, adoptedHello = next, nextHello
			continue
		}
		idx++ // 断线：轮换实例
		c.sleep(ctx, backoff)
		backoff = min(backoff*2, c.BackoffMax)
	}
}

func (c *Controller) sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(c.jitter(d))
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

type session struct{ conn Conn }

func (s session) Send(m agent.Out) error { return s.conn.Send(m) }

type recvResult struct {
	m   agent.In
	err error
}

// serve 运行一条连接。first 非空表示探测流转正（Hello 已发送、HubHello 已读出）。
// 返回要转正的探测流（切换时）以及这条连接是否曾建立（收到 HubHello）。
func (c *Controller) serve(ctx context.Context, conn Conn, first agent.In) (Conn, agent.In, bool) {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if first != nil {
		c.Agent.Adopt(session{conn})
	} else {
		c.Agent.Connected(session{conn})
	}
	recv := make(chan recvResult, 16)
	go func() {
		for {
			m, err := conn.Recv(cctx)
			select {
			case recv <- recvResult{m, err}:
			case <-cctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	established := false
	probe := time.NewTicker(c.ProbeEvery)
	defer probe.Stop()
	handle := func(m agent.In) {
		switch m := m.(type) {
		case agent.HubHello:
			established = true
			c.mu.Lock()
			c.current, c.mode = m.Hub, m.Mode
			c.mu.Unlock()
		case agent.ModeChange:
			c.mu.Lock()
			c.mode = m.Mode
			c.mu.Unlock()
		}
		c.Agent.HandleHub(ctx, m)
	}
	if first != nil {
		handle(first)
	}
	for {
		select {
		case <-ctx.Done():
			conn.Close()
			c.Agent.Disconnected()
			return nil, nil, established
		case r := <-recv:
			if r.err != nil {
				conn.Close()
				c.Agent.Disconnected()
				return nil, nil, established
			}
			handle(r.m)
		case <-probe.C:
			c.mu.Lock()
			cur, mode := c.current, c.mode
			c.mu.Unlock()
			if mode != agent.ModeAutonomous {
				continue
			}
			if pc, hello := c.probe(ctx, cur); pc != nil {
				cancel()
				conn.Close()
				c.Agent.Disconnected()
				return pc, hello, established
			}
		}
	}
}

// probe 依次探测其他实例，返回第一个声明在线的实例的流与其 HubHello。
func (c *Controller) probe(ctx context.Context, current types.UUID) (Conn, agent.In) {
	for _, ep := range c.Hubs {
		if ep.ID == current {
			continue
		}
		pctx, cancel := context.WithTimeout(ctx, c.HelloTimeout)
		pc, err := c.Dialer.Dial(pctx, ep)
		if err != nil {
			cancel()
			continue
		}
		if err := pc.Send(c.Agent.Hello()); err != nil {
			pc.Close()
			cancel()
			continue
		}
		m, err := pc.Recv(pctx)
		cancel()
		if hh, ok := m.(agent.HubHello); err == nil && ok && hh.Mode == agent.ModeOnline {
			return pc, hh
		}
		pc.Close()
	}
	return nil, nil
}
