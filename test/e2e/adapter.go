// Package e2e 是 Agent 与 Edge Hub 的端到端测试（M3）：真实 Engine + Agent + Controller，经内存控制流连接
// 真实的 Edge Hub 会话处理。消息在两侧 Go 类型之间转换，位置与 gRPC 接入后的适配层相同。
// 后端由本包的 Backend 模拟：经真实的 upstream.Link 下发期望配置、Commit、份额与额度，确认账本，驱动对账。
package e2e

import (
	"context"
	"errors"
	"sync"

	"vortex/edgehub/issuer"
	"vortex/edgehub/model"
	"vortex/edgehub/session"
	"vortex/edgehub/upstream"
	hubwire "vortex/edgehub/wire"
	"vortex/engine/types"
	"vortex/node/agent"
	"vortex/node/agent/control"
	nodewire "vortex/node/agent/wire"
	edgehubv1 "vortex/proto/gen/go/vortex/edgehub/v1"

	"google.golang.org/protobuf/proto"
)

func toModel(u types.UUID) model.UUID { return model.UUID(u) }
func toTypes(u model.UUID) types.UUID { return types.UUID(u) }

// toHub / toAgent 走与 gRPC 相同的转换：Agent 类型 → 契约消息 → 线上字节 → 契约消息 → 会话类型（反之亦然），
// 所以内存连接的端到端测试同样覆盖了两侧的 wire 包。
func toHub(m agent.Out) session.AgentMsg {
	pm, err := nodewire.AgentMessage(m)
	if err != nil || pm == nil {
		return nil
	}
	b, _ := proto.Marshal(pm)
	var back edgehubv1.AgentMessage
	if err := proto.Unmarshal(b, &back); err != nil {
		return nil
	}
	am, err := hubwire.FromAgent(&back)
	if err != nil {
		return nil
	}
	return am
}

func toAgent(hub model.UUID, m session.HubMsg) agent.In {
	pm, err := hubwire.ToAgent(hub, "e2e", m)
	if err != nil {
		return nil
	}
	b, _ := proto.Marshal(pm)
	var back edgehubv1.HubMessage
	if err := proto.Unmarshal(b, &back); err != nil {
		return nil
	}
	in, err := nodewire.HubMessage(&back)
	if err != nil {
		return nil
	}
	return in
}

// Hub 是测试中的一个 Edge Hub 实例与其后端链路。
type Hub struct {
	*session.Hub
	Link *upstream.Link
	B    *Backend

	mu    sync.Mutex
	Down  bool
	conns []*conn
}

// Kill 模拟实例宕机：断开全部 Agent 流，之后的连接被拒绝。
func (h *Hub) Kill() {
	h.mu.Lock()
	h.Down = true
	cs := h.conns
	h.conns = nil
	h.mu.Unlock()
	for _, c := range cs {
		c.Close()
	}
}

// Revive 让实例重新接受连接。
func (h *Hub) Revive() {
	h.mu.Lock()
	h.Down = false
	h.mu.Unlock()
}

// Dialer 按 Endpoint.ID 找到 Hub，建立内存流。
type Dialer struct {
	Node types.UUID
	Hubs map[types.UUID]*Hub
}

type conn struct {
	hub    *Hub
	s      *session.Session
	in     chan agent.In
	done   chan struct{}
	once   sync.Once
	mu     sync.Mutex
	closed bool
}

func (d Dialer) Dial(_ context.Context, ep control.Endpoint) (control.Conn, error) {
	h := d.Hubs[ep.ID]
	h.mu.Lock()
	down := h.Down
	h.mu.Unlock()
	if down {
		return nil, errors.New("connection refused")
	}
	c := &conn{hub: h, in: make(chan agent.In, 4096), done: make(chan struct{})}
	h.mu.Lock()
	h.conns = append(h.conns, c)
	h.mu.Unlock()
	c.s = h.NewSession(toModel(d.Node), func(m session.HubMsg) {
		if a := toAgent(h.ID, m); a != nil {
			c.push(a)
		}
	})
	return c, nil
}

func (c *conn) push(m agent.In) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.in <- m
	}
}

func (c *conn) Send(m agent.Out) error {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return errors.New("closed")
	}
	am := toHub(m)
	if am == nil {
		return nil
	}
	if err := c.s.Handle(am); err != nil {
		c.Close()
		return err
	}
	return nil
}

func (c *conn) Recv(ctx context.Context) (agent.In, error) {
	select {
	case m := <-c.in:
		return m, nil
	case <-c.done:
		return nil, errors.New("stream closed")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *conn) Close() error {
	c.once.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		close(c.done)
		c.s.Close()
	})
	return nil
}

// RulesOf 从配置得出 Edge Hub 需要的规则状态。
func RulesOf(cfg *types.RuntimeConfig) session.RulesFunc {
	return func(model.UUID) []issuer.RuleState {
		var out []issuer.RuleState
		for _, r := range cfg.Rules {
			out = append(out, issuer.RuleState{RuleID: toModel(r.RuleID), TenantID: toModel(r.TenantID),
				EnforcementPoint: r.EnforcementPoint, PathCoefMilli: r.PathCoefMilli})
		}
		return out
	}
}
