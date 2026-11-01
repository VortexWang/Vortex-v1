// Package e2e 是 Agent 与 Edge Hub 的端到端测试（M3）：真实 Engine + Agent + Controller，经内存控制流连接
// 真实的 Edge Hub 会话处理。消息在两侧 Go 类型之间转换，位置与 gRPC 接入后的适配层相同。
// 后端由本包的 Backend 模拟：经真实的 upstream.Link 下发期望配置、Commit、份额与额度，确认账本，驱动对账。
package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"vortex/edgehub/issuer"
	"vortex/edgehub/model"
	"vortex/edgehub/session"
	"vortex/edgehub/upstream"
	"vortex/engine/types"
	"vortex/node/agent"
	"vortex/node/agent/control"
	"vortex/node/agent/grants"
	"vortex/node/agent/reconcile"
)

func toModel(u types.UUID) model.UUID { return model.UUID(u) }
func toTypes(u model.UUID) types.UUID { return types.UUID(u) }

// toHub 把 Agent 消息转为 Edge Hub 会话消息；不归会话处理的返回 nil。
func toHub(m agent.Out) session.AgentMsg {
	switch m := m.(type) {
	case agent.Hello:
		return session.Hello{Node: toModel(m.Node), Boot: toModel(m.Boot), PreviousBoot: toModel(m.PreviousBoot), AgentVersion: m.AgentVersion,
			AppliedConfigVersion: m.AppliedConfigVersion, UnackedUsageFromSeq: m.UnackedUsageFromSeq}
	case agent.GrantRequest:
		return session.GrantRequest{ID: m.ID, Sync: m.Sync, Kind: model.GrantKind(m.Kind), Tenant: toModel(m.Tenant),
			Rule: toModel(m.Rule), CurrentGrant: toModel(m.CurrentGrant), RateBps60s: m.RateBps60s}
	case agent.UsageBatch:
		b := session.UsageBatch{Boot: toModel(m.Boot), Seq: m.Seq}
		for _, c := range m.Snapshot.Consumptions {
			b.Consumptions = append(b.Consumptions, session.Consumption{Grant: toModel(c.GrantID), Used: c.UsedRawBytes, Final: c.Final})
		}
		for _, d := range m.Snapshot.Demands {
			b.Demands = append(b.Demands, session.Demand{Tenant: toModel(d.TenantID), Bytes60s: d.Bytes60s, StarvedPermille: d.StarvedPermille})
		}
		b.Snapshot, _ = json.Marshal(m.Snapshot)
		return b
	case agent.PrepareReport:
		payload, _ := json.Marshal(m)
		ok := m.Err == "" && m.Result != nil && len(m.Result.Errors) == 0
		return session.PrepareReport{Version: m.Version, OK: ok, Payload: payload}
	case agent.ApplyReport:
		payload, _ := json.Marshal(m)
		return session.ApplyReport{Version: m.Version, Committed: m.Action == reconcile.ActionCommit && m.Err == "" && m.Result != nil, Payload: payload}
	case agent.EngineEvent:
		payload, _ := json.Marshal(m)
		return session.Passthrough{Kind: "event", Payload: payload}
	case agent.ProbeReport:
		payload, _ := json.Marshal(m)
		return session.Passthrough{Kind: "probes", Payload: payload}
	case agent.StateReport:
		payload, _ := json.Marshal(m)
		return session.Passthrough{Kind: "state", Payload: payload}
	case agent.HeldGrants:
		var hs session.HeldGrants
		for _, g := range m.Grants {
			hs.Grants = append(hs.Grants, session.HeldGrant{HeldGrant: issuer.HeldGrant{ID: toModel(g.ID), IssuerHub: toModel(g.Issuer),
				Kind: model.GrantKind(g.Kind), Tenant: toModel(g.Tenant), Rule: toModel(g.Rule), Amount: g.Amount, Used: g.Used},
				Remaining: g.RemainingTTL})
		}
		return hs
	}
	return nil
}

func meta(hub model.UUID, id, boot model.UUID, gen uint64, ttl time.Duration) types.GrantMeta {
	return types.GrantMeta{GrantID: toTypes(id), IssuerHub: toTypes(hub), BootID: toTypes(boot), Generation: gen, TTL: ttl}
}

// toAgent 把 Edge Hub 会话消息转为 Agent 消息。
func toAgent(hub model.UUID, m session.HubMsg) agent.In {
	switch m := m.(type) {
	case session.HubHello:
		return agent.HubHello{Hub: toTypes(m.Hub), Mode: agent.Mode(m.Mode)}
	case session.ModeChange:
		return agent.ModeChange{Mode: agent.Mode(m.Mode)}
	case session.UsageAck:
		return agent.UsageAck{Boot: toTypes(m.Boot), Through: m.Through}
	case session.DesiredConfig:
		var cfg types.RuntimeConfig
		if err := json.Unmarshal(m.Payload, &cfg); err != nil {
			return nil
		}
		return agent.DesiredConfig{Config: &cfg}
	case session.ApplyCommand:
		a := reconcile.ActionAbort
		if m.Commit {
			a = reconcile.ActionCommit
		}
		return agent.ApplyCommand{Version: m.Version, Action: a}
	case session.DrainCommand:
		return agent.DrainCommand{}
	case session.GrantResponse:
		r := grants.Response{RequestID: m.RequestID, Error: m.Error}
		for _, g := range m.Run {
			rg := types.RunGrant{Meta: meta(hub, g.ID, g.Boot, g.Generation, g.TTL)}
			for _, id := range g.Rules {
				rg.RuleIDs = append(rg.RuleIDs, toTypes(id))
			}
			r.Set.Run = append(r.Set.Run, rg)
		}
		for _, g := range m.Billing {
			r.Set.Billing = append(r.Set.Billing, types.BillingGrant{Meta: meta(hub, g.ID, g.Boot, g.Generation, g.TTL),
				RuleID: toTypes(g.Rule), BillingVersion: g.BillingVersion, RawBytes: g.RawBytes})
		}
		for _, l := range m.Shares {
			k := types.ShareRate
			if l.Kind == model.GrantShareConn {
				k = types.ShareConn
			}
			r.Set.Shares = append(r.Set.Shares, types.ShareLease{Meta: meta(hub, l.ID, l.Boot, 0, l.TTL), TenantID: toTypes(l.Tenant), Kind: k, Amount: l.Amount})
		}
		for _, id := range m.Revoked {
			r.Revoked = append(r.Revoked, toTypes(id))
		}
		return agent.GrantResponse{Response: r}
	}
	return nil
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
