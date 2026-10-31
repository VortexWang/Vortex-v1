// Package e2e 是 Agent 与 Edge Hub 的端到端测试（M3）：真实 Engine + Agent + Controller，经内存控制流连接
// 真实的 Edge Hub 会话处理。消息在两侧 Go 类型之间转换，位置与 gRPC 接入后的适配层相同。
// 配置分发（HUB-T04）尚未实现，由本包的 distributor 代为下发期望配置并自动 Commit。
package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"vortex/edgehub/issuer"
	"vortex/edgehub/leases"
	"vortex/edgehub/model"
	"vortex/edgehub/session"
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
		return session.Hello{Node: toModel(m.Node), Boot: toModel(m.Boot), PreviousBoot: toModel(m.PreviousBoot),
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

// Hub 是测试中的一个 Edge Hub 实例，外加代为分发配置的 distributor。
type Hub struct {
	*session.Hub
	Cfg    *types.RuntimeConfig
	Shares []leases.Lease // 后端为节点生成的初始份额（在线时）

	mu       sync.Mutex
	Prepares []agent.PrepareReport
	Applies  []agent.ApplyReport
	Down     bool
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
	h := c.hub
	switch m := m.(type) {
	case agent.PrepareReport: // distributor：没有资源错误就 Commit
		h.mu.Lock()
		h.Prepares = append(h.Prepares, m)
		h.mu.Unlock()
		if m.Err == "" && m.Result != nil && len(m.Result.Errors) == 0 {
			c.push(agent.ApplyCommand{Version: m.Version, Action: reconcile.ActionCommit})
		}
		return nil
	case agent.ApplyReport:
		h.mu.Lock()
		h.Applies = append(h.Applies, m)
		h.mu.Unlock()
		return nil
	}
	am := toHub(m)
	if am == nil {
		return nil
	}
	if err := c.s.Handle(am); err != nil {
		c.Close()
		return err
	}
	if hello, ok := m.(agent.Hello); ok {
		// 后端在节点接入后为其生成份额（在线时）；distributor 下发期望配置（尚未生效时）
		if len(h.Leases.Current(toModel(hello.Node))) == 0 && len(h.Shares) > 0 && h.Mode.AcceptsConfigChanges() {
			fresh, _ := h.Leases.ApplyUpdate(toModel(hello.Node), toModel(hello.Boot), h.Shares)
			_ = fresh
		}
		if h.Cfg != nil && hello.AppliedConfigVersion < h.Cfg.Version {
			c.push(agent.DesiredConfig{Config: h.Cfg})
		}
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
