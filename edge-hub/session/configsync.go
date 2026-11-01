package session

import (
	"errors"

	"vortex/edgehub/issuer"
	"vortex/edgehub/leases"
	"vortex/edgehub/model"
)

// 运行配置分发与两阶段指令转发（HUB-002、RUL-010，HUB-T04）；自治范围限制（HUB-007，HUB-T14）。
//
// Edge Hub 不生成、不修改配置，只转发后端的期望配置与 Commit / Abort，并跟踪节点的 Prepare / Apply 结果
// （结果原样转发给后端，两阶段的编排在后端）。
//
//   - 每个节点只保留最新的期望版本；节点接入时若已生效版本较旧，重新下发；
//   - Commit / Abort 只针对当前期望版本，节点报告该版本 Prepare 成功后才转发；节点不在线时保留，
//     重新接入并再次 Prepare 后转发；
//   - 不在线模式（对账中）收到的配置与指令积压，回到在线后放行（HUB-013 ⑤）；自治期间与后端无连接，
//     不会收到新配置；注册、端口与 DNS 指令同样只在在线时接受（AcceptsConfigChanges）。

var ErrStaleCommand = errors.New("session: command for a version that is not the desired one")

type nodeConfig struct {
	desired   *DesiredConfig
	command   *ApplyCommand
	prepared  uint64 // 节点报告 Prepare 成功的版本
	applied   uint64 // 节点报告 Commit 成功的版本（或 Hello 中的已生效版本）
	delivered bool   // 当前期望版本已在当前会话上下发
}

func (h *Hub) cfgLocked(node model.UUID) *nodeConfig {
	c := h.configs[node]
	if c == nil {
		c = &nodeConfig{}
		h.configs[node] = c
	}
	return c
}

// SetDesired 接收后端的期望配置（NodeDesiredConfig）。
func (h *Hub) SetDesired(node model.UUID, version uint64, payload []byte) {
	h.mu.Lock()
	c := h.cfgLocked(node)
	if c.desired != nil && version <= c.desired.Version {
		h.mu.Unlock()
		return // 旧版本或重复
	}
	c.desired = &DesiredConfig{Version: version, Payload: payload}
	c.command, c.delivered = nil, false
	h.mu.Unlock()
	if h.opt.ParseRules != nil {
		if rules, err := h.opt.ParseRules(payload); err == nil {
			_ = h.recordRules(node, version, rules)
		}
	}
	h.flushConfig(node)
}

// ---------------------------------------------------------------- 签发所需的规则信息

// KindRules 记录节点期望配置中的规则信息（签发运行授权与计费授权的依据），重启（包括自治中）后可恢复。
const KindRules = "config.rules"

type rulesRecord struct {
	Node    model.UUID         `json:"node"`
	Version uint64             `json:"version"`
	Rules   []issuer.RuleState `json:"rules"`
}

type nodeRules struct {
	version   uint64
	cur, prev []issuer.RuleState
}

func (h *Hub) applyRulesLocked(r rulesRecord) {
	nr := h.rules[r.Node]
	if nr == nil {
		nr = &nodeRules{}
		h.rules[r.Node] = nr
	}
	if r.Version <= nr.version {
		return
	}
	nr.version, nr.prev, nr.cur = r.Version, nr.cur, r.Rules
}

func (h *Hub) recordRules(node model.UUID, version uint64, rules []issuer.RuleState) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if nr := h.rules[node]; nr != nil && version <= nr.version {
		return nil
	}
	r := rulesRecord{Node: node, Version: version, Rules: rules}
	if _, err := h.Ledger.Append(KindRules, r); err != nil {
		return err
	}
	h.applyRulesLocked(r)
	return nil
}

// configRules 返回节点最新与上一个期望配置中规则的并集：新版本 Commit 之前节点仍在运行旧版本的规则，
// 两者都要能续期（被删除的规则在 Commit 后不再出现在下一次期望配置中，随之停止续期）。
func (h *Hub) configRules(node model.UUID) []issuer.RuleState {
	h.mu.Lock()
	defer h.mu.Unlock()
	nr := h.rules[node]
	if nr == nil {
		return nil
	}
	seen := map[model.UUID]bool{}
	var out []issuer.RuleState
	for _, set := range [][]issuer.RuleState{nr.cur, nr.prev} {
		for _, r := range set {
			if !seen[r.RuleID] {
				seen[r.RuleID] = true
				out = append(out, r)
			}
		}
	}
	return out
}

// SetCommand 接收后端的两阶段指令（NodeApplyCommand）。
func (h *Hub) SetCommand(node model.UUID, version uint64, commit bool) error {
	h.mu.Lock()
	c := h.cfgLocked(node)
	if c.desired == nil || c.desired.Version != version {
		h.mu.Unlock()
		return ErrStaleCommand
	}
	c.command = &ApplyCommand{Version: version, Commit: commit}
	h.mu.Unlock()
	h.flushConfig(node)
	return nil
}

// flushConfig 在允许时把节点的期望配置与指令下发到其会话。
func (h *Hub) flushConfig(node model.UUID) {
	if !h.Mode.AcceptsConfigChanges() {
		return // 积压，回到在线后 ReleaseConfig
	}
	h.mu.Lock()
	c := h.configs[node]
	s := h.sessions[node]
	if c == nil || s == nil || c.desired == nil {
		h.mu.Unlock()
		return
	}
	var out []HubMsg
	if c.desired.Version > c.applied && !c.delivered {
		c.delivered = true
		out = append(out, *c.desired)
	}
	if c.command != nil && c.prepared == c.command.Version {
		out = append(out, *c.command)
		c.command = nil
	}
	h.mu.Unlock()
	for _, m := range out {
		s.send(m)
	}
}

// deliverConfig：节点接入（新会话）时调用。
func (h *Hub) deliverConfig(node model.UUID, applied uint64) {
	h.mu.Lock()
	c := h.cfgLocked(node)
	c.applied = max(c.applied, applied)
	c.delivered = false
	c.prepared = 0 // 新会话：需要重新 Prepare（例如节点重启后恢复的是旧版本）
	h.mu.Unlock()
	h.flushConfig(node)
}

func (h *Hub) onPrepared(node model.UUID, r PrepareReport) {
	h.mu.Lock()
	if r.OK {
		h.cfgLocked(node).prepared = r.Version
	}
	h.mu.Unlock()
	h.flushConfig(node)
}

func (h *Hub) onApplied(node model.UUID, r ApplyReport) {
	if !r.Committed {
		return
	}
	h.mu.Lock()
	c := h.cfgLocked(node)
	c.applied = max(c.applied, r.Version)
	h.mu.Unlock()
}

// ReleaseConfig 在回到在线后放行积压的配置与指令（HUB-013 ⑤）。
func (h *Hub) ReleaseConfig() {
	h.mu.Lock()
	nodes := make([]model.UUID, 0, len(h.configs))
	for n := range h.configs {
		nodes = append(nodes, n)
	}
	h.mu.Unlock()
	for _, n := range nodes {
		h.flushConfig(n)
	}
}

// AcceptsEnrollment：自治与对账期间不接受节点注册（HUB-007）。端口与 DNS 指令由后端在线下发，同样受此限制。
func (h *Hub) AcceptsEnrollment() bool { return h.Mode.AcceptsConfigChanges() }

// SendDrain 转发后端的排空指令（NodeOperation.drain）。节点不在线时返回 false。
func (h *Hub) SendDrain(node model.UUID, payload []byte) bool {
	h.mu.Lock()
	s := h.sessions[node]
	h.mu.Unlock()
	if s == nil {
		return false
	}
	s.send(DrainCommand{Payload: payload})
	return true
}

// SendUpgrade 转发后端的升级指令（NodeOperation.upgrade）。节点不在线时返回 false（后端据节点状态重发）。
func (h *Hub) SendUpgrade(node model.UUID, payload []byte) bool {
	h.mu.Lock()
	s := h.sessions[node]
	h.mu.Unlock()
	if s == nil {
		return false
	}
	s.send(UpgradeCommand{Payload: payload})
	return true
}

var ErrUnknownNode = errors.New("session: node has not connected to this hub")

// PushShares 转发后端为节点当前启动生成的份额租约（在线时，LIM-004）：记录并推送给节点。
// 自治与对账期间份额由本实例冻结与再平衡，不接受后端的新租约（不得增加份额，HUB-007）。
func (h *Hub) PushShares(node model.UUID, ls []leases.Lease) ([]leases.Lease, error) {
	if !h.Mode.AcceptsConfigChanges() {
		return nil, ErrNotOnline
	}
	boot, ok := h.Issuer.Boot(node)
	if !ok {
		return nil, ErrUnknownNode
	}
	out, err := h.Leases.ApplyUpdate(node, boot, ls)
	if err != nil {
		return nil, err
	}
	h.mu.Lock()
	s := h.sessions[node]
	h.mu.Unlock()
	if s != nil {
		rel := make([]leases.Lease, len(out))
		for i, l := range out {
			rel[i] = h.relLease(l)
		}
		s.send(GrantResponse{Shares: rel})
	}
	return out, nil
}

var ErrNotOnline = errors.New("session: not online")
