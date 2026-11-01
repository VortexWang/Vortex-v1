package session

import (
	"encoding/json"
	"sort"
	"time"

	"vortex/edgehub/issuer"
	"vortex/edgehub/ledger"
	"vortex/edgehub/model"
)

// 账本检查点与保留期清理（HUB-012）。
//
// 检查点是一条账本条目，带各模块的完整状态；重放遇到它时各模块以它替换状态，之前的条目只用于上报。
// 清理只删除：早于最近一次检查点、已被后端确认、且超过保留期（默认 7 天）的条目；对账完成前（不在线）
// 不清理（HUB-012）。计量批次的幂等键只保留保留期内的（Agent 不会重发已确认的批次；后端按幂等键去重）。

const (
	KindCheckpoint = "checkpoint"
	Retention      = 7 * 24 * time.Hour
)

type checkpoint struct {
	Mode   json.RawMessage `json:"mode"`
	Quota  json.RawMessage `json:"quota"`
	Issuer json.RawMessage `json:"issuer"`
	Leases json.RawMessage `json:"leases"`
	Usage  []usageSnap     `json:"usage,omitempty"`
	Rules  []rulesSnap     `json:"rules,omitempty"`
}

type rulesSnap struct {
	Node    model.UUID         `json:"node"`
	Version uint64             `json:"version"`
	Cur     []issuer.RuleState `json:"cur"`
	Prev    []issuer.RuleState `json:"prev,omitempty"`
}

type usageSnap struct {
	Node   model.UUID `json:"node"`
	Boot   model.UUID `json:"boot"`
	Seq    uint64     `json:"seq"`
	Ledger uint64     `json:"ledger"`
	At     time.Time  `json:"at"`
}

func (h *Hub) restore(data json.RawMessage) error {
	var c checkpoint
	if err := json.Unmarshal(data, &c); err != nil {
		return err
	}
	if err := h.Mode.Restore(c.Mode); err != nil {
		return err
	}
	if err := h.Quota.Restore(c.Quota); err != nil {
		return err
	}
	if err := h.Issuer.Restore(c.Issuer); err != nil {
		return err
	}
	if err := h.Leases.Restore(c.Leases); err != nil {
		return err
	}
	h.rules = map[model.UUID]*nodeRules{}
	for _, r := range c.Rules {
		h.rules[r.Node] = &nodeRules{version: r.Version, cur: r.Cur, prev: r.Prev}
	}
	h.usage = map[usageKey]usageSeen{}
	for _, u := range c.Usage {
		h.usage[usageKey{u.Node, u.Boot, u.Seq}] = usageSeen{u.Ledger, u.At}
	}
	return nil
}

// Checkpoint 写入一个检查点，返回其序号。
//
// 一致性：各模块的每次变更都是“写账本 + 改内存”在该模块的锁内完成；快照前后账本最大序号不变
// （AppendIf）即说明快照期间没有任何变更，各模块快照拼起来就是同一时刻的状态。有并发写入时重试。
func (h *Hub) Checkpoint() (uint64, error) {
	for i := 0; ; i++ {
		before := h.Ledger.LastSeq()
		c, err := h.snapshot()
		if err != nil {
			return 0, err
		}
		seq, err := h.Ledger.AppendIf(before, KindCheckpoint, c)
		if err == ledger.ErrConflict && i < 100 {
			continue
		}
		return seq, err
	}
}

func (h *Hub) snapshot() (checkpoint, error) {
	var c checkpoint
	var err error
	if c.Mode, err = h.Mode.Snapshot(); err != nil {
		return c, err
	}
	if c.Quota, err = h.Quota.Snapshot(); err != nil {
		return c, err
	}
	if c.Issuer, err = h.Issuer.Snapshot(); err != nil {
		return c, err
	}
	if c.Leases, err = h.Leases.Snapshot(); err != nil {
		return c, err
	}
	now := h.Clk.Now()
	acked := h.Ledger.Acked()
	h.mu.Lock()
	for k, u := range h.usage {
		if now.Sub(u.at) < Retention || u.seq > acked {
			c.Usage = append(c.Usage, usageSnap{k.node, k.boot, k.seq, u.seq, u.at})
		}
	}
	for n, r := range h.rules {
		c.Rules = append(c.Rules, rulesSnap{n, r.version, r.cur, r.prev})
	}
	h.mu.Unlock()
	sort.Slice(c.Usage, func(i, j int) bool { return c.Usage[i].Ledger < c.Usage[j].Ledger })
	sort.Slice(c.Rules, func(i, j int) bool { return string(c.Rules[i].Node[:]) < string(c.Rules[j].Node[:]) })
	return c, nil
}

// Compact 写检查点并清理保留期外的条目。不在线时不做任何事。返回删除的条数。
func (h *Hub) Compact() (int, error) {
	if h.Mode.Mode() != model.ModeOnline {
		return 0, nil
	}
	cp, err := h.Checkpoint()
	if err != nil {
		return 0, err
	}
	now := h.Clk.Now()
	acked := h.Ledger.Acked()
	h.mu.Lock()
	for k, u := range h.usage { // 与检查点相同的保留规则
		if now.Sub(u.at) >= Retention && u.seq <= acked {
			delete(h.usage, k)
		}
	}
	h.mu.Unlock()
	return h.Ledger.Prune(func(e ledger.Entry) bool {
		return e.Seq < cp && now.Sub(e.At) >= Retention
	})
}
