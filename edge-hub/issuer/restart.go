package issuer

import (
	"encoding/json"
	"sort"
	"time"

	"vortex/edgehub/model"
)

const (
	KindBoot     = "grant.boot"
	KindTransfer = "grant.transfer"
	KindHeld     = "grant.held"
)

type bootData struct {
	Node model.UUID `json:"node"`
	Boot model.UUID `json:"boot"`
}

type transferData struct {
	Node    model.UUID   `json:"node"`
	OldBoot model.UUID   `json:"old_boot"`
	NewBoot model.UUID   `json:"new_boot"`
	Revoked []model.UUID `json:"revoked"`  // 作废的运行授权
	Settle  []model.UUID `json:"settling"` // 转入待结算的计费授权
}

// Transfer 是一次重启转移的结果。
type Transfer struct {
	OldBoot     model.UUID
	RevokedRun  []model.UUID
	SettlingIDs []model.UUID // 旧启动的计费授权：只接受补报与结算
}

func (is *Issuer) noteBootLocked(node, boot model.UUID) {
	if _, ok := is.boots[node]; !ok && !is.dead[node][boot] {
		is.boots[node] = boot
	}
}

func (is *Issuer) applyTransferLocked(d transferData) {
	if is.dead[d.Node] == nil {
		is.dead[d.Node] = map[model.UUID]bool{}
	}
	is.dead[d.Node][d.OldBoot] = true
	is.boots[d.Node] = d.NewBoot
	for _, id := range d.Revoked {
		delete(is.grants, id)
	}
	for _, id := range d.Settle {
		if g := is.grants[id]; g != nil {
			g.Settling = true
		}
	}
}

// Connect 处理节点接入（NodeHello）。同一 node_id 以新 boot_id 接入时按 LIM-010 立即转移：作废旧启动的
// 运行授权；旧启动的计费授权转入待结算（预留额度保留至最终上报，AGT-009），新计费授权只能从 available
// 签发；份额由 leases.Manager.Transfer 按原值转移。已被取代的启动再次接入返回 ErrStaleBoot。
// 自治期间同样执行（HUB-007）。转移只按 node_id 匹配，不同节点之间不会发生。
func (is *Issuer) Connect(node, boot model.UUID) (*Transfer, error) {
	is.mu.Lock()
	defer is.mu.Unlock()
	if is.dead[node][boot] {
		return nil, ErrStaleBoot
	}
	cur, ok := is.boots[node]
	if !ok { // 首次接入：记入账本，重启后仍能识别下一次的新启动（否则 LIM-010 转移会漏掉）
		if _, err := is.l.Append(KindBoot, bootData{Node: node, Boot: boot}); err != nil {
			return nil, err
		}
		is.boots[node] = boot
		return nil, nil
	}
	if cur == boot {
		return nil, nil
	}
	d := transferData{Node: node, OldBoot: cur, NewBoot: boot}
	for id, g := range is.grants {
		if g.Node != node || g.Boot == boot {
			continue
		}
		switch g.Kind {
		case model.GrantRun:
			d.Revoked = append(d.Revoked, id)
		case model.GrantBilling:
			d.Settle = append(d.Settle, id)
		}
	}
	sortIDs(d.Revoked)
	sortIDs(d.Settle)
	if _, err := is.l.Append(KindTransfer, d); err != nil {
		return nil, err
	}
	is.applyTransferLocked(d)
	return &Transfer{OldBoot: cur, RevokedRun: d.Revoked, SettlingIDs: d.Settle}, nil
}

// Boot 返回节点当前的启动。
func (is *Issuer) Boot(node model.UUID) (model.UUID, bool) {
	is.mu.Lock()
	defer is.mu.Unlock()
	b, ok := is.boots[node]
	return b, ok
}

func sortIDs(ids []model.UUID) {
	sort.Slice(ids, func(i, j int) bool { return string(ids[i][:]) < string(ids[j][:]) })
}

// HeldGrant 是节点报告持有的一份授权（HeldGrantsReport，HUB-014）。
type HeldGrant struct {
	ID        model.UUID      `json:"id"`
	IssuerHub model.UUID      `json:"issuer_hub"`
	Kind      model.GrantKind `json:"kind"`
	Tenant    model.UUID      `json:"tenant"`
	Rule      model.UUID      `json:"rule,omitempty"`
	Amount    uint64          `json:"amount"`
	Used      uint64          `json:"used"`
	Node      model.UUID      `json:"node"`
	Boot      model.UUID      `json:"boot"`
	ExpiresAt time.Time       `json:"expires_at"`
}

type heldData struct {
	Node   model.UUID  `json:"node"`
	Grants []HeldGrant `json:"grants"`
}

func (is *Issuer) applyHeldLocked(d heldData) {
	for id, h := range is.held { // 一份报告是该节点持有授权的完整列表
		if h.Node == d.Node {
			delete(is.held, id)
		}
	}
	for _, h := range d.Grants {
		is.held[h.ID] = h
	}
}

// RecordHeld 处理节点切换到本实例后的 HeldGrantsReport（HUB-011、HUB-014）：
// 其他实例签发的授权只记录、随上报带给后端（HUB-015 回收判定），不进入本实例的区域额度，也不会被本实例
// 续期或重复签发；本实例自己签发的授权只更新已用量。返回记录的他实例授权数。
func (is *Issuer) RecordHeld(node, boot model.UUID, grants []HeldGrant, remaining func(HeldGrant) time.Duration) (int, error) {
	now := is.clk.Now()
	is.mu.Lock()
	defer is.mu.Unlock()
	d := heldData{Node: node}
	for _, h := range grants {
		if h.IssuerHub == is.HubID {
			if g := is.grants[h.ID]; g != nil && g.Kind == model.GrantBilling {
				g.Used = max(g.Used, h.Used)
			}
			continue
		}
		h.Node, h.Boot = node, boot
		if remaining != nil {
			h.ExpiresAt = now.Add(remaining(h))
		}
		d.Grants = append(d.Grants, h)
	}
	sort.Slice(d.Grants, func(i, j int) bool { return string(d.Grants[i].ID[:]) < string(d.Grants[j].ID[:]) })
	if _, err := is.l.Append(KindHeld, d); err != nil {
		return 0, err
	}
	is.applyHeldLocked(d)
	return len(d.Grants), nil
}

// Held 返回未过期的他实例授权（上报后端），按签发实例与 ID 排序。
func (is *Issuer) Held() []HeldGrant {
	now := is.clk.Now()
	is.mu.Lock()
	defer is.mu.Unlock()
	var out []HeldGrant
	for id, h := range is.held {
		if !h.ExpiresAt.IsZero() && !now.Before(h.ExpiresAt) {
			delete(is.held, id)
			continue
		}
		out = append(out, h)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].IssuerHub != out[j].IssuerHub {
			return string(out[i].IssuerHub[:]) < string(out[j].IssuerHub[:])
		}
		return string(out[i].ID[:]) < string(out[j].ID[:])
	})
	return out
}

// TransferRecord 是 grant.transfer 条目的内容（上报后端的 GrantTransferred）。
type TransferRecord struct {
	Node, OldBoot, NewBoot model.UUID
	Revoked, Settling      []model.UUID
}

// DecodeTransfer 解析 grant.transfer 条目。
func DecodeTransfer(data []byte) (TransferRecord, error) {
	var d transferData
	err := json.Unmarshal(data, &d)
	return TransferRecord{d.Node, d.OldBoot, d.NewBoot, d.Revoked, d.Settle}, err
}
