package leases

import (
	"encoding/json"
	"sort"

	"vortex/edgehub/model"
)

type frozenSnap struct {
	Key
	Total uint64 `json:"total"`
}

type snapshot struct {
	Leases []Lease                   `json:"leases,omitempty"`
	Boots  map[model.UUID]model.UUID `json:"boots,omitempty"`
	Frozen []frozenSnap              `json:"frozen,omitempty"`
	IsFrz  bool                      `json:"is_frozen,omitempty"`
}

// Snapshot 返回租约与冻结状态（账本检查点）。到期的租约不再保留。需求信号不持久化（与账本一致）。
func (m *Manager) Snapshot() (json.RawMessage, error) {
	now := m.clk.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	s := snapshot{Boots: m.boot, IsFrz: m.frozen != nil}
	for _, h := range m.held {
		for _, l := range h.leases {
			if l.validAt(now) {
				s.Leases = append(s.Leases, l)
			}
		}
	}
	sort.SliceStable(s.Leases, func(i, j int) bool { return s.Leases[i].IssuedAt.Before(s.Leases[j].IssuedAt) })
	for k, v := range m.frozen {
		s.Frozen = append(s.Frozen, frozenSnap{k, v})
	}
	sort.Slice(s.Frozen, func(i, j int) bool { return lessKey(s.Frozen[i].Key, s.Frozen[j].Key) })
	return json.Marshal(s)
}

// Restore 以检查点替换租约与冻结状态。
func (m *Manager) Restore(data json.RawMessage) error {
	var s snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.held = map[nodeKey]*held{}
	m.boot = map[model.UUID]model.UUID{}
	for _, l := range s.Leases {
		m.addLocked(l)
	}
	for n, b := range s.Boots {
		m.boot[n] = b
	}
	m.frozen = nil
	if s.IsFrz {
		m.frozen = map[Key]uint64{}
		for _, f := range s.Frozen {
			m.frozen[f.Key] = f.Total
		}
	}
	return nil
}
