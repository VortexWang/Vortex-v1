package quota

import (
	"encoding/json"

	"vortex/edgehub/model"
)

type accountSnap struct {
	Version   uint64                     `json:"version"`
	Delegated uint64                     `json:"delegated"`
	Acked     uint64                     `json:"acked"`
	Settled   uint64                     `json:"settled"`
	Issued    uint64                     `json:"issued"`
	Grants    map[model.UUID]reservation `json:"grants,omitempty"`
}

// Snapshot 返回全部账户的完整状态（账本检查点，HUB-012 保留期清理）。
func (q *Accounts) Snapshot() (json.RawMessage, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := map[model.UUID]accountSnap{}
	for t, a := range q.tenants {
		g := make(map[model.UUID]reservation, len(a.grants))
		for id, r := range a.grants {
			g[id] = r
		}
		out[t] = accountSnap{a.version, a.delegated, a.acked, a.settled, a.issued, g}
	}
	return json.Marshal(out)
}

// Restore 以检查点替换全部账户。
func (q *Accounts) Restore(data json.RawMessage) error {
	var in map[model.UUID]accountSnap
	if err := json.Unmarshal(data, &in); err != nil {
		return err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.tenants = map[model.UUID]*account{}
	for t, s := range in {
		g := s.Grants
		if g == nil {
			g = map[model.UUID]reservation{}
		}
		q.tenants[t] = &account{version: s.Version, delegated: s.Delegated, acked: s.Acked, settled: s.Settled, issued: s.Issued, grants: g}
	}
	return nil
}
