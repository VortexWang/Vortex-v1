package issuer

import (
	"encoding/json"
	"sort"

	"vortex/edgehub/model"
)

// grantSnap：Used（最近一次非 final 上报）与普通账本一样不持久化，重启后由下一次上报补上。
type grantSnap struct {
	Grant
	Settling bool `json:"settling,omitempty"`
}

type snapshot struct {
	Restrict []TenantRestriction         `json:"restrict,omitempty"`
	RVer     uint64                      `json:"rver"`
	Versions []BillingVersion            `json:"versions,omitempty"`
	Grants   []grantSnap                 `json:"grants,omitempty"`
	Boots    map[model.UUID]model.UUID   `json:"boots,omitempty"`
	Dead     map[model.UUID][]model.UUID `json:"dead,omitempty"`
	Held     []HeldGrant                 `json:"held,omitempty"`
	Revoked  []model.UUID                `json:"revoked,omitempty"`
}

// Snapshot 返回签发器的完整状态（账本检查点）。过期的运行授权不再保留。
func (is *Issuer) Snapshot() (json.RawMessage, error) {
	now := is.clk.Now()
	is.mu.Lock()
	defer is.mu.Unlock()
	is.pruneRunLocked(now)
	s := snapshot{RVer: is.rver, Versions: is.versions, Boots: is.boots, Dead: map[model.UUID][]model.UUID{}}
	for _, r := range is.restrict {
		s.Restrict = append(s.Restrict, r)
	}
	sort.Slice(s.Restrict, func(i, j int) bool { return string(s.Restrict[i].Tenant[:]) < string(s.Restrict[j].Tenant[:]) })
	for _, g := range is.grants {
		s.Grants = append(s.Grants, grantSnap{Grant: *g, Settling: g.Settling})
	}
	sort.Slice(s.Grants, func(i, j int) bool { return string(s.Grants[i].ID[:]) < string(s.Grants[j].ID[:]) })
	for n, bs := range is.dead {
		for b := range bs {
			s.Dead[n] = append(s.Dead[n], b)
		}
		sortIDs(s.Dead[n])
	}
	for _, h := range is.held {
		s.Held = append(s.Held, h)
	}
	sort.Slice(s.Held, func(i, j int) bool { return string(s.Held[i].ID[:]) < string(s.Held[j].ID[:]) })
	for t := range is.revoked {
		s.Revoked = append(s.Revoked, t)
	}
	sortIDs(s.Revoked)
	return json.Marshal(s)
}

// Restore 以检查点替换签发器状态。
func (is *Issuer) Restore(data json.RawMessage) error {
	var s snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	is.mu.Lock()
	defer is.mu.Unlock()
	is.restrict = map[model.UUID]TenantRestriction{}
	for _, r := range s.Restrict {
		is.restrict[r.Tenant] = r
	}
	is.rver, is.versions = s.RVer, s.Versions
	is.grants = map[model.UUID]*Grant{}
	for _, gs := range s.Grants {
		g := gs.Grant
		g.Settling = gs.Settling
		is.grants[g.ID] = &g
	}
	is.boots = map[model.UUID]model.UUID{}
	for n, b := range s.Boots {
		is.boots[n] = b
	}
	is.dead = map[model.UUID]map[model.UUID]bool{}
	for n, bs := range s.Dead {
		is.dead[n] = map[model.UUID]bool{}
		for _, b := range bs {
			is.dead[n][b] = true
		}
	}
	is.held = map[model.UUID]HeldGrant{}
	for _, h := range s.Held {
		is.held[h.ID] = h
	}
	is.applyRevokedLocked(s.Revoked)
	return nil
}
