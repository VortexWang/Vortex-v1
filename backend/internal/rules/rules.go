// Package rules 是规则的保存与校验（BE-T13；RUL-001～RUL-006、SEC-011、ENG-009）以及编译快照的装载（BE-T14 的输入）。
// 保存前用编译器对“当前平台状态 + 这条规则”做完整校验，不合法即拒绝；每次保存生成新的期望版本并记录完整快照
// （rule_versions，RUL-012 回滚与 RUL-016 重新校验的依据）。全部租户数据访问都带租户条件（TEN-004）。
package rules

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"vortex/backend/compiler"
	"vortex/backend/internal/db"
	p "vortex/backend/internal/platform"
)

var (
	ErrNotFound = errors.New("rules: not found")
	ErrConflict = errors.New("rules: modified concurrently") // row_version 不符（乐观并发）
)

// ValidationError 是保存时的校验失败。
type ValidationError struct{ Problems []compiler.Problem }

func (e *ValidationError) Error() string {
	var s []string
	for _, p := range e.Problems {
		s = append(s, p.Code+": "+p.Detail)
	}
	return "rules: invalid rule: " + strings.Join(s, "; ")
}

// Spec 是用户可编辑的规则内容（RUL-001）。
type Spec struct {
	Name           string             `json:"name"`
	Network        string             `json:"network"`
	Mode           string             `json:"mode"`
	EntryGroup     compiler.UUID      `json:"entry_group"`
	ServicePort    uint16             `json:"service_port"` // 0 = 自动分配（PRT-003；端口分配在 BE-T12，本批要求指定）
	Path           []compiler.Segment `json:"path"`
	Targets        []compiler.Target  `json:"targets"`
	TargetSchedule string             `json:"target_schedule"`
	ProxyProtocol  string             `json:"proxy_protocol"`
	RateBps        uint64             `json:"rate_bps,omitempty"`
	MaxConns       uint32             `json:"max_conns,omitempty"`
	Enabled        bool               `json:"enabled"`
}

type Service struct{ DB *pgxpool.Pool }

// Saved 是保存结果。
type Saved struct {
	ID         p.UUID
	Version    uint64 // desired_version
	RowVersion int64
}

// normalize 把规则中的组 ID 规范为小写 8-4-4-4-12。
func normalize(spec *Spec) error {
	g, err := ParseUUID(string(spec.EntryGroup))
	if err != nil {
		return &ValidationError{[]compiler.Problem{{Code: compiler.ErrGroupMissing, Detail: err.Error()}}}
	}
	spec.EntryGroup = CID(g)
	path := make([]compiler.Segment, len(spec.Path))
	for i, sg := range spec.Path {
		g, err := ParseUUID(string(sg.Group))
		if err != nil {
			return &ValidationError{[]compiler.Problem{{Code: compiler.ErrGroupMissing, Detail: err.Error()}}}
		}
		path[i] = compiler.Segment{Group: CID(g), Dial: sg.Dial}
		if path[i].Dial == "" {
			path[i].Dial = "auto"
		}
	}
	spec.Path = path
	return nil
}

// Create 新建规则。
func (s *Service) Create(ctx context.Context, tenant p.UUID, spec Spec) (*Saved, error) {
	if err := normalize(&spec); err != nil {
		return nil, err
	}
	if strings.TrimSpace(spec.Name) == "" {
		return nil, &ValidationError{[]compiler.Problem{{Code: compiler.ErrInvalid, Detail: "name is required"}}}
	}
	entry, err := ParseUUID(string(spec.EntryGroup))
	if err != nil {
		return nil, &ValidationError{[]compiler.Problem{{Code: compiler.ErrGroupMissing, Detail: err.Error()}}}
	}
	id := p.NewUUIDv7()
	var out *Saved
	err = p.InTx(ctx, s.DB, func(tx pgx.Tx) error {
		q := db.New(tx)
		if err := s.validate(ctx, q, tenant, id, 1, spec); err != nil {
			return err
		}
		if err := q.InsertRule(ctx, db.InsertRuleParams{ID: id.PG(), TenantID: tenant.PG(), Name: spec.Name, Network: spec.Network,
			ForwardMode: spec.Mode, EntryGroupID: entry.PG(), ServicePort: port(spec.ServicePort), TargetSchedule: spec.TargetSchedule,
			ProxyProtocol: spec.ProxyProtocol, LimitRateBps: i64(spec.RateBps), LimitMaxConns: i32(spec.MaxConns), Enabled: spec.Enabled}); err != nil {
			return err
		}
		if err := writeChildren(ctx, q, id, spec); err != nil {
			return err
		}
		if err := recordVersion(ctx, q, id, 1, spec); err != nil {
			return err
		}
		out = &Saved{ID: id, Version: 1, RowVersion: 1}
		return nil
	})
	return out, err
}

// Update 修改规则：rowVersion 须与当前一致（乐观并发）；期望版本递增。
func (s *Service) Update(ctx context.Context, tenant, id p.UUID, rowVersion int64, spec Spec) (*Saved, error) {
	if err := normalize(&spec); err != nil {
		return nil, err
	}
	entry, err := ParseUUID(string(spec.EntryGroup))
	if err != nil {
		return nil, &ValidationError{[]compiler.Problem{{Code: compiler.ErrGroupMissing, Detail: err.Error()}}}
	}
	var out *Saved
	err = p.InTx(ctx, s.DB, func(tx pgx.Tx) error {
		q := db.New(tx)
		cur, err := q.LockRuleForTenant(ctx, db.LockRuleForTenantParams{ID: id.PG(), TenantID: tenant.PG()})
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		} else if err != nil {
			return err
		}
		next := uint64(cur.DesiredVersion) + 1
		if err := s.validate(ctx, q, tenant, id, next, spec); err != nil {
			return err
		}
		n, err := q.UpdateRuleForTenant(ctx, db.UpdateRuleForTenantParams{ID: id.PG(), TenantID: tenant.PG(), Name: spec.Name,
			Network: spec.Network, ForwardMode: spec.Mode, EntryGroupID: entry.PG(), ServicePort: port(spec.ServicePort),
			TargetSchedule: spec.TargetSchedule, ProxyProtocol: spec.ProxyProtocol, LimitRateBps: i64(spec.RateBps),
			LimitMaxConns: i32(spec.MaxConns), Enabled: spec.Enabled, RowVersion: rowVersion})
		if err != nil {
			return err
		}
		if n == 0 {
			return ErrConflict
		}
		if err := q.DeleteRulePaths(ctx, id.PG()); err != nil {
			return err
		}
		if err := q.DeleteRuleTargets(ctx, id.PG()); err != nil {
			return err
		}
		if err := writeChildren(ctx, q, id, spec); err != nil {
			return err
		}
		if err := recordVersion(ctx, q, id, next, spec); err != nil {
			return err
		}
		out = &Saved{ID: id, Version: next, RowVersion: rowVersion + 1}
		return nil
	})
	return out, err
}

// Delete 删除规则（软删除，保留历史）。
func (s *Service) Delete(ctx context.Context, tenant, id p.UUID) error {
	n, err := db.New(s.DB).SoftDeleteRuleForTenant(ctx, db.SoftDeleteRuleForTenantParams{ID: id.PG(), TenantID: tenant.PG()})
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// Get 读取规则的当前期望内容与版本。
func (s *Service) Get(ctx context.Context, tenant, id p.UUID) (*Spec, *Saved, error) {
	q := db.New(s.DB)
	r, err := q.GetRuleForTenant(ctx, db.GetRuleForTenantParams{ID: id.PG(), TenantID: tenant.PG()})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, ErrNotFound
	} else if err != nil {
		return nil, nil, err
	}
	v, err := q.GetRuleVersion(ctx, db.GetRuleVersionParams{RuleID: id.PG(), Version: r.DesiredVersion})
	if err != nil {
		return nil, nil, err
	}
	var spec Spec
	if err := json.Unmarshal(v.Snapshot, &spec); err != nil {
		return nil, nil, err
	}
	return &spec, &Saved{ID: id, Version: uint64(r.DesiredVersion), RowVersion: r.RowVersion}, nil
}

// validate 用当前平台状态校验这条规则（编译器的完整路径与目标检查）。
func (s *Service) validate(ctx context.Context, q *db.Queries, tenant, id p.UUID, version uint64, spec Spec) error {
	snap, err := LoadSnapshot(ctx, q)
	if err != nil {
		return err
	}
	r := ToCompiler(id, tenant, version, 0, spec)
	if probs := compiler.Validate(snap, r); len(probs) > 0 {
		return &ValidationError{Problems: probs}
	}
	return nil
}

func writeChildren(ctx context.Context, q *db.Queries, id p.UUID, spec Spec) error {
	for i, sg := range spec.Path {
		g, err := ParseUUID(string(sg.Group))
		if err != nil {
			return err
		}
		d := sg.Dial
		if d == "" {
			d = "auto"
		}
		if err := q.InsertRulePath(ctx, db.InsertRulePathParams{RuleID: id.PG(), Position: int16(i + 1), NodeGroupID: g.PG(), DialDirection: d}); err != nil {
			return err
		}
	}
	for i, t := range spec.Targets {
		if err := q.InsertRuleTarget(ctx, db.InsertRuleTargetParams{RuleID: id.PG(), Position: int16(i + 1), Host: t.Host, Port: int32(t.Port),
			Weight: int32(max(t.Weight, 1)), Priority: int32(t.Priority)}); err != nil {
			return err
		}
	}
	return nil
}

func recordVersion(ctx context.Context, q *db.Queries, id p.UUID, v uint64, spec Spec) error {
	b, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	return q.InsertRuleVersion(ctx, db.InsertRuleVersionParams{RuleID: id.PG(), Version: int64(v), Snapshot: b})
}

// ToCompiler 把保存的规则内容转为编译器输入。
func ToCompiler(id, tenant p.UUID, version, pathGen uint64, spec Spec) compiler.Rule {
	if pathGen == 0 {
		pathGen = version
	}
	return compiler.Rule{ID: CID(id), Tenant: CID(tenant), Version: version, PathGeneration: pathGen, Network: spec.Network, Mode: spec.Mode,
		EntryGroup: spec.EntryGroup, Path: spec.Path, Port: spec.ServicePort, Targets: spec.Targets, TargetSchedule: spec.TargetSchedule,
		ProxyProtocol: spec.ProxyProtocol, RateBps: spec.RateBps, MaxConns: spec.MaxConns, Enabled: spec.Enabled}
}

// CID 把 UUID 转为编译器的文本形式。
func CID(u p.UUID) compiler.UUID { return compiler.UUID(u.String()) }

// ParseUUID 解析 8-4-4-4-12。
func ParseUUID(s string) (p.UUID, error) {
	var u p.UUID
	b, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	if err != nil || len(b) != 16 {
		return u, fmt.Errorf("invalid uuid %q", s)
	}
	copy(u[:], b)
	return u, nil
}

func port(v uint16) *int32 {
	if v == 0 {
		return nil
	}
	x := int32(v)
	return &x
}

func i64(v uint64) *int64 {
	if v == 0 {
		return nil
	}
	x := int64(v)
	return &x
}

func i32(v uint32) *int32 {
	if v == 0 {
		return nil
	}
	x := int32(v)
	return &x
}
