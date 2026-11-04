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
	"vortex/backend/internal/billing"
	"vortex/backend/internal/db"
	p "vortex/backend/internal/platform"
	"vortex/backend/internal/ports"
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
		if err := s.validate(ctx, q, tenant, id, 1, withPort(spec)); err != nil {
			return err
		}
		if err := q.InsertRule(ctx, db.InsertRuleParams{ID: id.PG(), TenantID: tenant.PG(), Name: spec.Name, Network: spec.Network,
			ForwardMode: spec.Mode, EntryGroupID: entry.PG(), ServicePort: port(spec.ServicePort), TargetSchedule: spec.TargetSchedule,
			ProxyProtocol: spec.ProxyProtocol, LimitRateBps: i64(spec.RateBps), LimitMaxConns: i32(spec.MaxConns), Enabled: spec.Enabled}); err != nil {
			return err
		}
		if err := reserve(ctx, q, tenant, id, entry, &spec); err != nil {
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
	var out *Saved
	err := p.InTx(ctx, s.DB, func(tx pgx.Tx) error {
		var err error
		out, err = s.update(ctx, db.New(tx), tenant, id, rowVersion, spec)
		return err
	})
	return out, err
}

// Rollback 以版本 to 的内容生成新的递增版本（RUL-012）：与修改走同一校验（RUL-016：重新检查当前权限与资源，
// 旧内容不得重新开放已失效的权限）与端口预留。
func (s *Service) Rollback(ctx context.Context, tenant, id p.UUID, to uint64) (*Saved, error) {
	var out *Saved
	err := p.InTx(ctx, s.DB, func(tx pgx.Tx) error {
		q := db.New(tx)
		cur, err := q.LockRuleForTenant(ctx, db.LockRuleForTenantParams{ID: id.PG(), TenantID: tenant.PG()})
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		} else if err != nil {
			return err
		}
		v, err := q.GetRuleVersion(ctx, db.GetRuleVersionParams{RuleID: id.PG(), Version: int64(to)})
		if err != nil {
			return err
		}
		var spec Spec
		if err := json.Unmarshal(v.Snapshot, &spec); err != nil {
			return err
		}
		out, err = s.update(ctx, q, tenant, id, cur.RowVersion, spec)
		return err
	})
	return out, err
}

func (s *Service) update(ctx context.Context, q *db.Queries, tenant, id p.UUID, rowVersion int64, spec Spec) (*Saved, error) {
	if err := normalize(&spec); err != nil {
		return nil, err
	}
	entry, err := ParseUUID(string(spec.EntryGroup))
	if err != nil {
		return nil, &ValidationError{[]compiler.Problem{{Code: compiler.ErrGroupMissing, Detail: err.Error()}}}
	}
	cur, err := q.LockRuleForTenant(ctx, db.LockRuleForTenantParams{ID: id.PG(), TenantID: tenant.PG()})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}
	next := uint64(cur.DesiredVersion) + 1
	if err := s.validate(ctx, q, tenant, id, next, withPort(spec)); err != nil {
		return nil, err
	}
	n, err := q.UpdateRuleForTenant(ctx, db.UpdateRuleForTenantParams{ID: id.PG(), TenantID: tenant.PG(), Name: spec.Name,
		Network: spec.Network, ForwardMode: spec.Mode, EntryGroupID: entry.PG(), ServicePort: port(spec.ServicePort),
		TargetSchedule: spec.TargetSchedule, ProxyProtocol: spec.ProxyProtocol, LimitRateBps: i64(spec.RateBps),
		LimitMaxConns: i32(spec.MaxConns), Enabled: spec.Enabled, RowVersion: rowVersion})
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, ErrConflict
	}
	if err := reserve(ctx, q, tenant, id, entry, &spec); err != nil {
		return nil, err
	}
	if err := q.DeleteRulePaths(ctx, id.PG()); err != nil {
		return nil, err
	}
	if err := q.DeleteRuleTargets(ctx, id.PG()); err != nil {
		return nil, err
	}
	if err := writeChildren(ctx, q, id, spec); err != nil {
		return nil, err
	}
	if err := recordVersion(ctx, q, id, next, spec); err != nil {
		return nil, err
	}
	return &Saved{ID: id, Version: next, RowVersion: rowVersion + 1}, nil
}

// Delete 删除规则（软删除，保留历史）；端口预留转为 releasing，节点确认不再监听后释放（PRT-009）。
func (s *Service) Delete(ctx context.Context, tenant, id p.UUID) error {
	return p.InTx(ctx, s.DB, func(tx pgx.Tx) error {
		q := db.New(tx)
		n, err := q.SoftDeleteRuleForTenant(ctx, db.SoftDeleteRuleForTenantParams{ID: id.PG(), TenantID: tenant.PG()})
		if err != nil {
			return err
		}
		if n == 0 {
			return ErrNotFound
		}
		return ports.Release(ctx, q, id)
	})
}

// withPort：保存前校验时，自动分配的端口尚未选定，以占位端口校验其余内容（端口由 reserve 检查）。
func withPort(spec Spec) Spec {
	if spec.ServicePort == 0 {
		spec.ServicePort = ports.DefaultMin
	}
	return spec
}

// reserve 预留端口（指定或自动分配，PRT-003）。
// reserve 预留端口，并把选定的端口写回 spec（版本快照记录实际端口，回退时恢复同一端口）。
func reserve(ctx context.Context, q *db.Queries, tenant, id, entry p.UUID, spec *Spec) error {
	port, err := ports.Reserve(ctx, q, tenant, id, entry, spec.Network, spec.ServicePort)
	var ue *ports.UnavailableError
	if errors.As(err, &ue) || errors.Is(err, ports.ErrExhausted) {
		return &ValidationError{[]compiler.Problem{{Code: ErrPortUnavailable, Detail: err.Error()}}}
	}
	if err == nil {
		spec.ServicePort = port
	}
	return err
}

// ErrPortUnavailable 是端口不可用的校验错误码（PRT-003）。
const ErrPortUnavailable = "PORT_UNAVAILABLE"

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
	ent, err := billing.Entitlements(ctx, q, tenant)
	if err != nil {
		return err
	}
	if probs := entitled(ctx, q, snap, ent, tenant, version, spec); len(probs) > 0 {
		return &ValidationError{Problems: probs}
	}
	snap.Entitlements[CID(tenant)] = compiler.Entitled{ReverseDial: ent.ReverseDial}
	r := ToCompiler(id, tenant, version, 0, spec)
	if probs := compiler.Validate(snap, r); len(probs) > 0 {
		return &ValidationError{Problems: probs}
	}
	return nil
}

// entitled 按有效权益检查规则（PLN-001）：套餐有效、规则数（新建时）、平台节点组、个人组、协议、转发方式、规则级限制。
func entitled(ctx context.Context, q *db.Queries, snap *compiler.Snapshot, e *billing.Effective, tenant p.UUID, version uint64, spec Spec) []compiler.Problem {
	if !e.Active {
		return []compiler.Problem{{Code: billing.ErrPlanInactive, Detail: "no active plan"}}
	}
	var out []compiler.Problem
	bad := func(code, format string, args ...any) {
		out = append(out, compiler.Problem{Code: code, Detail: fmt.Sprintf(format, args...)})
	}
	if version == 1 {
		n, err := q.CountTenantRules(ctx, tenant.PG())
		if err != nil {
			bad(compiler.ErrInvalid, "%v", err)
		} else if n >= e.MaxRules { // 新建的规则已插入或尚未插入均不超过上限
			bad(billing.ErrRuleLimit, "the plan allows %d rules", e.MaxRules)
		}
	}
	groups := []compiler.UUID{spec.EntryGroup}
	for _, sg := range spec.Path {
		groups = append(groups, sg.Group)
	}
	for _, g := range groups {
		gr, ok := snap.Groups[g]
		if !ok {
			continue // 由编译器报告不存在
		}
		if gr.Tenant != "" {
			if !e.SelfHosted {
				bad(billing.ErrSelfHosted, "personal group %s needs the self_hosted entitlement", g)
			}
			continue
		}
		if u, err := ParseUUID(string(g)); err != nil || !e.Groups[u] {
			bad(billing.ErrGroupEntitlement, "node group %s is not granted by the plan", g)
		}
	}
	protos := []string{spec.Network}
	if spec.Network == "tcp_udp" {
		protos = []string{"tcp", "udp"}
	}
	for _, pr := range protos {
		if !e.Protocols[pr] {
			bad(billing.ErrProtocol, "protocol %s is not granted by the plan", pr)
		}
	}
	if !e.ForwardModes[spec.Mode] {
		bad(billing.ErrForwardMode, "forward mode %s is not granted by the plan", spec.Mode)
	}
	if spec.RateBps > 0 && int64(spec.RateBps) > e.RateBps {
		bad(billing.ErrRuleRate, "rule rate %d bit/s exceeds the plan's %d", spec.RateBps, e.RateBps)
	}
	if spec.MaxConns > 0 && int64(spec.MaxConns) > e.MaxConns {
		bad(billing.ErrRuleConns, "rule connections %d exceed the plan's %d", spec.MaxConns, e.MaxConns)
	}
	return out
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
