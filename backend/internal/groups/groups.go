// Package groups 是节点组管理（BE-T09；NOD-006～NOD-014、MTR-003、TEN-005）：节点组、成员、组倍率（经计费版本）、
// 段许可。成员规则（NOD-005～NOD-007：能力与用途相符、平台组只含平台节点、个人组只含同租户自托管节点、入口组成员须
// 有公网地址）由数据库触发器在写入时保证，这里映射为错误码。管理操作一律写审计。
//
// 组倍率（NOD-014）：平台组 > 0，个人组固定为 0（不登记）。新建平台组的初始倍率写入当前生效的计费版本及全部未来版本，
// 立即生效；此后修改必须生成新计费版本，生效时刻不早于创建后 24 小时（MTR-003），并同步到全部 Edge Hub（HUB-018）。
package groups

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"vortex/backend/internal/db"
	p "vortex/backend/internal/platform"
)

// MultiplierLead 是组倍率修改的最小提前量（MTR-003）。
const MultiplierLead = 24 * time.Hour

// Error 是带错误码的管理操作错误。
type Error struct {
	Code   string
	Detail string
}

func (e *Error) Error() string { return e.Code + ": " + e.Detail }

const (
	ErrInvalid          = "NODE_GROUP_INVALID"
	ErrNotFound         = "NODE_GROUP_NOT_FOUND"
	ErrConflict         = "NODE_GROUP_CONFLICT"
	ErrInUse            = "NODE_GROUP_IN_USE"
	ErrMultiplier       = "NODE_GROUP_MULTIPLIER_INVALID"
	ErrMultiplierLead   = "BILLING_VERSION_TOO_SOON"
	ErrMemberExists     = "NODE_GROUP_MEMBER_EXISTS"
	ErrMemberNotFound   = "NODE_GROUP_MEMBER_NOT_FOUND"
	ErrPermitInUse      = "SEGMENT_PERMIT_IN_USE"
	ErrPermitNotFound   = "SEGMENT_PERMIT_NOT_FOUND"
	ErrPermitSameGroups = "SEGMENT_PERMIT_SAME_GROUP"
)

// Health 是组的健康探测参数（NOD-009）。
type Health struct {
	IntervalS        int `json:"interval_s"`
	FailThreshold    int `json:"fail_threshold"`
	RecoverThreshold int `json:"recover_threshold"`
}

// Spec 是节点组的内容。
type Spec struct {
	Name            string
	Purpose         string  // entry / relay / exit
	Tenant          *p.UUID // nil = 平台组
	Schedule        string  // weighted_round_robin / primary_backup；入口组不使用（由 DNS 分配，NOD-009）
	Health          Health
	MultiplierMilli uint64 // 平台组的初始组倍率 ×1000（> 0）；个人组必须为 0
}

type Service struct {
	DB *pgxpool.Pool
	// Changed 在成员、调度或段许可变化后调用（重新部署，PRT-007）；BillingChanged 在新计费版本后调用（同步到 Edge Hub）。
	Changed, BillingChanged func(context.Context)
}

func (s *Service) changed(ctx context.Context) {
	if s.Changed != nil {
		s.Changed(ctx)
	}
}

func audit(ctx context.Context, q *db.Queries, actor p.UUID, tenant *p.UUID, action, kind, target string, detail any) error {
	raw, _ := json.Marshal(detail)
	t := p.UUID{}.PG()
	t.Valid = false
	if tenant != nil {
		t = tenant.PG()
	}
	return q.InsertAudit(ctx, db.InsertAuditParams{ActorKind: "user", ActorID: actor.PG(), TenantID: t, Action: action, TargetKind: &kind,
		TargetID: &target, Detail: raw})
}

func normalize(spec *Spec) error {
	spec.Name = strings.TrimSpace(spec.Name)
	if spec.Name == "" {
		return &Error{ErrInvalid, "name is required"}
	}
	switch spec.Purpose {
	case "entry", "relay", "exit":
	default:
		return &Error{ErrInvalid, "purpose must be entry, relay or exit"}
	}
	if spec.Schedule == "" {
		spec.Schedule = "weighted_round_robin"
	}
	if spec.Schedule != "weighted_round_robin" && spec.Schedule != "primary_backup" {
		return &Error{ErrInvalid, "schedule must be weighted_round_robin or primary_backup"}
	}
	if spec.Health == (Health{}) {
		spec.Health = Health{IntervalS: 3, FailThreshold: 3, RecoverThreshold: 2}
	}
	if spec.Health.IntervalS <= 0 || spec.Health.FailThreshold <= 0 || spec.Health.RecoverThreshold <= 0 {
		return &Error{ErrInvalid, "health parameters must be positive"}
	}
	return nil
}

// Create 新建节点组。
func (s *Service) Create(ctx context.Context, actor p.UUID, spec Spec) (p.UUID, error) {
	if err := normalize(&spec); err != nil {
		return p.UUID{}, err
	}
	if spec.Tenant == nil && spec.MultiplierMilli == 0 {
		return p.UUID{}, &Error{ErrMultiplier, "a platform group needs a multiplier > 0"}
	}
	if spec.Tenant != nil && spec.MultiplierMilli != 0 {
		return p.UUID{}, &Error{ErrMultiplier, "a personal group's multiplier is fixed at 0"}
	}
	id := p.NewUUIDv7()
	health, _ := json.Marshal(spec.Health)
	err := p.InTx(ctx, s.DB, func(tx pgx.Tx) error {
		q := db.New(tx)
		t := p.UUID{}.PG()
		t.Valid = false
		if spec.Tenant != nil {
			t = spec.Tenant.PG()
		}
		if err := q.InsertNodeGroup(ctx, db.InsertNodeGroupParams{ID: id.PG(), TenantID: t, Name: spec.Name, Purpose: spec.Purpose,
			MemberSchedule: spec.Schedule, HealthParams: health}); err != nil {
			return err
		}
		if spec.Tenant == nil { // NOD-014：初始倍率立即生效
			if err := q.LockBillingVersions(ctx); err != nil {
				return err
			}
			vs, err := q.CurrentAndFutureBillingVersions(ctx)
			if err != nil {
				return err
			}
			for _, v := range vs {
				if err := q.InsertGroupMultiplier(ctx, db.InsertGroupMultiplierParams{BillingVersionID: v, NodeGroupID: id.PG(),
					MultiplierMilli: int64(spec.MultiplierMilli)}); err != nil {
					return err
				}
			}
		}
		return audit(ctx, q, actor, spec.Tenant, "node_group.create", "node_group", id.String(),
			map[string]any{"name": spec.Name, "purpose": spec.Purpose, "multiplier_milli": spec.MultiplierMilli})
	})
	if err == nil && spec.Tenant == nil && s.BillingChanged != nil {
		s.BillingChanged(ctx)
	}
	return id, err
}

// Update 修改名称、调度方式与健康探测参数（乐观并发）。
func (s *Service) Update(ctx context.Context, actor, id p.UUID, rowVersion int64, name, schedule string, health Health) error {
	err := p.InTx(ctx, s.DB, func(tx pgx.Tx) error {
		q := db.New(tx)
		g, err := q.GetNodeGroup(ctx, id.PG())
		if errors.Is(err, pgx.ErrNoRows) {
			return &Error{ErrNotFound, id.String()}
		} else if err != nil {
			return err
		}
		spec := Spec{Name: name, Purpose: g.Purpose, Schedule: schedule, Health: health}
		if err := normalize(&spec); err != nil {
			return err
		}
		raw, _ := json.Marshal(spec.Health)
		n, err := q.UpdateNodeGroup(ctx, db.UpdateNodeGroupParams{ID: id.PG(), Name: spec.Name, MemberSchedule: spec.Schedule,
			HealthParams: raw, RowVersion: rowVersion})
		if err != nil {
			return err
		}
		if n == 0 {
			return &Error{ErrConflict, "the group was changed by someone else"}
		}
		return audit(ctx, q, actor, tenantOf(g), "node_group.update", "node_group", id.String(), map[string]any{"schedule": spec.Schedule})
	})
	if err == nil {
		s.changed(ctx)
	}
	return err
}

func tenantOf(g db.NodeGroup) *p.UUID {
	if !g.TenantID.Valid {
		return nil
	}
	t := p.FromPG(g.TenantID)
	return &t
}

// Delete 删除节点组（软删除）：仍被规则使用时拒绝。
func (s *Service) Delete(ctx context.Context, actor, id p.UUID) error {
	err := p.InTx(ctx, s.DB, func(tx pgx.Tx) error {
		q := db.New(tx)
		g, err := q.GetNodeGroup(ctx, id.PG())
		if errors.Is(err, pgx.ErrNoRows) {
			return &Error{ErrNotFound, id.String()}
		} else if err != nil {
			return err
		}
		used, err := q.GroupInUse(ctx, id.PG())
		if err != nil {
			return err
		}
		if used {
			return &Error{ErrInUse, "rules still use this group"}
		}
		if err := q.SoftDeleteNodeGroup(ctx, id.PG()); err != nil {
			return err
		}
		return audit(ctx, q, actor, tenantOf(g), "node_group.delete", "node_group", id.String(), map[string]any{})
	})
	if err == nil {
		s.changed(ctx)
	}
	return err
}

// memberError 把成员触发器与约束的错误映射为错误码。
func memberError(err error) error {
	var pe *pgconn.PgError
	if !errors.As(err, &pe) {
		return err
	}
	switch {
	case pe.Code == "23505":
		return &Error{ErrMemberExists, pe.Detail}
	case pe.Code == "23503":
		return &Error{ErrInvalid, "unknown node or group"}
	}
	for _, code := range []string{"NODE_GROUP_TENANT_MISMATCH", "NODE_GROUP_CAPABILITY_MISMATCH", "NODE_GROUP_ENTRY_NEEDS_PUBLIC_ADDRESS"} {
		if strings.HasPrefix(pe.Message, code) {
			return &Error{code, pe.Message}
		}
	}
	return err
}

// AddMember 把节点加入组（入口组同时登记入口成员状态 joining，PRT-012）；随后重新部署（PRT-007 端口补齐）。
func (s *Service) AddMember(ctx context.Context, actor, group, node p.UUID, weight, priority int) error {
	if weight <= 0 {
		weight = 1
	}
	if priority < 0 {
		return &Error{ErrInvalid, "priority must be >= 0"}
	}
	err := p.InTx(ctx, s.DB, func(tx pgx.Tx) error {
		q := db.New(tx)
		g, err := q.GetNodeGroup(ctx, group.PG())
		if errors.Is(err, pgx.ErrNoRows) {
			return &Error{ErrNotFound, group.String()}
		} else if err != nil {
			return err
		}
		if err := q.InsertGroupMember(ctx, db.InsertGroupMemberParams{NodeGroupID: group.PG(), NodeID: node.PG(), Weight: int32(weight),
			Priority: int32(priority)}); err != nil {
			return memberError(err)
		}
		if g.Purpose == "entry" {
			if err := q.InsertEntryMember(ctx, db.InsertEntryMemberParams{NodeGroupID: group.PG(), NodeID: node.PG()}); err != nil {
				return err
			}
		}
		return audit(ctx, q, actor, tenantOf(g), "node_group.member_add", "node_group", group.String(), map[string]any{"node": node.String()})
	})
	if err == nil {
		s.changed(ctx)
	}
	return err
}

// RemoveMember 把节点移出组；该节点上的端口预留随部署转为 releasing（PRT-007、PRT-009）。
func (s *Service) RemoveMember(ctx context.Context, actor, group, node p.UUID) error {
	err := p.InTx(ctx, s.DB, func(tx pgx.Tx) error {
		q := db.New(tx)
		g, err := q.GetNodeGroup(ctx, group.PG())
		if errors.Is(err, pgx.ErrNoRows) {
			return &Error{ErrNotFound, group.String()}
		} else if err != nil {
			return err
		}
		n, err := q.DeleteGroupMember(ctx, db.DeleteGroupMemberParams{NodeGroupID: group.PG(), NodeID: node.PG()})
		if err != nil {
			return err
		}
		if n == 0 {
			return &Error{ErrMemberNotFound, node.String()}
		}
		return audit(ctx, q, actor, tenantOf(g), "node_group.member_remove", "node_group", group.String(), map[string]any{"node": node.String()})
	})
	if err == nil {
		s.changed(ctx)
	}
	return err
}

// SetMultiplier 修改平台组的组倍率：生成新计费版本（复制最新版本的全部组倍率并改动这一组），生效时刻不早于 24 小时后
// （MTR-003）；随后同步到全部 Edge Hub（HUB-018）。返回新版本号。
func (s *Service) SetMultiplier(ctx context.Context, actor, group p.UUID, milli uint64, effectiveAt time.Time) (int64, error) {
	if milli == 0 {
		return 0, &Error{ErrMultiplier, "a platform group's multiplier must be > 0"}
	}
	if effectiveAt.Before(time.Now().Add(MultiplierLead)) {
		return 0, &Error{ErrMultiplierLead, fmt.Sprintf("a multiplier change takes effect no earlier than %s from now", MultiplierLead)}
	}
	var version int64
	err := p.InTx(ctx, s.DB, func(tx pgx.Tx) error {
		q := db.New(tx)
		g, err := q.GetNodeGroup(ctx, group.PG())
		if errors.Is(err, pgx.ErrNoRows) {
			return &Error{ErrNotFound, group.String()}
		} else if err != nil {
			return err
		}
		if g.TenantID.Valid {
			return &Error{ErrMultiplier, "a personal group's multiplier is fixed at 0"}
		}
		if err := q.LockBillingVersions(ctx); err != nil {
			return err
		}
		latest, err := q.LatestBillingVersion(ctx)
		if err != nil {
			return err
		}
		if effectiveAt.Before(latest.EffectiveAt.Time) {
			return &Error{ErrMultiplierLead, "a new version cannot take effect before the latest scheduled version"}
		}
		id := p.NewUUIDv7()
		version = latest.Version + 1
		if err := q.InsertBillingVersion(ctx, db.InsertBillingVersionParams{ID: id.PG(), Version: version, EffectiveAt: p.TS(effectiveAt),
			CreatedBy: actor.PG()}); err != nil {
			var pe *pgconn.PgError
			if errors.As(err, &pe) && pe.ConstraintName == "billing_versions_lead" {
				return &Error{ErrMultiplierLead, pe.Message}
			}
			return err
		}
		if err := q.CopyMultipliers(ctx, db.CopyMultipliersParams{Dst: id.PG(), Grp: group.PG(), Milli: int64(milli), Src: latest.ID}); err != nil {
			return err
		}
		if err := q.AddMultiplierIfMissing(ctx, db.AddMultiplierIfMissingParams{BillingVersionID: id.PG(), NodeGroupID: group.PG(),
			MultiplierMilli: int64(milli)}); err != nil {
			return err
		}
		return audit(ctx, q, actor, nil, "billing.multiplier_change", "node_group", group.String(),
			map[string]any{"multiplier_milli": milli, "version": version, "effective_at": effectiveAt.UTC()})
	})
	if err == nil && s.BillingChanged != nil {
		s.BillingChanged(ctx)
	}
	return version, err
}

// Permit 创建段许可（NOD-010）；Revoke 删除段许可，仍有规则使用这一段时拒绝。
func (s *Service) Permit(ctx context.Context, actor, up, down p.UUID) error {
	if up == down {
		return &Error{ErrPermitSameGroups, "a segment needs two different groups"}
	}
	err := p.InTx(ctx, s.DB, func(tx pgx.Tx) error {
		q := db.New(tx)
		for _, g := range []p.UUID{up, down} {
			if _, err := q.GetNodeGroup(ctx, g.PG()); errors.Is(err, pgx.ErrNoRows) {
				return &Error{ErrNotFound, g.String()}
			} else if err != nil {
				return err
			}
		}
		if _, err := q.InsertSegmentPermit(ctx, db.InsertSegmentPermitParams{UpstreamGroupID: up.PG(), DownstreamGroupID: down.PG(), CreatedBy: actor.PG()}); err != nil {
			return err
		}
		return audit(ctx, q, actor, nil, "segment_permit.create", "segment_permit", up.String()+">"+down.String(), map[string]any{})
	})
	if err == nil {
		s.changed(ctx)
	}
	return err
}

func (s *Service) Revoke(ctx context.Context, actor, up, down p.UUID) error {
	return p.InTx(ctx, s.DB, func(tx pgx.Tx) error {
		q := db.New(tx)
		used, err := q.PermitInUse(ctx, db.PermitInUseParams{Up: up.PG(), Down: down.PG()})
		if err != nil {
			return err
		}
		if used {
			return &Error{ErrPermitInUse, "rules still use this segment"}
		}
		n, err := q.DeleteSegmentPermit(ctx, db.DeleteSegmentPermitParams{UpstreamGroupID: up.PG(), DownstreamGroupID: down.PG()})
		if err != nil {
			return err
		}
		if n == 0 {
			return &Error{ErrPermitNotFound, up.String() + ">" + down.String()}
		}
		return audit(ctx, q, actor, nil, "segment_permit.delete", "segment_permit", up.String()+">"+down.String(), map[string]any{})
	})
}
