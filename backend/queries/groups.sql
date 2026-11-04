-- 节点组、组倍率、段许可（BE-T09：NOD-006～NOD-014、MTR-003）

-- name: InsertNodeGroup :exec
INSERT INTO node_groups (id, tenant_id, name, purpose, member_schedule, health_params) VALUES ($1, $2, $3, $4, $5, $6);

-- name: GetNodeGroup :one
SELECT * FROM node_groups WHERE id = $1 AND deleted_at IS NULL;

-- name: UpdateNodeGroup :execrows
UPDATE node_groups SET name = $2, member_schedule = $3, health_params = $4, row_version = row_version + 1, updated_at = now()
WHERE id = $1 AND row_version = $5 AND deleted_at IS NULL;

-- name: GroupInUse :one
SELECT (EXISTS (SELECT 1 FROM rules r1 WHERE r1.entry_group_id = sqlc.arg(id)::uuid AND r1.deleted_at IS NULL)
    OR EXISTS (SELECT 1 FROM rule_paths rp JOIN rules r ON r.id = rp.rule_id WHERE rp.node_group_id = sqlc.arg(id)::uuid AND r.deleted_at IS NULL))::boolean;

-- name: SoftDeleteNodeGroup :exec
UPDATE node_groups SET deleted_at = now(), updated_at = now() WHERE id = $1;

-- name: InsertGroupMember :exec
INSERT INTO node_group_members (node_group_id, node_id, weight, priority) VALUES ($1, $2, $3, $4);

-- name: InsertEntryMember :exec
INSERT INTO entry_members (node_group_id, node_id) VALUES ($1, $2) ON CONFLICT DO NOTHING;

-- name: DeleteGroupMember :execrows
DELETE FROM node_group_members WHERE node_group_id = $1 AND node_id = $2;

-- name: CurrentAndFutureBillingVersions :many
-- NOD-014：新建平台组的初始倍率写入当前生效版本及全部未来版本
SELECT id FROM billing_versions
WHERE effective_at > now() OR id = (SELECT id FROM billing_versions WHERE effective_at <= now() ORDER BY version DESC LIMIT 1)
ORDER BY version;

-- name: InsertGroupMultiplier :exec
INSERT INTO group_multipliers (billing_version_id, node_group_id, multiplier_milli) VALUES ($1, $2, $3);

-- name: LockBillingVersions :exec
SELECT pg_advisory_xact_lock(7420003);

-- name: LatestBillingVersion :one
SELECT * FROM billing_versions ORDER BY version DESC LIMIT 1;

-- name: InsertBillingVersion :exec
INSERT INTO billing_versions (id, version, effective_at, created_by) VALUES ($1, $2, $3, $4);

-- name: CopyMultipliers :exec
INSERT INTO group_multipliers (billing_version_id, node_group_id, multiplier_milli)
SELECT sqlc.arg(dst)::uuid, gm.node_group_id, CASE WHEN gm.node_group_id = sqlc.arg(grp)::uuid THEN sqlc.arg(milli)::bigint ELSE gm.multiplier_milli END
FROM group_multipliers gm WHERE gm.billing_version_id = sqlc.arg(src)::uuid;

-- name: AddMultiplierIfMissing :exec
INSERT INTO group_multipliers (billing_version_id, node_group_id, multiplier_milli) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING;

-- name: ListAllGroupMultipliers :many
SELECT billing_version_id, node_group_id, multiplier_milli FROM group_multipliers ORDER BY billing_version_id, node_group_id;

-- name: InsertSegmentPermit :execrows
INSERT INTO segment_permits (upstream_group_id, downstream_group_id, created_by) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING;

-- name: DeleteSegmentPermit :execrows
DELETE FROM segment_permits WHERE upstream_group_id = $1 AND downstream_group_id = $2;

-- name: PermitInUse :one
-- 删除段许可前检查：有规则正使用这一段（入口组或上一段 → 下一段）
SELECT EXISTS (
  SELECT 1 FROM rules r JOIN rule_paths rp ON rp.rule_id = r.id
  LEFT JOIN rule_paths prev ON prev.rule_id = r.id AND prev.position = rp.position - 1
  WHERE r.deleted_at IS NULL AND rp.node_group_id = sqlc.arg(down)::uuid
    AND coalesce(prev.node_group_id, r.entry_group_id) = sqlc.arg(up)::uuid)::boolean;
