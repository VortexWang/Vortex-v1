-- 规则（BE-T13）。租户表查询一律带 tenant_id（*ForTenant，BE-004）。

-- name: InsertRule :exec
INSERT INTO rules (id, tenant_id, name, network, forward_mode, entry_group_id, service_port, target_schedule, proxy_protocol,
                   limit_rate_bps, limit_max_conns, enabled)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12);

-- name: GetRuleForTenant :one
SELECT * FROM rules WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL;

-- name: LockRuleForTenant :one
SELECT * FROM rules WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL FOR UPDATE;

-- name: UpdateRuleForTenant :execrows
UPDATE rules SET name = $3, network = $4, forward_mode = $5, entry_group_id = $6, service_port = $7, target_schedule = $8,
  proxy_protocol = $9, limit_rate_bps = $10, limit_max_conns = $11, enabled = $12,
  desired_version = desired_version + 1, sync_status = 'pending', row_version = row_version + 1, updated_at = now()
WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL AND row_version = sqlc.arg(row_version);

-- name: SoftDeleteRuleForTenant :execrows
UPDATE rules SET deleted_at = now(), enabled = false, desired_version = desired_version + 1, row_version = row_version + 1, updated_at = now()
WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL;

-- name: ListRulesForTenant :many
SELECT * FROM rules WHERE tenant_id = $1 AND deleted_at IS NULL ORDER BY created_at, id;

-- name: DeleteRulePaths :exec
DELETE FROM rule_paths WHERE rule_id = $1;

-- name: InsertRulePath :exec
INSERT INTO rule_paths (rule_id, position, node_group_id, dial_direction) VALUES ($1, $2, $3, $4);

-- name: DeleteRuleTargets :exec
DELETE FROM rule_targets WHERE rule_id = $1;

-- name: InsertRuleTarget :exec
INSERT INTO rule_targets (rule_id, position, host, port, weight, priority) VALUES ($1, $2, $3, $4, $5, $6);

-- name: InsertRuleVersion :exec
INSERT INTO rule_versions (rule_id, version, snapshot) VALUES ($1, $2, $3);

-- name: GetRuleVersion :one
SELECT * FROM rule_versions WHERE rule_id = $1 AND version = $2;

-- 编译快照（全平台；后端内部使用，不经租户过滤）

-- name: ListAllActiveRules :many
SELECT * FROM rules WHERE deleted_at IS NULL ORDER BY id;

-- name: ListAllRulePaths :many
SELECT p.* FROM rule_paths p JOIN rules r ON r.id = p.rule_id WHERE r.deleted_at IS NULL ORDER BY p.rule_id, p.position;

-- name: ListAllRuleTargets :many
SELECT t.* FROM rule_targets t JOIN rules r ON r.id = t.rule_id WHERE r.deleted_at IS NULL ORDER BY t.rule_id, t.position;

-- name: ListAllGroups :many
SELECT * FROM node_groups WHERE deleted_at IS NULL ORDER BY id;

-- name: ListAllGroupMembers :many
SELECT * FROM node_group_members ORDER BY node_group_id, node_id;

-- name: ListAllNodes :many
SELECT * FROM nodes ORDER BY id;

-- name: ListLinkPorts :many
SELECT node_id, port_min FROM node_port_ranges WHERE kind = 'link' ORDER BY node_id, port_min;

-- name: ListPermits :many
SELECT upstream_group_id, downstream_group_id FROM segment_permits ORDER BY upstream_group_id, downstream_group_id;

-- name: CurrentBillingVersion :one
SELECT * FROM billing_versions WHERE effective_at <= now() ORDER BY version DESC LIMIT 1;

-- name: ListGroupMultipliers :many
SELECT node_group_id, multiplier_milli FROM group_multipliers WHERE billing_version_id = $1;

-- name: CurrentGeneration :one
SELECT generation FROM platform_generation;
