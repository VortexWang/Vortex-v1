-- 下发编排（BE-T15）与端口分配（BE-T12）

-- name: LockDeploy :exec
SELECT pg_advisory_xact_lock(7420001);

-- name: ListNodeConfigs :many
SELECT * FROM node_configs ORDER BY node_id;

-- name: GetNodeConfig :one
SELECT * FROM node_configs WHERE node_id = $1;

-- name: UpsertNodeConfig :exec
INSERT INTO node_configs (node_id, version, config, content_digest) VALUES ($1, $2, $3, $4)
ON CONFLICT (node_id) DO UPDATE SET version = EXCLUDED.version, config = EXCLUDED.config, content_digest = EXCLUDED.content_digest,
  last_error = NULL, compiled_at = now(), updated_at = now();

-- name: NodePrepared :exec
UPDATE node_configs SET prepared_version = greatest(coalesce(prepared_version, 0), sqlc.arg(version)::bigint), updated_at = now()
WHERE node_id = $1 AND version >= sqlc.arg(version)::bigint;

-- name: NodeApplyFailed :exec
UPDATE node_configs SET last_error = $2, updated_at = now() WHERE node_id = $1;

-- name: NodeApplied :exec
UPDATE node_configs SET applied_version = greatest(coalesce(applied_version, 0), sqlc.arg(version)::bigint), updated_at = now()
WHERE node_id = $1 AND version >= sqlc.arg(version)::bigint;

-- name: MarkCommitSent :exec
UPDATE node_configs SET commit_sent = $2, updated_at = now() WHERE node_id = $1;

-- name: InsertOutbox :exec
INSERT INTO outbox (id, topic, key, payload) VALUES ($1, $2, $3, $4);

-- name: ClaimOutbox :many
SELECT * FROM outbox WHERE published_at IS NULL ORDER BY created_at, id LIMIT 200 FOR UPDATE SKIP LOCKED;

-- name: MarkOutboxPublished :exec
UPDATE outbox SET published_at = now() WHERE id = $1;

-- name: ListHubNodes :many
SELECT id FROM nodes WHERE edge_hub_id = $1 AND deleted_at IS NULL;

-- 编排记录

-- name: GetApplyRun :one
SELECT * FROM apply_runs WHERE rule_id = $1 AND version = $2;

-- name: LatestApplyRun :one
SELECT * FROM apply_runs WHERE rule_id = $1 ORDER BY version DESC LIMIT 1;

-- name: LastAppliedVersion :one
SELECT coalesce(max(version), 0)::bigint FROM apply_runs WHERE rule_id = $1 AND phase = 'applied';

-- name: InsertApplyRun :exec
INSERT INTO apply_runs (id, tenant_id, rule_id, version, phase, deadline_at, finished_at, is_rollback) VALUES ($1, $2, $3, $4, $5, $6, $7, $8);

-- name: InsertApplyNodeState :exec
INSERT INTO apply_node_states (run_id, node_id, status, required_version) VALUES ($1, $2, 'pending', $3);

-- name: ListOpenRuns :many
SELECT * FROM apply_runs WHERE phase IN ('preparing', 'committing', 'partially_applied', 'unknown') ORDER BY started_at, id;

-- name: ListRunNodes :many
SELECT s.node_id, s.required_version, s.status, c.version, c.prepared_version, c.applied_version, c.last_error, c.commit_sent,
  coalesce(n.lifecycle, 'offline') AS lifecycle
FROM apply_node_states s LEFT JOIN node_configs c ON c.node_id = s.node_id LEFT JOIN nodes n ON n.id = s.node_id
WHERE s.run_id = $1 ORDER BY s.node_id;

-- name: SetRunNodeStatus :exec
UPDATE apply_node_states SET status = $3, actual_version = $4, error_code = $5, updated_at = now() WHERE run_id = $1 AND node_id = $2;

-- name: SetRunPhase :exec
UPDATE apply_runs SET phase = $2, finished_at = CASE WHEN $2 IN ('applied','failed') THEN now() ELSE finished_at END WHERE id = $1;

-- name: SetRuleSync :exec
UPDATE rules SET sync_status = $2, updated_at = now() WHERE id = $1 AND desired_version = $3;

-- 端口分配（PRT-001～PRT-009）

-- name: LockNodes :many
SELECT id FROM nodes WHERE id = ANY(sqlc.arg(ids)::uuid[]) ORDER BY id FOR UPDATE;

-- name: ListServiceRanges :many
SELECT node_id, kind, port_min, port_max FROM node_port_ranges WHERE node_id = ANY(sqlc.arg(ids)::uuid[]) ORDER BY node_id, port_min;

-- name: ListActivePortsOnNodes :many
SELECT node_id, network, port, rule_id FROM port_allocations WHERE node_id = ANY(sqlc.arg(ids)::uuid[]) AND state <> 'released';

-- name: ListRuleAllocations :many
SELECT * FROM port_allocations WHERE rule_id = $1 AND state <> 'released' ORDER BY node_id, network;

-- name: InsertPortAllocation :exec
INSERT INTO port_allocations (id, tenant_id, rule_id, node_id, network, bind_address, port, state) VALUES ($1, $2, $3, $4, $5, '0.0.0.0', $6, 'reserved');

-- name: SetAllocationState :exec
UPDATE port_allocations SET state = $2, released_confirmed_at = CASE WHEN $2 = 'released' THEN now() ELSE released_confirmed_at END WHERE id = $1;

-- name: ListAllocationsInFlux :many
SELECT * FROM port_allocations WHERE state IN ('reserved', 'releasing') ORDER BY node_id;

-- name: SetRuleServicePort :exec
UPDATE rules SET service_port = $2 WHERE id = $1;

-- name: EntryMembersOfGroup :many
SELECT m.node_id FROM node_group_members m JOIN nodes n ON n.id = m.node_id
WHERE m.node_group_id = $1 AND n.deleted_at IS NULL AND n.lifecycle <> 'disabled' ORDER BY m.node_id;

-- name: ListRulesForDeploy :many
SELECT id, tenant_id, desired_version, service_port, deleted_at FROM rules ORDER BY id;

-- name: SetRunDeadline :exec
-- RUL-012：部分应用的回滚期限自进入提交起算
UPDATE apply_runs SET deadline_at = $2 WHERE id = $1;

-- name: RulesNeedingPortSync :many
-- PRT-007：入口组成员变化后需要重新检查端口的规则（有成员缺少预留，或有预留的节点已不是成员）
SELECT r.id, r.tenant_id, r.entry_group_id, r.network, r.service_port FROM rules r
WHERE r.deleted_at IS NULL AND r.enabled AND r.service_port IS NOT NULL AND (
  EXISTS (SELECT 1 FROM node_group_members m JOIN nodes n ON n.id = m.node_id
          WHERE m.node_group_id = r.entry_group_id AND n.deleted_at IS NULL AND n.lifecycle <> 'disabled'
            AND NOT EXISTS (SELECT 1 FROM port_allocations a WHERE a.rule_id = r.id AND a.node_id = m.node_id AND a.state <> 'released'))
  OR EXISTS (SELECT 1 FROM port_allocations a WHERE a.rule_id = r.id AND a.state IN ('reserved', 'occupied')
          AND NOT EXISTS (SELECT 1 FROM node_group_members m JOIN nodes n ON n.id = m.node_id
                          WHERE m.node_group_id = r.entry_group_id AND m.node_id = a.node_id AND n.deleted_at IS NULL AND n.lifecycle <> 'disabled')))
ORDER BY r.id;
