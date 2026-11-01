-- 实例引导令牌（HUB-T19）：一次性；只存哈希（SEC-004）

-- name: CreateHubBootstrapToken :exec
INSERT INTO hub_bootstrap_tokens (id, token_hash, region, expires_at) VALUES ($1, $2, $3, $4);

-- name: LockHubBootstrapToken :one
SELECT * FROM hub_bootstrap_tokens WHERE token_hash = $1 FOR UPDATE;

-- name: UseHubBootstrapToken :exec
UPDATE hub_bootstrap_tokens SET used_at = now(), used_by_hub = $2 WHERE id = $1 AND used_at IS NULL;

-- name: CreateEdgeHub :exec
INSERT INTO edge_hubs (id, region, address, hub_version) VALUES ($1, $2, $3, $4);

-- name: GetEdgeHub :one
SELECT * FROM edge_hubs WHERE id = $1;

-- name: ListRegionHubs :many
SELECT * FROM edge_hubs WHERE region = $1 ORDER BY created_at, id;

-- name: UpdateHubRegister :exec
UPDATE edge_hubs SET mode = $2, hub_version = $3, last_contact_at = now(), autonomy_started_at = $4 WHERE id = $1;

-- name: TouchHub :exec
UPDATE edge_hubs SET last_contact_at = now() WHERE id = $1;

-- name: SetHubOffline :exec
UPDATE edge_hubs SET mode = 'offline' WHERE id = $1;

-- 节点注册令牌（AGT-001）：一次性、30 分钟

-- name: CreateEnrollToken :exec
INSERT INTO enroll_tokens (id, tenant_id, token_hash, expires_at, created_by) VALUES ($1, $2, $3, $4, $5);

-- name: LockEnrollToken :one
SELECT * FROM enroll_tokens WHERE token_hash = $1 FOR UPDATE;

-- name: UseEnrollToken :exec
UPDATE enroll_tokens SET used_at = now(), used_by_node = $2 WHERE id = $1 AND used_at IS NULL;

-- name: CreateNode :exec
INSERT INTO nodes (id, tenant_id, name, region, edge_hub_id, can_entry, can_relay, can_exit, public_addresses, agent_version)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);

-- name: GetNode :one
SELECT * FROM nodes WHERE id = $1;

-- name: UpdateNodeStatus :exec
UPDATE nodes SET edge_hub_id = $2, lifecycle = CASE WHEN lifecycle IN ('draining','disabled') THEN lifecycle
                                                     WHEN sqlc.arg(connected)::boolean THEN 'online' ELSE 'offline' END,
  agent_version = coalesce(sqlc.narg(agent_version), agent_version), current_boot_id = coalesce(sqlc.narg(boot_id), current_boot_id),
  updated_at = now()
WHERE id = $1 AND deleted_at IS NULL;

-- 证书与吊销（SEC-001、SEC-002、SEC-012）

-- name: InsertCertificate :exec
INSERT INTO certificates (id, subject_kind, subject_id, serial, not_before, not_after) VALUES ($1, $2, $3, $4, $5, $6);

-- name: GetCertificateBySerial :one
SELECT * FROM certificates WHERE serial = $1;

-- name: RevokeCertificate :execrows
UPDATE certificates SET revoked_at = now(), revoke_reason = $2 WHERE serial = $1 AND revoked_at IS NULL;

-- name: ListRevokedSerials :many
SELECT serial FROM certificates WHERE revoked_at IS NOT NULL AND not_after > now() ORDER BY serial;

-- name: BumpRevocationList :one
INSERT INTO revocation_list_versions (reason) VALUES ($1) RETURNING serial;

-- name: CurrentRevocationSerial :one
SELECT coalesce(max(serial), 0)::bigint FROM revocation_list_versions;

-- Edge Hub 账本（HUB-012）：(hub_id, seq) 幂等

-- name: InsertLedgerEntry :execrows
INSERT INTO hub_ledger_entries (hub_id, seq, kind, tenant_id, payload) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (hub_id, seq) DO NOTHING;

-- name: ListBillingVersions :many
SELECT * FROM billing_versions ORDER BY version;

-- name: MaxLedgerSeq :one
SELECT coalesce(max(seq), 0)::bigint FROM hub_ledger_entries WHERE hub_id = $1;

-- name: SetHubMode :exec
UPDATE edge_hubs SET mode = $2, autonomy_started_at = CASE WHEN $2 = 'online' THEN NULL ELSE autonomy_started_at END WHERE id = $1;
