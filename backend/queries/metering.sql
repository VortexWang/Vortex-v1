-- 计量入账（BE-T18）与区域额度结算（BE-T21，MTR-004）

-- name: InsertBillingGrant :execrows
-- Edge Hub 账本的签发条目；规则或节点未知时不入库（由调用方记录）
INSERT INTO billing_grants (id, tenant_id, rule_id, node_id, boot_id, hub_id, billing_version, path_coefficient_milli,
  raw_bytes, reserved_billing_bytes, status)
SELECT sqlc.arg(id), sqlc.arg(tenant_id), sqlc.arg(rule_id), sqlc.arg(node_id), sqlc.arg(boot_id), sqlc.arg(hub_id),
  sqlc.arg(billing_version), sqlc.arg(path_coefficient_milli), sqlc.arg(raw_bytes), sqlc.arg(reserved_billing_bytes), 'active'
WHERE EXISTS (SELECT 1 FROM rules WHERE id = sqlc.arg(rule_id)) AND EXISTS (SELECT 1 FROM nodes WHERE id = sqlc.arg(node_id))
ON CONFLICT (id) DO NOTHING;

-- name: LockBillingGrant :many
SELECT * FROM billing_grants WHERE id = $1 FOR UPDATE;

-- name: NoteGrantUsage :exec
UPDATE billing_grants SET used_raw_bytes = greatest(coalesce(used_raw_bytes, 0), least(sqlc.arg(used)::bigint, raw_bytes))
WHERE id = $1 AND status <> 'settled';

-- name: SettleBillingGrant :exec
UPDATE billing_grants SET status = 'settled', used_raw_bytes = least(sqlc.arg(used)::bigint, raw_bytes),
  settled_billing_bytes = sqlc.arg(settled), written_off_billing = sqlc.arg(written_off)::numeric, settled_at = now()
WHERE id = $1;

-- name: MarkGrantsSettling :exec
-- 重启转移（MTR-007、LIM-010）：旧启动的授权进入 settling
UPDATE billing_grants SET status = 'settling' WHERE id = ANY(sqlc.arg(ids)::uuid[]) AND status IN ('reserved','active');

-- name: GetBillingCarry :many
SELECT remainder_milli FROM tenant_billing_carry WHERE tenant_id = $1 FOR UPDATE;

-- name: SetBillingCarry :exec
INSERT INTO tenant_billing_carry (tenant_id, remainder_milli) VALUES ($1, $2)
ON CONFLICT (tenant_id) DO UPDATE SET remainder_milli = EXCLUDED.remainder_milli, updated_at = now();

-- name: BaseTraffic :many
-- PLN-007：当期基础流量（套餐有效且当期未结束；调用方已锁订阅行）
SELECT base_remaining_bytes FROM subscriptions WHERE tenant_id = $1 AND expires_at > now() AND base_period_end > now();

-- name: DeductBaseTraffic :exec
UPDATE subscriptions SET base_remaining_bytes = base_remaining_bytes - $2, row_version = row_version + 1 WHERE tenant_id = $1;

-- name: PlanActive :one
SELECT EXISTS (SELECT 1 FROM subscriptions WHERE tenant_id = $1 AND expires_at > now())::boolean;

-- name: LockConsumablePacks :many
-- PLN-003：最早到期优先，同到期按发放时间
SELECT id, bytes_remaining FROM traffic_packs
WHERE tenant_id = $1 AND expires_at > now() AND bytes_remaining > 0
ORDER BY expires_at, granted_at, id
FOR UPDATE;

-- name: DeductPack :exec
UPDATE traffic_packs SET bytes_remaining = bytes_remaining - $2 WHERE id = $1;

-- name: LockBudget :many
SELECT delegated, settled_acked_total, budget_version FROM regional_budgets WHERE tenant_id = $1 AND hub_id = $2 FOR UPDATE;

-- name: SettleBudget :one
INSERT INTO regional_budgets (tenant_id, hub_id, delegated, settled_acked_total, budget_version) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (tenant_id, hub_id) DO UPDATE SET delegated = EXCLUDED.delegated, settled_acked_total = EXCLUDED.settled_acked_total,
  budget_version = EXCLUDED.budget_version
RETURNING tenant_id, hub_id, delegated, settled_acked_total, budget_version;

-- name: InsertUsageBatch :execrows
-- MTR-006：按 (node_id, boot_id, seq) 幂等
INSERT INTO usage_batches (node_id, boot_id, seq, received_via_hub, payload) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT DO NOTHING;

-- name: RuleTenant :many
SELECT tenant_id FROM rules WHERE id = $1;

-- name: GrantCoefficient :many
SELECT path_coefficient_milli FROM billing_grants WHERE id = $1;

-- name: AddUsageMinute :exec
INSERT INTO usage_minutely (minute, tenant_id, rule_id, raw_bytes_up, raw_bytes_down, billed_bytes) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, rule_id, minute) DO UPDATE SET raw_bytes_up = usage_minutely.raw_bytes_up + EXCLUDED.raw_bytes_up,
  raw_bytes_down = usage_minutely.raw_bytes_down + EXCLUDED.raw_bytes_down, billed_bytes = usage_minutely.billed_bytes + EXCLUDED.billed_bytes;

-- name: AddUsageDay :exec
INSERT INTO usage_daily (day, tenant_id, rule_id, raw_bytes, billed_bytes) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (tenant_id, rule_id, day) DO UPDATE SET raw_bytes = usage_daily.raw_bytes + EXCLUDED.raw_bytes,
  billed_bytes = usage_daily.billed_bytes + EXCLUDED.billed_bytes;
