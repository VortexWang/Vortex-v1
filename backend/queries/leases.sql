-- 限制快照、份额租约与区域额度（BE-T20、BE-T21）

-- name: EnsureInitialBillingVersion :exec
-- 系统初始化时的首个计费版本（version = 0，立即生效；MTR-003 的 24 小时提前量不约束它）
INSERT INTO billing_versions (id, version, effective_at) VALUES ($1, 0, now()) ON CONFLICT (version) DO NOTHING;

-- name: ListTenantStanding :many
-- 每个租户的套餐到期时刻（无套餐为空）与当前余额（MTR-004）
SELECT t.id AS tenant_id, s.expires_at, b.balance_bytes
FROM tenants t
LEFT JOIN subscriptions s ON s.tenant_id = t.id
JOIN tenant_balances b ON b.tenant_id = t.id
ORDER BY t.id;

-- name: ListActiveRestrictions :many
SELECT tenant_id, rule_id, kind FROM restrictions WHERE cleared_at IS NULL ORDER BY tenant_id, kind, rule_id;

-- name: ListTenantRuleIDs :many
SELECT id, tenant_id FROM rules ORDER BY tenant_id, id;

-- name: TenantPlan :one
-- 生效中的套餐的速率与连接上限（LIM-002、LIM-003）
SELECT p.rate_bps, p.max_conns FROM subscriptions s JOIN plans p ON p.id = s.plan_id
WHERE s.tenant_id = $1 AND s.expires_at > now();

-- name: ListLeaseNodes :many
-- 有运行配置的节点及其接入状态（执行点从配置中的规则得出）
SELECT nc.node_id, nc.config, nc.version, nc.applied_version, n.edge_hub_id, n.current_boot_id, n.lifecycle
FROM node_configs nc JOIN nodes n ON n.id = nc.node_id
WHERE n.deleted_at IS NULL
ORDER BY nc.node_id;

-- name: LockTenantLeases :exec
-- 同一租户的份额重算串行化（LIM-002：后端用事务切分上限）
SELECT pg_advisory_xact_lock(7420002, hashtext(sqlc.arg(tenant_id)::uuid::text));

-- name: ListLiveLeases :many
-- 计入占用的份额：未释放且未到期（LIM-004）
SELECT * FROM share_leases WHERE tenant_id = $1 AND released_at IS NULL AND expires_at > now() ORDER BY node_id, kind, expires_at DESC;

-- name: InsertShareLease :exec
INSERT INTO share_leases (id, tenant_id, node_id, boot_id, kind, amount, expires_at) VALUES ($1, $2, $3, $4, $5, $6, $7);

-- name: ExtendShareLease :exec
UPDATE share_leases SET expires_at = greatest(expires_at, $2) WHERE id = $1 AND released_at IS NULL;

-- name: ReleaseShareLeases :exec
UPDATE share_leases SET released_at = now() WHERE id = ANY(sqlc.arg(ids)::uuid[]) AND released_at IS NULL;

-- name: LockSubscription :many
-- MTR-004：先锁订阅行，再校验 Σ delegated ≤ 租户余额（无订阅时返回空，余额为 0）
SELECT tenant_id FROM subscriptions WHERE tenant_id = $1 FOR UPDATE;

-- name: TenantBalance :one
SELECT balance_bytes FROM tenant_balances WHERE tenant_id = $1;

-- name: ListTenantBudgets :many
SELECT rb.tenant_id, rb.hub_id, rb.delegated, rb.settled_acked_total, rb.budget_version, rb.frozen, h.mode
FROM regional_budgets rb JOIN edge_hubs h ON h.id = rb.hub_id
WHERE rb.tenant_id = $1
ORDER BY rb.hub_id
FOR UPDATE OF rb;

-- name: UpsertBudget :one
INSERT INTO regional_budgets (tenant_id, hub_id, delegated, budget_version) VALUES ($1, $2, $3, $4)
ON CONFLICT (tenant_id, hub_id) DO UPDATE SET delegated = EXCLUDED.delegated, budget_version = EXCLUDED.budget_version
RETURNING tenant_id, hub_id, delegated, settled_acked_total, budget_version;

-- name: ListHubBudgets :many
SELECT tenant_id, hub_id, delegated, settled_acked_total, budget_version FROM regional_budgets WHERE hub_id = $1 ORDER BY tenant_id;

-- name: ListHubModes :many
SELECT id, mode FROM edge_hubs ORDER BY id;

-- name: ListLeaseTenants :many
-- 持有未到期份额或区域额度的租户
SELECT DISTINCT tenant_id FROM share_leases WHERE released_at IS NULL AND expires_at > now()
UNION SELECT DISTINCT tenant_id FROM regional_budgets WHERE delegated > 0;

-- name: OutstandingByHub :many
-- 各实例已签发、尚未结算的计费字节（区域额度重算不收回这部分，MTR-013“收回尚未签发的部分”）
SELECT hub_id, sum(reserved_billing_bytes)::bigint AS reserved FROM billing_grants
WHERE tenant_id = $1 AND status <> 'settled' GROUP BY hub_id ORDER BY hub_id;
