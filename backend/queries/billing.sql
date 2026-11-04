-- 套餐与权益（BE-T16：PLN-001～PLN-003、PLN-006、PLN-007；MTR-013）

-- name: ActivePlan :many
SELECT p.id, p.max_rules, p.rate_bps, p.max_conns FROM subscriptions s JOIN plans p ON p.id = s.plan_id
WHERE s.tenant_id = $1 AND s.expires_at > now();

-- name: PlanEntitlements :many
SELECT kind, value FROM plan_entitlements WHERE plan_id = $1 ORDER BY kind, value;

-- name: GetSetting :many
SELECT value FROM settings WHERE key = $1;

-- name: CountTenantRules :one
SELECT count(*) FROM rules WHERE tenant_id = $1 AND deleted_at IS NULL;

-- name: ListAllTenantPlatformRules :many
SELECT id FROM rules WHERE tenant_id = $1 AND deleted_at IS NULL ORDER BY id;

-- name: DueBaseResets :many
-- PLN-006：按月重置、当期已结束且套餐仍有效
SELECT s.tenant_id, s.started_at, s.expires_at, s.base_period_end, p.base_traffic_bytes FROM subscriptions s JOIN plans p ON p.id = s.plan_id
WHERE p.traffic_reset = 'monthly' AND s.base_period_end <= now() AND s.expires_at > now()
ORDER BY s.tenant_id FOR UPDATE OF s SKIP LOCKED;

-- name: ResetBaseTraffic :exec
UPDATE subscriptions SET base_remaining_bytes = $2, base_period_end = $3, row_version = row_version + 1 WHERE tenant_id = $1;

-- name: ExpiredPacks :many
-- 已过期仍有余量的流量包：作废（MTR-013）
SELECT id, tenant_id, bytes_remaining FROM traffic_packs WHERE expires_at <= now() AND bytes_remaining > 0 ORDER BY tenant_id, id FOR UPDATE SKIP LOCKED;

-- name: VoidPack :exec
UPDATE traffic_packs SET bytes_remaining = 0 WHERE id = $1;

-- name: ListTenantsWithStanding :many
-- 套餐是否有效、余额、当前生效的 plan_expired / traffic_exhausted 限制
SELECT t.id AS tenant_id,
  EXISTS (SELECT 1 FROM subscriptions s WHERE s.tenant_id = t.id)::boolean AS subscribed,
  EXISTS (SELECT 1 FROM subscriptions s WHERE s.tenant_id = t.id AND s.expires_at > now())::boolean AS plan_active,
  b.balance_bytes,
  EXISTS (SELECT 1 FROM restrictions r WHERE r.tenant_id = t.id AND r.rule_id IS NULL AND r.kind = 'plan_expired' AND r.cleared_at IS NULL)::boolean AS expired_flag,
  EXISTS (SELECT 1 FROM restrictions r WHERE r.tenant_id = t.id AND r.rule_id IS NULL AND r.kind = 'traffic_exhausted' AND r.cleared_at IS NULL)::boolean AS exhausted_flag
FROM tenants t JOIN tenant_balances b ON b.tenant_id = t.id
ORDER BY t.id;

-- name: OpenTenantRestriction :exec
INSERT INTO restrictions (id, tenant_id, kind) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING;

-- name: ClearTenantRestriction :exec
UPDATE restrictions SET cleared_at = now() WHERE tenant_id = $1 AND rule_id IS NULL AND kind = $2 AND cleared_at IS NULL;
