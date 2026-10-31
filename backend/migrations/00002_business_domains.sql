-- 契约 C3 · 业务领域表（第二批，M4 前锁定）
-- 需求基线：《VORTEX 需求规格说明书 V1.7（已冻结）》第 4～7、10、12 节；契约 C2（额度语义 H3、H4）
-- 依赖：00001_control_plane_base.sql
-- 约定同第一批。能在数据库层表达的规则用约束或触发器强制；涉及多表余额计算的不变量
-- （如 Σ delegated ≤ 租户余额）由应用在同一事务内加锁校验，见各表注释。

-- +goose Up
-- +goose StatementBegin

-- ===================== identity =====================
CREATE TABLE user_totp (
  user_id     uuid PRIMARY KEY REFERENCES users(id),
  secret_enc  bytea NOT NULL,                           -- 加密存储（SEC-004）
  enabled_at  timestamptz
);

CREATE TABLE login_attempts (
  id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  email      text NOT NULL,
  ip         inet NOT NULL,
  success    boolean NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX login_attempts_recent ON login_attempts (lower(email), created_at DESC);

-- 业务限制（TEN-008、TEN-009、7.5 节限制层）。规则级限制填 rule_id；租户级为空。
CREATE TABLE restrictions (
  id          uuid PRIMARY KEY,
  tenant_id   uuid NOT NULL REFERENCES tenants(id),
  rule_id     uuid,                                     -- 外键见 rules 建表后
  kind        text NOT NULL CHECK (kind IN ('manual_stop','account_suspended','account_banned',
                                            'plan_expired','traffic_exhausted','over_limit','grant_pending')),
  started_at  timestamptz NOT NULL DEFAULT now(),
  cleared_at  timestamptz,
  CONSTRAINT restrictions_cleared_after CHECK (cleared_at IS NULL OR cleared_at >= started_at)
);
-- 同一对象同一原因只能有一条生效中的限制；全部解除后才恢复业务（TEN-009）
CREATE UNIQUE INDEX restrictions_active_uq
  ON restrictions (tenant_id, coalesce(rule_id, '00000000-0000-0000-0000-000000000000'::uuid), kind)
  WHERE cleared_at IS NULL;

-- ===================== fleet（续） =====================
-- 段许可（NOD-010～NOD-012）。个人组之间默认允许，不需要记录（NOD-011）。
CREATE TABLE segment_permits (
  upstream_group_id   uuid NOT NULL REFERENCES node_groups(id),
  downstream_group_id uuid NOT NULL REFERENCES node_groups(id),
  created_by          uuid NOT NULL REFERENCES users(id),
  created_at          timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (upstream_group_id, downstream_group_id),
  CONSTRAINT segment_permits_distinct CHECK (upstream_group_id <> downstream_group_id)
);

-- 入口组成员状态（5.4 节）。复合外键保证只有组成员才有状态。
CREATE TABLE entry_members (
  node_group_id uuid NOT NULL,
  node_id       uuid NOT NULL,
  state         text NOT NULL DEFAULT 'joining' CHECK (state IN ('joining','serving','draining','unavailable')),
  updated_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (node_group_id, node_id),
  FOREIGN KEY (node_group_id, node_id) REFERENCES node_group_members (node_group_id, node_id) ON DELETE CASCADE
);

CREATE FUNCTION check_entry_member() RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
  IF (SELECT purpose FROM node_groups WHERE id = NEW.node_group_id) <> 'entry' THEN
    RAISE EXCEPTION 'ENTRY_MEMBER_NOT_ENTRY_GROUP: group %', NEW.node_group_id USING ERRCODE = 'check_violation';
  END IF;
  RETURN NEW;
END $fn$;
CREATE TRIGGER entry_members_check BEFORE INSERT OR UPDATE ON entry_members
  FOR EACH ROW EXECUTE FUNCTION check_entry_member();

-- ===================== ports（6 节） =====================
CREATE TABLE node_port_ranges (
  node_id   uuid NOT NULL REFERENCES nodes(id),
  kind      text NOT NULL CHECK (kind IN ('service','link','control','metrics')),   -- PRT-001
  port_min  integer NOT NULL CHECK (port_min BETWEEN 1 AND 65535),
  port_max  integer NOT NULL CHECK (port_max BETWEEN 1 AND 65535),
  PRIMARY KEY (node_id, kind, port_min),
  CONSTRAINT node_port_ranges_order CHECK (port_min <= port_max)
);

-- ===================== rules（7 节） =====================
CREATE TABLE rules (
  id              uuid PRIMARY KEY,
  tenant_id       uuid NOT NULL REFERENCES tenants(id),
  name            text NOT NULL,
  network         text NOT NULL CHECK (network IN ('tcp','udp','tcp_udp')),
  forward_mode    text NOT NULL CHECK (forward_mode IN ('direct','tunnel','multi_hop')),
  entry_group_id  uuid NOT NULL REFERENCES node_groups(id),
  service_port    integer CHECK (service_port BETWEEN 1 AND 65535),     -- 空 = 自动分配（PRT-003）
  target_schedule text NOT NULL DEFAULT 'weighted_round_robin'
                    CHECK (target_schedule IN ('weighted_round_robin','primary_backup')),
  proxy_protocol  text NOT NULL DEFAULT 'off' CHECK (proxy_protocol IN ('off','v1','v2')),
  limit_rate_bps  bigint CHECK (limit_rate_bps > 0),
  limit_max_conns integer CHECK (limit_max_conns > 0),
  enabled         boolean NOT NULL DEFAULT true,
  desired_version bigint NOT NULL DEFAULT 1 CHECK (desired_version >= 1),   -- 7.5 期望层
  sync_status     text NOT NULL DEFAULT 'pending'
                    CHECK (sync_status IN ('pending','preparing','committing','applied','partially_applied',
                                           'rolling_back','failed','unknown')),
  run_status      text NOT NULL DEFAULT 'stopped' CHECK (run_status IN ('running','stopped','degraded')),
  row_version     bigint NOT NULL DEFAULT 1,
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now(),
  deleted_at      timestamptz,
  CONSTRAINT rules_proxy_v1_tcp_only CHECK (proxy_protocol <> 'v1' OR network = 'tcp')   -- ENG-009
);
CREATE INDEX rules_tenant ON rules (tenant_id) WHERE deleted_at IS NULL;
ALTER TABLE restrictions ADD CONSTRAINT restrictions_rule_fk FOREIGN KEY (rule_id) REFERENCES rules(id);

-- 路径（RUL-003、RUL-004）：position 1..n 为入口之后的各段下游组；最后一段为出口组。
CREATE TABLE rule_paths (
  rule_id        uuid NOT NULL REFERENCES rules(id) ON DELETE CASCADE,
  position       smallint NOT NULL CHECK (position BETWEEN 1 AND 4),   -- 至多 3 个中继 + 出口
  node_group_id  uuid NOT NULL REFERENCES node_groups(id),
  dial_direction text NOT NULL DEFAULT 'auto' CHECK (dial_direction IN ('auto','forward','reverse')),
  PRIMARY KEY (rule_id, position),
  CONSTRAINT rule_paths_no_repeat_group UNIQUE (rule_id, node_group_id)   -- RUL-003
);

CREATE TABLE rule_targets (
  rule_id  uuid NOT NULL REFERENCES rules(id) ON DELETE CASCADE,
  position smallint NOT NULL CHECK (position >= 1),
  host     text NOT NULL CHECK (length(host) BETWEEN 1 AND 253),
  port     integer NOT NULL CHECK (port BETWEEN 1 AND 65535),
  weight   integer NOT NULL DEFAULT 1 CHECK (weight > 0),
  priority integer NOT NULL DEFAULT 0 CHECK (priority >= 0),
  PRIMARY KEY (rule_id, position)
);

-- 路径形状与转发方式一致（7.1 节）、入口组不得出现在路径中（RUL-003）、
-- 各组用途与位置相符、至少一个目标。延迟到提交时检查，允许在同一事务内分步写入。
CREATE FUNCTION check_rule_shape() RETURNS trigger LANGUAGE plpgsql AS $fn$
DECLARE
  rid uuid;
  r rules%ROWTYPE;
  n int; maxpos int; bad int; ntargets int;
BEGIN
  IF TG_TABLE_NAME = 'rules' THEN
    rid := NEW.id;
  ELSIF TG_OP = 'DELETE' THEN
    rid := OLD.rule_id;
  ELSE
    rid := NEW.rule_id;
  END IF;
  SELECT * INTO r FROM rules WHERE id = rid;
  IF NOT FOUND OR r.deleted_at IS NOT NULL THEN RETURN NULL; END IF;
  IF (SELECT purpose FROM node_groups WHERE id = r.entry_group_id) <> 'entry' THEN
    RAISE EXCEPTION 'RULE_ENTRY_GROUP_INVALID: rule %', rid USING ERRCODE = 'check_violation';
  END IF;
  SELECT count(*), coalesce(max(position), 0) INTO n, maxpos FROM rule_paths WHERE rule_id = rid;
  IF n <> maxpos THEN
    RAISE EXCEPTION 'RULE_PATH_GAP: rule %', rid USING ERRCODE = 'check_violation';
  END IF;
  IF (r.forward_mode = 'direct' AND n <> 0)
     OR (r.forward_mode = 'tunnel' AND n <> 1)
     OR (r.forward_mode = 'multi_hop' AND n NOT BETWEEN 2 AND 4) THEN
    RAISE EXCEPTION 'RULE_PATH_SHAPE: rule % mode % has % segments', rid, r.forward_mode, n
      USING ERRCODE = 'check_violation';
  END IF;
  IF EXISTS (SELECT 1 FROM rule_paths WHERE rule_id = rid AND node_group_id = r.entry_group_id) THEN
    RAISE EXCEPTION 'RULE_DUPLICATE_GROUP: entry group repeated in path of rule %', rid USING ERRCODE = 'check_violation';
  END IF;
  SELECT count(*) INTO bad FROM rule_paths p JOIN node_groups g ON g.id = p.node_group_id
   WHERE p.rule_id = rid AND ((p.position = maxpos AND g.purpose <> 'exit') OR (p.position < maxpos AND g.purpose <> 'relay'));
  IF bad > 0 THEN
    RAISE EXCEPTION 'RULE_PATH_GROUP_PURPOSE: rule %', rid USING ERRCODE = 'check_violation';
  END IF;
  SELECT count(*) INTO ntargets FROM rule_targets WHERE rule_id = rid;
  IF ntargets = 0 THEN
    RAISE EXCEPTION 'RULE_NO_TARGET: rule %', rid USING ERRCODE = 'check_violation';
  END IF;
  RETURN NULL;
END $fn$;
CREATE CONSTRAINT TRIGGER rules_shape AFTER INSERT OR UPDATE ON rules
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_rule_shape();
CREATE CONSTRAINT TRIGGER rule_paths_shape AFTER INSERT OR UPDATE OR DELETE ON rule_paths
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_rule_shape();
CREATE CONSTRAINT TRIGGER rule_targets_shape AFTER INSERT OR UPDATE OR DELETE ON rule_targets
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_rule_shape();

-- 每个期望版本的完整快照（RUL-012 回滚、RUL-016 重新校验）
CREATE TABLE rule_versions (
  rule_id    uuid NOT NULL REFERENCES rules(id),
  version    bigint NOT NULL,
  snapshot   jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (rule_id, version)
);

-- 端口分配（6.1～6.2 节）
CREATE TABLE port_allocations (
  id                    uuid PRIMARY KEY,
  tenant_id             uuid NOT NULL REFERENCES tenants(id),
  rule_id               uuid NOT NULL REFERENCES rules(id),
  node_id               uuid NOT NULL REFERENCES nodes(id),
  network               text NOT NULL CHECK (network IN ('tcp','udp')),
  bind_address          inet NOT NULL,
  port                  integer NOT NULL CHECK (port BETWEEN 1 AND 65535),
  state                 text NOT NULL CHECK (state IN ('reserved','occupied','held','releasing','released')),
  hold_started_at       timestamptz,                    -- PRT-008 起算点
  released_confirmed_at timestamptz,                    -- PRT-009 节点确认
  created_at            timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT port_alloc_hold_ts CHECK ((state = 'held') <= (hold_started_at IS NOT NULL)),
  CONSTRAINT port_alloc_release_proof CHECK ((state = 'released') = (released_confirmed_at IS NOT NULL))
);
-- 同一节点、协议、端口只能有一个未释放分配；通配地址与具体地址的交叉冲突由下方触发器处理（PRT-002）
CREATE UNIQUE INDEX port_allocations_active_uq ON port_allocations (node_id, network, bind_address, port)
  WHERE state <> 'released';

CREATE FUNCTION check_port_wildcard() RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
  IF NEW.state = 'released' THEN RETURN NEW; END IF;
  IF EXISTS (
    SELECT 1 FROM port_allocations a
     WHERE a.node_id = NEW.node_id AND a.network = NEW.network AND a.port = NEW.port
       AND a.state <> 'released' AND a.id <> NEW.id
       AND family(a.bind_address) = family(NEW.bind_address)
       AND (host(a.bind_address) IN ('0.0.0.0','::') OR host(NEW.bind_address) IN ('0.0.0.0','::'))
  ) THEN
    RAISE EXCEPTION 'PORT_CONFLICT_WILDCARD: node % % port %', NEW.node_id, NEW.network, NEW.port
      USING ERRCODE = 'exclusion_violation';
  END IF;
  RETURN NEW;
END $fn$;
CREATE TRIGGER port_allocations_wildcard BEFORE INSERT OR UPDATE ON port_allocations
  FOR EACH ROW EXECUTE FUNCTION check_port_wildcard();

-- ===================== apply（7.4～7.6 节） =====================
CREATE TABLE apply_runs (
  id          uuid PRIMARY KEY,
  tenant_id   uuid NOT NULL REFERENCES tenants(id),
  rule_id     uuid NOT NULL REFERENCES rules(id),
  version     bigint NOT NULL,
  is_rollback boolean NOT NULL DEFAULT false,
  phase       text NOT NULL CHECK (phase IN ('pending','preparing','committing','applied','partially_applied',
                                             'rolling_back','failed','unknown')),
  deadline_at timestamptz NOT NULL,                     -- 部分应用 120 秒回滚（RUL-012）
  started_at  timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  UNIQUE (rule_id, version)
);

CREATE TABLE apply_node_states (
  run_id         uuid NOT NULL REFERENCES apply_runs(id) ON DELETE CASCADE,
  node_id        uuid NOT NULL REFERENCES nodes(id),
  actual_version bigint,
  status         text NOT NULL CHECK (status IN ('pending','prepared','prepare_failed','active','commit_failed','aborted','unknown')),
  error_code     text,
  updated_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, node_id)
);

-- ===================== billing（10.1～10.2 节） =====================
CREATE TABLE plans (
  id                 uuid PRIMARY KEY,
  name               text NOT NULL,
  duration_days      integer NOT NULL CHECK (duration_days > 0),
  base_traffic_bytes bigint NOT NULL CHECK (base_traffic_bytes >= 0),
  traffic_reset      text NOT NULL CHECK (traffic_reset IN ('none','monthly')),     -- PLN-006
  max_rules          integer NOT NULL CHECK (max_rules >= 0),
  rate_bps           bigint NOT NULL CHECK (rate_bps > 0),
  max_conns          integer NOT NULL CHECK (max_conns > 0),
  active             boolean NOT NULL DEFAULT true,
  created_at         timestamptz NOT NULL DEFAULT now()
);

-- 权益（10.1 节）：套餐授予的资源与能力
CREATE TABLE plan_entitlements (
  plan_id uuid NOT NULL REFERENCES plans(id),
  kind    text NOT NULL CHECK (kind IN ('node_group','protocol','forward_mode','reverse_dial','self_hosted')),
  value   text NOT NULL,                                -- node_group：组 ID；protocol：tcp/udp；forward_mode：direct/tunnel/multi_hop；其余 'true'
  PRIMARY KEY (plan_id, kind, value)
);

-- 主套餐，每个租户至多一个（PLN-002）
CREATE TABLE subscriptions (
  tenant_id            uuid PRIMARY KEY REFERENCES tenants(id),
  plan_id              uuid NOT NULL REFERENCES plans(id),
  started_at           timestamptz NOT NULL,
  expires_at           timestamptz NOT NULL,
  base_period_end      timestamptz NOT NULL,            -- 当期基础流量的截止（月度重置或到期）
  base_remaining_bytes bigint NOT NULL CHECK (base_remaining_bytes >= 0),
  row_version          bigint NOT NULL DEFAULT 1,
  CONSTRAINT subscriptions_period CHECK (expires_at > started_at AND base_period_end <= expires_at)
);

CREATE TABLE traffic_packs (
  id              uuid PRIMARY KEY,
  tenant_id       uuid NOT NULL REFERENCES tenants(id),
  bytes_total     bigint NOT NULL CHECK (bytes_total > 0),
  bytes_remaining bigint NOT NULL CHECK (bytes_remaining >= 0),
  expires_at      timestamptz NOT NULL,
  granted_at      timestamptz NOT NULL DEFAULT now(),
  source          text NOT NULL,                        -- 'redeem:<code_id>' 等
  CONSTRAINT traffic_packs_remaining CHECK (bytes_remaining <= bytes_total)
);
-- 扣减顺序：最早到期优先，同到期按发放时间（PLN-003）
CREATE INDEX traffic_packs_consume_order ON traffic_packs (tenant_id, expires_at, granted_at) WHERE bytes_remaining > 0;

CREATE TABLE redeem_codes (
  id            uuid PRIMARY KEY,
  code_hash     bytea NOT NULL UNIQUE,
  kind          text NOT NULL CHECK (kind IN ('plan','duration','traffic')),
  plan_id       uuid REFERENCES plans(id),
  duration_days integer CHECK (duration_days > 0),
  traffic_bytes bigint CHECK (traffic_bytes > 0),
  pack_valid_days integer CHECK (pack_valid_days > 0),
  max_uses      integer NOT NULL DEFAULT 1 CHECK (max_uses > 0),
  used_count    integer NOT NULL DEFAULT 0,
  valid_from    timestamptz NOT NULL DEFAULT now(),
  valid_until   timestamptz NOT NULL,
  voided_at     timestamptz,
  created_by    uuid NOT NULL REFERENCES users(id),
  created_at    timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT redeem_codes_uses CHECK (used_count BETWEEN 0 AND max_uses),            -- PLN-004
  CONSTRAINT redeem_codes_shape CHECK (
    (kind = 'plan'     AND plan_id IS NOT NULL AND traffic_bytes IS NULL) OR
    (kind = 'duration' AND duration_days IS NOT NULL AND plan_id IS NULL AND traffic_bytes IS NULL) OR
    (kind = 'traffic'  AND traffic_bytes IS NOT NULL AND pack_valid_days IS NOT NULL AND plan_id IS NULL)),
  CONSTRAINT redeem_codes_window CHECK (valid_until > valid_from)
);

CREATE TABLE redemptions (
  id              uuid PRIMARY KEY,
  code_id         uuid NOT NULL REFERENCES redeem_codes(id),
  tenant_id       uuid NOT NULL REFERENCES tenants(id),
  idempotency_key text NOT NULL,
  effect          jsonb NOT NULL,                       -- 兑换前后套餐、时长、作废额度（PLN-005、PLN-008）
  created_at      timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, idempotency_key)                   -- 重复请求不得重复发放（PLN-004）
);

-- 租户余额（MTR-004 定义）：当期基础流量剩余 + 未过期流量包余额。套餐无效时流量包暂停（PLN-003），余额为 0。
CREATE VIEW tenant_balances AS
SELECT t.id AS tenant_id,
       CASE WHEN s.tenant_id IS NULL OR s.expires_at <= now() THEN 0
            ELSE (CASE WHEN s.base_period_end > now() THEN s.base_remaining_bytes ELSE 0 END)
                 + coalesce((SELECT sum(p.bytes_remaining) FROM traffic_packs p
                              WHERE p.tenant_id = t.id AND p.expires_at > now()), 0)
       END::bigint AS balance_bytes
FROM tenants t LEFT JOIN subscriptions s ON s.tenant_id = t.id;

-- ===================== metering（10.3～10.4 节） =====================
CREATE TABLE billing_versions (
  id           uuid PRIMARY KEY,
  version      bigint NOT NULL UNIQUE,
  effective_at timestamptz NOT NULL,
  created_by   uuid REFERENCES users(id),
  created_at   timestamptz NOT NULL DEFAULT now(),
  -- MTR-003：倍率修改须提前 24 小时。version = 0 为系统初始化时创建的首个版本，不受此约束。
  -- 新建平台组的初始倍率写入“当前生效版本”及全部未来版本的 group_multipliers，不新建版本（NOD-014）。
  CONSTRAINT billing_versions_lead CHECK (version = 0 OR effective_at >= created_at + interval '24 hours')
);

CREATE TABLE group_multipliers (
  billing_version_id uuid NOT NULL REFERENCES billing_versions(id),
  node_group_id      uuid NOT NULL REFERENCES node_groups(id),
  multiplier_milli   bigint NOT NULL CHECK (multiplier_milli > 0),   -- 平台组 > 0；个人组固定 0，不入表
  PRIMARY KEY (billing_version_id, node_group_id)
);

CREATE FUNCTION check_multiplier_platform_group() RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
  IF (SELECT tenant_id FROM node_groups WHERE id = NEW.node_group_id) IS NOT NULL THEN
    RAISE EXCEPTION 'MULTIPLIER_PERSONAL_GROUP: personal group % is fixed at 0', NEW.node_group_id
      USING ERRCODE = 'check_violation';
  END IF;
  RETURN NEW;
END $fn$;
CREATE TRIGGER group_multipliers_platform BEFORE INSERT OR UPDATE ON group_multipliers
  FOR EACH ROW EXECUTE FUNCTION check_multiplier_platform_group();

CREATE TABLE usage_batches (
  node_id          uuid NOT NULL,
  boot_id          uuid NOT NULL,
  seq              bigint NOT NULL CHECK (seq >= 1),
  received_via_hub uuid NOT NULL REFERENCES edge_hubs(id),
  payload          bytea NOT NULL,
  received_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (node_id, boot_id, seq)                   -- 幂等入账（MTR-006）
);

-- 分钟级用量，按月分区（C3-Q1）；保留 90 天（SEC-013）
CREATE TABLE usage_minutely (
  minute          timestamptz NOT NULL,
  tenant_id       uuid NOT NULL,
  rule_id         uuid NOT NULL,
  raw_bytes_up    bigint NOT NULL DEFAULT 0 CHECK (raw_bytes_up >= 0),
  raw_bytes_down  bigint NOT NULL DEFAULT 0 CHECK (raw_bytes_down >= 0),
  billed_bytes    bigint NOT NULL DEFAULT 0 CHECK (billed_bytes >= 0),
  PRIMARY KEY (tenant_id, rule_id, minute)
) PARTITION BY RANGE (minute);
CREATE TABLE usage_minutely_default PARTITION OF usage_minutely DEFAULT;

CREATE TABLE usage_daily (
  day            date NOT NULL,
  tenant_id      uuid NOT NULL,
  rule_id        uuid NOT NULL,
  raw_bytes      bigint NOT NULL DEFAULT 0 CHECK (raw_bytes >= 0),
  billed_bytes   bigint NOT NULL DEFAULT 0 CHECK (billed_bytes >= 0),
  PRIMARY KEY (tenant_id, rule_id, day)
);

-- 计费授权（10.4 节）；由 Edge Hub 账本入账生成
CREATE TABLE billing_grants (
  id                     uuid PRIMARY KEY,
  tenant_id              uuid NOT NULL REFERENCES tenants(id),
  rule_id                uuid NOT NULL REFERENCES rules(id),
  node_id                uuid NOT NULL REFERENCES nodes(id),
  boot_id                uuid NOT NULL,
  hub_id                 uuid NOT NULL REFERENCES edge_hubs(id),
  billing_version        bigint NOT NULL,
  path_coefficient_milli bigint NOT NULL CHECK (path_coefficient_milli > 0),
  raw_bytes              bigint NOT NULL CHECK (raw_bytes > 0),
  reserved_billing_bytes bigint NOT NULL CHECK (reserved_billing_bytes > 0),
  status                 text NOT NULL CHECK (status IN ('reserved','active','settling','settled','disputed')),
  used_raw_bytes         bigint CHECK (used_raw_bytes >= 0),
  settled_billing_bytes  bigint CHECK (settled_billing_bytes >= 0),
  written_off_billing    numeric(30,3) NOT NULL DEFAULT 0 CHECK (written_off_billing >= 0),   -- C2b H4
  created_at             timestamptz NOT NULL DEFAULT now(),
  settled_at             timestamptz,
  CONSTRAINT billing_grants_used_le_raw CHECK (used_raw_bytes IS NULL OR used_raw_bytes <= raw_bytes),
  CONSTRAINT billing_grants_settled_consistent CHECK ((status = 'settled') = (settled_at IS NOT NULL))
);
CREATE INDEX billing_grants_open ON billing_grants (tenant_id) WHERE status NOT IN ('settled');

CREATE TABLE disputes (
  grant_id    uuid PRIMARY KEY REFERENCES billing_grants(id),
  opened_at   timestamptz NOT NULL DEFAULT now(),
  auto_settle_at timestamptz NOT NULL,                  -- opened_at + 72 小时（MTR-009）
  resolved_at timestamptz,
  resolution  jsonb
);

-- ===================== leases（10.5 节、HUB-006～HUB-017） =====================
-- 区域额度。不变量 Σ delegated ≤ tenant_balances.balance_bytes 由应用在同一事务内
-- 先 SELECT … FROM subscriptions WHERE tenant_id = $1 FOR UPDATE 再校验（MTR-004）。
CREATE TABLE regional_budgets (
  tenant_id           uuid NOT NULL REFERENCES tenants(id),
  hub_id              uuid NOT NULL REFERENCES edge_hubs(id),
  delegated           bigint NOT NULL DEFAULT 0 CHECK (delegated >= 0),             -- C2b H4 下限
  settled_acked_total bigint NOT NULL DEFAULT 0 CHECK (settled_acked_total >= 0),   -- 累计值，只增不减
  budget_version      bigint NOT NULL DEFAULT 0,
  frozen              boolean NOT NULL DEFAULT false,                               -- HUB-015
  frozen_at           timestamptz,
  PRIMARY KEY (tenant_id, hub_id),
  CONSTRAINT regional_budgets_frozen_ts CHECK (frozen = (frozen_at IS NOT NULL))
);

CREATE FUNCTION check_budget_monotonic() RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
  IF NEW.settled_acked_total < OLD.settled_acked_total OR NEW.budget_version < OLD.budget_version THEN
    RAISE EXCEPTION 'BUDGET_NOT_MONOTONIC: tenant % hub %', NEW.tenant_id, NEW.hub_id USING ERRCODE = 'check_violation';
  END IF;
  RETURN NEW;
END $fn$;
CREATE TRIGGER regional_budgets_monotonic BEFORE UPDATE ON regional_budgets
  FOR EACH ROW EXECUTE FUNCTION check_budget_monotonic();

-- Edge Hub 账本（HUB-012、HUB-013）：(hub_id, seq) 幂等
CREATE TABLE hub_ledger_entries (
  hub_id     uuid NOT NULL REFERENCES edge_hubs(id),
  seq        bigint NOT NULL CHECK (seq >= 1),
  kind       text NOT NULL CHECK (kind IN ('grant_issued','grant_settled','node_usage','grant_transferred')),
  tenant_id  uuid,
  payload    bytea NOT NULL,
  applied_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (hub_id, seq)
);

CREATE TABLE share_leases (
  id         uuid PRIMARY KEY,
  tenant_id  uuid NOT NULL REFERENCES tenants(id),
  node_id    uuid NOT NULL REFERENCES nodes(id),
  boot_id    uuid NOT NULL,
  kind       text NOT NULL CHECK (kind IN ('rate','conn')),
  amount     bigint NOT NULL CHECK (amount >= 0),
  expires_at timestamptz NOT NULL,
  released_at timestamptz
);
-- 计入占用的份额：未释放且未安全到期（LIM-004）
CREATE INDEX share_leases_live ON share_leases (tenant_id, kind) WHERE released_at IS NULL;

-- ===================== dns（6.3 节） =====================
CREATE TABLE dns_providers (
  id         uuid PRIMARY KEY,
  kind       text NOT NULL CHECK (kind IN ('cloudflare')),          -- PRT-015
  zone       text NOT NULL,
  token_enc  bytea NOT NULL,                                        -- 加密存储，按域名最小授权
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (kind, zone)
);

CREATE TABLE dns_records (
  id             uuid PRIMARY KEY,
  provider_id    uuid NOT NULL REFERENCES dns_providers(id),
  entry_group_id uuid NOT NULL REFERENCES node_groups(id),
  name           text NOT NULL,
  address        inet NOT NULL,
  ttl            integer NOT NULL DEFAULT 60 CHECK (ttl > 0),
  remote_id      text,
  created_at     timestamptz NOT NULL DEFAULT now(),
  UNIQUE (provider_id, name, address)
);

-- ===================== notify / audit / settings =====================
CREATE TABLE notifications (
  id         uuid PRIMARY KEY,
  tenant_id  uuid REFERENCES tenants(id),
  user_id    uuid REFERENCES users(id),
  kind       text NOT NULL CHECK (kind IN ('plan_expiring','traffic_low','node_offline','config_failed','announcement')),   -- FE-010
  payload    jsonb NOT NULL,
  read_at    timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX notifications_unread ON notifications (user_id, created_at DESC) WHERE read_at IS NULL;

CREATE TABLE announcements (
  id           uuid PRIMARY KEY,
  title        text NOT NULL,
  body         text NOT NULL,
  published_at timestamptz,
  created_by   uuid NOT NULL REFERENCES users(id),
  created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE audit_logs (
  id            bigint GENERATED ALWAYS AS IDENTITY,
  created_at    timestamptz NOT NULL DEFAULT now(),
  actor_kind    text NOT NULL CHECK (actor_kind IN ('user','system','node','hub')),
  actor_id      uuid,
  tenant_id     uuid,
  action        text NOT NULL,
  target_kind   text,
  target_id     text,
  detail        jsonb NOT NULL DEFAULT '{}',
  PRIMARY KEY (created_at, id)
) PARTITION BY RANGE (created_at);
CREATE TABLE audit_logs_default PARTITION OF audit_logs DEFAULT;
CREATE INDEX audit_logs_tenant ON audit_logs (tenant_id, created_at DESC);

CREATE TABLE settings (
  key        text PRIMARY KEY,
  value      jsonb NOT NULL,
  updated_by uuid REFERENCES users(id),
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE settings;
DROP TABLE audit_logs;
DROP TABLE announcements;
DROP TABLE notifications;
DROP TABLE dns_records;
DROP TABLE dns_providers;
DROP TABLE share_leases;
DROP TABLE hub_ledger_entries;
DROP TRIGGER regional_budgets_monotonic ON regional_budgets;
DROP FUNCTION check_budget_monotonic();
DROP TABLE regional_budgets;
DROP TABLE disputes;
DROP TABLE billing_grants;
DROP TABLE usage_daily;
DROP TABLE usage_minutely;
DROP TABLE usage_batches;
DROP TRIGGER group_multipliers_platform ON group_multipliers;
DROP FUNCTION check_multiplier_platform_group();
DROP TABLE group_multipliers;
DROP TABLE billing_versions;
DROP VIEW tenant_balances;
DROP TABLE redemptions;
DROP TABLE redeem_codes;
DROP TABLE traffic_packs;
DROP TABLE subscriptions;
DROP TABLE plan_entitlements;
DROP TABLE plans;
DROP TABLE apply_node_states;
DROP TABLE apply_runs;
DROP TRIGGER port_allocations_wildcard ON port_allocations;
DROP FUNCTION check_port_wildcard();
DROP TABLE port_allocations;
DROP TABLE rule_versions;
DROP TRIGGER rule_targets_shape ON rule_targets;
DROP TRIGGER rule_paths_shape ON rule_paths;
DROP TRIGGER rules_shape ON rules;
DROP FUNCTION check_rule_shape();
DROP TABLE rule_targets;
DROP TABLE rule_paths;
ALTER TABLE restrictions DROP CONSTRAINT restrictions_rule_fk;
DROP TABLE rules;
DROP TABLE node_port_ranges;
DROP TRIGGER entry_members_check ON entry_members;
DROP FUNCTION check_entry_member();
DROP TABLE entry_members;
DROP TABLE segment_permits;
DROP TABLE restrictions;
DROP TABLE login_attempts;
DROP TABLE user_totp;
-- +goose StatementEnd
