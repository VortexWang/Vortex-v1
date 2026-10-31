-- 契约 C3 · 控制面基础表（第一批，M3 前锁定）
-- 需求基线：《VORTEX 需求规格说明书 V1.7（已冻结）》第 2.3、4、5 节，AGT-001、SEC-001、SEC-002、SEC-012、BE-003
-- 约定：主键 UUIDv7 由应用生成；时间 timestamptz（UTC）；枚举为 text + CHECK，取值与需求 2.3 节一致。
-- 已在 PostgreSQL 16 上执行 Up / Down / 再次 Up，并通过 C3b 记录中的约束测试。

-- +goose Up
-- +goose StatementBegin

-- ---------- 租户与用户（TEN-001、TEN-007） ----------
CREATE TABLE tenants (
  id          uuid PRIMARY KEY,
  name        text NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE users (
  id            uuid PRIMARY KEY,
  tenant_id     uuid REFERENCES tenants(id),          -- 管理员可为空；普通用户必填
  email         text NOT NULL,
  password_hash text NOT NULL,                        -- argon2id
  role          text NOT NULL CHECK (role IN ('admin','member')),
  status        text NOT NULL DEFAULT 'active' CHECK (status IN ('active','suspended','banned')),
  row_version   bigint NOT NULL DEFAULT 1,
  created_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT users_member_has_tenant CHECK (role = 'admin' OR tenant_id IS NOT NULL)
);
CREATE UNIQUE INDEX users_email_uq ON users (lower(email));
-- V1.0 一个普通用户对应一个租户（TEN-001）
CREATE UNIQUE INDEX users_member_tenant_uq ON users (tenant_id) WHERE role = 'member';

-- ---------- Edge Hub（HUB-T19） ----------
CREATE TABLE edge_hubs (
  id                  uuid PRIMARY KEY,
  region              text NOT NULL,
  address             text NOT NULL,
  mode                text NOT NULL DEFAULT 'offline'
                        CHECK (mode IN ('online','autonomous','reconciling','autonomy_expired','offline')),
  hub_version         text,
  last_contact_at     timestamptz,
  autonomy_started_at timestamptz,
  created_at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX edge_hubs_region ON edge_hubs (region);

CREATE TABLE hub_bootstrap_tokens (
  id          uuid PRIMARY KEY,
  token_hash  bytea NOT NULL UNIQUE,                  -- 只存哈希（SEC-004）
  region      text NOT NULL,
  expires_at  timestamptz NOT NULL,
  used_at     timestamptz,
  used_by_hub uuid REFERENCES edge_hubs(id),
  created_at  timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT hub_token_used_consistent CHECK ((used_at IS NULL) = (used_by_hub IS NULL))
);

-- ---------- 节点（NOD-001～NOD-005） ----------
CREATE TABLE nodes (
  id               uuid PRIMARY KEY,
  tenant_id        uuid REFERENCES tenants(id),       -- 空 = 平台节点；非空 = 自托管节点（TEN-002）
  name             text NOT NULL,
  region           text NOT NULL,
  edge_hub_id      uuid REFERENCES edge_hubs(id),
  can_entry        boolean NOT NULL DEFAULT false,
  can_relay        boolean NOT NULL DEFAULT false,
  can_exit         boolean NOT NULL DEFAULT false,
  public_addresses inet[] NOT NULL DEFAULT '{}',      -- 空数组 = 无公网地址（NOD-005）
  lifecycle        text NOT NULL DEFAULT 'pending'
                     CHECK (lifecycle IN ('pending','online','offline','draining','disabled')),
  agent_version    text,
  current_boot_id  uuid,
  capacity         jsonb NOT NULL DEFAULT '{}',
  row_version      bigint NOT NULL DEFAULT 1,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now(),
  deleted_at       timestamptz                         -- NOD-004：删除不清除必要历史
);
CREATE INDEX nodes_tenant ON nodes (tenant_id) WHERE deleted_at IS NULL;
CREATE INDEX nodes_hub ON nodes (edge_hub_id) WHERE deleted_at IS NULL;

-- ---------- 节点组（NOD-006～NOD-009） ----------
CREATE TABLE node_groups (
  id              uuid PRIMARY KEY,
  tenant_id       uuid REFERENCES tenants(id),        -- 空 = 平台组；非空 = 个人组
  name            text NOT NULL,
  purpose         text NOT NULL CHECK (purpose IN ('entry','relay','exit')),
  member_schedule text NOT NULL DEFAULT 'weighted_round_robin'
                    CHECK (member_schedule IN ('weighted_round_robin','primary_backup')),
  health_params   jsonb NOT NULL DEFAULT '{"interval_s":3,"fail_threshold":3,"recover_threshold":2}',
  row_version     bigint NOT NULL DEFAULT 1,
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now(),
  deleted_at      timestamptz
);
CREATE INDEX node_groups_tenant ON node_groups (tenant_id) WHERE deleted_at IS NULL;

CREATE TABLE node_group_members (
  node_group_id uuid NOT NULL REFERENCES node_groups(id),
  node_id       uuid NOT NULL REFERENCES nodes(id),
  weight        integer NOT NULL DEFAULT 1 CHECK (weight > 0),
  priority      integer NOT NULL DEFAULT 0 CHECK (priority >= 0),
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (node_group_id, node_id)
);
CREATE INDEX node_group_members_node ON node_group_members (node_id);

-- 跨表约束：NOD-006（能力与用途相符）、NOD-007（平台组只含平台节点，个人组只含同租户自托管节点）、
-- NOD-005（无公网地址的节点不得加入入口组）。
CREATE FUNCTION check_node_group_member() RETURNS trigger LANGUAGE plpgsql AS $fn$
DECLARE
  g node_groups%ROWTYPE;
  n nodes%ROWTYPE;
BEGIN
  SELECT * INTO g FROM node_groups WHERE id = NEW.node_group_id;
  SELECT * INTO n FROM nodes WHERE id = NEW.node_id;
  IF g.tenant_id IS DISTINCT FROM n.tenant_id THEN
    RAISE EXCEPTION 'NODE_GROUP_TENANT_MISMATCH: group % and node % belong to different owners', g.id, n.id
      USING ERRCODE = 'check_violation';
  END IF;
  IF (g.purpose = 'entry' AND NOT n.can_entry)
     OR (g.purpose = 'relay' AND NOT n.can_relay)
     OR (g.purpose = 'exit'  AND NOT n.can_exit) THEN
    RAISE EXCEPTION 'NODE_GROUP_CAPABILITY_MISMATCH: node % cannot serve as %', n.id, g.purpose
      USING ERRCODE = 'check_violation';
  END IF;
  IF g.purpose = 'entry' AND cardinality(n.public_addresses) = 0 THEN
    RAISE EXCEPTION 'NODE_GROUP_ENTRY_NEEDS_PUBLIC_ADDRESS: node %', n.id
      USING ERRCODE = 'check_violation';
  END IF;
  RETURN NEW;
END
$fn$;
CREATE TRIGGER node_group_members_check
  BEFORE INSERT OR UPDATE ON node_group_members
  FOR EACH ROW EXECUTE FUNCTION check_node_group_member();

-- 反向检查：节点或节点组本身变化后，已有成员关系必须仍然满足上述规则（否则约束可被事后绕过）。
CREATE FUNCTION recheck_memberships() RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
  -- 通过对相关成员行做一次空更新，复用 check_node_group_member 的全部规则
  IF TG_TABLE_NAME = 'nodes' THEN
    UPDATE node_group_members SET node_id = node_id WHERE node_id = NEW.id;
  ELSE
    UPDATE node_group_members SET node_group_id = node_group_id WHERE node_group_id = NEW.id;
  END IF;
  RETURN NEW;
END
$fn$;
CREATE TRIGGER nodes_recheck_memberships
  AFTER UPDATE OF tenant_id, can_entry, can_relay, can_exit, public_addresses ON nodes
  FOR EACH ROW EXECUTE FUNCTION recheck_memberships();
CREATE TRIGGER node_groups_recheck_memberships
  AFTER UPDATE OF tenant_id, purpose ON node_groups
  FOR EACH ROW EXECUTE FUNCTION recheck_memberships();

-- ---------- 证书与吊销（SEC-001、SEC-002、SEC-012） ----------
CREATE TABLE certificates (
  id             uuid PRIMARY KEY,
  subject_kind   text NOT NULL CHECK (subject_kind IN ('node','hub')),
  subject_id     uuid NOT NULL,
  serial         bytea NOT NULL UNIQUE,
  not_before     timestamptz NOT NULL,
  not_after      timestamptz NOT NULL,
  revoked_at     timestamptz,
  revoke_reason  text,
  created_at     timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT certificates_validity CHECK (not_after > not_before),
  CONSTRAINT certificates_revoke_consistent CHECK ((revoked_at IS NULL) = (revoke_reason IS NULL))
);
CREATE INDEX certificates_subject ON certificates (subject_kind, subject_id);
CREATE INDEX certificates_revoked ON certificates (revoked_at) WHERE revoked_at IS NOT NULL;

-- 吊销列表序号单调递增（runtime.v1.RevocationList.serial）
CREATE TABLE revocation_list_versions (
  serial     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  reason     text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- ---------- 节点注册令牌（AGT-001） ----------
CREATE TABLE enroll_tokens (
  id            uuid PRIMARY KEY,
  tenant_id     uuid REFERENCES tenants(id),          -- 空 = 平台节点令牌
  token_hash    bytea NOT NULL UNIQUE,
  expires_at    timestamptz NOT NULL,                  -- 默认 created_at + 30 分钟，由应用设置
  used_at       timestamptz,
  used_by_node  uuid REFERENCES nodes(id),
  created_by    uuid NOT NULL REFERENCES users(id),
  created_at    timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT enroll_token_used_consistent CHECK ((used_at IS NULL) = (used_by_node IS NULL)),
  CONSTRAINT enroll_token_expiry CHECK (expires_at > created_at)
);

-- ---------- 任务表与 Outbox（BE-003） ----------
CREATE TABLE tasks (
  id              uuid PRIMARY KEY,
  kind            text NOT NULL,
  idempotency_key text UNIQUE,
  payload         jsonb NOT NULL,
  status          text NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','running','succeeded','failed','dead')),
  attempts        integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  next_run_at     timestamptz NOT NULL DEFAULT now(),
  locked_until    timestamptz,
  last_error      text,
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX tasks_due ON tasks (next_run_at) WHERE status = 'pending';

CREATE TABLE outbox (
  id           uuid PRIMARY KEY,
  topic        text NOT NULL,                          -- 如 node.desired_config、dns.change
  key          text NOT NULL,                          -- 分区键，保证同一对象按序投递
  payload      bytea NOT NULL,                         -- protobuf 或 JSON
  created_at   timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz
);
CREATE INDEX outbox_unpublished ON outbox (created_at) WHERE published_at IS NULL;

-- ---------- 平台代次（12.4 节恢复流程） ----------
CREATE TABLE platform_generation (
  singleton  boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  generation bigint NOT NULL DEFAULT 1 CHECK (generation >= 1),
  updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO platform_generation DEFAULT VALUES;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE platform_generation;
DROP TABLE outbox;
DROP TABLE tasks;
DROP TABLE enroll_tokens;
DROP TABLE revocation_list_versions;
DROP TABLE certificates;
DROP TRIGGER node_groups_recheck_memberships ON node_groups;
DROP TRIGGER nodes_recheck_memberships ON nodes;
DROP FUNCTION recheck_memberships();
DROP TRIGGER node_group_members_check ON node_group_members;
DROP FUNCTION check_node_group_member();
DROP TABLE node_group_members;
DROP TABLE node_groups;
DROP TABLE nodes;
DROP TABLE hub_bootstrap_tokens;
DROP TABLE edge_hubs;
DROP TABLE users;
DROP TABLE tenants;
-- +goose StatementEnd
