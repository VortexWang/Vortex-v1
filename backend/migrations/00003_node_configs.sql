-- 契约 C3 · 变更 C3-CC-01：节点运行配置（下发编排，BE-T15）
-- 需求基线：《VORTEX 需求规格说明书 V1.7（已冻结）》2.3 节（version 单调递增）、7.4～7.5 节；契约 C2 RuntimeConfig.version
-- 依赖：00001、00002。只新增，不修改已锁定的表。

-- +goose Up
-- +goose StatementBegin

-- 每个节点最新编译的运行配置（期望），以及节点报告的准备 / 生效进度。
-- version 单调递增：只有配置内容（不含 version）变化时才递增（content_digest 判定）；节点重连或切换实例时重发同一版本。
CREATE TABLE node_configs (
  node_id          uuid PRIMARY KEY REFERENCES nodes(id),
  version          bigint NOT NULL CHECK (version >= 1),
  config           bytea NOT NULL,                 -- runtime.v1.RuntimeConfig（protobuf）
  content_digest   bytea NOT NULL,                 -- SHA-256(去掉 version 的配置)
  prepared_version bigint CHECK (prepared_version >= 1),   -- 节点报告 Prepare 成功的最高版本
  commit_sent      bigint CHECK (commit_sent >= 1),        -- 已下发 Commit 的最高版本
  applied_version  bigint CHECK (applied_version >= 1),    -- 节点报告 Commit 成功（生效）的最高版本（7.5 实际层）
  last_error       text,                           -- 最近一次 Prepare / Commit 失败（资源错误原样保存）
  compiled_at      timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT node_configs_progress CHECK (prepared_version IS NULL OR prepared_version <= version),
  CONSTRAINT node_configs_commit CHECK (commit_sent IS NULL OR commit_sent <= coalesce(prepared_version, 0)),
  CONSTRAINT node_configs_applied CHECK (applied_version IS NULL OR applied_version <= version)
);

CREATE FUNCTION check_node_config_monotonic() RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
  IF NEW.version < OLD.version
     OR coalesce(NEW.prepared_version, 0) < coalesce(OLD.prepared_version, 0)
     OR coalesce(NEW.commit_sent, 0) < coalesce(OLD.commit_sent, 0)
     OR coalesce(NEW.applied_version, 0) < coalesce(OLD.applied_version, 0) THEN
    RAISE EXCEPTION 'NODE_CONFIG_NOT_MONOTONIC: node %', NEW.node_id USING ERRCODE = 'check_violation';
  END IF;
  RETURN NEW;
END $fn$;
CREATE TRIGGER node_configs_monotonic BEFORE UPDATE ON node_configs
  FOR EACH ROW EXECUTE FUNCTION check_node_config_monotonic();

-- 每次编排中每个节点需要达到的配置版本（包含该规则版本的最早节点配置版本）
ALTER TABLE apply_node_states ADD COLUMN required_version bigint CHECK (required_version >= 1);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE apply_node_states DROP COLUMN required_version;
DROP TRIGGER node_configs_monotonic ON node_configs;
DROP FUNCTION check_node_config_monotonic();
DROP TABLE node_configs;
-- +goose StatementEnd
