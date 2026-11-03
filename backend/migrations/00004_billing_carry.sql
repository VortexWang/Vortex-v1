-- 契约 C3 · 变更 C3-CC-02：租户计费字节的累计小数余量（计量入账，BE-T18）
-- 需求基线：《VORTEX 需求规格说明书 V1.7（已冻结）》MTR-002（倍率采用定点数计算并累计小数余量）、MTR-004
-- 依赖：00001、00002。只新增，不修改已锁定的表。

-- +goose Up
-- +goose StatementBegin

-- 结算按 原始字节 × 路径系数（千分位）得到千分之一字节；每个租户累计不足 1 字节的部分，满 1000 再从余额扣 1 字节。
-- 与 tenant_balances 的扣减在同一事务内更新（先锁订阅行，MTR-004）。
CREATE TABLE tenant_billing_carry (
  tenant_id       uuid PRIMARY KEY REFERENCES tenants(id),
  remainder_milli smallint NOT NULL DEFAULT 0 CHECK (remainder_milli BETWEEN 0 AND 999),
  updated_at      timestamptz NOT NULL DEFAULT now()
);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE tenant_billing_carry;
-- +goose StatementEnd
