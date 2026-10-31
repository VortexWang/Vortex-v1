# 契约补充记录

契约锁定后的修正，只限不改变字段编号、线上格式与语义的调整（契约 C2a 第 2 节规则）。

| 编号 | 日期 | 契约 | 内容 | 原因 | 线上格式 / 语义 |
| --- | --- | --- | --- | --- | --- |
| L1 | 2026-10-31 | C2 | `proto/buf.yaml` 排除 4 条 buf STANDARD 规则：SERVICE_SUFFIX、RPC_REQUEST_STANDARD_NAME、RPC_RESPONSE_STANDARD_NAME、RPC_REQUEST_RESPONSE_UNIQUE | 首次 CI 中 buf lint 失败。需求 2.3 节规定服务名 `NodeControl`，服务名属于 gRPC 线上路径；双向流消息按发送方命名；Edge Hub 原样代理注册响应。锁定时自写的检查只覆盖命名类规则，未发现这些 | 不变 |
| L2 | 2026-10-31 | C4 | `api/openapi.yaml` 由 OpenAPI 3.1 改为 3.0.3 输出：`nullable` 取代 null 类型（31 处）、布尔 `exclusiveMinimum`、删除空 `required`、去掉 `license.identifier` | CI 中 oapi-codegen 生成失败：其依赖 kin-openapi 只支持 3.0。改后生成并编译通过，确认了原因 | 不变：92 个操作、86 个模型；前端类型严格编译仍通过 |
