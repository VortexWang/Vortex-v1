# 契约补充记录

契约锁定后的修正，只限不改变字段编号、线上格式与语义的调整（契约 C2a 第 2 节规则）。

| 编号 | 日期 | 契约 | 内容 | 原因 | 线上格式 / 语义 |
| --- | --- | --- | --- | --- | --- |
| L1 | 2026-10-31 | C2 | `proto/buf.yaml` 排除 4 条 buf STANDARD 规则：SERVICE_SUFFIX、RPC_REQUEST_STANDARD_NAME、RPC_RESPONSE_STANDARD_NAME、RPC_REQUEST_RESPONSE_UNIQUE | 首次 CI 中 buf lint 失败。需求 2.3 节规定服务名 `NodeControl`，服务名属于 gRPC 线上路径；双向流消息按发送方命名；Edge Hub 原样代理注册响应。锁定时自写的检查只覆盖命名类规则，未发现这些 | 不变 |
| L2 | 2026-10-31 | C4 | `api/openapi.yaml` 由 OpenAPI 3.1 改为 3.0.3 输出：`nullable` 取代 null 类型（31 处）、布尔 `exclusiveMinimum`、删除空 `required`、去掉 `license.identifier` | CI 中 oapi-codegen 生成失败：其依赖 kin-openapi 只支持 3.0。改后生成并编译通过，确认了原因 | 不变：92 个操作、86 个模型；前端类型严格编译仍通过 |
| L3 | 2026-11-01 | C2 | 澄清 `LedgerEntries` 的序号语义：Edge Hub 只上报 oneof 中的四类条目（签发、结算、计量批次、重启转移），本地专用条目（模式、限制快照、份额租约、确认记录等）不上报，上报序列中的序号因此**不连续**；后端按 (hub_id, seq) 幂等应用，不得等待缺失的序号；`LedgerAck.acked_through_seq` 表示 ≤ 该序号的已上报条目均已入账。原注释“seq 在实例内从 1 起连续递增”指 Edge Hub 本地账本 | M3 实现上行链路时发现：oneof 没有本地条目的形式，若要求连续，后端会一直等待永远不会上报的序号 | 不变（字段与编号不变，只澄清后端实现约束） |
