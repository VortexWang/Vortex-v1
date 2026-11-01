# 契约补充记录

契约锁定后的修正，只限不改变字段编号、线上格式与语义的调整（契约 C2a 第 2 节规则）。

| 编号 | 日期 | 契约 | 内容 | 原因 | 线上格式 / 语义 |
| --- | --- | --- | --- | --- | --- |
| L1 | 2026-10-31 | C2 | `proto/buf.yaml` 排除 4 条 buf STANDARD 规则：SERVICE_SUFFIX、RPC_REQUEST_STANDARD_NAME、RPC_RESPONSE_STANDARD_NAME、RPC_REQUEST_RESPONSE_UNIQUE | 首次 CI 中 buf lint 失败。需求 2.3 节规定服务名 `NodeControl`，服务名属于 gRPC 线上路径；双向流消息按发送方命名；Edge Hub 原样代理注册响应。锁定时自写的检查只覆盖命名类规则，未发现这些 | 不变 |
| L2 | 2026-10-31 | C4 | `api/openapi.yaml` 由 OpenAPI 3.1 改为 3.0.3 输出：`nullable` 取代 null 类型（31 处）、布尔 `exclusiveMinimum`、删除空 `required`、去掉 `license.identifier` | CI 中 oapi-codegen 生成失败：其依赖 kin-openapi 只支持 3.0。改后生成并编译通过，确认了原因 | 不变：92 个操作、86 个模型；前端类型严格编译仍通过 |
| L3 | 2026-11-01 | C2 | 澄清 `LedgerEntries` 的序号语义：Edge Hub 只上报 oneof 中的四类条目（签发、结算、计量批次、重启转移），本地专用条目（模式、限制快照、份额租约、确认记录等）不上报，上报序列中的序号因此**不连续**；后端按 (hub_id, seq) 幂等应用，不得等待缺失的序号；`LedgerAck.acked_through_seq` 表示 ≤ 该序号的已上报条目均已入账。原注释“seq 在实例内从 1 起连续递增”指 Edge Hub 本地账本 | M3 实现上行链路时发现：oneof 没有本地条目的形式，若要求连续，后端会一直等待永远不会上报的序号 | 不变（字段与编号不变，只澄清后端实现约束） |
| L4 | 2026-11-01 | C2 | 约定引擎错误的表示：`PrepareReport` / `ApplyReport` 没有错误字段，引擎拒绝配置或指令（配置无效、版本过旧、未决版本不符）时，Prepare 在 `PrepareResult.errors` 加一条对象为空、`code = ENGINE_ERROR`、`detail` 为错误文本的记录；Apply 在 `ApplyResult.objects` 加一条对象为空、`status = FAILED`、`error_code = "ENGINE_ERROR: <文本>"` 的记录 | M3 接入 gRPC 时发现：Agent 必须如实上报这类失败（RUL-010～013 的编排在后端），契约没有对应字段 | 不变（只约定已有字段的取值） |
| L5 | 2026-11-01 | C2 | 约定 Edge Hub 证书身份：SAN URI `spiffe://vortex/hub/<hub_id>`（8-4-4-4-12 小写），与节点的 `spiffe://vortex/node/<node_id>`（C1 第 1 节）对应；Agent 按平台 CA 校验链并要求 URI 与实例列表中的 `hub_id` 一致，不做主机名校验。后端证书 SAN URI 为 `spiffe://vortex/backend`，Edge Hub 同样按平台 CA 校验并要求该 URI。Edge Hub 证书同时是 Agent 的服务端证书与后端的客户端证书，扩展用途须同时含 serverAuth 与 clientAuth | 契约规定组件间双向证书认证（SEC-001），但只定义了节点身份的格式 | 不变 |
| L6 | 2026-11-01 | C2 | 约定 Edge Hub 墙钟的传递：`NodeControl.Connect` 的 gRPC 响应头（metadata）`vortex-hub-time-unix-nano`，值为 Edge Hub 墙钟的 Unix 纳秒（十进制），在任何 HubMessage 之前发出。Agent 以“流建立 → 收到响应头”的往返中点估计偏差、往返的一半为不确定度，扣除不确定度后仍超过 5 秒即告警（AGT-005：只告警，授权按相对时长计算，不受影响）。缺少该头时不比对 | AGT-005 要求检测本地与 Edge Hub 的时钟偏差，但 C2 的消息里没有 Edge Hub 时间字段 | protobuf 消息不变；新增的是传输层 metadata，旧版本任一端忽略它即可互通。**这是新增约定而非澄清，请在下次契约评审时确认**；若评审要求改为消息字段，可在 `HubHello` 增加 `google.protobuf.Timestamp hub_time`（新编号，向后兼容） |
| L7 | 2026-11-01 | C2 | 约定 `UpgradeCommand.signature`（“平台发布签名”）的格式：Ed25519，签名内容为 `"vortex-node-release-v1" 0x00 version 0x00 sha256(32 字节) irreversible(1 字节，0/1)`；不可逆标记是发布的属性，一并签名，控制通道无法改动。公钥随安装包分发（节点配置 `release_key_file`，PEM `PUBLIC KEY`）。兼容矩阵由新程序自己给出：`vortex-node version -json` 输出 `{version, store_schema, store_schema_min, store_schema_max}`，旧版本在校验签名与摘要后执行它读取 | 契约只有 `signature` 字段，没有规定算法与签名内容；“升级包携带兼容矩阵”没有规定载体 | 不变（约定已有字段的取值；兼容矩阵在程序内，不经线上）。2026-11-01 已确认 |

## 契约变更

改变线上格式的修改（只允许向后兼容的新增），须经确认后实施，并通过 `buf breaking`。

| 编号 | 日期 | 契约 | 内容 | 原因 | 兼容性 | 确认 |
| --- | --- | --- | --- | --- | --- | --- |
| CC-01 | 2026-11-01 | C2 | `backend.v1.HubUpMessage` 新增 `NodeUpgradeForward node_upgrade = 9`（`node_id`、`vortex.edgehub.v1.UpgradeStatus status`）：Edge Hub 原样转发节点的升级进度 | 原契约中 Agent 上报 `UpgradeStatus`，但 Edge Hub → 后端没有对应字段，后端看不到“已回退”与失败原因（AGT-008） | 新增 oneof 成员与消息，编号未用过；旧后端忽略未知字段；`buf breaking` 相对 main 通过 | 2026-11-01 已确认 |
