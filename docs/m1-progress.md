# M1 进度（引擎链路）

> 更新：2026-10-31（第一批）· 对应开发方案 01（转发引擎）第 5 节 ENG-T20～T30、T40；契约 C1

## 已完成

| 任务 | 内容 | 验证 |
| --- | --- | --- |
| ENG-T20 | 承载层 `engine/vlink/carrier.go`：TLS 1.3 双向认证、ALPN `vortex-vlink/1`、平台 CA 链（含中间 CA）校验、身份只取自 SAN URI `spiffe://vortex/node/<id>`（恰好一个）、吊销列表（证书序列号与 node_id，可原子替换）、发起方校验对端是否为预期节点 | 见下表 |
| ENG-T21 | 帧编解码 `frame.go`（读取零分配、超长在读载荷前拒绝、stream_id 规则、未知类型、扩展帧需协商、PADDING 丢弃）；HELLO / OPEN / 原因码 / WINDOW / GOAWAY 载荷；TLV（重复、截断、缺失、长度不符为错误，未知 tag 忽略）；握手 `handshake.go`（双方立即发送 HELLO 不等待对方、必须是第一帧、5 秒时限、node_id 与证书一致、exporter 签名、代次不低于已知值、版本取 min、能力位取交集、served 摘要比对） | 15 个测试向量逐字节一致；与参考实现完成 TLS 互连 |

| 测试 | 覆盖 |
| --- | --- |
| TestVectors | `ref/testdata/vectors.json` 全部 15 个向量：读取 → 载荷解码再编码 → 整帧编码，逐字节一致 |
| TestCrossCodecWithReference | 生产实现与参考实现互相编解码 OPEN（含 IPv6、v4 映射地址、4 个已经过节点）、HELLO（含 served 摘要）；served 摘要计算一致 |
| TestInteropWithReference | 参考实现作为发起方在真实 TLS 上发送并校验 HELLO，生产实现作为接受方，双方互相校验通过 |
| TestFrameRules / TestTLVRules / TestOpenConstraints | 帧头、TLV 与 OPEN 取值约束（R5、R9、R10） |
| TestReaderZeroAlloc | 读取 16 KiB DATA 帧 0 次分配 |
| TestHandshakeAndFirstFrames | 中间 CA 签发的证书互认；能力交集、对端模式与代次；HELLO 之后的帧经同一 Reader 读取 |
| TestHandshakeRejections | 外部 CA、无节点 URI、两个节点 URI、连到非预期节点、吊销序列号（含前导零）、吊销 node_id、旧代次（发送 GOAWAY）、served 摘要一致与不一致（不一致仍建链并标记） |
| TestHandshakeProtocolViolations | 第一帧不是 HELLO（含载荷是合法 HELLO 的其他类型帧）、node_id 与证书不符、版本 0（GOAWAY version_unsupported）、篡改代次、重放另一条连接的 HELLO、版本更高的对端协商为 1、5 秒时限 |
| TestALPNRequired / TestVerifyPeerRejectsChaCha | 未协商 ALPN 拒绝；TLS 1.3 下 Go 不能配置套件，握手后拒绝 ChaCha20 |
| FuzzFrames | 任意输入不 panic；解析成功的 HELLO / OPEN 再编码后内容不变（45 秒、35 万次） |

变异检查：20 项全部被发现（stream_id 规则、超长检查、TLV 重复、签名、node_id 一致、代次、版本、首帧类型、两类吊销、序列号前导零、预期节点、套件、ALPN、多个 URI、证书链、served 摘要、visited 上限、握手时限、link_mode 取值）。初始测试漏掉了“首帧类型”与 ALPN 两项（前者因载荷解析失败碰巧报错，后者因 5 秒超时碰巧报错），已补用例并要求具体错误。

## 对契约的解释（未改变契约）

| 位置 | 解释 |
| --- | --- |
| OPEN 载荷 TLV 重复或截断 | 与 R10 的取值约束一样回 `OPEN_ERR(unauthorized)`，不作为链路级协议错误，避免一条坏流关闭整条链路上的其他流 |
| `WINDOW` 增量 0 | 契约未禁止，按无操作处理 |
| `link_mode` 缺失 | 缺省为 mux；取值不是 0 / 1 为协议错误 |
| 允许的 TLS 套件 | Go 的 TLS 1.3 套件不可配置，握手完成后检查，ChaCha20 拒绝 |

## 下一步

ENG-T22 流复用（OPEN / OPEN_OK / OPEN_ERR / DATA / FIN / RST、0-RTT 开流）→ T23 流控 → T25 隧道 TCP 端到端。
