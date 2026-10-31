# M1 进度（引擎链路）

> 更新：2026-10-31（第二批）· 对应开发方案 01（转发引擎）第 5 节 ENG-T20～T30、T40；契约 C1

## 已完成

| 任务 | 内容 | 验证 |
| --- | --- | --- |
| ENG-T20 | 承载层 `engine/vlink/carrier.go`：TLS 1.3 双向认证、ALPN `vortex-vlink/1`、平台 CA 链（含中间 CA）校验、身份只取自 SAN URI `spiffe://vortex/node/<id>`（恰好一个）、吊销列表（证书序列号与 node_id，可原子替换）、发起方校验对端是否为预期节点 | 见下表 |
| ENG-T21 | 帧编解码 `frame.go`（读取零分配、超长在读载荷前拒绝、stream_id 规则、未知类型、扩展帧需协商、PADDING 丢弃）；HELLO / OPEN / 原因码 / WINDOW / GOAWAY 载荷；TLV（重复、截断、缺失、长度不符为错误，未知 tag 忽略）；握手 `handshake.go`（双方立即发送 HELLO 不等待对方、必须是第一帧、5 秒时限、node_id 与证书一致、exporter 签名、代次不低于已知值、版本取 min、能力位取交集、served 摘要比对） | 15 个测试向量逐字节一致；与参考实现完成 TLS 互连 |

| ENG-T22 | 流复用 `link.go` / `stream.go`：每条链路固定读、写两个 goroutine，流没有 goroutine（会话 goroutine 预算与直接转发相同，ENG-045）；发起方奇数、接受方偶数流 ID，单调递增；OPEN 后立即可写（0-RTT），OPEN_OK 只用于统计时延；OPEN_ERR / RST 原因码以 `StreamError` 返回；FIN 半关闭；`Stream` 实现 `net.Conn` 与 `CloseWrite`，可直接交给 pipe；GOAWAY 排空与流 ID 耗尽；PING / PONG | 见“流复用与流控”表 |
| ENG-T23 | 流控：流初始窗口 256 KiB、链路窗口 16 MiB；数据被读取（或丢弃）后才归还额度，内存有上界；一个窗口在 2 个 RTT 内用完则翻倍，上限 4 MiB（RTT 建链后以 PING 实测）；对端超出窗口为协议错误 | 同上 |

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

### 流复用与流控

| 测试 | 覆盖 |
| --- | --- |
| TestEchoLargeTransfer | 24 MiB（超过链路窗口）回显逐字节一致；结束后两端流表为空 |
| TestManyStreamsBothDirections | 双方各开 100 条流并发回显；流 ID 奇偶正确 |
| TestZeroRTTDataBufferedUntilAccept | 接受方开流检查期间到达的数据被缓存，接受后读出 |
| TestRejectReturnsReasonAndDiscards | 80 条被拒绝的流各带 256 KiB 0-RTT 数据（合计超过链路窗口）：原因码与说明回到发起方，丢弃的数据归还链路额度，之后新流正常传输 |
| TestLateDataReturnsLinkCredit | 已结束的流的迟到数据被丢弃并归还链路额度 |
| TestHalfCloseAndReset | 半关闭后反方向继续；写已关闭方向报错；RST 原因码；对端未结束时 Close 发送 RST(no_route) |
| TestStreamWindowBackpressureIsPerStream | 无人读取的流恰好发出一个窗口后停住，其他流不受影响；读取后发送方恢复 |
| TestWindowGrowsWhenItLimitsThroughput | 窗口在上限内增长 |
| TestReadDeadline | 读、写截止时间（`net.Error` 超时）；延后截止时间后恢复 |
| TestDrainAndGoaway / TestDrainRejectsNewPeerStreams | GOAWAY 后对端不再开流、已有流继续；排空后新流回 OPEN_ERR(no_route)，GOAWAY 携带 last_stream_id |
| TestStreamIDExhaustion | 耗尽时开流失败并发送 GOAWAY，对端也停止在该链路开流 |
| TestProtocolViolationsCloseLink | 未开过的流的 DATA、接受方奇偶的 OPEN、流 ID 不递增、超出流窗口、超出链路窗口、FIN 之后的 DATA、第二个 HELLO、PING 长度、未知帧类型：均发送 GOAWAY(protocol_error) 并关闭 |
| TestMalformedOpenRejectedLinkSurvives | 畸形 OPEN 回 OPEN_ERR(unauthorized)，链路继续 |
| TestLinkCloseEndsStreams | 链路关闭时两端的流都结束，goroutine 无泄漏 |
| TestPipeOverStream | pipe 在 TCP 连接与流之间转发 5 MiB 逐字节一致（隧道入口的数据路径） |
| TestThroughputReport | 单流经 TLS 回环约 11 Gbit/s（参考值） |

变异检查：19 项中 18 项被发现；未被发现的一项（写入时忽略流窗口）实际不会多发数据，只是在额度为 0 时空转，属于等价变异。最初有两项未被发现（终止流时未归还未读数据的额度、迟到数据未归还额度）：原测试只检查拒绝原因而没有检查之后链路能否继续传输，已补“之后新流正常传输”与迟到数据专项测试。

## 对契约的解释（未改变契约）

| 位置 | 解释 |
| --- | --- |
| OPEN 载荷 TLV 重复或截断 | 与 R10 的取值约束一样回 `OPEN_ERR(unauthorized)`，不作为链路级协议错误，避免一条坏流关闭整条链路上的其他流 |
| `WINDOW` 增量 0 | 契约未禁止，按无操作处理 |
| `link_mode` 缺失 | 缺省为 mux；取值不是 0 / 1 为协议错误 |
| 允许的 TLS 套件 | Go 的 TLS 1.3 套件不可配置，握手完成后检查，ChaCha20 拒绝 |

## 下一步

ENG-T24 开流校验与防绕过 → T25 隧道 TCP 端到端（入口 → 出口 → 目标，接入引擎运行时）→ T26 DGRAM。
