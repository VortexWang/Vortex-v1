# M1 进度（引擎链路）

> 更新：2026-10-31（第五批）· 对应开发方案 01（转发引擎）第 5 节 ENG-T20～T30、T40；契约 C1

## 已完成

| 任务 | 内容 | 验证 |
| --- | --- | --- |
| ENG-T20 | 承载层 `engine/vlink/carrier.go`：TLS 1.3 双向认证、ALPN `vortex-vlink/1`、平台 CA 链（含中间 CA）校验、身份只取自 SAN URI `spiffe://vortex/node/<id>`（恰好一个）、吊销列表（证书序列号与 node_id，可原子替换）、发起方校验对端是否为预期节点 | 见下表 |
| ENG-T21 | 帧编解码 `frame.go`（读取零分配、超长在读载荷前拒绝、stream_id 规则、未知类型、扩展帧需协商、PADDING 丢弃）；HELLO / OPEN / 原因码 / WINDOW / GOAWAY 载荷；TLV（重复、截断、缺失、长度不符为错误，未知 tag 忽略）；握手 `handshake.go`（双方立即发送 HELLO 不等待对方、必须是第一帧、5 秒时限、node_id 与证书一致、exporter 签名、代次不低于已知值、版本取 min、能力位取交集、served 摘要比对） | 15 个测试向量逐字节一致；与参考实现完成 TLS 互连 |

| ENG-T22 | 流复用 `link.go` / `stream.go`：每条链路固定读、写两个 goroutine，流没有 goroutine（会话 goroutine 预算与直接转发相同，ENG-045）；发起方奇数、接受方偶数流 ID，单调递增；OPEN 后立即可写（0-RTT），OPEN_OK 只用于统计时延；OPEN_ERR / RST 原因码以 `StreamError` 返回；FIN 半关闭；`Stream` 实现 `net.Conn` 与 `CloseWrite`，可直接交给 pipe；GOAWAY 排空与流 ID 耗尽；PING / PONG | 见“流复用与流控”表 |
| ENG-T23 | 流控：流初始窗口 256 KiB、链路窗口 16 MiB；数据被读取（或丢弃）后才归还额度，内存有上界；一个窗口在 2 个 RTT 内用完则翻倍，上限 4 MiB（RTT 建链后以 PING 实测）；对端超出窗口为协议错误 | 同上 |

| ENG-T24 | 开流检查 `engine/tunnel/router.go`（ENG-020）：① 规则已激活且规则版本、路径代次一致 ② 对端 node_id（取自证书）属于允许的上游 ③ hop_index 等于本节点跳位 ④ 已经过节点不含本节点，另检查网络类型与入口角色；⑤ 门控。拒绝回 OPEN_ERR 并产生含对端、规则、原因的审计事件（ENG-023）；下一跳与目标只取本地配置（ENG-021） | 见“隧道端到端”表 |
| ENG-T25 | 隧道 TCP 端到端：`tunnel.Peers` 按需建链（同一对端并发请求共用一次建链，失败后 5 秒内视为不可用）、接受入站链路、每条链路一个接受流的 goroutine；`tunnel.Router` 在下一组中选成员（排除已经过节点与本节点，失败换下一个成员，无可用成员 no_route）；中继与出口经 pipe 转发并逐跳计量；下游原因码逐段回传，入口记事件；运行时接入：入口监听支持下一跳规则，提交时更新对端地址、吊销列表并关闭被吊销对端的链路；下一组成员移除、路径代次与跳位变化属于权限收紧 | 同上 |

| ENG-T26 | UDP 隧道：网络类型为 udp 的 OPEN 后立即可发 DGRAM（0-RTT），开流检查完成前的数据报在会话队列中排队（默认 512 个），切换为回调时按序交付；DGRAM 独立发送队列（每链路 1 MiB），优先于 DATA 但有 DATA 等待时每批不超过一半字节，只在队列满时丢弃；超过单帧上限丢弃；未开流、已关闭、TCP 流上的 DGRAM 丢弃；各类丢弃分别计数。中继与出口的上行在链路读 goroutine 中直接转发（非阻塞整包计量，额度不足即丢包），出口每会话一个独立已连接 socket 和一个 goroutine（ENG-042、ENG-045）；空闲超时回收并 RST 回传；入口 UDP 监听支持下一跳规则 | 见“UDP 隧道”表 |

| ENG-T27 | 链路模式（按对端配置 `Peer.link_mode`，默认 auto）：配置提交即预热；mux 每个对端 2～4 条，新流选在途字节最少的链路，全部链路在途超过链路窗口一半时增加一条，超过下限的空闲链路 60 秒后关闭；pool 预热空闲链路、每条同时只承载一个流、流结束归还复用，空闲目标 = max(2, 近 1 分钟池需求峰值 × 0.5)，上限 64，多余的空闲链路关闭；池空时该会话改用 mux 并补充池（不等待建链）；UDP 固定 mux；auto 按规则近 10 分钟 TCP 会话时长中位数（< 10 秒用 pool，无历史用 mux）；会话到达时若预热建链仍在进行则等待它而不另建 | 见“链路模式”表 |

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

### 隧道端到端（同一进程内多个引擎节点，各自证书、链路监听与模拟 Agent）

| 测试 | 覆盖 |
| --- | --- |
| TestThreeHopTunnelForwardsAndMetersEveryHop | 入口 → 中继 → 出口 → 目标转发 8 MiB 逐字节一致；三个节点的计量上行、下行都与实际字节一致（入口为执行点，按计费授权）；20 个会话共用链路 |
| TestOpenChecksPreventBypass | 入口跳过中继直连出口被拒（中继不可绕过）；同 CA 签发的任意节点被拒；冒充中继时逐项检查：规则未激活（no_route）、规则版本、路径代次、跳位、环路、网络类型（unauthorized）；每次拒绝一条审计事件；正确的 OPEN 被接受，目标由出口本地配置决定 |
| TestRelayChecksUpstreamToo | 中继同样检查上游 |
| TestReasonPropagatesToEntry / TestTargetUnreachableReason | 出口无运行授权（no_route）、目标不可达（target_unreachable）：原因经中继回传，入口事件指出下一跳与原因 |
| TestMemberFailoverAndNoRoute | 主成员不可达时切到备成员；下一组无可达成员时拒绝新会话，不跳过下一组 |
| TestNextHopExcludesVisitedNodes | 中继的下一组包含已经过的入口时选择其他成员 |
| TestRemovedMemberClosesSessionsAndRevocationClosesLinks | 中继移除下一组成员时已有会话关闭；入口吊销中继身份后到中继的链路被关闭，新会话不再经过它 |

变异检查：14 项中 13 项被发现（各项开流检查、排除已经过节点、原因回传、入口事件、门控、审计事件、成员移除收紧、吊销关闭链路）。未被发现的一项是“入口规则不接受入站流”：入口规则的允许上游集合为空，上游检查已先拒绝，这一项是冗余防线。

### UDP 隧道

| 测试 | 覆盖 |
| --- | --- |
| TestDgramZeroRTTOrderAndBothDirections | 开流检查前发出的 5 个数据报排队，接受后与之后的数据报按序交付；反方向 ReadDgram；收发计数 |
| TestDgramDrops | 未开流会话、TCP 流上的 DGRAM 丢弃且链路不关闭；超长、发送队列满、入站队列满分别计数；对端终止后发送返回原因 |
| TestDgramBatchShare | 控制帧在前；有 DATA 等待时 DGRAM 不超过一批的一半；没有 DATA 时 DGRAM 可占满整批；队列字节记账 |
| TestDgramUnderTCPLoad | 同一链路上 TCP 流满速传输时，UDP 1,200 字节、约 100 Mbit/s 持续 2 秒：20,828 个全部收到，丢包 0（ENG-048 混合负载） |
| TestUDPThreeHopTunnel | 入口 → 中继 → 出口 → 回显，200 个 1,200 字节数据报全部回显；三个节点上下行整包计量一致 |
| TestUDPTunnelIdleAndReopen | 出口空闲回收后 RST 经中继回传，入口会话结束，下一个数据报重新开流 |
| TestUDPTunnelTargetForbiddenReason | 平台出口拒绝回环目标（SEC-011），target_unreachable 回传到入口事件 |
| TestUDPOpenChecks | 未知节点的 UDP 开流被拒；仅启用 UDP 的规则拒绝 TCP 开流；合法开流可收发数据报 |

变异检查：11 项中 10 项被发现（TCP 流上的 DGRAM、一半字节限制、发送队列上限、切换回调时的顺序、入站队列上限、超长检查、入口上游结束时的空转、中继下游结束、网络类型检查）。未被发现的一项是出口空闲时显式发送 RST：会话关闭本身就会发送 RST，两者效果相同。

实现中修正：入口 UDP 回包循环原先把除“已关闭”以外的错误都当作读超时处理，上游是隧道流时，流被对端 RST 后会在空闲期内空转；改为只有超时才进入空闲判断。

### 链路模式

| 测试 | 覆盖 |
| --- | --- |
| TestMuxWarmupBeforeAnySession | 配置提交后不等会话即建好 2 条 mux 链路；会话不触发建链 |
| TestMuxBalanceGrowAndShrink | 30 条不读取的流分散到 3 条以上链路；全部繁忙时增加、从不超过 4 条；流结束后空闲链路缩回 2 条 |
| TestPoolReuseAndOneStreamPerLink | 每个 pool 流独占一条链路；接受方从 HELLO 识别 pool；流结束后链路归还并被复用；UDP 不用 pool |
| TestPoolExhaustedFallsBackToMuxAndGrows | 池空时 6 个会话都不等待（< 50 ms），超出部分走 mux；需求峰值 6 使空闲目标增至 3；会话结束后多余的空闲链路关闭 |
| TestDurationsMedianWindow / TestUsePool | 中位数、10 分钟窗口、无历史用 mux；各模式与网络类型的选择 |
| TestAutoModeSwitchesShortSessionsToPool | 三节点：无历史时只有 mux；出现短会话后入口为下一跳预热 pool，之后的会话使用 pool 链路 |

变异检查：12 项全部被发现（在途字节均衡、扩容、上限、缩容、预热、pool 归还、UDP 固定 mux、池空需求计数、池目标、池缩减、时长窗口、auto 判定）。

实现中修正：
- 预热建链进行中到达的会话原先会另建一条链路（并发首批会话时到对端有 3 条链路）；改为等待正在进行的建链。
- 池空改用 mux 的会话原先只按“占用 + 1”计入需求，并发需求被低估、池长不起来；改为把这些会话也计入池需求直到结束。
- 流终止时先回调再从链路摘除，回调中看到的流数仍包含本流，pool 链路无法归还；改为先摘除再回调。
- `types.Peer` 补上契约中已有的 `link_mode` 字段（Go 镜像遗漏）。

## 对契约的解释（未改变契约）

| 位置 | 解释 |
| --- | --- |
| OPEN 载荷 TLV 重复或截断 | 与 R10 的取值约束一样回 `OPEN_ERR(unauthorized)`，不作为链路级协议错误，避免一条坏流关闭整条链路上的其他流 |
| `WINDOW` 增量 0 | 契约未禁止，按无操作处理 |
| `link_mode` 缺失 | 缺省为 mux；取值不是 0 / 1 为协议错误 |
| 允许的 TLS 套件 | Go 的 TLS 1.3 套件不可配置，握手完成后检查，ChaCha20 拒绝 |
| ENG-020 ① “版本、路径代次有效” | **严格相等**（2026-10-31 确认保持）：OPEN 的规则版本与路径代次必须等于本节点当前配置。代价是配置提交的瞬间（上下游节点切换有先后）可能拒绝少量新会话 |
| 出口 PROXY protocol 的目的地址 | 隧道中没有入口监听地址，使用出口连接目标时的本地地址 |
| 门控拒绝的原因码 | 无运行授权 → no_route；无份额 → rate_limited；连接数 → conn_limited；计费授权耗尽 → quota_exhausted |

## 下一步

T28 反向建链 → T29 保活与失效检测 → T30 写合并与 TLS 记录对齐 → T40 RST 传播；之后在网络命名空间拓扑中测量 M1 退出指标 NFR-002、NFR-004、NFR-007。

M1 期间尚不支持、在 Prepare 中报告 NOT_IMPLEMENTED 的规则形态：反向建链（T28）。
