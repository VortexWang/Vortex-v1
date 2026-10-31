# M2 进度（引擎完备）

> 更新：2026-10-31（第一批）· 对应开发方案 01（转发引擎）第 5、6 节；退出指标 NFR-003、NFR-006、NFR-011

M2 范围（开发方案 01 第 6 节）：T19、T31～T35、T37～T39。其中 T33～T35（Prepare 分类、Commit / Abort / Drain、State 与 Events）已在 M0 第五批随引擎运行时完成，T19 的引擎侧（无授权同步等待 200 ms、低水位事件）已在 M0 完成。ENG-050 的“闲置 10 分钟缩为 1 MiB 常驻额度、24 小时回收”是 Edge Hub 的签发策略（HUB-T11）：引擎以全集语义接受被替换为更小额度的授权（M0 第四批），并照常上报消耗，不需要额外逻辑。

## 已完成

| 任务 | 内容 | 验证 |
| --- | --- | --- |
| ENG-T31 | 多跳：逐跳从本地配置选下一组成员，已经过节点列表逐跳追加（上限入口加 3 个中继，超过时 no_route），跳位与路径代次逐跳检查（M1 第三批已实现，本批补齐 3 中继与超限场景的端到端验证） | 见下表 |
| ENG-T32 | 执行点判定与自托管入口（ENG-049）：执行点由后端编译进规则（`EnforcementPoint`），引擎在执行点执行运行授权、份额、连接数与计费授权，其他节点只计原始字节；入口为自托管节点时第一个平台节点是执行点 | 见下表 |
| ENG-T37 | 帧解析与链路帧处理模糊测试：`FuzzFrames`（M1 第一批）与新增 `FuzzLinkFrames`——把任意字节作为对端握手后发来的帧序列送入链路，要求不 panic、不死锁、关闭后流与 goroutine 全部释放；伪造开流与伪造 DGRAM 用例见 M1 第三、四批与本批。两项模糊测试以短时长加入 CI | 本地 `FuzzLinkFrames` 连续 3 分钟无失败；植入“发往未知流的帧不做空检查”的缺陷后 60 秒内被找到 |

链路改为通过 `transport` 接口使用连接（生产仍为 `*tls.Conn`），模糊测试可在内存管道上运行，不必每个输入做一次 TLS 握手。

| 测试 | 覆盖 |
| --- | --- |
| TestThreeRelayPathLargeFileIntegrity | 入口 → 3 个中继 → 出口（5 个引擎节点）转发 32 MiB，SHA-256 一致；每一跳上行计量与实际字节一致（NFR-006 大文件校验） |
| TestFourRelaysExceedVisitedLimit | 4 个中继的路径在第 4 个中继处因已经过节点超限而 no_route，原因回传到入口 |
| TestMultiHopOrderCannotBeBypassed | 跳过中继 2 直接连中继 3 在链路层被拒；下游节点反向开流、合法上游但跳位不符、已经过列表含本节点均回 unauthorized；正确的开流经中继 2、3 到达目标（NFR-006 顺序不可绕过） |
| TestEnforcementPointAtFirstPlatformNode | 自托管入口 → 中继（执行点）→ 出口：两跳都计原始字节；计费授权只在执行点消耗，恰好等于上下行字节之和，入口不消耗 |
| TestEnforcementPointWithoutBillingRejectsAndReasonReachesEntry | 执行点没有计费授权：等待后拒绝，quota_exhausted 回传到入口事件 |
| TestEnforcementPointRateLimitsWholePath | 执行点 16 Mbps 份额对整条路径生效：4 MiB 回显（经执行点 8 MiB）用时约 3.2 秒 |

变异检查：已经过节点上限、执行点身份、跳位检查 3 项全部被发现（另有模糊测试的植入缺陷检查，见上）。

## 下一步

ENG-T39 泄漏与长稳回归（goroutine、文件描述符、内存，NFR-008～NFR-011）；在 7 个命名空间的拓扑上测量 NFR-003（三中继多跳 ≥ 400 Mbps 且 ≥ 同路径最小基准的 70%）；ENG-T38 性能调优视测量结果进行。
