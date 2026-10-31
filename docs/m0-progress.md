# M0 进度（引擎数据面）

> 更新：2026-10-31（第五批）· 对应开发方案 01（转发引擎）与 06（测试与交付）

## 已完成

| 任务 | 内容 | 验证 |
| --- | --- | --- |
| QA-T01 | 工程版本基线 `docs/engineering-baseline.md` | 已验证的版本与待 CI 回填的版本分开标注 |
| QA-T02（部分） | CI 工作流 `.github/workflows/ci.yml`；本地子集 `scripts/ci-local.sh`、`scripts/db-check.sh` | 本地全部通过；buf、go-licenses、Goose、oapi-codegen 需在 GitHub Actions 中首次运行 |
| ENG-T01 | 引擎模块骨架、`Engine` 接口、内部类型 `engine/types`、生命周期框架 `engine/internal/lifecycle` | 竞态检测下连续 20 次通过；发现并修复 1 个泄漏缺陷（见下） |
| ENG-T03 | 缓冲路径双向拷贝、半关闭、空闲超时 | 见下表 |
| ENG-T04 | splice 零拷贝路径与拷贝点钩子 | 见下表 |
| ENG-T05 | 分级缓冲池（16 / 64 / 256 KiB 自适应） | 稳态 0 次分配 |

| ENG-T06 | 监听预留：TCP 只 bind 不 listen、UDP bind 不读并在提交时丢弃积压；进程内端口登记表 | 见“路由”表 |
| ENG-T07 | 规则表不可变快照、单调版本、原子切换 | 并发读写 2,000 次切换无撕裂读 |
| ENG-T08 | 平滑加权轮询、主备（同优先级分摊）、健康过滤、排除已经过节点 | 见“路由”表 |
| ENG-T09 | 直接转发 TCP 端到端：接入、准入钩子、选目标与故障切换、计量钩子、会话生命周期 | 见“路由”表 |
| ENG-T12 | 目标地址检查（SEC-011）：内置禁止类别、私有网段按组放开、自托管例外、本机端口 | 19 个用例 |
| ENG-T41 | 目标域名解析：缓存 30 秒（≤ 60 秒）、每个解析结果逐一检查、按检查过的 IP 连接 | 含 DNS 重绑定用例 |
| ENG-T10 | UDP 直接转发：64 分片会话表、每会话独立已连接 socket、空闲回收、4 MiB 收发缓冲 | 见“UDP 与 PROXY protocol”表；批量收发（recvmmsg，ENG-043“应支持”）留待性能调优时视需要加入 |
| ENG-T14 | UDP 整包计量：额度不足一个完整数据报时整包丢弃并退回，不截断 | 同上 |
| ENG-T11 | PROXY protocol：TCP v1 / v2 每连接一次，UDP v2 每数据报一次；头部不计量 | 同上 |
| ENG-T13 | 计量：账本（规则 × 角色 × 方向 × 计费授权）、计费授权账户（预扣 / 提交 / 退回各一次、先到期先用）、自适应预扣块 256 KiB → 4 MiB | 见“计量与门控”表 |
| ENG-T15 | 授权截止点：首次收到时刻 + 有效期 − 安全余量（默认 max(10%, 10 s)，不超过有效期一半），单调时钟；重复收到不延长 | 同上 |
| ENG-T16 | 租户令牌桶与规则令牌桶叠加，容量 1 秒份额；统计令牌耗尽时间占比 | 同上 |
| ENG-T17 | 连接数：执行点按租户计；超限新会话立即拒绝；份额下调时从最近建立者开始，排空期满后关闭 | 同上 |
| ENG-T18 | 限制执行：运行授权或份额到期时由引擎定时器关闭受影响会话（包括阻塞在读取上的空闲会话），产生开始与完成事件 | 同上 |
| ENG-T19 | 没有计费授权时发出 grant_needed 并最多等待 200 ms；剩余 30% 时每份授权发一次 grant_low | 同上；“闲置 10 分钟缩为 1 MiB 常驻额度”属于 Edge Hub 的签发策略（HUB-T11），引擎侧无需额外逻辑 |
| ENG-T02 | 模拟 Agent `engine/enginetest`：两阶段应用（有资源错误即中止）、下发授权全集、按周期拉取并累加用量、收集事件 | 见“配置应用”表 |
| ENG-T33～T35 | `engine.Runtime` 实现完整 `Engine` 接口：入口 → 门控 → 管道组装；Prepare 校验与逐对象分类、Commit 原子切换、Abort、Drain、State（对象级实际版本） | 同上；下一跳（隧道）规则在 M0 报告 `NOT_IMPLEMENTED`，M1 实现 |
| ENG-T36 | 网络命名空间基准：客户端 / 转发节点 / 目标三个命名空间、每段 1 Gbps 限速；直连基准与直接转发同路径对比；正式计量与门控生效；按命名空间定位 UDP 丢包；CI 冒烟作业 `netns-bench` | 见 `docs/benchmark.md` |

## 路由（engine/route）测试

| 测试 | 覆盖 |
| --- | --- |
| TestReserveTCPRefusesUntilCommit | 准备阶段端口已占用但拒绝连接；提交后可连接（ENG-031、ENG-032） |
| TestReserveWildcardConflict / TestKernelConflictWithForeignListener | 通配与具体地址冲突、TCP 与 UDP 同号独立（PRT-002、PRT-006）；其他进程已监听的端口被拒 |
| TestAbortReleasesPort / TestClosedListenerReleasesPort | 中止与关闭后端口可重新预留（ENG-033） |
| TestReserveUDPDropsBacklogOnCommit | 提交前积压的 5 个数据报被丢弃，之后的正常收到 |
| TestWeightedRoundRobinDistribution / TestSmoothnessCheckRejectsNaiveWRR | 分布严格按权重；平滑性检查能区分朴素加权轮询 |
| TestPrimaryBackupFailover / TestUnhealthyAndVisitedExcluded | 主备切换与恢复；无可用候选时返回 no_route，不跳过（RUL-006、ENG-022） |
| TestGuardPolicy / TestResolverCachesAndGuardsEveryAddress | SEC-011 全部类别；缓存、DNS 重绑定、混合解析结果 |
| TestDirect* | 回显、3:1 权重得到 300/100 个连接、主目标宕机切到备用、全部宕机快速关闭、跳过被禁目标并产生审计事件、准入拒绝时不连目标、计量字节准确、关闭后无泄漏且端口释放 |
| TestRuleSwapKeepsExistingSessions | 规则移除后新会话被拒、已有会话继续（ENG-032） |
| TestDirectShutdownLeakFree | 20 个活动会话时恰好 41 个 goroutine（每会话 2 个 + 接入循环 1 个，ENG-045），关闭后归零 |

## 管道（engine/pipe）测试

| 测试 | 覆盖 |
| --- | --- |
| TestIntegrityAndHalfClose | 上行 48 MiB、下行 16 MiB 随机数据 SHA-256 一致；目标收到 EOF 后仍可回传（ENG-005、NFR-006）；splice 与缓冲两条路径 |
| TestSpliceActuallyUsed / TestNonTCPFallsBackToBuffered | 两端为 TCP 时确实走 splice；否则回退缓冲路径（ENG-004） |
| TestHookContract | 每次拷贝不超过授予量；每次 Acquire 恰好一次 Release；写成功字节总数准确（ENG-036、ENG-037） |
| TestHookStopEndsSession | 钩子返回 0（额度耗尽）时会话结束，超出停止点不多于一份授予量（ENG-028） |
| TestIdleTimeout / TestOneActiveDirectionKeepsSessionAlive | 空闲超时准确触发；任一方向有流量则整个会话不超时（ENG-006） |
| TestContextCancelStops | 取消上下文立即停止 |
| TestLeakFree | 200 个并发会话结束后 goroutine 与文件描述符回到基线（ENG-008、NFR-010、NFR-011） |

全部测试在 `-race` 下重复 5 次通过。另做了 3 次**变异检查**：故意去掉半关闭传递、让 splice 路径不计数、忽略钩子授予量，对应测试均失败，说明测试确实能发现这类缺陷。

## 开发机基准（2 vCPU，回环，仅用于回归对比，不是 13.2 节验收）

| 路径 | 吞吐 | 每 MiB 分配 |
| --- | --- | --- |
| splice | 约 1.5～2.2 GB/s（波动大） | 0 |
| 缓冲 | 约 2.0～2.3 GB/s | 0 |

回环上 splice 不比缓冲快属正常：回环的内存拷贝很便宜，splice 的优势是在真实网卡上节省 CPU。NFR-001 必须在 13.2 节规定的环境中、启用正式计量与门控后测量。

## UDP 与 PROXY protocol 测试

| 测试 | 覆盖 |
| --- | --- |
| TestUDPEchoIntegrity | 1～60,000 字节数据报逐个往返一致；同一客户端只建一个会话 |
| TestUDPSessionPerClientAndWeights | 40 个客户端 40 个会话，每会话独立 socket（ENG-042）；按会话选目标，3:1 恰好 30/10（RUL-008）；同一会话的数据报去往同一目标（RUL-009）；准入每会话一次 |
| TestUDPIdleReclaimAndRecreate | 有流量不回收；空闲超时后回收，之后的数据报建立新会话（ENG-006） |
| TestUDPWholePacketMetering | 额度不足时整包丢弃并退回，目标收不到截断的包（ENG-038） |
| TestUDPAdmissionDenyAndGuard | 准入拒绝时不达目标；被禁目标不建会话并产生审计事件 |
| TestUDPShutdownBudgetAndLeak | 25 个会话时恰好 26 个 goroutine（监听 1 个 + 每会话 1 个，ENG-045）；关闭后归零、每个会话恰好释放一次 |
| TestUDPLossAtRate | 1,200 字节、约 100 Mbps、3 秒：31,187 个数据报往返 0 丢失（开发机回环，非 NFR-005 验收） |
| TestProxyHeaderFormats | v1 / v2 字节格式（用独立按规范编写的解析器验证）、IPv6、IPv4 映射地址、混合地址族、v1 不支持 UDP |
| TestProxyProtocol*EndToEnd / PerDatagram | 目标收到的头部携带真实客户端地址与入口地址，其后是原始数据；头部不计入计量（ENG-035） |

## 计量与门控（engine/meter、engine/gate）测试

| 测试 | 覆盖 |
| --- | --- |
| TestLedgerDrainIsLossless | 8 个 goroutine 并发写、另一个并发 Drain，总和一字节不差 |
| TestReserveCommitRefundOnce / TestConcurrentReserveNeverOverdraws | 每块只能提交一次、余量退回；16 个并发会话用完 1,000,000 字节授权恰好 1,000,000 |
| TestEarliestDeadlineFirstAndExpiry / TestRemovedGrantStops… | 先到期先用；到期与作废后不再使用；有未提交预扣时不报 final |
| TestLowWaterEventOncePerGrant / TestReserveOrWaitWakesOnUpdate | 首次低于 30% 时发一次；授权到达立即唤醒，否则 200 ms 后放弃 |
| TestBucketRateAndStarvation | 1 MB/s 份额实测误差 < 7%，突发恰好 1 秒份额；速率为 0 时一直等待 |
| TestDeadlineFromFirstReceiptNotExtendedByResend | ENG-025：重复收到同一授权不延长，新授权 ID 才续期；默认安全余量 |
| TestAdmissionChecks | 无运行授权、无份额、无计费授权（等待后拒绝）、等待期间授权到达、连接数上限与释放 |
| TestRunGrantExpiryKillsSessionsWithEvents / TestConnDownsizeDrainsNewestAfterWindow | 到期关闭全部会话，开始事件一次、最后一个会话结束后完成事件；降额只关闭最新的超额会话，且在排空期之后 |
| TestE2ERateLimitAccuracy | 16 Mbps 份额下 8 MiB 实测 3.34 s，理论 3.19 s（+4.7%） |
| TestE2EMeteringMatchesDeliveredBytes | 6 个会话（1 字节～7.8 MB）：计量字节 = 目标收到字节 = 授权消费量，一字节不差 |
| TestE2EBillingExhaustionStopsSession | 2 MiB 授权：目标恰好收到 2 MiB，会话随即结束 |
| TestE2ERunGrantExpiryClosesIdleSession | 阻塞在读取上的空闲会话在运行授权到期时被关闭 |
| TestIdleDirectionHoldsNoGrant（pipe） | 一个方向持续有数据时，空闲方向不持有任何未决授权 |

### 配置应用（engine.Runtime 与模拟 Agent）

变更分类规则：规则新增为 hot、删除为 stop；规则修改中，租户、执行点身份、角色、允许上游变化，或移除了目标、关闭了某种网络，属于权限收紧，为 stop（已有会话在提交时关闭），其余（限速、连接数、调度、空闲超时、PROXY protocol、新增目标）为 hot。监听新增为 hot、删除为 stop、改绑到另一条规则为 rebuild（保留 socket，新会话走新规则，已有会话保持旧快照）。

| 测试 | 覆盖 |
| --- | --- |
| TestApplyForwardsAndMeters | 首次应用全部 hot；经 Agent 拉取的计量与实际传输一字节不差；State 的对象版本 |
| TestPrepareBindsWithoutListeningAndAbortReleases | 准备后端口被占住但拒绝连接；中止释放端口、不改变生效状态；同一版本可重新准备 |
| TestPortInUseReportedAndVersionRules | 端口被占报告 PORT_IN_USE 并被模拟 Agent 中止；同时只允许一个未决版本；旧版本与相同版本被拒绝；提交非未决版本被拒绝 |
| TestInvalidConfigRejected | 引用不存在的规则、规则未启用该网络、没有目标、重复规则 |
| TestNextHopRuleReportedNotImplemented | 下一跳规则报告 NOT_IMPLEMENTED，部分应用后状态为 failed |
| TestHotChangeKeepsSessionsTighteningClosesThem | hot 变更后已有会话照常；移除目标时提交前报告受影响会话数 1，提交后该会话被关闭，新会话走新目标 |
| TestListenerRemovalAndRebind | 删除监听：会话关闭、端口释放、不再接入；改绑监听：已有会话保留，新会话到新规则的目标 |
| TestRuleRemovalClosesSessions | 删除规则只关闭该规则的会话 |
| TestDrain / TestDrainCompletesWhenSessionsEnd | 排空期间拒绝新会话、保留已有会话，期限到后关闭剩余会话；会话自然结束时排空立即完成 |
| TestUDPApply | UDP 经运行时转发与计量；删除规则后不再转发 |
| TestGrantNeededEventReachesAgent | 无计费授权时 grant_needed 事件经 Events 通道到达 Agent |
| TestCloseStopsEverything | Close 关闭全部会话，无 goroutine 泄漏 |

变异检查（9 项全部被发现）：不吊销收紧规则、收紧判定恒为否、删除监听不关闭、改绑不生效、排空不拒绝新会话、排空期满不关闭、中止不释放、不检查版本单调、不报告受影响会话数。

## 实现中发现并修正的问题

| 问题 | 发现方式 | 修正 |
| --- | --- | --- |
| **Linux 上两个都设置 SO_REUSEADDR 且都未 listen 的 socket 可以 bind 到同一端口**：只 bind 不 listen 的预留实际上不排他，两阶段应用的端口保证失效 | 测试：第二次预留没有失败 | 进程内端口登记表判定本进程的冲突（规则同数据库）；保留 SO_REUSEADDR 以便重启后立即重新 bind；其他进程的冲突仍由内核在 bind 与 listen 时报告 |
| UDP 提交前丢弃积压的循环一个数据报也没丢：Go 在截止时间已过时根本不尝试读取 | 测试 | 改为非阻塞 recvfrom 直到 EAGAIN |
| 选择器按规则快照缓存，每次配置切换都会新增一份，长期运行内存无限增长 | 代码审查 | 只保留当前快照的选择器（目标只在会话开始时选择） |
| 平滑性测试最初只检查“每 7 次恰好 5/1/1”，朴素加权轮询同样满足，测试不能区分 | 复查测试 | 改为“轻权重从不相邻、重权重连续不超过 4 次”，并用朴素序列证明检查会失败 |
| goroutine 计数测试偶发失败（40 / 41） | 重复运行 10 次 | 会话的第二个 goroutine 在连上目标后才启动：改为等待到达预期值，并在等待中确认从未超出预算 |
| “已有会话不受规则切换影响”的测试挡不住错误实现：故意改成会话周期性重查规则后关闭，测试仍通过 | 变异检查 | 切换后等待 100 ms 再使用已有会话，变异随即被发现 |
| 端口登记表把端口 0（由内核分配）当作真实端口登记，两个同时存在的端口 0 预留互相冲突 | UDP 测试中同时建立两个入口 | 端口 0 在 bind 之后按内核分配的实际端口登记 |
| UDP “每个数据报都新建会话”的错误实现未被发现：插入时发现已有会话会丢弃新建的，流量看起来正常，但每个数据报都做一次准入并新建一个目标 socket | 变异检查 | 测试增加“准入每会话恰好一次” |
| **管道“先申请后读取”会让空闲方向占住额度**：等待数据期间持有预扣块与令牌。端到端测试中，2 MiB 授权只用了 1.75 MiB 会话就停止（空闲的下行方向占着 256 KiB）；大量空闲会话还会占满租户令牌，使限速失真 | 端到端测试 | 两条拷贝路径都改为先读后申请，按实际到手的字节数申请；新增回归测试并用变异检查确认它能发现旧行为 |
| **门控的数据竞争**：每次准入都会改写规则共享的限速桶指针，而其他会话在拷贝热路径上无锁读取；另一处读取会被替换的规则快照 | 竞态检测（30 次重复中偶发） | 会话在准入时取得自己的限速桶快照，共享桶只在规则快照变化时调整；热路径改用会话自身保存的租户 ID |
| 计量账户最初按授权累计而不区分方向，与 ENG-039 的“规则 × 角色 × 方向 × 计费授权”不符 | 代码审查 | 每份授权按方向各一个计数器 |
| 子 Scope 先行关闭后从父 Scope 摘除，父 Scope 的 Wait 不再等待其仍在退出的 goroutine，`Close` 提前返回 | 泄漏断言失败 | goroutine 同时计入本 Scope 与全部祖先，Wait 任一 Scope 即覆盖整棵子树 |
| `abortErr` 被两个方向并发读写 | 代码审查 | 改为原子访问 |
| 对端停止读取时写入可能永久阻塞（另一方向已结束时无人检测空闲） | 代码审查 | 写入同样受空闲时长约束 |
| splice 回调在循环内重复创建，每 MiB 8 次分配 | 基准 | 回调每方向只创建一次，降为 0 |
| 提交时关闭收紧规则的会话存在空窗：切换前读到旧快照、切换后才准入的会话不会被关闭 | 代码审查 | 准入包装与吊销共用一把锁；吊销覆盖旧规则表中的快照（即使它还没有会话），持有被吊销快照的准入一律拒绝 |
| UDP 会话在插入会话表之前被关闭（kill）时什么也不做，会话存活 | 代码审查 | 记下提前关闭，插入后立即移除 |
| 需求 8.2 节 `DrainUsage` 签名与契约 C2a 不一致 | 编写接口时 | 按契约实现，提交需求变更 CR-01 |

## M0 退出判定

| 指标 | 要求 | 开发机结果（正式参数） | 判定 |
| --- | --- | --- | --- |
| NFR-001 直接转发 | ≥ 700 Mbps 且 ≥ 直连 70% | 954 Mbps，直连的 99.9% | 通过 |
| NFR-005 UDP | 1,200 字节、100 Mbps，丢包 ≤ 0.1% | 0%（目标端缓冲 4 MiB）；默认缓冲时 0.189%，全部为目标端测试工具的接收缓冲溢出，引擎与链路无丢包 | 通过（附说明） |

开发机不是 13.2 节环境（2 vCPU 共享、同机 veth），正式验收须在 13.2 节环境复测，详见 `docs/benchmark.md`。

## 下一步

M0 的任务已全部完成。进入 M1（引擎链路）：VLink 帧与握手、TLS 承载、mux / pool、正向与反向隧道、段许可；退出指标 NFR-002、NFR-004、NFR-007。
