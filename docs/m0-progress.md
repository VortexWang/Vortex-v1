# M0 进度（引擎数据面）

> 更新：2026-10-31 · 对应开发方案 01（转发引擎）与 06（测试与交付）

## 已完成

| 任务 | 内容 | 验证 |
| --- | --- | --- |
| QA-T01 | 工程版本基线 `docs/engineering-baseline.md` | 已验证的版本与待 CI 回填的版本分开标注 |
| QA-T02（部分） | CI 工作流 `.github/workflows/ci.yml`；本地子集 `scripts/ci-local.sh`、`scripts/db-check.sh` | 本地全部通过；buf、go-licenses、Goose、oapi-codegen 需在 GitHub Actions 中首次运行 |
| ENG-T01 | 引擎模块骨架、`Engine` 接口、内部类型 `engine/types`、生命周期框架 `engine/internal/lifecycle` | 竞态检测下连续 20 次通过；发现并修复 1 个泄漏缺陷（见下） |
| ENG-T03 | 缓冲路径双向拷贝、半关闭、空闲超时 | 见下表 |
| ENG-T04 | splice 零拷贝路径与拷贝点钩子 | 见下表 |
| ENG-T05 | 分级缓冲池（16 / 64 / 256 KiB 自适应） | 稳态 0 次分配 |

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

## 实现中发现并修正的问题

| 问题 | 发现方式 | 修正 |
| --- | --- | --- |
| 子 Scope 先行关闭后从父 Scope 摘除，父 Scope 的 Wait 不再等待其仍在退出的 goroutine，`Close` 提前返回 | 泄漏断言失败 | goroutine 同时计入本 Scope 与全部祖先，Wait 任一 Scope 即覆盖整棵子树 |
| `abortErr` 被两个方向并发读写 | 代码审查 | 改为原子访问 |
| 对端停止读取时写入可能永久阻塞（另一方向已结束时无人检测空闲） | 代码审查 | 写入同样受空闲时长约束 |
| splice 回调在循环内重复创建，每 MiB 8 次分配 | 基准 | 回调每方向只创建一次，降为 0 |
| 需求 8.2 节 `DrainUsage` 签名与契约 C2a 不一致 | 编写接口时 | 按契约实现，提交需求变更 CR-01 |

## 下一步（M0 剩余）

ENG-T02 模拟 Agent → ENG-T06～T08 监听与规则表、选路 → ENG-T09 直接转发 TCP 端到端 → ENG-T10 UDP → ENG-T12 目标地址检查与 ENG-T41 域名解析 → ENG-T13～T18 计量与门控 → ENG-T36 网络命名空间基准框架。M0 退出标准：NFR-001、NFR-005。
