# node

`vortex-node`：Agent + 嵌入的 Engine（开发方案 02）。当前进度见 `docs/m3-progress.md`。

| 目录 | 内容 |
| --- | --- |
| `agent/store` | 本地持久化（AGT-003）。Go 模块代理放行前为只追加 JSON 行文件，之后换 SQLite（WAL） |
| `agent/boot` | 启动标识：每次启动新 boot_id，记住上一次（AGT-009） |
| `agent/report` | 计量落盘、封批、确认、重发、7 天保留；磁盘水位保护（AGT-006、MTR-006） |
| `agent/grants` | 授权与份额：续期、计费授权申请、只收当前实例签发的授权、切换实例后报告持有授权（AGT-005、HUB-014、ENG-050） |
| `agent/reconcile` | 配置协调：Prepare / Commit / Abort 驱动、最后生效配置持久化、启动恢复与“授权待确认”（AGT-004、AGT-010） |
| `agent/control` | 控制连接：实例列表、退避与轮换、自治时探测并切换到在线实例（AGT-002、HUB-014） |
| `agent`（根包） | 组装：事件循环、控制流消息、Edge Hub 消息处理、status（AGT-T19） |
| `agent/probe` | 探测：成员 VLink 端口与 TCP 目标的握手，3 次失败 / 2 次成功，结果供引擎选路并上报（AGT-007） |
| `agent/localapi` | 本地 Unix socket 运维接口（status / drain，AGT-T16） |
| `internal/supervisor` | 模块 goroutine 的异常重启与退避 |
| `cmd/vortex-node` | 可执行文件：run / status / drain / version（enroll 待 gRPC） |
| `internal/clock` | 可注入时钟 |
