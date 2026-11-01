# M4 进度（后端）

基线：需求 V1.7、开发方案 04、契约 C2（含变更 CC-01～CC-03）、C3。退出指标：NFR-012～015、NFR-019、NFR-021～023、NFR-026；ENGR-006。

## 第一批：骨架、平台 CA、实例与节点身份、HubControl 主通道（基础）

| 任务 | 内容 | 验证 |
| --- | --- | --- |
| BE-T01（部分） | 模块 `backend/`（加入 go.work）：`internal/platform`（JSON 配置、slog JSON 日志且令牌 / 口令 / 私钥等字段脱敏、pgx 连接池、事务助手（序列化失败与死锁重试）、嵌入式 Goose 迁移、UUIDv7）；sqlc 流水线（`backend/queries` → `backend/internal/db`，CI 重新生成比对）；`cmd/backend`（run / version）；`backend/app`（组装，供进程与集成测试共用）；`backend/apptest`（每个测试一个新数据库） | 全部集成测试连真实 PostgreSQL 16 |
| BE-T10 | 平台 CA `internal/pki`（SEC-012）：根 CA 只签中间 CA；中间 CA 私钥为加密文件（scrypt + AES-256-GCM，口令取自环境变量，至少 12 字节）；按 CSR 签发（只收 ECDSA P-256、签名有效；身份只取自 SAN URI，CSR 中的主题与扩展一律忽略；有效期不超过中间 CA）；后端自己的服务端证书（`spiffe://vortex/backend`）由中间 CA 签发、剩余 1/3 时更换；`vortexctl ca init`；证书记录、吊销与吊销列表版本 | 3 个测试：签发与链校验、身份解析拒绝 5 种非法 URI、加密文件（错误口令、篡改、密钥不匹配、他根签发的中间 CA） |
| BE-T08、BE-T10（身份） | `internal/fleet`：实例引导令牌（区域以令牌为准）与节点注册令牌（30 分钟、一次性；平台节点或某租户的自托管节点），只存 SHA-256 哈希；EnrollHub（新建实例与证书，记录对外地址，CC-03）；EnrollNode（节点归属取自令牌、区域取自经由实例、公网地址须为公网单播，响应带同区域实例列表）；RenewNodeCert（节点存在、未删除、未停用，且与经由实例同区域）；RenewHubCert（CC-02）；`vortexctl token hub/node` | 2 个测试（令牌明文不落库、错误 / 他区域 / 重复使用 / 过期令牌、对外地址校验、节点归属与能力、续签的拒绝情形、吊销） |
| BE-T11（基础） | `internal/grpcgw` HubControl：TLS 1.3，客户端证书“提供了就校验”（EnrollHub 允许无证书），其余调用要求本平台签发给该实例、已登记且未吊销的 Edge Hub 证书；Connect：注册时实例状态入库，先下发吊销列表（HUB-010），再下发计费版本与账本确认位置；对账中先下发限制快照、再 STARTED，账本静默 1 秒后 COMPLETED（暂行，BE-T23 替换为结清与重算之后）；节点状态入库（HUB-004）；账本条目按 (hub_id, seq) 幂等入库并累计确认（入账在 BE-T18 起）；断开即标记实例离线 | 2 个测试（真实 gRPC + mTLS：无证书、节点证书冒充实例、他 CA 证书、未登记的本平台证书、吊销后旧证书被拒而续签证书可用；注册、账本幂等与确认、节点状态、对账顺序、离线） |
| 端到端 | `test/e2e TestRealBackendEnrollment`：真实后端（迁移、CA、HubControl）← edge-hub ← vortex-node：后端签发的引导令牌换取实例身份；节点经 Edge Hub 用注册令牌注册，实例列表来自后端；节点上线后后端记录 online 与程序版本；实例证书（有效期 6 秒）经 RenewHubCert 续签，原证书过期后后端重启，Edge Hub 以新证书重新连接 | 1 个测试 |
| CC-03 | 契约变更：`EnrollHubRequest.advertise_address = 5`（节点连接本实例的地址）；`edge-hub enroll -advertise`（缺省取带主机部分的 `-listen`）、安装脚本 `--advertise` | 评审选定：引导请求带对外地址 |

变异检查 18 项中 17 项被发现（证书登记与吊销判定、令牌已用 / 过期、区域、对外地址、公网地址、续签的区域 / 停用 / 删除、吊销列表版本、令牌哈希、令牌租户、确认位置、幂等入库、离线标记、对账完成后的模式、注册时的确认位置）；1 项为纵深防御：实例身份解析中的 “kind 必须为 hub” 与证书登记检查（主体类型须为 hub）重复，节点证书在后者被拒。

## 下一步

规则模型与编译器（BE-T13、BE-T14）及配置下发（BE-T11 剩余：期望配置、两阶段指令经 Outbox 进入 grpcgw），使全栈测试改由真实后端驱动；随后计量入账（BE-T18）与区域额度（BE-T21～T23）。
