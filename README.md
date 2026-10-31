# VORTEX

流量转发平台 Monorepo。需求基线：《VORTEX 需求规格说明书 V1.7（已冻结）》；开发方案见独立的开发方案文档集。

| 目录 | 内容 | 状态 |
| --- | --- | --- |
| `engine/` | 转发引擎（独立 Go 模块，只用标准库） | M0 进行中：骨架、生命周期、管道完成 |
| `engine/vlink/ref/` | VLink 参考实现与 15 个测试向量（契约 C1） | 已锁定 |
| `proto/` | 控制协议与运行配置（契约 C2） | 已锁定 |
| `api/` | 对外 API（契约 C4，OpenAPI 3.1） | 已锁定 |
| `backend/migrations/` | 数据库迁移与约束测试（契约 C3） | 已锁定 |
| `test/budget-sim/` | 区域额度消息语义模拟 | — |
| `node/`、`edge-hub/`、`backend/`、`frontend/`、`deploy/` | 各组件 | 待 M1～M5 |
| `docs/` | 工程基线、M0 进度、需求变更申请 | — |

本地检查：`scripts/ci-local.sh`（设置 `PG*` 环境变量后同时检查数据库）。完整 CI 见 `.github/workflows/ci.yml`。
