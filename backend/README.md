# backend

后端（M4，开发方案 04）。进度见 `docs/m4-progress.md`。

- `migrations/`：契约 C3 的 Goose 迁移（嵌入进程）
- `queries/` → `internal/db/`：sqlc（`sqlc generate`，v1.27.0）
- `internal/platform`：配置、日志、数据库、事务、UUIDv7
- `internal/pki`：平台 CA
- `internal/fleet`：实例与节点身份（令牌、引导、注册、续签、吊销）
- `internal/grpcgw`：面向 Edge Hub 的 HubControl 服务
- `app/`：组装（`cmd/backend` 与集成测试共用）；`apptest/`：集成测试设施
- `cmd/backend`、`cmd/vortexctl`（`ca init`、`token hub`、`token node`）

集成测试需要 PostgreSQL：设置 `VORTEX_TEST_DATABASE_URL`（有建库权限），每个测试新建并删除一个数据库；未设置时跳过。
