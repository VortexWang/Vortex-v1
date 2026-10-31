# 工程版本基线（QA-T01，ENGR-001）

> 需求基线：《VORTEX 需求规格说明书 V1.7（已冻结）》 · 建立日期：2026-10-31
> 规则：全部依赖与镜像锁定版本，禁止浮动 `latest`；修改本文件须走评审。

## 1. 语言与运行时

| 组件 | 版本 | 状态 | 说明 |
| --- | --- | --- | --- |
| Go | 1.24.7 | 已在开发环境验证 | `go.work` 与各模块 `go.mod` 声明 `go 1.24`；CI 使用 1.24.7 |
| Node.js | 22.23.4 | 已在开发环境验证 | 前端构建与契约工具 |
| npm | 10.9.9 | 已在开发环境验证 | 前端使用 `pnpm-lock.yaml` 锁定依赖（FE-T01 建立） |
| PostgreSQL | 16.15 | 已用于迁移与约束测试 | 生产部署 16.x 最新小版本 |
| Valkey | 8.x | 选定，未验证 | 只做缓存、限流、短期通知（3.3 节） |

## 2. 契约与代码生成工具

| 工具 | 版本 | 状态 |
| --- | --- | --- |
| protoc | 35.1（grpcio-tools 1.85.0 内置） | 已验证：编译全部 proto |
| buf | 1.47.2 | 已验证：CI 与本地（经 npm 包 `@bufbuild/buf`）lint、build 通过；排除的 4 条规则见 `proto/buf.yaml` |
| protoc-gen-go / protoc-gen-go-grpc | v1.36.6 / v1.5.1 | 已验证：CI 生成全部 proto 的 Go 代码并编译通过 |
| Goose | v3.24.1 | 已验证：CI 执行 up / status / reset / up，约束测试通过 |
| oapi-codegen | v2.4.1 | 已验证：CI 生成 Gin 服务端接口并编译通过（要求 OpenAPI 3.0，见 `api/README.md`） |
| @redocly/cli | 1.34.0 | 已验证：0 错误 0 警告 |
| openapi-typescript | 7.4.4（必须加 `--default-non-nullable false`） | 已验证 |
| TypeScript | 5.6.3 | 已验证：`--strict` 编译类型契约测试 |

全部工具已在 CI 中验证（2026-10-31，运行 41170780929）。

## 3. 容器镜像

镜像一律以摘要（`@sha256:…`）引用。摘要在仓库首次构建时从镜像仓库获取并写入 `deploy/images.lock`，开发环境无法访问镜像仓库，因此本文件暂不列出摘要。

| 用途 | 镜像 | 标签 |
| --- | --- | --- |
| Go 构建 | `golang` | `1.24.7-bookworm` |
| 运行时基础镜像 | `gcr.io/distroless/static-debian12` | `nonroot` |
| PostgreSQL（Compose） | `postgres` | `16` 最新小版本 |
| Valkey（Compose） | `valkey/valkey` | `8` 最新小版本 |

## 4. 平台

- 服务端与节点：Linux amd64、arm64；标准验收系统 Ubuntu 24.04 LTS（3.4 节）。
- 引擎依赖：只用标准库。splice 使用 `syscall.Splice`；批量 UDP 收发（ENG-043）如需 `recvmmsg`，以 `syscall.Syscall6` 直接调用，不引入 `golang.org/x/sys`，避免第三方依赖。

## 5. 许可证（ENGR-002）

白名单：MIT、BSD-2-Clause、BSD-3-Clause、Apache-2.0、ISC。CI 中 Go 依赖用 `go-licenses`，前端依赖用 `license-checker` 检查，出现 GPL / AGPL 直接失败。当前引擎模块无第三方依赖。

## 6. 开发环境记录

本基线建立时的开发机：2 vCPU Intel Xeon 2.10 GHz（支持 AES-NI），Linux 6.18。**这不是 13.2 节规定的性能验收环境**，在此测得的吞吐只用于发现回归与粗略对比。
