# 部署

Edge Hub 与节点端以二进制 + systemd 部署，另提供容器镜像（需求 3.3 节；AGT-T17、HUB-T17）。后端与前端的 Docker Compose 部署在 M4 提供。

支持 Linux amd64 / arm64，标准验收系统 Ubuntu 24.04 LTS。

## 安装包内容

| 文件 | 来源 |
| --- | --- |
| `edge-hub`、`vortex-node` | `go build`（`-ldflags "-X vortex/edgehub/app.Version=…"` / `-X vortex/node/app.Version=…`） |
| `ca.crt` | 平台 CA（校验对端与签发结果） |
| `release.pub` | 平台发布公钥（Ed25519，PEM `PUBLIC KEY`；节点校验升级包，契约补充 L7） |
| `deploy/edge-hub/`、`deploy/node/` | 安装脚本与 systemd 单元 |

## Edge Hub

```sh
sudo VORTEX_BOOTSTRAP_TOKEN=<引导令牌> deploy/edge-hub/install.sh \
     --binary ./edge-hub --ca ./ca.crt --backend <后端地址:端口> --region <区域> [--listen :7400]
```

脚本创建系统用户 `vortex-hub`，安装 `/opt/vortex/edge-hub/edge-hub`、`/etc/vortex/ca.crt` 与 `edge-hub.service`，用引导令牌向后端换取 `hub_id` 与实例证书（私钥在本机生成，0600），写入 `/etc/vortex/edge-hub.json`，然后启动服务。数据（SQLite 账本、证书）在 `/var/lib/vortex-edge-hub`（0700）。

## 节点

```sh
sudo VORTEX_ENROLL_TOKEN=<注册令牌> deploy/node/install.sh \
     --binary ./vortex-node --ca ./ca.crt --release-key ./release.pub \
     --hub <edge-hub 地址:端口> [--link-listen 0.0.0.0:7443]
```

脚本创建系统用户 `vortex`，安装 `/opt/vortex/node/vortex-node`、CA、发布公钥与 `vortex-node.service`，经 Edge Hub 用注册令牌换取节点证书，写入 `/etc/vortex/node.json`，然后启动服务。

- `--link-listen`：节点间链路（VLink）的监听地址。中继、出口以及反向建链段的上游需要；只做入口、或只能反向建链的节点可不设。
- 运维：`vortex-node status`、`vortex-node drain [-timeout 300s] [-rules id,…]`（本地 socket `/run/vortex/node.sock`）。
- 升级（AGT-008）：由后端下发，节点自行下载、校验、排空、替换 `/opt/vortex/node/vortex-node`（上一版本保留为 `.prev`），以退出码 75 退出，由 systemd 重启；新版本 60 秒内不健康或连续 3 次启动失败则回退。单元中的 `ExecStartPre` 由上一版本执行 `upgrade-guard`，保证新版本即使在初始化阶段就崩溃也能回退。

### 单元要点

- `Restart=always`、`StartLimitIntervalSec=0`：升级、回退与异常退出都由 systemd 重启。
- 权限：服务用户只保留 `CAP_NET_BIND_SERVICE`（入口可监听低端口）；文件系统只读，只有 `/var/lib/vortex` 与程序目录 `/opt/vortex/node`（升级时替换程序）可写；`systemd-analyze security` 评估为 OK（`scripts/deploy-check.sh` 在 CI 中检查）。
- 重复执行安装脚本只更新程序、CA、公钥与单元，不重新注册。

## 容器镜像

```sh
docker build -f deploy/node/Dockerfile     --build-arg VERSION=1.0.0 -t vortex-node:1.0.0 .
docker build -f deploy/edge-hub/Dockerfile --build-arg VERSION=1.0.0 -t vortex-edge-hub:1.0.0 .
```

镜像基于 distroless（静态、非 root）。容器中配置与数据放在同一个卷里。

```sh
# Edge Hub
docker run --rm -v hub:/var/lib/vortex-edge-hub -v $PWD/ca.crt:/ca.crt:ro vortex-edge-hub:1.0.0 \
  enroll -token <引导令牌> -backend <后端> -region <区域> -ca /ca.crt \
  -data-dir /var/lib/vortex-edge-hub -config /var/lib/vortex-edge-hub/edge-hub.json
docker run -d --restart=always --name edge-hub -p 7400:7400 -v hub:/var/lib/vortex-edge-hub vortex-edge-hub:1.0.0

# 节点（转发端口由配置动态决定，使用主机网络）
docker run --rm --network host -v node:/var/lib/vortex -v $PWD/ca.crt:/ca.crt:ro vortex-node:1.0.0 \
  enroll -token <注册令牌> -hub <edge-hub> -ca /ca.crt -data-dir /var/lib/vortex \
  -config /var/lib/vortex/node.json -socket /var/lib/vortex/node.sock [-link-listen 0.0.0.0:7443]
docker run -d --restart=always --name vortex-node --network host -v node:/var/lib/vortex vortex-node:1.0.0
```

- 容器部署由镜像升级：镜像设置 `VORTEX_DISABLE_SELF_UPGRADE=1`，节点拒绝后端的升级指令并报告失败（“update the image”）。
- 入口需要监听 1024 以下端口时，以 `--user 0 --cap-drop ALL --cap-add NET_BIND_SERVICE` 运行节点容器。
