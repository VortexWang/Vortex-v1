# VORTEX 节点端容器镜像（AGT-T17）。构建上下文为仓库根目录：
#   docker build -f deploy/node/Dockerfile --build-arg VERSION=1.0.0 -t vortex-node:1.0.0 .
# 容器部署由镜像升级：镜像内设置 VORTEX_DISABLE_SELF_UPGRADE=1，节点拒绝升级指令并报告失败。用法见 deploy/README.md。
FROM golang:1.24.7-bookworm AS build
ARG VERSION=dev
WORKDIR /src
COPY engine engine
COPY proto/gen/go proto/gen/go
COPY node node
WORKDIR /src/node
RUN CGO_ENABLED=0 GOWORK=off GOTOOLCHAIN=local go build -trimpath -ldflags "-s -w -X vortex/node/app.Version=${VERSION}" \
      -o /out/vortex-node ./cmd/vortex-node

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/vortex-node /usr/local/bin/vortex-node
ENV VORTEX_DISABLE_SELF_UPGRADE=1
# 数据目录：证书、私钥、本地数据库与节点配置（容器内配置与数据放在同一个卷）
VOLUME ["/var/lib/vortex"]
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/vortex-node"]
CMD ["run", "-config", "/var/lib/vortex/node.json"]
