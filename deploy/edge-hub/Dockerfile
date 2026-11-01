# VORTEX Edge Hub 容器镜像（HUB-T17）。构建上下文为仓库根目录：
#   docker build -f deploy/edge-hub/Dockerfile --build-arg VERSION=1.0.0 -t vortex-edge-hub:1.0.0 .
FROM golang:1.24.7-bookworm AS build
ARG VERSION=dev
WORKDIR /src
COPY proto/gen/go proto/gen/go
COPY edge-hub edge-hub
WORKDIR /src/edge-hub
RUN CGO_ENABLED=0 GOWORK=off GOTOOLCHAIN=local go build -trimpath -ldflags "-s -w -X vortex/edgehub/app.Version=${VERSION}" \
      -o /out/edge-hub ./cmd/edge-hub

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/edge-hub /usr/local/bin/edge-hub
# 账本（SQLite）、实例证书、私钥与实例配置
VOLUME ["/var/lib/vortex-edge-hub"]
EXPOSE 7400
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/edge-hub"]
CMD ["run", "-config", "/var/lib/vortex-edge-hub/edge-hub.json"]
