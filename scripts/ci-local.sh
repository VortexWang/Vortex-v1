#!/usr/bin/env bash
# 本地可运行的 CI 子集（buf 经 npm 获取）。go-licenses、Goose、代码生成需要 Go 模块代理，只在 GitHub Actions 中运行。
set -euo pipefail
cd "$(dirname "$0")/.."
step() { printf '\n== %s\n' "$*"; }
step gofmt;   test -z "$(gofmt -l engine)" && echo ok
step vet;     go vet ./engine/...
step "test (race)"; go test -race -count=1 ./engine/...
step "proto 编译与命名检查"
INC="$(python3 -c 'import grpc_tools,os;print(os.path.join(os.path.dirname(grpc_tools.__file__),"_proto"))')"
python3 -m grpc_tools.protoc -I proto -I "$INC" --descriptor_set_out=/tmp/vortex.pb --include_imports $(cd proto && find vortex -name '*.proto' | sort | sed 's#^#proto/#' | sed 's#^proto/##') >/dev/null
python3 scripts/protolint.py /tmp/vortex.pb
if command -v npx >/dev/null; then npx -y @bufbuild/buf@1.47.2 lint proto && echo "buf lint ok"; fi
step "OpenAPI 生成一致与结构校验"
(cd api && python3 gen_openapi.py | diff -q openapi.yaml - && echo "generated == committed")
python3 -c "import yaml; from openapi_spec_validator import validate; validate(yaml.safe_load(open('api/openapi.yaml'))); print('valid')"
step "额度语义模拟"
python3 test/budget-sim/budget_sim.py && python3 test/budget-sim/budget_sim.py --delta >/dev/null && echo "对照组按预期出现违例"
if [ -n "${PGHOST:-}" ]; then step "数据库"; scripts/db-check.sh; else echo; echo "（未设置 PGHOST，跳过数据库检查）"; fi
