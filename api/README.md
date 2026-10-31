# api/ — 契约 C4（OpenAPI，v1 已锁定）

| 文件 | 说明 |
| --- | --- |
| `openapi.yaml` | **契约本身**（OpenAPI 3.1） |
| `gen_openapi.py` | 生成脚本：保证错误响应、CSRF、If-Match、Idempotency-Key、分页在全部接口上一致。改契约时改脚本并重新生成，两者一起提交 |
| `tests/typecheck.ts` | 前端类型契约测试：正常用法必须通过，4 处故意错误必须被拒绝 |

验证命令（CI：QA-T03）：

```bash
python3 gen_openapi.py > openapi.yaml && git diff --exit-code openapi.yaml    # 生成结果与提交一致
npx @redocly/cli@1.34.0 lint openapi.yaml                                   # 0 错误 0 警告
npx openapi-typescript@7.4.4 openapi.yaml --default-non-nullable false -o tests/vortex-api.d.ts
npx -p typescript@5.6.3 tsc --strict --noEmit --target es2022 --moduleResolution bundler --module esnext tests/typecheck.ts
```

`--default-non-nullable false` 为必选项：否则带默认值的请求字段（如目标的 `weight`）会被生成为必填。
