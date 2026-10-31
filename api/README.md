# api/ — 契约 C4（OpenAPI，v1 已锁定）

| 文件 | 说明 |
| --- | --- |
| `openapi.yaml` | **契约本身**（OpenAPI 3.0.3；生成脚本从 3.1 写法机械转换，见下） |
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

**为什么是 3.0.3**：后端使用的 oapi-codegen 依赖 kin-openapi，只支持 OpenAPI 3.0。生成脚本先按 3.1 的写法构造，
再机械转换为 3.0.3（`type: [X, null]` → `nullable: true`、数值 `exclusiveMinimum` → 布尔形式、删除空的 `required`），
字段与语义不变。
