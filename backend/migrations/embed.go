// Package migrations 嵌入契约 C3 的 Goose 迁移，供后端启动与 vortexctl 执行。
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
