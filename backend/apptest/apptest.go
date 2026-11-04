// Package apptest 提供后端集成测试的公共设施：每个测试一个独立的 PostgreSQL 数据库（迁移到最新）、平台 CA 文件。
// 数据库服务器取自环境变量 VORTEX_TEST_DATABASE_URL（须有建库权限）；未设置时跳过测试。
package apptest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"vortex/backend/internal/pki"
	"vortex/backend/internal/platform"
)

// EnvURL 是测试数据库服务器的连接串环境变量。
const EnvURL = "VORTEX_TEST_DATABASE_URL"

// Database 创建一个新数据库并迁移，返回连接串；测试结束时删除。
func Database(t testing.TB) string {
	t.Helper()
	base := os.Getenv(EnvURL)
	if base == "" {
		t.Skip(EnvURL + " not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatalf("test database server: %v", err)
	}
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	name := "vxt_" + hex.EncodeToString(b)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		admin.Close(ctx)
		t.Fatal(err)
	}
	admin.Close(ctx)
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	dsn := u.String()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if c, err := pgx.Connect(ctx, base); err == nil {
			_, _ = c.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
			c.Close(ctx)
		}
	})
	if err := platform.Migrate(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	return dsn
}

// Passphrase 是测试 CA 的中间 CA 私钥口令。
const Passphrase = "test passphrase for the CA"

// CA 在 dir 下生成平台 CA 文件，设置口令环境变量，返回配置与 CA。
func CA(t testing.TB, dir string) (platform.CAConfig, *pki.CA) {
	t.Helper()
	g, err := pki.Generate("VORTEX test")
	if err != nil {
		t.Fatal(err)
	}
	enc, err := pki.EncryptKey(g.CA.IntermediateKey(), []byte(Passphrase))
	if err != nil {
		t.Fatal(err)
	}
	c := platform.CAConfig{RootCertFile: filepath.Join(dir, "root.crt"), IntermediateCertFile: filepath.Join(dir, "intermediate.crt"),
		IntermediateKeyFile: filepath.Join(dir, "intermediate.key"), KeyPassphraseEnv: "VORTEX_TEST_CA_PASSPHRASE"}
	_ = os.WriteFile(c.RootCertFile, pki.CertPEM(g.CA.Root), 0o644)
	_ = os.WriteFile(c.IntermediateCertFile, pki.CertPEM(g.CA.Inter), 0o644)
	_ = os.WriteFile(c.IntermediateKeyFile, enc, 0o600)
	t.Setenv(c.KeyPassphraseEnv, Passphrase)
	return c, g.CA
}

// RootPEM 返回根证书 PEM（随安装包分发给组件）。
func RootPEM(ca *pki.CA) []byte { return pki.CertPEM(ca.Root) }

// Admin 插入一个管理员用户（令牌的 created_by）并返回其 ID。
func Admin(t testing.TB, dsn string) platform.UUID {
	t.Helper()
	id := platform.NewUUIDv7()
	exec(t, dsn, "INSERT INTO users (id, email, password_hash, role) VALUES ($1, $2, 'x', 'admin')", id.PG(), id.String()+"@test")
	return id
}

// Tenant 插入一个租户并返回其 ID。
func Tenant(t testing.TB, dsn string) platform.UUID {
	t.Helper()
	id := platform.NewUUIDv7()
	exec(t, dsn, "INSERT INTO tenants (id, name) VALUES ($1, 't')", id.PG())
	return id
}

func exec(t testing.TB, dsn, sql string, args ...any) {
	t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	if _, err := c.Exec(ctx, sql, args...); err != nil {
		t.Fatal(err)
	}
}

// Exec 在测试数据库上执行一条语句。
func Exec(t testing.TB, dsn, sql string, args ...any) { t.Helper(); exec(t, dsn, sql, args...) }

// QueryRow 在测试数据库上查询一行。
func QueryRow(t testing.TB, dsn, sql string, args []any, dest ...any) {
	t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	if err := c.QueryRow(ctx, sql, args...).Scan(dest...); err != nil {
		t.Fatal(err)
	}
}

// Entitle 让租户可以保存规则（测试用）：没有主套餐时建一个宽松的套餐（1 TiB、10 Gbit/s、10000 连接、1000 条规则）；
// 给其主套餐授予全部协议与转发方式、自托管，以及调用时已存在的全部平台节点组（不含反向建链）。可重复调用。
func Entitle(t testing.TB, dsn string, tenant platform.UUID) {
	t.Helper()
	plan := platform.NewUUIDv7()
	exec(t, dsn, `INSERT INTO plans (id, name, duration_days, base_traffic_bytes, traffic_reset, max_rules, rate_bps, max_conns)
		SELECT $1, 'test', 365, 1099511627776, 'monthly', 1000, 10000000000, 10000
		WHERE NOT EXISTS (SELECT 1 FROM subscriptions WHERE tenant_id = $2)`, plan.PG(), tenant.PG())
	exec(t, dsn, `INSERT INTO subscriptions (tenant_id, plan_id, started_at, expires_at, base_period_end, base_remaining_bytes)
		SELECT $1, id, now() - interval '1 day', now() + interval '364 days', now() + interval '29 days', base_traffic_bytes
		FROM plans WHERE id = $2 AND NOT EXISTS (SELECT 1 FROM subscriptions WHERE tenant_id = $1)`, tenant.PG(), plan.PG())
	exec(t, dsn, `INSERT INTO plan_entitlements (plan_id, kind, value)
		SELECT s.plan_id, e.kind, e.value FROM subscriptions s,
		  (VALUES ('protocol','tcp'), ('protocol','udp'), ('forward_mode','direct'), ('forward_mode','tunnel'), ('forward_mode','multi_hop'),
		          ('self_hosted','true')) AS e(kind, value)
		WHERE s.tenant_id = $1
		UNION ALL
		SELECT s.plan_id, 'node_group', g.id::text FROM subscriptions s, node_groups g WHERE s.tenant_id = $1 AND g.tenant_id IS NULL
		ON CONFLICT DO NOTHING`, tenant.PG())
}
