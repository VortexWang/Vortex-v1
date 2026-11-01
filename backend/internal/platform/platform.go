// Package platform 是后端的基础设施（BE-T01）：配置、日志（slog JSON、敏感字段脱敏）、数据库连接池、事务助手、
// 迁移与 UUIDv7。
package platform

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // goose 使用的 database/sql 驱动
	"github.com/pressly/goose/v3"

	"vortex/backend/migrations"
)

// Config 是后端配置文件（JSON）。数据库连接串可由环境变量 VORTEX_DATABASE_URL 覆盖。
type Config struct {
	DatabaseURL string `json:"database_url"`
	// GRPCListen 是 HubControl（面向 Edge Hub）的监听地址，如 ":7500"。
	GRPCListen string   `json:"grpc_listen"`
	CA         CAConfig `json:"ca"`
}

// CAConfig：平台 CA（SEC-012）。根 CA 只提供证书（私钥离线）；中间 CA 私钥为加密文件，口令取自环境变量。
type CAConfig struct {
	RootCertFile         string `json:"root_cert_file"`
	IntermediateCertFile string `json:"intermediate_cert_file"`
	IntermediateKeyFile  string `json:"intermediate_key_file"` // VORTEX-CA-KEY 加密格式（见 internal/pki）
	KeyPassphraseEnv     string `json:"key_passphrase_env"`    // 默认 VORTEX_CA_PASSPHRASE
}

func LoadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if v := os.Getenv("VORTEX_DATABASE_URL"); v != "" {
		c.DatabaseURL = v
	}
	if c.GRPCListen == "" {
		c.GRPCListen = ":7500"
	}
	if c.CA.KeyPassphraseEnv == "" {
		c.CA.KeyPassphraseEnv = "VORTEX_CA_PASSPHRASE"
	}
	return &c, nil
}

// ---------------------------------------------------------------- 日志

// sensitive 是日志中一律脱敏的字段名片段（SEC-004：令牌、口令、私钥、密钥不得进入日志）。
var sensitive = []string{"token", "password", "passphrase", "secret", "private_key", "authorization", "cookie"}

// Redacted 是脱敏后的占位值。
const Redacted = "[REDACTED]"

// NewLogger 返回 JSON 日志，属性名包含敏感片段的值替换为 Redacted。
func NewLogger(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level, ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
		k := strings.ToLower(a.Key)
		for _, s := range sensitive {
			if strings.Contains(k, s) {
				return slog.String(a.Key, Redacted)
			}
		}
		return a
	}}))
}

// ---------------------------------------------------------------- 数据库

// OpenDB 打开连接池并确认可用。
func OpenDB(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := p.Ping(ctx); err != nil {
		p.Close()
		return nil, err
	}
	return p, nil
}

// Migrate 执行嵌入的全部迁移（契约 C3）。
func Migrate(ctx context.Context, url string) error {
	db, err := goose.OpenDBWithDriver("pgx", url)
	if err != nil {
		return err
	}
	defer db.Close()
	gooseMu.Lock()
	defer gooseMu.Unlock()
	goose.SetBaseFS(migrations.FS)
	goose.SetLogger(goose.NopLogger())
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}
	return goose.UpContext(ctx, db, ".")
}

var gooseMu sync.Mutex // goose 的设置是包级状态

// InTx 在一个事务中执行 f；序列化失败与死锁时重试（最多 5 次）。f 返回错误则回滚。
func InTx(ctx context.Context, p *pgxpool.Pool, f func(pgx.Tx) error) error {
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		err = pgx.BeginFunc(ctx, p, f)
		var pe *pgconn.PgError
		if errors.As(err, &pe) && (pe.Code == "40001" || pe.Code == "40P01") {
			time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
			continue
		}
		return err
	}
	return err
}

// ---------------------------------------------------------------- UUID

// UUID 是 16 字节标识。
type UUID [16]byte

// NewUUIDv7 生成 UUIDv7（C3 约定：主键由应用生成）：48 位毫秒时间戳 + 随机数。
func NewUUIDv7() UUID {
	var u UUID
	ms := uint64(time.Now().UnixMilli())
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], ms)
	copy(u[0:6], b[2:8])
	_, _ = rand.Read(u[6:])
	u[6] = u[6]&0x0f | 0x70
	u[8] = u[8]&0x3f | 0x80
	return u
}

// PG 转为 pgtype.UUID。
func (u UUID) PG() pgtype.UUID { return pgtype.UUID{Bytes: u, Valid: true} }

// FromPG 从 pgtype.UUID 取值（NULL 为零值）。
func FromPG(p pgtype.UUID) UUID {
	if !p.Valid {
		return UUID{}
	}
	return UUID(p.Bytes)
}

func (u UUID) String() string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
}

// TS 转为 pgtype.Timestamptz。
func TS(t time.Time) pgtype.Timestamptz { return pgtype.Timestamptz{Time: t, Valid: true} }
