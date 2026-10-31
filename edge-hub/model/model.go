// Package model 是 Edge Hub 内部的基本类型。与契约 C2（vortex.edgehub.v1 / vortex.backend.v1）对应；
// 接入 gRPC 后由适配层在生成代码与这些类型之间转换。
package model

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

type UUID [16]byte

// NewUUID 生成随机 UUID（版本 4）。
func NewUUID() UUID {
	var u UUID
	_, _ = rand.Read(u[:])
	u[6] = u[6]&0x0f | 0x40
	u[8] = u[8]&0x3f | 0x80
	return u
}

func (u UUID) String() string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
}

func (u UUID) MarshalText() ([]byte, error) { return []byte(hex.EncodeToString(u[:])), nil }

func (u *UUID) UnmarshalText(b []byte) error {
	if hex.DecodedLen(len(b)) != 16 {
		return fmt.Errorf("uuid: length %d", len(b))
	}
	_, err := hex.Decode(u[:], b)
	return err
}

func (u UUID) IsZero() bool { return u == UUID{} }

type GrantKind uint8

const (
	GrantUnspecified GrantKind = iota
	GrantRun
	GrantBilling
	GrantShareRate
	GrantShareConn
)

type Mode uint8

const (
	ModeUnspecified Mode = iota
	ModeOnline
	ModeAutonomous
	ModeReconciling
	ModeAutonomyExpired
)

func (m Mode) String() string {
	return [...]string{"unspecified", "online", "autonomous", "reconciling", "autonomy_expired"}[m]
}

// CeilBilling 把原始字节按路径系数（千分位）换算为计费字节，向上取整（C2 第 7 节、C2b H3）。
func CeilBilling(raw, coefMilli uint64) uint64 {
	if raw == 0 || coefMilli == 0 {
		return 0
	}
	hi, lo := mul64(raw, coefMilli)
	// (hi:lo + 999) / 1000：raw × coef 最大约 2^64 × 2^20，用 128 位避免溢出
	lo2 := lo + 999
	if lo2 < lo {
		hi++
	}
	q, _ := div128(hi, lo2, 1000)
	return q
}

func mul64(a, b uint64) (hi, lo uint64) {
	const mask32 = 1<<32 - 1
	a0, a1 := a&mask32, a>>32
	b0, b1 := b&mask32, b>>32
	w0 := a0 * b0
	t := a1*b0 + w0>>32
	w1, w2 := t&mask32, t>>32
	w1 += a0 * b1
	return a1*b1 + w2 + w1>>32, a * b
}

// div128 返回 (hi:lo) / d，商超过 64 位时饱和为最大值（计费字节不会达到）。
func div128(hi, lo, d uint64) (uint64, uint64) {
	if hi >= d {
		return ^uint64(0), 0
	}
	var q, r uint64 = 0, hi
	for i := 63; i >= 0; i-- {
		carry := r >> 63
		r = r<<1 | (lo>>uint(i))&1
		q <<= 1
		if carry == 1 || r >= d {
			r -= d
			q |= 1
		}
	}
	return q, r
}
