// Package ref 是 VLink v1（契约 C1）的最小参考编解码实现，仅依赖标准库。
// 用途：锁定契约时验证格式、生成互操作测试向量；不是生产实现。
package ref

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	HeaderLen       = 8
	MaxPayload      = 16384 // ENG-012：单帧载荷 ≤ 16 KiB
	ProtocolVersion = 1
	ALPN            = "vortex-vlink/1"
)

type FrameType uint8

const (
	TypeHello   FrameType = 0x01
	TypeOpen    FrameType = 0x02
	TypeOpenOK  FrameType = 0x03
	TypeOpenErr FrameType = 0x04
	TypeData    FrameType = 0x05
	TypeFin     FrameType = 0x06
	TypeRst     FrameType = 0x07
	TypeDgram   FrameType = 0x08
	TypeWindow  FrameType = 0x09
	TypePing    FrameType = 0x0A
	TypePong    FrameType = 0x0B
	TypeGoaway  FrameType = 0x0C
	TypePadding FrameType = 0x7F // 保留给 VTP
)

var known = map[FrameType]bool{
	TypeHello: true, TypeOpen: true, TypeOpenOK: true, TypeOpenErr: true, TypeData: true,
	TypeFin: true, TypeRst: true, TypeDgram: true, TypeWindow: true, TypePing: true,
	TypePong: true, TypeGoaway: true, TypePadding: true,
}

// linkLevel 记录必须使用 stream_id = 0 的帧类型。
var linkLevel = map[FrameType]bool{TypeHello: true, TypePing: true, TypePong: true, TypeGoaway: true, TypePadding: true}

var (
	ErrPayloadTooLarge = errors.New("vlink: payload exceeds 16384")
	ErrUnknownType     = errors.New("vlink: unknown frame type")
	ErrStreamID        = errors.New("vlink: invalid stream_id for frame type")
)

type Frame struct {
	Type     FrameType
	Flags    uint8
	StreamID uint32
	Payload  []byte
}

// Validate 检查帧是否满足 C1 第 2、3 节。negotiatedExt 表示是否已协商扩展帧（0x80～0xFF）。
func (f Frame) Validate(negotiatedExt bool) error {
	if len(f.Payload) > MaxPayload {
		return ErrPayloadTooLarge
	}
	if f.Type >= 0x80 {
		if !negotiatedExt {
			return ErrUnknownType
		}
		return nil
	}
	if !known[f.Type] {
		return ErrUnknownType
	}
	if linkLevel[f.Type] && f.StreamID != 0 {
		return ErrStreamID
	}
	if !linkLevel[f.Type] && f.Type != TypeWindow && f.StreamID == 0 {
		return ErrStreamID
	}
	return nil
}

func (f Frame) MarshalBinary() ([]byte, error) {
	if err := f.Validate(true); err != nil {
		return nil, err
	}
	b := make([]byte, HeaderLen+len(f.Payload))
	b[0] = byte(f.Type)
	b[1] = f.Flags
	binary.BigEndian.PutUint16(b[2:4], uint16(len(f.Payload)))
	binary.BigEndian.PutUint32(b[4:8], f.StreamID)
	copy(b[8:], f.Payload)
	return b, nil
}

// ReadFrame 从流中读取一帧并校验。
func ReadFrame(r io.Reader, negotiatedExt bool) (Frame, error) {
	var h [HeaderLen]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return Frame{}, err
	}
	n := int(binary.BigEndian.Uint16(h[2:4]))
	if n > MaxPayload {
		return Frame{}, ErrPayloadTooLarge
	}
	f := Frame{Type: FrameType(h[0]), Flags: h[1], StreamID: binary.BigEndian.Uint32(h[4:8]), Payload: make([]byte, n)}
	if _, err := io.ReadFull(r, f.Payload); err != nil {
		return Frame{}, err
	}
	if err := f.Validate(negotiatedExt); err != nil {
		return Frame{}, fmt.Errorf("type 0x%02x: %w", byte(f.Type), err)
	}
	return f, nil
}

// ---- 简单载荷 ----

type ErrorCode uint16

const (
	CodeNoRoute           ErrorCode = 1
	CodeUnauthorized      ErrorCode = 2
	CodeQuotaExhausted    ErrorCode = 3
	CodeRateLimited       ErrorCode = 4
	CodeConnLimited       ErrorCode = 5
	CodeTargetUnreachable ErrorCode = 6
)

type GoawayCode uint16

const (
	GoawayNormal             GoawayCode = 0
	GoawayProtocolError      GoawayCode = 1
	GoawayVersionUnsupported GoawayCode = 2
	GoawayRevoked            GoawayCode = 3
	GoawayStreamIDExhausted  GoawayCode = 4
	GoawayDraining           GoawayCode = 5
)

func ErrPayload(code ErrorCode) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, uint16(code))
	return b
}

func WindowPayload(inc uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, inc)
	return b
}

func GoawayPayload(lastStreamID uint32, code GoawayCode) []byte {
	b := make([]byte, 6)
	binary.BigEndian.PutUint32(b[0:4], lastStreamID)
	binary.BigEndian.PutUint16(b[4:6], uint16(code))
	return b
}
