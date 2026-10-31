// Package vlink 是 VLink v1（契约 C1）的生产实现：帧编解码、HELLO 握手、承载层（TLS 1.3 mTLS）。
// 互操作以 vlink/ref（锁定契约时的参考实现）与其测试向量为准。
package vlink

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	HeaderLen       = 8
	MaxPayload      = 16384 // ENG-012
	ProtocolVersion = 1
	MinVersion      = 1 // 本实现支持的最低版本
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
	TypePadding FrameType = 0x7F // 保留给 VTP：v1 不发送，收到即丢弃
)

func (t FrameType) String() string {
	names := [...]string{"", "HELLO", "OPEN", "OPEN_OK", "OPEN_ERR", "DATA", "FIN", "RST", "DGRAM", "WINDOW", "PING", "PONG", "GOAWAY"}
	if int(t) < len(names) && names[t] != "" {
		return names[t]
	}
	if t == TypePadding {
		return "PADDING"
	}
	return fmt.Sprintf("0x%02x", uint8(t))
}

// streamRule：0 = 必须为 0，1 = 必须非 0，2 = 均可（WINDOW），3 = 未定义类型
var streamRule = func() (r [128]uint8) {
	for i := range r {
		r[i] = 3
	}
	for _, t := range []FrameType{TypeHello, TypePing, TypePong, TypeGoaway, TypePadding} {
		r[t] = 0
	}
	for _, t := range []FrameType{TypeOpen, TypeOpenOK, TypeOpenErr, TypeData, TypeFin, TypeRst, TypeDgram} {
		r[t] = 1
	}
	r[TypeWindow] = 2
	return
}()

// 协议错误：接收方发送 GOAWAY(protocol_error) 并关闭链路（C1 第 3 节）。
var (
	ErrProtocol        = errors.New("vlink: protocol error")
	ErrPayloadTooLarge = fmt.Errorf("%w: payload exceeds %d", ErrProtocol, MaxPayload)
	ErrUnknownType     = fmt.Errorf("%w: unknown frame type", ErrProtocol)
	ErrStreamID        = fmt.Errorf("%w: invalid stream_id for frame type", ErrProtocol)
)

// Header 是 8 字节帧头。
type Header struct {
	Type     FrameType
	Flags    uint8 // v1 全部保留；接收方忽略
	Length   uint16
	StreamID uint32
}

// Check 按 C1 第 2、3 节校验帧头。ext 表示已在 HELLO 中协商扩展帧（0x80～0xFF）。
func (h Header) Check(ext bool) error {
	if h.Length > MaxPayload {
		return ErrPayloadTooLarge
	}
	if h.Type >= 0x80 {
		if !ext {
			return ErrUnknownType
		}
		return nil
	}
	switch streamRule[h.Type] {
	case 0:
		if h.StreamID != 0 {
			return ErrStreamID
		}
	case 1:
		if h.StreamID == 0 {
			return ErrStreamID
		}
	case 3:
		return ErrUnknownType
	}
	return nil
}

func putHeader(b []byte, t FrameType, length int, sid uint32) {
	b[0], b[1] = byte(t), 0
	binary.BigEndian.PutUint16(b[2:4], uint16(length))
	binary.BigEndian.PutUint32(b[4:8], sid)
}

// AppendFrame 把一帧追加到 dst。载荷超过上限或帧头不合规时 panic：发送方构造非法帧是程序错误。
func AppendFrame(dst []byte, t FrameType, sid uint32, payload []byte) []byte {
	if len(payload) > MaxPayload {
		panic(fmt.Sprintf("vlink: building %v frame with %d-byte payload", t, len(payload)))
	}
	if err := (Header{Type: t, Length: uint16(len(payload)), StreamID: sid}).Check(true); err != nil {
		panic(fmt.Sprintf("vlink: building invalid %v frame (stream %d): %v", t, sid, err))
	}
	n := len(dst)
	dst = append(dst, make([]byte, HeaderLen)...)
	putHeader(dst[n:], t, len(payload), sid)
	return append(dst, payload...)
}

// Reader 逐帧读取。Next 返回的载荷指向内部缓冲，只在下一次调用前有效；稳态零分配。
type Reader struct {
	r   *bufio.Reader
	hb  [HeaderLen]byte // 放在结构体里：局部数组经接口调用会逃逸到堆上
	buf [MaxPayload]byte
	Ext bool // 已协商扩展帧
}

// NewReader 包装底层连接（通常是 *tls.Conn）。
func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReaderSize(r, 64<<10)}
}

// Next 读取一帧。帧头不合规时在读取载荷之前返回协议错误（不为超长载荷分配或等待数据）。
// PADDING 帧被跳过。
func (fr *Reader) Next() (Header, []byte, error) {
	for {
		hb := fr.hb[:]
		if _, err := io.ReadFull(fr.r, hb); err != nil {
			return Header{}, nil, err
		}
		h := Header{Type: FrameType(hb[0]), Flags: hb[1], Length: binary.BigEndian.Uint16(hb[2:4]), StreamID: binary.BigEndian.Uint32(hb[4:8])}
		if err := h.Check(fr.Ext); err != nil {
			return h, nil, fmt.Errorf("%v frame: %w", h.Type, err)
		}
		p := fr.buf[:h.Length]
		if _, err := io.ReadFull(fr.r, p); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return h, nil, err
		}
		if h.Type == TypePadding {
			continue
		}
		return h, p, nil
	}
}

// ---------------------------------------------------------------- 简单载荷（C1 第 6～8 节）

type ErrorCode uint16

const (
	CodeNoRoute           ErrorCode = 1
	CodeUnauthorized      ErrorCode = 2
	CodeQuotaExhausted    ErrorCode = 3
	CodeRateLimited       ErrorCode = 4
	CodeConnLimited       ErrorCode = 5
	CodeTargetUnreachable ErrorCode = 6
)

func (c ErrorCode) String() string {
	names := [...]string{"", "no_route", "unauthorized", "quota_exhausted", "rate_limited", "conn_limited", "target_unreachable"}
	if int(c) < len(names) && c != 0 {
		return names[c]
	}
	return fmt.Sprintf("code_%d", uint16(c))
}

type GoawayCode uint16

const (
	GoawayNormal             GoawayCode = 0
	GoawayProtocolError      GoawayCode = 1
	GoawayVersionUnsupported GoawayCode = 2
	GoawayRevoked            GoawayCode = 3
	GoawayStreamIDExhausted  GoawayCode = 4
	GoawayDraining           GoawayCode = 5
)

const maxReasonText = 256

// AppendErrPayload 编码 OPEN_ERR / RST 载荷：code(2) + 可选说明（截断到 256 字节）。
func AppendErrPayload(dst []byte, code ErrorCode, text string) []byte {
	dst = binary.BigEndian.AppendUint16(dst, uint16(code))
	if len(text) > maxReasonText {
		text = text[:maxReasonText]
	}
	return append(dst, text...)
}

// ParseErrPayload 解码 OPEN_ERR / RST 载荷。
func ParseErrPayload(p []byte) (ErrorCode, string, error) {
	if len(p) < 2 || len(p) > 2+maxReasonText {
		return 0, "", fmt.Errorf("%w: error payload length %d", ErrProtocol, len(p))
	}
	return ErrorCode(binary.BigEndian.Uint16(p)), string(p[2:]), nil
}

func AppendWindowPayload(dst []byte, inc uint32) []byte {
	return binary.BigEndian.AppendUint32(dst, inc)
}

// ParseWindowPayload：契约未禁止增量 0，收到时按无操作处理。
func ParseWindowPayload(p []byte) (uint32, error) {
	if len(p) != 4 {
		return 0, fmt.Errorf("%w: WINDOW payload length %d", ErrProtocol, len(p))
	}
	return binary.BigEndian.Uint32(p), nil
}

func AppendGoawayPayload(dst []byte, last uint32, code GoawayCode) []byte {
	dst = binary.BigEndian.AppendUint32(dst, last)
	return binary.BigEndian.AppendUint16(dst, uint16(code))
}

func ParseGoawayPayload(p []byte) (uint32, GoawayCode, error) {
	if len(p) != 6 {
		return 0, 0, fmt.Errorf("%w: GOAWAY payload length %d", ErrProtocol, len(p))
	}
	return binary.BigEndian.Uint32(p[0:4]), GoawayCode(binary.BigEndian.Uint16(p[4:6])), nil
}

// ---------------------------------------------------------------- TLV（C1 第 4、5 节）

// tlvSet 是解码后的 TLV，按 tag 直接索引，避免 map 分配。
type tlvSet struct {
	v   [256][]byte
	has [4]uint64
}

func (s *tlvSet) get(tag uint8) ([]byte, bool) {
	return s.v[tag], s.has[tag>>6]&(1<<(tag&63)) != 0
}

// decodeTLV：截断与重复 tag 为协议错误（评审决定 R5）；未知 tag 由调用方忽略。
func decodeTLV(b []byte, s *tlvSet) error {
	for len(b) > 0 {
		if len(b) < 3 {
			return fmt.Errorf("%w: truncated TLV", ErrProtocol)
		}
		tag, n := b[0], int(binary.BigEndian.Uint16(b[1:3]))
		if len(b) < 3+n {
			return fmt.Errorf("%w: truncated TLV", ErrProtocol)
		}
		if _, dup := s.get(tag); dup {
			return fmt.Errorf("%w: duplicate TLV tag 0x%02x", ErrProtocol, tag)
		}
		s.has[tag>>6] |= 1 << (tag & 63)
		s.v[tag] = b[3 : 3+n]
		b = b[3+n:]
	}
	return nil
}

func appendTLV(dst []byte, tag uint8, v []byte) []byte {
	dst = append(dst, tag)
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(v)))
	return append(dst, v...)
}

// need 取必需 tag；n < 0 表示不限长度。
func (s *tlvSet) need(what string, tag uint8, n int) ([]byte, error) {
	v, ok := s.get(tag)
	if !ok {
		return nil, fmt.Errorf("%w: %s tag 0x%02x missing", ErrProtocol, what, tag)
	}
	if n >= 0 && len(v) != n {
		return nil, fmt.Errorf("%w: %s tag 0x%02x length %d, want %d", ErrProtocol, what, tag, len(v), n)
	}
	return v, nil
}
