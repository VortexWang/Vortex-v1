package ref

import (
	"encoding/binary"
	"errors"
)

var (
	ErrTLVTruncated = errors.New("vlink: truncated TLV")
	ErrTLVDuplicate = errors.New("vlink: duplicate TLV tag")
)

type TLV struct {
	Tag   uint8
	Value []byte
}

// EncodeTLV 按给定顺序编码：tag(1) len(2, 大端) value。
func EncodeTLV(items []TLV) []byte {
	n := 0
	for _, it := range items {
		n += 3 + len(it.Value)
	}
	b := make([]byte, 0, n)
	for _, it := range items {
		b = append(b, it.Tag)
		b = binary.BigEndian.AppendUint16(b, uint16(len(it.Value)))
		b = append(b, it.Value...)
	}
	return b
}

// DecodeTLV 解码 TLV 序列。重复 tag 视为协议错误（C1 第 4、5 节，评审决定 R5），
// 未知 tag 由调用方忽略。
func DecodeTLV(b []byte) (map[uint8][]byte, error) {
	out := map[uint8][]byte{}
	for len(b) > 0 {
		if len(b) < 3 {
			return nil, ErrTLVTruncated
		}
		tag := b[0]
		n := int(binary.BigEndian.Uint16(b[1:3]))
		if len(b) < 3+n {
			return nil, ErrTLVTruncated
		}
		if _, dup := out[tag]; dup {
			return nil, ErrTLVDuplicate
		}
		out[tag] = b[3 : 3+n]
		b = b[3+n:]
	}
	return out, nil
}
