package ref

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
)

// OPEN TLV tag（C1 第 5 节）。
const (
	openRuleID     = 0x01
	openRuleVer    = 0x02
	openPathGen    = 0x03
	openHopIndex   = 0x04
	openNetwork    = 0x05
	openVisited    = 0x06
	openClientAddr = 0x07
)

const (
	NetworkTCP uint8 = 1
	NetworkUDP uint8 = 2
)

type Open struct {
	RuleID         UUID
	RuleVersion    uint64
	PathGeneration uint64
	HopIndex       uint8
	Network        uint8
	Visited        []UUID
	Client         netip.AddrPort
}

func (o *Open) MarshalPayload() []byte {
	u64 := func(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }
	visited := make([]byte, 0, 16*len(o.Visited))
	for _, v := range o.Visited {
		visited = append(visited, v[:]...)
	}
	addr := o.Client.Addr().Unmap()
	ca := []byte{4}
	if addr.Is6() {
		ca[0] = 6
	}
	ca = append(ca, addr.AsSlice()...)
	ca = binary.BigEndian.AppendUint16(ca, o.Client.Port())
	return EncodeTLV([]TLV{
		{openRuleID, o.RuleID[:]},
		{openRuleVer, u64(o.RuleVersion)},
		{openPathGen, u64(o.PathGeneration)},
		{openHopIndex, []byte{o.HopIndex}},
		{openNetwork, []byte{o.Network}},
		{openVisited, visited},
		{openClientAddr, ca},
	})
}

func UnmarshalOpen(p []byte) (*Open, error) {
	m, err := DecodeTLV(p)
	if err != nil {
		return nil, err
	}
	get := func(tag uint8, n int) ([]byte, error) {
		v, ok := m[tag]
		if !ok || (n >= 0 && len(v) != n) {
			return nil, fmt.Errorf("vlink: OPEN tag 0x%02x missing or wrong length", tag)
		}
		return v, nil
	}
	o := &Open{}
	var v []byte
	if v, err = get(openRuleID, 16); err != nil {
		return nil, err
	}
	copy(o.RuleID[:], v)
	if v, err = get(openRuleVer, 8); err != nil {
		return nil, err
	}
	o.RuleVersion = binary.BigEndian.Uint64(v)
	if v, err = get(openPathGen, 8); err != nil {
		return nil, err
	}
	o.PathGeneration = binary.BigEndian.Uint64(v)
	if v, err = get(openHopIndex, 1); err != nil {
		return nil, err
	}
	o.HopIndex = v[0]
	if v, err = get(openNetwork, 1); err != nil {
		return nil, err
	}
	if v[0] != NetworkTCP && v[0] != NetworkUDP {
		return nil, errors.New("vlink: bad network")
	}
	o.Network = v[0]
	if v, err = get(openVisited, -1); err != nil {
		return nil, err
	}
	if len(v)%16 != 0 || len(v)/16 > 4 { // 路径最多 入口 + 3 中继 在出口之前
		return nil, errors.New("vlink: bad visited list")
	}
	for i := 0; i < len(v); i += 16 {
		var u UUID
		copy(u[:], v[i:i+16])
		o.Visited = append(o.Visited, u)
	}
	if v, err = get(openClientAddr, -1); err != nil {
		return nil, err
	}
	switch {
	case len(v) == 7 && v[0] == 4:
		o.Client = netip.AddrPortFrom(netip.AddrFrom4([4]byte(v[1:5])), binary.BigEndian.Uint16(v[5:7]))
	case len(v) == 19 && v[0] == 6:
		o.Client = netip.AddrPortFrom(netip.AddrFrom16([16]byte(v[1:17])), binary.BigEndian.Uint16(v[17:19]))
	default:
		return nil, errors.New("vlink: bad client address")
	}
	return o, nil
}
