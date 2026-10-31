package route

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"

	"vortex/engine/types"
)

// PROXY protocol（ENG-009）：最终连接目标的节点在发送业务数据前写入头部，携带原始客户端地址。
// 规范：HAProxy PROXY protocol v1（文本）与 v2（二进制）。
//   - TCP：v1 或 v2，每条连接一次，在任何业务数据之前；
//   - UDP：只支持 v2，每个数据报前各加一份（RUL-001：UDP 仅 v2）。
// 头部由平台生成，不属于业务载荷，不计量（ENG-035）。

var proxyV2Sig = [12]byte{0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A}

var ErrProxyV1UDP = errors.New("route: PROXY protocol v1 does not support UDP")

// ProxyHeader 生成头部。src 为原始客户端地址，dst 为客户端连接的入口地址。
func ProxyHeader(v types.ProxyProtocol, udp bool, src, dst netip.AddrPort) ([]byte, error) {
	s, d := src.Addr().Unmap(), dst.Addr().Unmap()
	if s.Is4() != d.Is4() {
		// 地址族不一致（如 IPv4 客户端连 IPv6 监听）时把两者都表示为 IPv6
		s, d = netip.AddrFrom16(s.As16()), netip.AddrFrom16(d.As16())
	}
	switch v {
	case types.ProxyOff, types.ProxyUnspecified:
		return nil, nil
	case types.ProxyV1:
		if udp {
			return nil, ErrProxyV1UDP
		}
		fam := "TCP4"
		if s.Is6() {
			fam = "TCP6"
		}
		return []byte(fmt.Sprintf("PROXY %s %s %s %d %d\r\n", fam, s, d, src.Port(), dst.Port())), nil
	case types.ProxyV2:
		var fam byte
		var addrs []byte
		if s.Is4() {
			fam = 0x10 // AF_INET
			a, b := s.As4(), d.As4()
			addrs = append(append(addrs, a[:]...), b[:]...)
		} else {
			fam = 0x20 // AF_INET6
			a, b := s.As16(), d.As16()
			addrs = append(append(addrs, a[:]...), b[:]...)
		}
		if udp {
			fam |= 0x02 // DGRAM
		} else {
			fam |= 0x01 // STREAM
		}
		addrs = binary.BigEndian.AppendUint16(addrs, src.Port())
		addrs = binary.BigEndian.AppendUint16(addrs, dst.Port())
		h := make([]byte, 0, 16+len(addrs))
		h = append(h, proxyV2Sig[:]...)
		h = append(h, 0x21) // 版本 2，命令 PROXY
		h = append(h, fam)
		h = binary.BigEndian.AppendUint16(h, uint16(len(addrs)))
		return append(h, addrs...), nil
	}
	return nil, fmt.Errorf("route: unknown PROXY protocol version %d", v)
}
