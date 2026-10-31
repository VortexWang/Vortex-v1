package gate

import "net/netip"

func netipAddr() netip.AddrPort { return netip.MustParseAddrPort("198.51.100.7:40000") }
