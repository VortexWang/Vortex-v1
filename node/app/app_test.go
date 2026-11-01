package app

import (
	"crypto/tls"
	"testing"

	"vortex/engine/enginetest"
	"vortex/engine/types"
	"vortex/node/agent/certs"
	"vortex/node/agent/grpcconn"
	"vortex/node/agent/probe"
)

var self = types.UUID{0x0d}

func dialerFor(p *enginetest.PKI, id types.UUID) grpcconn.Dialer {
	c := p.Node(id).Cert
	return grpcconn.Dialer{Holder: certs.NewHolder(c), Roots: p.Roots}
}

func TestTunnelOptionsFromNodeCertificate(t *testing.T) {
	p := enginetest.NewPKI()
	d := dialerFor(p, self)
	pr := probe.New()
	tun, err := tunnelOptions(&Config{LinkListen: "127.0.0.1:0"}, self, types.UUID{0xb0}, d, pr)
	if err != nil {
		t.Fatal(err)
	}
	defer tun.LinkListener.Close()
	if tun.Identity.NodeID != self || tun.BootID != (types.UUID{0xb0}) || tun.Roots != p.Roots || tun.PeerHealthy == nil {
		t.Fatalf("tunnel options %+v", tun)
	}
	// 续签后，新链路取用新证书（AGT-012）
	renewed := p.Node(self).Cert
	d.Holder.Set(renewed)
	if cur := tun.CurrentIdentity(); cur.NodeID != self || !sameCert(cur.Cert, renewed) {
		t.Fatal("current identity does not follow the renewed certificate")
	}
	// 没有链路监听：仍可主动建链
	tun2, err := tunnelOptions(&Config{}, self, types.UUID{0xb0}, d, pr)
	if err != nil || tun2 == nil || tun2.LinkListener != nil {
		t.Fatalf("no listener: %+v %v", tun2, err)
	}
}

func sameCert(a, b tls.Certificate) bool {
	return len(a.Certificate) > 0 && len(b.Certificate) > 0 && string(a.Certificate[0]) == string(b.Certificate[0])
}

func TestTunnelOptionsRejectsMismatchAndMissingCertificate(t *testing.T) {
	p := enginetest.NewPKI()
	if _, err := tunnelOptions(&Config{NodeID: "x"}, self, types.UUID{}, dialerFor(p, types.UUID{0x99}), probe.New()); err == nil {
		t.Fatal("certificate of another node accepted")
	}
	if tun, err := tunnelOptions(&Config{}, self, types.UUID{}, nil, probe.New()); tun != nil || err != nil {
		t.Fatalf("without a certificate: %+v %v", tun, err)
	}
	if _, err := tunnelOptions(&Config{LinkListen: "127.0.0.1:0"}, self, types.UUID{}, nil, probe.New()); err == nil {
		t.Fatal("link_listen without a certificate accepted")
	}
}
