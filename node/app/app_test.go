package app

import (
	"crypto/tls"
	"os"
	"testing"
	"time"

	"vortex/engine/enginetest"
	"vortex/engine/types"
	"vortex/node/agent"
	"vortex/node/agent/certs"
	"vortex/node/agent/grpcconn"
	"vortex/node/agent/probe"
	"vortex/node/agent/upgrade"
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

func TestStatusViewClockSkew(t *testing.T) {
	if v := StatusView(agent.Status{}, nil); v.ClockSkewMs != nil || v.ClockSkewAlert {
		t.Fatalf("no sample yet: %+v", v)
	}
	v := StatusView(agent.Status{ClockSkew: &agent.ClockSkew{Offset: -7500 * time.Millisecond, Alert: true}}, nil)
	if v.ClockSkewMs == nil || *v.ClockSkewMs != -7500 || !v.ClockSkewAlert {
		t.Fatalf("status %+v", v)
	}
}

func TestDisableSelfUpgradeFromEnvironment(t *testing.T) {
	path := t.TempDir() + "/node.json"
	_ = os.WriteFile(path, []byte(`{"node_id":"0d000000-0000-4000-8000-000000000001"}`), 0o600)
	if c, err := LoadConfig(path); err != nil || c.DisableSelfUpgrade {
		t.Fatalf("default %+v %v", c, err)
	}
	t.Setenv(EnvDisableSelfUpgrade, "1")
	if c, _ := LoadConfig(path); !c.DisableSelfUpgrade {
		t.Fatal("environment not honoured")
	}
}

func TestRefuseUpgradeWhenDisabled(t *testing.T) {
	if refuseUpgrade(&Config{}, upgrade.Command{Version: "2"}) != nil {
		t.Fatal("refused while enabled")
	}
	st := refuseUpgrade(&Config{DisableSelfUpgrade: true}, upgrade.Command{Version: "2"})
	if st == nil || st.Phase != upgrade.PhaseFailed || st.Target != "2" || st.Error == "" {
		t.Fatalf("%+v", st)
	}
}

// 作为 .prev 执行 Guard 时，程序路径去掉 .prev 后缀；配置了 binary 时用配置。
func TestBinaryPath(t *testing.T) {
	if p, _ := binaryPath(&Config{Binary: "/opt/x"}); p != "/opt/x" {
		t.Fatal(p)
	}
	if p, err := binaryPath(&Config{}); err != nil || p == "" {
		t.Fatal(p, err)
	}
	if installedPath("/opt/vortex/node/vortex-node.prev") != "/opt/vortex/node/vortex-node" || installedPath("/a/vortex-node") != "/a/vortex-node" {
		t.Fatal("installed path")
	}
}
