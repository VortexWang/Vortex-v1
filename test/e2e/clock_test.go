package e2e

import (
	"testing"
	"time"

	"vortex/node/agent/control"
	"vortex/node/agent/grpcconn"
	"vortex/node/agent/reconcile"
)

// 时钟偏差（AGT-005）：Edge Hub 在 Connect 响应头中给出墙钟（契约补充 L6），节点比对并在偏差超过 5 秒时告警；
// 授权不受影响，转发照常。
func TestGRPCClockSkew(t *testing.T) {
	p := newPKI(t)
	for _, c := range []struct {
		name  string
		shift time.Duration
		alert bool
	}{{"in sync", 0, false}, {"hub 10s ahead", 10 * time.Second, true}, {"hub 10s behind", -10 * time.Second, true}} {
		t.Run(c.name, func(t *testing.T) {
			cfg := config(t)
			shift := c.shift
			h := serveHubNow(t, newHub(t, hubA, t0(), cfg), p.hub(t, hubA), p.roots, func() time.Time { return time.Now().Add(shift) })
			d := grpcconn.Dialer{Cert: p.node(t, nodeID), Roots: p.roots, DialTimeout: 2 * time.Second}
			n := startNodeGRPC(t, newLogs(), d, []control.Endpoint{{ID: hubA, Address: h.addr}})
			eventually(t, "clock compared", func() bool { return n.a.Status().ClockSkew != nil })
			s := n.a.Status().ClockSkew
			if s.Hub != hubA || s.Alert != c.alert {
				t.Fatalf("skew %+v", s)
			}
			if diff := s.Offset - c.shift; diff > time.Second || diff < -time.Second || s.Uncertainty > time.Second {
				t.Fatalf("offset %v ±%v, want about %v", s.Offset, s.Uncertainty, c.shift)
			}
			// 偏差只告警：配置照常应用、授权照常、转发照常
			eventually(t, "applied", func() bool { return n.a.Status().Phase == reconcile.PhaseApplied })
			eventually(t, "forwarding", func() bool { return ok(cfg.Listeners[0].Bind) })
		})
	}
}
