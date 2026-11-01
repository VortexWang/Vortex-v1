package e2e

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	hubapp "vortex/edgehub/app"
	"vortex/edgehub/backendgrpc"
	"vortex/engine/types"
	"vortex/engine/vlink"
	"vortex/node/agent/localapi"
	nodeapp "vortex/node/app"
	backendv1 "vortex/proto/gen/go/vortex/backend/v1"
)

var exitID = types.UUID{0x0e}

// 多跳完整组装（ENG-015、AGT-001）：两个真实的 vortex-node（app.Run）接入同一个 edge-hub，入口规则没有目标、
// 只有下一跳（出口节点）；出口在 link_listen 上接受节点间链路。两端都以平台 CA 签发的节点证书作为链路身份，
// 流量只能经 入口 → VLink → 出口 → 目标 到达。
func TestMultiHopFullStack(t *testing.T) {
	p := newPKI(t)
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.crt")
	_ = os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: p.ca.Raw}), 0o600)
	tg := target(t)
	entryAddr, linkAddr := freePort(t), freePort(t)
	const (
		rv  = 1
		gen = 1
	)
	pol := types.TargetPolicy{SelfHostedNode: true}
	entryCfg := &types.RuntimeConfig{Version: 1, Generation: gen, NodeID: nodeID,
		Listeners: []types.Listener{{Bind: entryAddr, Network: types.NetworkTCP, RuleID: ruleA}},
		Rules: []types.Rule{{RuleID: ruleA, TenantID: tenant, RuleVersion: rv, PathGeneration: gen, Role: types.RoleEntry, EnforcementPoint: true,
			Networks: []types.Network{types.NetworkTCP}, TargetSchedule: types.ScheduleWeightedRoundRobin, PathCoefMilli: 1500, BillingVersion: 1,
			NextHop: &types.NextHop{MemberSchedule: types.ScheduleWeightedRoundRobin, Dial: types.DialForward,
				Members: []types.Member{{NodeID: exitID, Weight: 1}}}}},
		Peers:        []types.Peer{{NodeID: exitID, Address: linkAddr.Addr().String(), LinkPort: linkAddr.Port(), Outbound: true}},
		TargetPolicy: pol}
	exitCfg := &types.RuntimeConfig{Version: 1, Generation: gen, NodeID: exitID,
		Rules: []types.Rule{{RuleID: ruleA, TenantID: tenant, RuleVersion: rv, PathGeneration: gen, Role: types.RoleExit, HopIndex: 1,
			Networks: []types.Network{types.NetworkTCP}, TargetSchedule: types.ScheduleWeightedRoundRobin, BillingVersion: 1,
			AllowedUpstreams: []types.UUID{nodeID},
			Targets:          []types.Target{{Host: tg.Addr().String(), Port: tg.Port(), Weight: 1}}}},
		Peers:        []types.Peer{{NodeID: nodeID}},
		TargetPolicy: pol}

	be := &protoBackend{cfg: entryCfg, more: []*types.RuntimeConfig{exitCfg}, pushed: map[string]bool{}}
	bln, _ := net.Listen("tcp4", "127.0.0.1:0")
	bsrv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{p.issue(t, backendgrpc.BackendURI, x509.ExtKeyUsageServerAuth)},
		ClientCAs: p.roots, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13})))
	backendv1.RegisterHubControlServer(bsrv, be)
	go func() { _ = bsrv.Serve(bln) }()
	defer bsrv.Stop()

	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	if os.Getenv("STACK_DEBUG") != "" {
		quiet = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	ctx, cancel := context.WithCancel(context.Background())
	hc, hk := writePEM(t, dir, "hub", p.hub(t, hubA))
	hubCfg := &hubapp.Config{HubID: vlink.FormatUUID(hubA), Region: "test", DataDir: filepath.Join(dir, "hub"), Listen: "127.0.0.1:0",
		CertFile: hc, KeyFile: hk, CAFile: caFile, BackendAddress: bln.Addr().String()}
	listen := make(chan string, 1)
	hubDone := make(chan error, 1)
	go func() {
		hubDone <- hubapp.Run(ctx, hubCfg, quiet, hubapp.Options{OnListen: func(a string) { listen <- a }})
	}()
	hubAddr := <-listen

	var done []chan error
	start := func(id types.UUID, name, link string) *nodeapp.Config {
		nc, nk := writePEM(t, dir, name, p.node(t, id))
		cfg := &nodeapp.Config{NodeID: vlink.FormatUUID(id), DataDir: filepath.Join(dir, name), Socket: filepath.Join(dir, name+".sock"),
			EdgeHubs: []nodeapp.HubEntry{{HubID: vlink.FormatUUID(hubA), Address: hubAddr}}, DiskLowBytes: 1, DiskHighBytes: 1,
			CertFile: nc, KeyFile: nk, CAFile: caFile, LinkListen: link}
		dialer, err := nodeapp.Dialer(cfg)
		if err != nil {
			t.Fatal(err)
		}
		ch := make(chan error, 1)
		done = append(done, ch)
		go func() { ch <- nodeapp.Run(ctx, cfg, dialer, quiet) }()
		return cfg
	}
	exit := start(exitID, "exit", linkAddr.String())
	entry := start(nodeID, "entry", "")
	defer func() {
		cancel()
		for _, ch := range done {
			if err := <-ch; err != nil {
				t.Error(err)
			}
		}
		if err := <-hubDone; err != nil {
			t.Error(err)
		}
	}()
	if os.Getenv("STACK_DEBUG") != "" {
		defer func() {
			for _, c := range []*nodeapp.Config{entry, exit} {
				s, e := localapi.GetStatus(context.Background(), c.Socket)
				t.Logf("%s status %+v %v", c.NodeID, s, e)
			}
		}()
	}
	eventually(t, "forwarding across the node link", func() bool { return ok(entryAddr) })
	for _, c := range []*nodeapp.Config{entry, exit} {
		st, err := localapi.GetStatus(context.Background(), c.Socket)
		if err != nil || !st.Connected || st.Phase != "applied" {
			t.Fatalf("%s status %+v %v", c.NodeID, st, err)
		}
	}
	for i := 0; i < 5; i++ {
		if !ok(entryAddr) {
			t.Fatal("session over the existing link")
		}
	}
}
