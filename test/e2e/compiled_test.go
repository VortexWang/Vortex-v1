package e2e

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"testing"
	"time"

	"vortex/backend/compiler"
	"vortex/engine"
	"vortex/engine/enginetest"
	"vortex/engine/types"
	"vortex/engine/vlink"
	nodewire "vortex/node/agent/wire"
)

// 编译器输出可被真实引擎执行（BE-T14 与 ENG-029 的衔接）：平台入口 → 平台中继 → 无公网地址的自托管出口
// （auto 判定为反向建链），目标为出口本机服务（自托管出口的 SEC-011 例外）。编译结果经 runtime.v1 →
// 引擎类型转换后由三个引擎 Prepare / Commit，流量经两段 VLink（第二段由出口反向建链）到达目标。
func TestCompiledConfigForwards(t *testing.T) {
	pki := enginetest.NewPKI()
	ids := map[string]types.UUID{"entry": {0xe1}, "relay": {0xe2}, "exit": {0xe3}}
	cid := func(u types.UUID) compiler.UUID { return compiler.UUID(vlink.FormatUUID(u)) }
	links := map[string]net.Listener{}
	for _, n := range []string{"entry", "relay"} { // 出口没有公网地址：不监听链路
		ln, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		links[n] = ln
	}
	port := func(n string) uint16 {
		if ln := links[n]; ln != nil {
			return netip.MustParseAddrPort(ln.Addr().String()).Port()
		}
		return 0
	}
	tg := target(t)
	entryPort := freePort(t)
	tenant := compiler.UUID("7e000000-0000-0000-0000-000000000001")
	gEntry, gRelay, gExit := compiler.UUID("9f000000-0000-0000-0000-000000000001"), compiler.UUID("9f000000-0000-0000-0000-000000000002"),
		compiler.UUID("9f000000-0000-0000-0000-000000000003")
	member := func(n string) []compiler.Member { return []compiler.Member{{Node: cid(ids[n]), Weight: 1}} }
	snap := &compiler.Snapshot{Generation: 1, BillingVersion: 1,
		Rules: []compiler.Rule{{ID: "aa000000-0000-0000-0000-000000000001", Tenant: tenant, Version: 1, PathGeneration: 1, Network: "tcp",
			Mode: "multi_hop", EntryGroup: gEntry, Path: []compiler.Segment{{Group: gRelay, Dial: "auto"}, {Group: gExit, Dial: "auto"}},
			Port: entryPort.Port(), Targets: []compiler.Target{{Host: tg.Addr().String(), Port: tg.Port(), Weight: 1}},
			TargetSchedule: "weighted_round_robin", ProxyProtocol: "off", Enabled: true}},
		Groups: map[compiler.UUID]compiler.Group{
			gEntry: {Purpose: "entry", Schedule: "weighted_round_robin", MultiplierMilli: 1000, Members: member("entry")},
			gRelay: {Purpose: "relay", Schedule: "weighted_round_robin", MultiplierMilli: 500, Members: member("relay")},
			gExit:  {Tenant: tenant, Purpose: "exit", Schedule: "weighted_round_robin", Members: member("exit")}},
		Nodes: map[compiler.UUID]compiler.Node{
			cid(ids["entry"]): {PublicAddresses: []string{"127.0.0.1"}, LinkPort: port("entry")},
			cid(ids["relay"]): {PublicAddresses: []string{"127.0.0.1"}, LinkPort: port("relay")},
			cid(ids["exit"]):  {Tenant: tenant}},
		Permits:      []compiler.Permit{{Upstream: gEntry, Downstream: gRelay}, {Upstream: gRelay, Downstream: gExit}},
		NodeVersions: map[compiler.UUID]uint64{cid(ids["entry"]): 1, cid(ids["relay"]): 1, cid(ids["exit"]): 1},
	}
	res := compiler.Compile(snap)
	if len(res.Problems) != 0 {
		t.Fatalf("problems %v", res.Problems)
	}
	if d := res.Dials["aa000000-0000-0000-0000-000000000001"]; fmt.Sprint(d) != "[forward reverse]" {
		t.Fatalf("dials %v", d)
	}
	for _, n := range []string{"exit", "relay", "entry"} {
		cfg, err := nodewire.FromConfig(res.Configs[cid(ids[n])])
		if err != nil {
			t.Fatalf("%s: compiled config does not convert: %v", n, err)
		}
		if n == "entry" && (!cfg.Rules[0].EnforcementPoint || cfg.Rules[0].PathCoefMilli != 1500) {
			t.Fatalf("entry rule %+v", cfg.Rules[0])
		}
		rt := engine.New(engine.Options{Tunnel: &engine.TunnelOptions{Identity: pki.Node(ids[n]), Roots: pki.Roots,
			BootID: types.UUID{0xb0, ids[n][0]}, LinkListener: links[n], DialTimeout: time.Second}})
		ag := enginetest.Start(rt, 20*time.Millisecond)
		t.Cleanup(func() { ag.Stop(); _ = rt.Close() })
		if pr, ar, err := ag.Apply(context.Background(), cfg); err != nil {
			t.Fatalf("%s: engine rejected the compiled config: %v %+v %+v", n, err, pr, ar)
		}
		ag.GrantAll(cfg, enginetest.Grants{TTL: time.Hour})
	}
	eventually(t, "traffic over the compiled multi-hop path", func() bool { return ok(entryPort) })
}
