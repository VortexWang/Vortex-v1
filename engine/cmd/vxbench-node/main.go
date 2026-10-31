// vxbench-node 在基准拓扑的转发节点上运行引擎（ENG-T36）：由模拟 Agent 应用一条直接转发规则，
// 并按执行点下发运行授权、份额与计费授权，使正式的计量与门控全程生效（需求 13.2 节）。
// 就绪后在标准输出打印 READY；收到 SIGINT / SIGTERM 时打印 JSON 统计并退出。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/netip"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"vortex/engine"
	"vortex/engine/enginetest"
	"vortex/engine/types"
)

type stats struct {
	MeteredUp     uint64  `json:"metered_up_bytes"`
	MeteredDown   uint64  `json:"metered_down_bytes"`
	BillingUsed   uint64  `json:"billing_used_bytes"`
	Events        int     `json:"events"`
	DroppedEvents uint64  `json:"dropped_events"`
	Goroutines    int     `json:"goroutines_at_exit"`
	CPUUserSec    float64 `json:"cpu_user_seconds"`
	CPUSysSec     float64 `json:"cpu_sys_seconds"`
	WallSec       float64 `json:"wall_seconds"`
	MaxRSSKiB     int64   `json:"max_rss_kib"`
}

func main() {
	listen := flag.String("listen", "", "监听地址 ip:port（TCP 与 UDP 同端口）")
	target := flag.String("target", "", "目标 ip:port")
	nets := flag.String("networks", "tcp,udp", "启用的网络")
	private := flag.String("allow-private", "", "允许的私有目标网段（逗号分隔，SEC-011 按组放开）")
	self := flag.String("self", "", "本机地址（逗号分隔，目标检查拒绝回环到本机）")
	rate := flag.Uint64("rate-share", 0, "租户速率份额 bit/s（0 = 100 Gbps）")
	ruleRate := flag.Uint64("rule-rate", 0, "规则限速 bit/s（0 = 不限）")
	billing := flag.Uint64("billing", 0, "计费授权原始字节（0 = 1 TiB）")
	flag.Parse()

	bind, err := netip.ParseAddrPort(*listen)
	check(err)
	tgt, err := netip.ParseAddrPort(*target)
	check(err)
	pol := types.TargetPolicy{}
	for _, s := range split(*private) {
		pol.AllowPrivatePrefix = append(pol.AllowPrivatePrefix, netip.MustParsePrefix(s))
	}
	var selfAddrs []netip.Addr
	for _, s := range split(*self) {
		selfAddrs = append(selfAddrs, netip.MustParseAddr(s))
	}

	ruleID, tenant := types.UUID{0xbe, 1}, types.UUID{0xbe, 2}
	r := types.Rule{RuleID: ruleID, TenantID: tenant, RuleVersion: 1, Role: types.RoleEntry, EnforcementPoint: true,
		TargetSchedule: types.ScheduleWeightedRoundRobin, PathCoefMilli: 1000, BillingVersion: 1,
		Targets: []types.Target{{Host: tgt.Addr().String(), Port: tgt.Port(), Weight: 1}},
		Limits:  types.RuleLimits{RateBps: *ruleRate}}
	cfg := &types.RuntimeConfig{Version: 1, Rules: nil, TargetPolicy: pol}
	for _, n := range split(*nets) {
		nw := types.NetworkTCP
		if n == "udp" {
			nw = types.NetworkUDP
		}
		r.Networks = append(r.Networks, nw)
		cfg.Listeners = append(cfg.Listeners, types.Listener{Bind: bind, Network: nw, RuleID: ruleID})
	}
	cfg.Rules = []types.Rule{r}

	rt := engine.New(engine.Options{SelfAddrs: selfAddrs})
	ag := enginetest.Start(rt, time.Second)
	if _, _, err := ag.Apply(context.Background(), cfg); err != nil {
		check(err)
	}
	set := ag.GrantAll(cfg, enginetest.Grants{TTL: 24 * time.Hour, RateBps: *rate, Billing: *billing})
	start := time.Now()
	fmt.Println("READY")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	ag.Stop()
	st := stats{MeteredUp: ag.Usage(ruleID, types.DirUp), MeteredDown: ag.Usage(ruleID, types.DirDown),
		Events: len(ag.Events(0)), DroppedEvents: rt.DroppedEvents(), WallSec: time.Since(start).Seconds()}
	if c, ok := ag.Consumption(set.Billing[0].Meta.GrantID); ok {
		st.BillingUsed = c.UsedRawBytes
	}
	_ = rt.Close()
	st.Goroutines = runtime.NumGoroutine()
	var ru syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &ru) == nil {
		st.CPUUserSec = float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6
		st.CPUSysSec = float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6
		st.MaxRSSKiB = ru.Maxrss
	}
	out, _ := json.Marshal(st)
	fmt.Println(string(out))
}

func split(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "vxbench-node:", err)
		os.Exit(1)
	}
}
