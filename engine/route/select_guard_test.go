package route

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"vortex/engine/types"
)

func TestWeightedRoundRobinDistribution(t *testing.T) {
	s := NewSelector(types.ScheduleWeightedRoundRobin, []Candidate{{Key: "a", Weight: 5}, {Key: "b", Weight: 1}, {Key: "c", Weight: 1}})
	count := map[string]int{}
	seq := make([]string, 0, 7000)
	for i := 0; i < 7000; i++ {
		k := s.At(s.Pick(nil, nil)).Key
		count[k]++
		seq = append(seq, k)
	}
	if count["a"] != 5000 || count["b"] != 1000 || count["c"] != 1000 {
		t.Fatalf("distribution %v, want exactly 5000/1000/1000", count)
	}
	if err := smoothness(seq); err != nil {
		t.Fatal(err)
	}
}

// smoothness 检查平滑性：轻权重候选（b、c）从不相邻，重权重候选（a）最长连续不超过 4 次。
// 朴素加权轮询的序列 aaaaabc 两条都不满足（a 连续 5 次，b 与 c 相邻）。
func smoothness(seq []string) error {
	run, maxRun := 0, 0
	for i, k := range seq {
		if k == "a" {
			run++
			if run > maxRun {
				maxRun = run
			}
		} else {
			run = 0
			if i > 0 && seq[i-1] != "a" {
				return fmt.Errorf("light candidates adjacent at %d: %s%s", i, seq[i-1], k)
			}
		}
	}
	if maxRun > 4 {
		return fmt.Errorf("heavy candidate picked %d times in a row", maxRun)
	}
	return nil
}

func TestSmoothnessCheckRejectsNaiveWRR(t *testing.T) {
	var naive []string
	for i := 0; i < 1000; i++ {
		naive = append(naive, "a", "a", "a", "a", "a", "b", "c")
	}
	if smoothness(naive) == nil {
		t.Fatal("smoothness check must reject naive weighted round robin")
	}
}

func TestUnhealthyAndVisitedExcluded(t *testing.T) {
	s := NewSelector(types.ScheduleWeightedRoundRobin, []Candidate{{Key: "a", Weight: 1}, {Key: "b", Weight: 1}, {Key: "c", Weight: 1}})
	down := map[string]bool{"a": true}
	visited := map[string]bool{"b": true} // ENG-022：已经过的节点
	for i := 0; i < 50; i++ {
		if k := s.At(s.Pick(func(k string) bool { return !down[k] }, func(k string) bool { return visited[k] })).Key; k != "c" {
			t.Fatalf("picked %s", k)
		}
	}
	down["c"] = true
	if s.Pick(func(k string) bool { return !down[k] }, func(k string) bool { return visited[k] }) != -1 {
		t.Fatal("no candidate must return -1 (no_route), never skip the group")
	}
}

func TestPrimaryBackupFailover(t *testing.T) {
	s := NewSelector(types.SchedulePrimaryBackup, []Candidate{
		{Key: "backup", Weight: 1, Priority: 10}, {Key: "p1", Weight: 1, Priority: 1}, {Key: "p2", Weight: 1, Priority: 1},
	})
	down := map[string]bool{}
	h := func(k string) bool { return !down[k] }
	got := map[string]int{}
	for i := 0; i < 100; i++ {
		got[s.At(s.Pick(h, nil)).Key]++
	}
	if got["backup"] != 0 || got["p1"] != 50 || got["p2"] != 50 {
		t.Fatalf("primaries should share load, backup idle: %v", got)
	}
	down["p1"], down["p2"] = true, true
	if k := s.At(s.Pick(h, nil)).Key; k != "backup" {
		t.Fatalf("failover picked %s", k)
	}
	down["p1"] = false
	if k := s.At(s.Pick(h, nil)).Key; k != "p1" {
		t.Fatalf("recovery picked %s", k)
	}
}

func TestGuardPolicy(t *testing.T) {
	self := []netip.Addr{netip.MustParseAddr("203.0.113.10")}
	selfPorts := []uint16{7443, 7000, 9100} // link / control / metrics
	platform := NewGuard(types.TargetPolicy{
		DenyPorts:          []uint16{25},
		DenyPrefixes:       []netip.Prefix{netip.MustParsePrefix("198.51.100.0/24")}, // 平台组件
		AllowPrivatePrefix: []netip.Prefix{netip.MustParsePrefix("10.20.0.0/16")},    // 某节点组放开的内网
	}, self, selfPorts)
	selfHosted := NewGuard(types.TargetPolicy{SelfHostedNode: true}, self, selfPorts)

	cases := []struct {
		g    *Guard
		addr string
		ok   bool
		why  string
	}{
		{platform, "93.184.216.34:443", true, "普通公网目标"},
		{platform, "93.184.216.34:25", false, "管理员禁止端口"},
		{platform, "198.51.100.5:443", false, "平台组件地址"},
		{platform, "127.0.0.1:8080", false, "回环"},
		{platform, "[::1]:8080", false, "IPv6 回环"},
		{platform, "169.254.169.254:80", false, "云元数据"},
		{platform, "[::ffff:169.254.169.254]:80", false, "IPv4 映射形式的云元数据"},
		{platform, "[fe80::1]:80", false, "链路本地"},
		{platform, "224.0.0.1:5000", false, "组播"},
		{platform, "0.0.0.0:80", false, "未指定地址"},
		{platform, "10.1.2.3:80", false, "私有网段默认禁止"},
		{platform, "10.20.3.4:80", true, "按节点组放开的私有网段"},
		{platform, "100.64.1.1:80", false, "CGNAT 视同私有"},
		{platform, "203.0.113.10:8080", false, "平台节点自身地址"},
		{selfHosted, "127.0.0.1:8080", true, "自托管出口访问本机服务"},
		{selfHosted, "203.0.113.10:8080", true, "自托管出口访问本机地址"},
		{selfHosted, "127.0.0.1:7443", false, "自托管也不能指向本机 link 端口"},
		{selfHosted, "203.0.113.10:9100", false, "自托管也不能指向本机 metrics 端口"},
		{selfHosted, "169.254.169.254:80", false, "自托管也不能访问云元数据"},
	}
	for _, c := range cases {
		err := c.g.Check(netip.MustParseAddrPort(c.addr))
		if (err == nil) != c.ok {
			t.Errorf("%s (%s): allowed=%v, want %v", c.why, c.addr, err == nil, c.ok)
		}
	}
}

func TestResolverCachesAndGuardsEveryAddress(t *testing.T) {
	calls := 0
	answers := []netip.Addr{netip.MustParseAddr("93.184.216.34")}
	lookup := func(_ context.Context, _, host string) ([]netip.Addr, error) {
		calls++
		if host == "nx.example" {
			return nil, fmt.Errorf("no such host")
		}
		return answers, nil
	}
	r := NewResolver(lookup, 30*time.Second)
	now := time.Unix(1000, 0)
	r.now = func() time.Time { return now }
	g := NewGuard(types.TargetPolicy{}, nil, nil)

	if aps, _, err := AllowedAddrs(context.Background(), r, g, "game.example", 443); err != nil || len(aps) != 1 {
		t.Fatalf("first resolve: %v %v", aps, err)
	}
	_, _, _ = AllowedAddrs(context.Background(), r, g, "game.example", 443)
	if calls != 1 {
		t.Fatalf("cache not used: %d lookups", calls)
	}
	// DNS 重绑定：缓存过期后域名改指向云元数据地址，必须被拒绝
	answers = []netip.Addr{netip.MustParseAddr("169.254.169.254")}
	now = now.Add(31 * time.Second)
	if _, rej, err := AllowedAddrs(context.Background(), r, g, "game.example", 443); !errors.Is(err, ErrTargetForbidden) || rej.String() != "169.254.169.254" {
		t.Fatalf("rebinding not blocked: %v %v", rej, err)
	}
	// 混合结果只保留允许的地址
	answers = []netip.Addr{netip.MustParseAddr("127.0.0.1"), netip.MustParseAddr("93.184.216.35")}
	now = now.Add(31 * time.Second)
	if aps, _, err := AllowedAddrs(context.Background(), r, g, "game.example", 443); err != nil || len(aps) != 1 || aps[0].Addr().String() != "93.184.216.35" {
		t.Fatalf("mixed answer: %v %v", aps, err)
	}
	// 字面 IP 不查询
	before := calls
	_, _, _ = AllowedAddrs(context.Background(), r, g, "93.184.216.36", 443)
	if calls != before {
		t.Fatal("literal IP must not be looked up")
	}
	if _, _, err := AllowedAddrs(context.Background(), r, g, "nx.example", 443); err == nil {
		t.Fatal("lookup error must propagate")
	}
	if NewResolver(lookup, 5*time.Minute).ttl != 30*time.Second {
		t.Fatal("cache TTL must be capped (≤ 60 s)")
	}
}
