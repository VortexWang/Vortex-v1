package compiler

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"

	commonv1 "vortex/proto/gen/go/vortex/common/v1"
	runtimev1 "vortex/proto/gen/go/vortex/runtime/v1"
)

var update = flag.Bool("update", false, "rewrite golden files")

func load(t *testing.T, path string) *Snapshot {
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var s Snapshot
	if err := json.Unmarshal(b, &s); err != nil {
		t.Fatal(err)
	}
	return &s
}

// render 把编译结果写成稳定的文本：按节点输出 protojson，随后是建链方向与校验错误。
func render(r *Result) string {
	var b strings.Builder
	var nodes []string
	for n := range r.Configs {
		nodes = append(nodes, string(n))
	}
	sort.Strings(nodes)
	for _, n := range nodes {
		// protojson 的空白故意不稳定：先压缩再统一缩进
		js, _ := protojson.Marshal(r.Configs[UUID(n)])
		var c, out bytes.Buffer
		_ = json.Compact(&c, js)
		_ = json.Indent(&out, c.Bytes(), "", "  ")
		b.WriteString("== node " + n + "\n" + out.String() + "\n")
	}
	var rules []string
	for id := range r.Dials {
		rules = append(rules, string(id))
	}
	sort.Strings(rules)
	for _, id := range rules {
		b.WriteString("== dials " + id + " " + strings.Join(r.Dials[UUID(id)], ",") + "\n")
	}
	rules = rules[:0]
	for id := range r.Problems {
		rules = append(rules, string(id))
	}
	sort.Strings(rules)
	for _, id := range rules {
		for _, p := range r.Problems[UUID(id)] {
			b.WriteString("== problem " + id + " " + p.Code + ": " + p.Detail + "\n")
		}
	}
	return b.String()
}

// 黄金文件（BE-T14）：testdata/*.json 快照 → *.golden。`go test ./compiler -update` 重写。
func TestGolden(t *testing.T) {
	files, _ := filepath.Glob("testdata/*.json")
	if len(files) == 0 {
		t.Fatal("no snapshots")
	}
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			s := load(t, f)
			got := render(Compile(s))
			golden := strings.TrimSuffix(f, ".json") + ".golden"
			if *update {
				_ = os.WriteFile(golden, []byte(got), 0o644)
				return
			}
			want, err := os.ReadFile(golden)
			if err != nil {
				t.Fatal(err)
			}
			if got != string(want) {
				t.Errorf("%s differs from %s; run with -update after review\n%s", f, golden, firstDiff(got, string(want)))
			}
			// 确定性：再编译一次结果相同
			if again := render(Compile(load(t, f))); again != got {
				t.Error("compile is not deterministic")
			}
		})
	}
}

func firstDiff(a, b string) string {
	al, bl := strings.Split(a, "\n"), strings.Split(b, "\n")
	for i := 0; i < len(al) && i < len(bl); i++ {
		if al[i] != bl[i] {
			return "line " + itoa(i+1) + ":\n got:  " + al[i] + "\n want: " + bl[i]
		}
	}
	return "length differs"
}

func itoa(i int) string { b, _ := json.Marshal(i); return string(b) }

// 一致性（对所有黄金快照）：每条规则在路径各节点上的跳位、允许上游与下一组成员相互吻合；下一组成员都在本节点的
// 对端列表中，且建链方向与对端角色一致；执行点至多在一个跳位（该组全部成员）；监听只在入口节点；枚举均已显式赋值。
func TestConsistency(t *testing.T) {
	files, _ := filepath.Glob("testdata/*.json")
	for _, f := range files {
		s := load(t, f)
		res := Compile(s)
		type key struct{ rule, node string }
		at := map[key]*runtimev1.Rule{}
		peers := map[string]map[string]*runtimev1.Peer{}
		ep := map[string]map[uint32]bool{} // 规则 → 执行点所在跳位（同一组的全部成员）
		for n, c := range res.Configs {
			peers[string(n)] = map[string]*runtimev1.Peer{}
			for _, p := range c.Peers {
				if peers[string(n)][hexID(p.NodeId.Value)] != nil {
					t.Errorf("%s: node %s has a duplicate peer", f, n)
				}
				peers[string(n)][hexID(p.NodeId.Value)] = p
				if p.Dial == runtimev1.PeerDial_PEER_DIAL_UNSPECIFIED || p.LinkMode == runtimev1.LinkMode_LINK_MODE_UNSPECIFIED {
					t.Errorf("%s: unspecified peer enum", f)
				}
				if p.Dial == runtimev1.PeerDial_PEER_DIAL_OUTBOUND && (p.Address == "" || p.LinkPort == 0) {
					t.Errorf("%s: outbound peer without address", f)
				}
			}
			for _, r := range c.Rules {
				at[key{hexID(r.RuleId.Value), string(n)}] = r
				if r.EnforcementPoint {
					if ep[hexID(r.RuleId.Value)] == nil {
						ep[hexID(r.RuleId.Value)] = map[uint32]bool{}
					}
					ep[hexID(r.RuleId.Value)][r.HopIndex] = true
				}
				if r.Role == 0 || r.TargetSchedule == 0 || r.ProxyProtocol == 0 || len(r.Networks) == 0 {
					t.Errorf("%s: unspecified rule enum", f)
				}
				if (r.NextHop == nil) == (len(r.Targets) == 0) {
					t.Errorf("%s: next_hop and targets must be exclusive", f)
				}
			}
			for _, l := range c.Listeners {
				r := at[key{hexID(l.RuleId.Value), string(n)}]
				if r == nil || r.HopIndex != 0 || l.Port == 0 {
					t.Errorf("%s: listener on a non-entry node %s", f, n)
				}
			}
		}
		for k, r := range at {
			if len(ep[k.rule]) > 1 {
				t.Errorf("%s: rule %s has enforcement points at %d hops", f, k.rule, len(ep[k.rule]))
			}
			if r.NextHop == nil {
				continue
			}
			for _, m := range r.NextHop.Members {
				dn := hexID(m.NodeId.Value)
				down := at[key{k.rule, dn}]
				if down == nil || down.HopIndex != r.HopIndex+1 {
					t.Errorf("%s: rule %s: next member %s has no rule at hop %d", f, k.rule, dn, r.HopIndex+1)
					continue
				}
				if !contains(down.AllowedUpstreams, k.node) {
					t.Errorf("%s: rule %s: %s does not allow upstream %s", f, k.rule, dn, k.node)
				}
				p, q := peers[k.node][dn], peers[dn][k.node]
				if p == nil || q == nil {
					t.Errorf("%s: rule %s: missing peer between %s and %s", f, k.rule, k.node, dn)
					continue
				}
				switch r.NextHop.DialDirection {
				case runtimev1.DialDirection_DIAL_DIRECTION_FORWARD:
					if p.Dial != runtimev1.PeerDial_PEER_DIAL_OUTBOUND {
						t.Errorf("%s: forward segment without an outbound peer", f)
					}
				case runtimev1.DialDirection_DIAL_DIRECTION_REVERSE:
					if q.Dial != runtimev1.PeerDial_PEER_DIAL_OUTBOUND || !contains(q.ServedRuleIds, k.rule) {
						t.Errorf("%s: reverse segment: downstream does not dial up serving the rule", f)
					}
				default:
					t.Errorf("%s: unspecified dial direction", f)
				}
			}
		}
	}
}

func hexID(b []byte) string {
	const h = "0123456789abcdef"
	out := make([]byte, 0, 36)
	for i, c := range b {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			out = append(out, '-')
		}
		out = append(out, h[c>>4], h[c&15])
	}
	return string(out)
}

func contains(ids []*commonv1.Uuid, id string) bool {
	for _, x := range ids {
		if hexID(x.Value) == id {
			return true
		}
	}
	return false
}

// 不合法的规则不进入任何节点的配置；Validate 与 Compile 的判定一致。
func TestInvalidExcluded(t *testing.T) {
	s := load(t, "testdata/invalid.json")
	res := Compile(s)
	for _, r := range s.Rules {
		probs := Validate(s, r)
		if (len(probs) > 0) != (len(res.Problems[r.ID]) > 0) {
			t.Errorf("rule %s: Validate and Compile disagree", r.ID)
		}
		found := false
		for _, c := range res.Configs {
			for _, cr := range c.Rules {
				if hexID(cr.RuleId.Value) == string(r.ID) {
					found = true
				}
			}
		}
		if found == (len(probs) > 0) {
			t.Errorf("rule %s: compiled %v with problems %v", r.ID, found, probs)
		}
	}
	if len(res.Problems) != len(s.Rules)-1 {
		t.Errorf("%d rules rejected, want %d", len(res.Problems), len(s.Rules)-1)
	}
}
