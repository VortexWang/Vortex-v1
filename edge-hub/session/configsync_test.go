package session

import (
	"encoding/json"
	"testing"
	"time"

	"vortex/edgehub/clock"
	"vortex/edgehub/issuer"
	"vortex/edgehub/ledger"
	"vortex/edgehub/model"
)

// 规则信息来自期望配置（写账本，重启后恢复）；签发用最新与上一个期望配置的并集。
func TestRulesFromDesiredConfig(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC))
	st := &ledger.MemStore{}
	parse := func(b []byte) ([]issuer.RuleState, error) {
		var rs []issuer.RuleState
		err := json.Unmarshal(b, &rs)
		return rs, err
	}
	openH := func() *Hub {
		h, err := Open(Options{HubID: hubID, Clock: clk, Store: st, ParseRules: parse})
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	h := openH()
	ruleB := model.UUID{0xbb}
	v1, _ := json.Marshal([]issuer.RuleState{{RuleID: ruleA, TenantID: tenant}})
	v2, _ := json.Marshal([]issuer.RuleState{{RuleID: ruleB, TenantID: tenant}})
	h.SetDesired(node, 1, v1)
	h.SetDesired(node, 2, v2)
	h.SetDesired(node, 1, v1) // 旧版本忽略
	rules := func(h *Hub) map[model.UUID]bool {
		out := map[model.UUID]bool{}
		for _, r := range h.configRules(node) {
			out[r.RuleID] = true
		}
		return out
	}
	if r := rules(h); !r[ruleA] || !r[ruleB] || len(r) != 2 {
		t.Fatalf("rules %v (latest ∪ previous)", r)
	}
	s, in := connect(t, h, boot1)
	_ = s.Handle(GrantRequest{ID: 1, Kind: model.GrantRun})
	ms := in.take()
	if g := ms[len(ms)-1].(GrantResponse); len(g.Run) != 1 || len(g.Run[0].Rules) != 2 {
		t.Fatalf("run grant %+v", g)
	}
	// 重启（例如自治中）后规则仍在
	if r := rules(openH()); len(r) != 2 {
		t.Fatalf("rules after restart %v", r)
	}
	// 检查点之后同样
	_, _ = h.Checkpoint()
	if r := rules(openH()); len(r) != 2 {
		t.Fatalf("rules after checkpoint %v", r)
	}
	v3, _ := json.Marshal([]issuer.RuleState{{RuleID: ruleB, TenantID: tenant}})
	h.SetDesired(node, 3, v3)
	if r := rules(h); r[ruleA] {
		t.Fatal("rule removed two versions ago still renewed")
	}
}
