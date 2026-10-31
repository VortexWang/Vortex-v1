package route

import (
	"sync/atomic"

	"vortex/engine/types"
)

// Table 是不可变的规则表快照。切换通过 Tables.Swap 整体原子完成（ENG-032）；
// 已建立的会话持有自己开始时的快照，不受切换影响。
type Table struct {
	Version uint64
	rules   map[types.UUID]*types.Rule
}

// NewTable 从运行配置构造快照。调用方之后不得再修改 cfg.Rules。
func NewTable(version uint64, rules []types.Rule) *Table {
	t := &Table{Version: version, rules: make(map[types.UUID]*types.Rule, len(rules))}
	for i := range rules {
		r := rules[i]
		t.rules[r.RuleID] = &r
	}
	return t
}

// Rule 返回规则；不存在时返回 nil（ENG-020 ①：规则未激活）。
func (t *Table) Rule(id types.UUID) *types.Rule {
	if t == nil {
		return nil
	}
	return t.rules[id]
}

func (t *Table) Len() int {
	if t == nil {
		return 0
	}
	return len(t.rules)
}

// Tables 持有当前生效的快照。
type Tables struct{ cur atomic.Pointer[Table] }

func (ts *Tables) Load() *Table { return ts.cur.Load() }

// Swap 原子替换快照；版本不得回退（单调版本号，需求 2.3 节）。返回旧快照。
func (ts *Tables) Swap(t *Table) (*Table, bool) {
	for {
		old := ts.cur.Load()
		if old != nil && t.Version <= old.Version {
			return old, false
		}
		if ts.cur.CompareAndSwap(old, t) {
			return old, true
		}
	}
}
