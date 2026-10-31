package route

import (
	"sync"

	"vortex/engine/types"
)

// Candidate 是可被选择的对象：组内成员（NOD-009）或规则目标（RUL-007）。
type Candidate struct {
	Key      string // 健康状态与已经过节点的判定键：成员为 node_id，目标为 "host:port"
	Weight   uint32
	Priority uint32
}

// Selector 按调度方式在候选中选择一个（RUL-008：TCP 按连接、UDP 按会话调用一次，会话存续期间不变）。
// 并发安全；每条规则每个下一跳组 / 目标集合一个 Selector。
type Selector struct {
	mode  types.Schedule
	cands []Candidate

	mu      sync.Mutex
	current []int64 // 平滑加权轮询的当前权重
}

func NewSelector(mode types.Schedule, cands []Candidate) *Selector {
	cs := append([]Candidate(nil), cands...)
	for i := range cs {
		if cs[i].Weight == 0 {
			cs[i].Weight = 1
		}
	}
	return &Selector{mode: mode, cands: cs, current: make([]int64, len(cs))}
}

// Pick 返回所选候选的下标；healthy 与 excluded 用于过滤（ENG-022：排除已经过的节点；不健康成员不参与）。
// 没有可用候选时返回 -1（调用方返回 no_route，RUL-006：不得跳过指定的中继）。
func (s *Selector) Pick(healthy func(key string) bool, excluded func(key string) bool) int {
	ok := func(i int) bool {
		k := s.cands[i].Key
		if excluded != nil && excluded(k) {
			return false
		}
		return healthy == nil || healthy(k)
	}
	if s.mode == types.SchedulePrimaryBackup {
		// 主备：取可用候选中 priority 最小者；相同优先级按平滑加权轮询分摊
		best := ^uint32(0)
		for i := range s.cands {
			if ok(i) && s.cands[i].Priority < best {
				best = s.cands[i].Priority
			}
		}
		if best == ^uint32(0) {
			return -1
		}
		return s.wrr(func(i int) bool { return ok(i) && s.cands[i].Priority == best })
	}
	return s.wrr(ok)
}

// wrr 是平滑加权轮询：每轮所有可用候选的当前权重加上各自权重，选当前权重最大者，再减去可用权重总和。
// 结果分布严格按权重比例，且同一候选不会被连续选中过多次。
func (s *Selector) wrr(ok func(int) bool) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	best, total := -1, int64(0)
	for i := range s.cands {
		if !ok(i) {
			continue
		}
		w := int64(s.cands[i].Weight)
		s.current[i] += w
		total += w
		if best < 0 || s.current[i] > s.current[best] {
			best = i
		}
	}
	if best >= 0 {
		s.current[best] -= total
	}
	return best
}

func (s *Selector) Len() int { return len(s.cands) }

func (s *Selector) At(i int) Candidate { return s.cands[i] }
