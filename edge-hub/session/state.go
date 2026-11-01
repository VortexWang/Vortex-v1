package session

import (
	"encoding/json"
	"sort"
	"time"

	"vortex/edgehub/model"
)

// 节点状态缓存（HUB-004，HUB-T07）：在线、启动、已生效版本、程序版本、最后联系时刻、实际状态与探测汇总。
// 变化的节点在下一次批量上报（NodeStatusBatch）中带上；节点接入时立即上报一次（后端据此为新启动生成份额）。

// NodeState 是一个节点的缓存状态。
type NodeState struct {
	Node                 model.UUID
	Boot                 model.UUID
	Connected            bool
	AppliedConfigVersion uint64
	AgentVersion         string
	LastSeen             time.Time
	State                json.RawMessage // 最近一次 StateReport
	Probes               json.RawMessage // 最近一次 ProbeReport
}

type nodeState struct {
	NodeState
	dirty bool
}

func (h *Hub) stateLocked(node model.UUID) *nodeState {
	st := h.states[node]
	if st == nil {
		st = &nodeState{NodeState: NodeState{Node: node}}
		h.states[node] = st
	}
	return st
}

func (h *Hub) touch(node model.UUID, f func(*NodeState)) {
	h.mu.Lock()
	st := h.stateLocked(node)
	st.LastSeen = h.Clk.Now()
	if f != nil {
		f(&st.NodeState)
		st.dirty = true
	}
	h.mu.Unlock()
}

// NodeStates 返回全部节点的缓存状态（按节点排序）。
func (h *Hub) NodeStates() []NodeState {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]NodeState, 0, len(h.states))
	for _, st := range h.states {
		out = append(out, st.NodeState)
	}
	sort.Slice(out, func(i, j int) bool { return string(out[i].Node[:]) < string(out[j].Node[:]) })
	return out
}

// TakeChangedStates 返回自上次调用以来变化的节点状态并清除变化标记（批量上报）。
func (h *Hub) TakeChangedStates() []NodeState {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []NodeState
	for _, st := range h.states {
		if st.dirty {
			out = append(out, st.NodeState)
			st.dirty = false
		}
	}
	sort.Slice(out, func(i, j int) bool { return string(out[i].Node[:]) < string(out[j].Node[:]) })
	return out
}
