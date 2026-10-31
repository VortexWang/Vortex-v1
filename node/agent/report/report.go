// Package report 是 Agent 的计量落盘与上报（AGT-006、AGT-009、MTR-006，02 方案 3.6）。
//
// 每秒把 Engine.DrainUsage() 的结果并入当前批次并落盘（fsync）；每 10 秒封批，序号在每个 boot_id 内从 1
// 起连续递增；收到 UsageAck（累计确认）后标记已确认。未确认批次在重连或切换实例后按序重发，Edge Hub 与
// 后端按 (node_id, boot_id, seq) 幂等。重启后先上报上次启动的批次：上次启动未封的部分在启动时封为该启动
// 的最后一批（AGT-009）。已确认批次保留不少于 7 天，之后清理；未确认批次不清理。
//
// 已知限制：DrainUsage 取走的增量在落盘前进程崩溃会丢失（最多约 1 秒的计量）。
package report

import (
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"

	"vortex/engine/types"
	"vortex/node/agent/store"
	"vortex/node/internal/clock"
)

const (
	SealEvery = 10 * time.Second
	Retention = 7 * 24 * time.Hour

	kindAdd   = "usage.add"
	kindSeal  = "usage.seal"
	kindAck   = "usage.ack"
	kindBatch = "usage.batch" // 清理后的紧凑形式：一条记录一个已封批次
	kindBase  = "usage.base"  // 清理后保留的每个 boot 的序号与确认位置
)

var ErrUnknownBoot = errors.New("report: ack for unknown boot")

// Batch 是一个已封批次（UsageBatch）。
type Batch struct {
	Boot     types.UUID          `json:"boot"`
	Seq      uint64              `json:"seq"`
	Snapshot types.UsageSnapshot `json:"snap"`
	SealedAt time.Time           `json:"sealed_at"`
	AckedAt  time.Time           `json:"acked_at,omitempty"` // 零值：未确认
}

type addData struct {
	Boot types.UUID          `json:"boot"`
	Snap types.UsageSnapshot `json:"snap"`
}

type sealData struct {
	Boot types.UUID `json:"boot"`
	Seq  uint64     `json:"seq"`
	At   time.Time  `json:"at"`
}

type ackData struct {
	Boot    types.UUID `json:"boot"`
	Through uint64     `json:"through"`
	At      time.Time  `json:"at"`
}

type baseData struct {
	Boot    types.UUID `json:"boot"`
	LastSeq uint64     `json:"last_seq"`
	Acked   uint64     `json:"acked"`
}

type bootState struct {
	lastSeq uint64
	acked   uint64
	open    *types.UsageSnapshot
}

// Reporter 管理计量批次。
type Reporter struct {
	mu      sync.Mutex
	clk     clock.Clock
	log     store.Log
	boot    types.UUID
	boots   map[types.UUID]*bootState
	order   []types.UUID // boot 首次出现的次序
	batches []*Batch     // 已封批次，按封批次序
}

// Open 重放日志并开始本次启动 boot。上次（及更早）启动未封的计量封为各自启动的最后一批。
func Open(clk clock.Clock, log store.Log, boot types.UUID) (*Reporter, error) {
	r := &Reporter{clk: clk, log: log, boot: boot, boots: map[types.UUID]*bootState{}}
	if err := log.Load(r.replay); err != nil {
		return nil, err
	}
	for _, b := range r.order {
		if b != boot && r.boots[b].open != nil {
			if _, err := r.sealLocked(b); err != nil {
				return nil, err
			}
		}
	}
	r.stateLocked(boot)
	return r, nil
}

func (r *Reporter) stateLocked(boot types.UUID) *bootState {
	s := r.boots[boot]
	if s == nil {
		s = &bootState{}
		r.boots[boot] = s
		r.order = append(r.order, boot)
	}
	return s
}

func (r *Reporter) replay(rec store.Record) error {
	switch rec.Kind {
	case kindAdd:
		var d addData
		if err := json.Unmarshal(rec.Data, &d); err != nil {
			return err
		}
		s := r.stateLocked(d.Boot)
		if s.open == nil {
			s.open = &types.UsageSnapshot{}
		}
		Merge(s.open, d.Snap)
	case kindSeal:
		var d sealData
		if err := json.Unmarshal(rec.Data, &d); err != nil {
			return err
		}
		s := r.stateLocked(d.Boot)
		snap := types.UsageSnapshot{}
		if s.open != nil {
			snap = *s.open
		}
		s.open, s.lastSeq = nil, d.Seq
		r.batches = append(r.batches, &Batch{Boot: d.Boot, Seq: d.Seq, Snapshot: snap, SealedAt: d.At})
	case kindBatch:
		var b Batch
		if err := json.Unmarshal(rec.Data, &b); err != nil {
			return err
		}
		s := r.stateLocked(b.Boot)
		s.lastSeq = max(s.lastSeq, b.Seq)
		r.batches = append(r.batches, &b)
	case kindBase:
		var d baseData
		if err := json.Unmarshal(rec.Data, &d); err != nil {
			return err
		}
		s := r.stateLocked(d.Boot)
		s.lastSeq, s.acked = max(s.lastSeq, d.LastSeq), max(s.acked, d.Acked)
	case kindAck:
		var d ackData
		if err := json.Unmarshal(rec.Data, &d); err != nil {
			return err
		}
		r.applyAckLocked(d)
	}
	return nil
}

func (r *Reporter) applyAckLocked(d ackData) {
	s := r.stateLocked(d.Boot)
	if d.Through <= s.acked {
		return
	}
	s.acked = d.Through
	for _, b := range r.batches {
		if b.Boot == d.Boot && b.Seq <= d.Through && b.AckedAt.IsZero() {
			b.AckedAt = d.At
		}
	}
}

// Merge 把 src 并入 dst：增量按（规则, 角色, 方向, 计费授权）累加；需求信号取最新；授权消耗取最大已用量，
// final 一旦出现保持。
func Merge(dst *types.UsageSnapshot, src types.UsageSnapshot) {
	type dk struct {
		rule  types.UUID
		role  types.HopRole
		dir   types.Direction
		grant types.UUID
	}
	idx := map[dk]int{}
	for i, d := range dst.Deltas {
		idx[dk{d.RuleID, d.Role, d.Direction, d.BillingGrantID}] = i
	}
	for _, d := range src.Deltas {
		k := dk{d.RuleID, d.Role, d.Direction, d.BillingGrantID}
		if i, ok := idx[k]; ok {
			dst.Deltas[i].RawBytes += d.RawBytes
		} else {
			idx[k] = len(dst.Deltas)
			dst.Deltas = append(dst.Deltas, d)
		}
	}
	for _, d := range src.Demands {
		found := false
		for i := range dst.Demands {
			if dst.Demands[i].TenantID == d.TenantID {
				dst.Demands[i], found = d, true
				break
			}
		}
		if !found {
			dst.Demands = append(dst.Demands, d)
		}
	}
	for _, c := range src.Consumptions {
		found := false
		for i := range dst.Consumptions {
			if x := &dst.Consumptions[i]; x.GrantID == c.GrantID {
				x.UsedRawBytes = max(x.UsedRawBytes, c.UsedRawBytes)
				x.Final = x.Final || c.Final
				found = true
				break
			}
		}
		if !found {
			dst.Consumptions = append(dst.Consumptions, c)
		}
	}
}

func empty(s types.UsageSnapshot) bool {
	return len(s.Deltas) == 0 && len(s.Demands) == 0 && len(s.Consumptions) == 0
}

// Add 把一次 DrainUsage 的结果并入当前批次并落盘。落盘失败时返回错误，内存状态不变（调用方重试，
// 磁盘满时按 AGT-006 停止新增业务）。
func (r *Reporter) Add(snap types.UsageSnapshot) error {
	if empty(snap) {
		return nil
	}
	rec, err := store.Make(kindAdd, addData{r.boot, snap})
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.log.Append(rec); err != nil {
		return err
	}
	s := r.boots[r.boot]
	if s.open == nil {
		s.open = &types.UsageSnapshot{}
	}
	Merge(s.open, snap)
	return nil
}

func (r *Reporter) sealLocked(boot types.UUID) (*Batch, error) {
	s := r.boots[boot]
	if s.open == nil {
		return nil, nil
	}
	d := sealData{Boot: boot, Seq: s.lastSeq + 1, At: r.clk.Now()}
	rec, err := store.Make(kindSeal, d)
	if err != nil {
		return nil, err
	}
	if err := r.log.Append(rec); err != nil {
		return nil, err
	}
	b := &Batch{Boot: boot, Seq: d.Seq, Snapshot: *s.open, SealedAt: d.At}
	s.open, s.lastSeq = nil, d.Seq
	r.batches = append(r.batches, b)
	return b, nil
}

// Seal 封当前批次（每 10 秒）；当前批次为空时返回 nil。
func (r *Reporter) Seal() (*Batch, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, err := r.sealLocked(r.boot)
	if b == nil {
		return nil, err
	}
	c := *b
	return &c, err
}

// Pending 返回全部未确认批次：上次启动的在前，同一启动内按序号。重连或切换实例后按此顺序重发。
func (r *Reporter) Pending() []Batch {
	r.mu.Lock()
	defer r.mu.Unlock()
	rank := map[types.UUID]int{}
	for i, b := range r.order {
		rank[b] = i
	}
	if _, ok := rank[r.boot]; ok {
		rank[r.boot] = len(r.order) // 本次启动排最后
	}
	var out []Batch
	for _, b := range r.batches {
		if b.AckedAt.IsZero() {
			out = append(out, *b)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Boot != out[j].Boot {
			return rank[out[i].Boot] < rank[out[j].Boot]
		}
		return out[i].Seq < out[j].Seq
	})
	return out
}

// Ack 处理 UsageAck（累计确认）。不会超过已封的最大序号；不会回退；重复确认无效果。
func (r *Reporter) Ack(boot types.UUID, through uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.boots[boot]
	if !ok {
		return ErrUnknownBoot
	}
	through = min(through, s.lastSeq)
	if through <= s.acked {
		return nil
	}
	d := ackData{Boot: boot, Through: through, At: r.clk.Now()}
	rec, err := store.Make(kindAck, d)
	if err != nil {
		return err
	}
	if err := r.log.Append(rec); err != nil {
		return err
	}
	r.applyAckLocked(d)
	return nil
}

// Prune 清理确认已超过 7 天的批次，并把日志压缩为紧凑形式。返回清理的批次数。
func (r *Reporter) Prune() (int, error) {
	now := r.clk.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	var keep []*Batch
	for _, b := range r.batches {
		if b.AckedAt.IsZero() || now.Sub(b.AckedAt) < Retention {
			keep = append(keep, b)
		}
	}
	n := len(r.batches) - len(keep)
	if n == 0 {
		return 0, nil
	}
	var recs []store.Record
	for _, boot := range r.order {
		s := r.boots[boot]
		rec, err := store.Make(kindBase, baseData{boot, s.lastSeq, s.acked})
		if err != nil {
			return 0, err
		}
		recs = append(recs, rec)
	}
	for _, b := range keep {
		rec, err := store.Make(kindBatch, b)
		if err != nil {
			return 0, err
		}
		recs = append(recs, rec)
	}
	for _, boot := range r.order {
		if s := r.boots[boot]; s.open != nil {
			rec, err := store.Make(kindAdd, addData{boot, *s.open})
			if err != nil {
				return 0, err
			}
			recs = append(recs, rec)
		}
	}
	if err := r.log.Rewrite(recs); err != nil {
		return 0, err
	}
	r.batches = keep
	return n, nil
}

// Stats 用于 status 子命令。
type Stats struct {
	Pending      int
	OldestSealed time.Time
}

func (r *Reporter) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	var s Stats
	for _, b := range r.batches {
		if b.AckedAt.IsZero() {
			if s.Pending == 0 || b.SealedAt.Before(s.OldestSealed) {
				s.OldestSealed = b.SealedAt
			}
			s.Pending++
		}
	}
	return s
}

// FirstUnacked 返回本次启动最早未确认批次的序号（AgentHello.unacked_usage_from_seq）；全部已确认时为 0。
func (r *Reporter) FirstUnacked() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.boots[r.boot]
	if s.acked < s.lastSeq {
		return s.acked + 1
	}
	return 0
}
