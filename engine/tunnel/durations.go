package tunnel

import (
	"slices"
	"sync"
	"time"

	"vortex/engine/types"
)

// durations 记录各规则在本节点发出的 TCP 会话时长，供 auto 模式判定（ENG-017：近 10 分钟中位数
// 短于 10 秒用 pool，否则 mux，无历史用 mux）。每条规则最多保留 maxSamples 个样本。
type durations struct {
	mu    sync.Mutex
	rules map[types.UUID]*durRing
	now   func() time.Time // 测试注入
}

const (
	durWindow     = 10 * time.Minute
	shortSession  = 10 * time.Second
	maxSamples    = 512
	medianRefresh = time.Second
)

type durRing struct {
	at    []time.Time
	d     []time.Duration
	next  int
	cache bool
	ok    bool
	when  time.Time
}

func (ds *durations) clock() time.Time {
	if ds.now != nil {
		return ds.now()
	}
	return time.Now()
}

func (ds *durations) record(rule types.UUID, d time.Duration) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	if ds.rules == nil {
		ds.rules = map[types.UUID]*durRing{}
	}
	r := ds.rules[rule]
	if r == nil {
		r = &durRing{}
		ds.rules[rule] = r
	}
	now := ds.clock()
	if len(r.d) < maxSamples {
		r.at, r.d = append(r.at, now), append(r.d, d)
	} else {
		r.at[r.next], r.d[r.next] = now, d
		r.next = (r.next + 1) % maxSamples
	}
	r.ok = false // 下次查询重新计算
}

// median 返回近 10 分钟的中位数；没有样本时 ok 为 false。
func (ds *durations) median(rule types.UUID) (time.Duration, bool) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	r := ds.rules[rule]
	if r == nil {
		return 0, false
	}
	now := ds.clock()
	var xs []time.Duration
	for i, at := range r.at {
		if now.Sub(at) <= durWindow {
			xs = append(xs, r.d[i])
		}
	}
	if len(xs) == 0 {
		return 0, false
	}
	slices.Sort(xs)
	n := len(xs)
	if n%2 == 1 {
		return xs[n/2], true
	}
	return (xs[n/2-1] + xs[n/2]) / 2, true
}

// short 判定该规则的会话是否短（结果缓存 1 秒，避免每个新会话都排序）。
func (ds *durations) short(rule types.UUID) bool {
	ds.mu.Lock()
	r := ds.rules[rule]
	if r != nil && r.ok && ds.clock().Sub(r.when) < medianRefresh {
		v := r.cache
		ds.mu.Unlock()
		return v
	}
	ds.mu.Unlock()
	m, ok := ds.median(rule)
	v := ok && m < shortSession
	ds.mu.Lock()
	if r := ds.rules[rule]; r != nil {
		r.cache, r.ok, r.when = v, true, ds.clock()
	}
	ds.mu.Unlock()
	return v
}

func (ds *durations) forget(keep func(types.UUID) bool) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	for id := range ds.rules {
		if !keep(id) {
			delete(ds.rules, id)
		}
	}
}
