// Package runner 是 Edge Hub 的运行循环（HUB-T01 的非 gRPC 部分）：
//   - 每秒：模式检查（失联 30 秒进入自治、自治满 12 小时到期，随模式冻结 / 解冻份额）与上行链路补报、
//     额度补充请求、节点状态批量上报；
//   - 每 60 秒：自治中的份额再平衡（LIM-009；在线时不做任何事）；
//   - 每小时：账本检查点与保留期清理（HUB-012；不在线时不清理）。
//
// 任何一步出错都交给 OnError，循环继续。
package runner

import (
	"context"
	"time"

	"vortex/edgehub/session"
	"vortex/edgehub/upstream"
)

type Runner struct {
	Hub  *session.Hub
	Link *upstream.Link

	Tick      time.Duration // 默认 1 秒
	Rebalance time.Duration // 默认 60 秒
	Compact   time.Duration // 默认 1 小时
	OnError   func(step string, err error)
}

func (r *Runner) report(step string, err error) {
	if err != nil && r.OnError != nil {
		r.OnError(step, err)
	}
}

// Run 运行直至 ctx 结束。
func (r *Runner) Run(ctx context.Context) {
	tick, reb, cmp := r.Tick, r.Rebalance, r.Compact
	if tick == 0 {
		tick = time.Second
	}
	if reb == 0 {
		reb = 60 * time.Second
	}
	if cmp == 0 {
		cmp = time.Hour
	}
	t1, t2, t3 := time.NewTicker(tick), time.NewTicker(reb), time.NewTicker(cmp)
	defer t1.Stop()
	defer t2.Stop()
	defer t3.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t1.C:
			r.report("tick", r.Hub.Tick())
			if r.Link != nil {
				r.report("upstream", r.Link.Tick())
			}
		case <-t2.C:
			r.report("rebalance", r.Hub.Rebalance())
		case <-t3.C:
			_, err := r.Hub.Compact()
			r.report("compact", err)
		}
	}
}
