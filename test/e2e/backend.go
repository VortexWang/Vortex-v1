package e2e

import (
	"encoding/json"
	"sync"
	"time"

	"vortex/edgehub/leases"
	"vortex/edgehub/model"
	"vortex/edgehub/upstream"
	"vortex/engine/types"
)

// Backend 模拟后端的一侧（契约 C2 HubControl.Connect）：在自己的 goroutine 里处理 Edge Hub 的上行消息。
//   - 节点接入（NodeStatus）→ 为该启动推送份额（每个启动一次）；
//   - Prepare 成功 → 下发 Commit；
//   - 账本条目 → 立即累计确认（并记录收到的计量批次与签发）；
//   - HubRegister 声明对账中 → 先下发限制，再 STARTED；收到确认后 COMPLETED。
type Backend struct {
	link   *upstream.Link
	ch     chan upstream.Up
	shares []leases.Lease
	stop   chan struct{}

	mu       sync.Mutex
	Usage    map[[2]model.UUID]map[uint64]bool // (node, boot) → 批次序号
	Issued   int
	Foreign  int
	Prepared []uint64
	Applied  []uint64
	pushed   map[[2]model.UUID]bool
	reconc   bool
}

func NewBackend(link *upstream.Link, shares []leases.Lease) *Backend {
	b := &Backend{link: link, ch: make(chan upstream.Up, 4096), shares: shares, stop: make(chan struct{}),
		Usage: map[[2]model.UUID]map[uint64]bool{}, pushed: map[[2]model.UUID]bool{}}
	go b.loop()
	return b
}

// Connect 建立（或重建）到 Edge Hub 的流。
func (b *Backend) Connect() error { return b.link.Connected(func(m upstream.Up) { b.ch <- m }) }

func (b *Backend) Close() { close(b.stop) }

// SendDesired 下发节点的期望配置。
func (b *Backend) SendDesired(node types.UUID, cfg *types.RuntimeConfig) error {
	raw, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	return b.link.Handle(upstream.Desired{Node: toModel(node), Version: cfg.Version, Config: raw})
}

func (b *Backend) loop() {
	for {
		select {
		case <-b.stop:
			return
		case m := <-b.ch:
			b.handle(m)
		}
	}
}

func (b *Backend) handle(m upstream.Up) {
	switch m := m.(type) {
	case upstream.Register:
		if m.Mode == model.ModeReconciling {
			b.mu.Lock()
			b.reconc = true
			b.mu.Unlock()
			_ = b.link.Handle(upstream.Restrictions{Version: uint64(time.Now().UnixNano())})
			_ = b.link.Handle(upstream.Reconcile{Phase: upstream.ReconcileStarted})
			time.AfterFunc(200*time.Millisecond, func() { // 没有待上报的条目：直接完成
				b.mu.Lock()
				done := b.reconc
				b.reconc = false
				b.mu.Unlock()
				if done {
					b.ch <- completeMsg{}
				}
			})
		}
	case completeMsg:
		_ = b.link.Handle(upstream.Reconcile{Phase: upstream.ReconcileCompleted})
	case upstream.ApplyProgress:
		switch {
		case m.Prepare != nil && m.Prepare.OK:
			b.mu.Lock()
			b.Prepared = append(b.Prepared, m.Prepare.Version)
			b.mu.Unlock()
			_ = b.link.Handle(upstream.Command{Node: m.Node, Version: m.Prepare.Version, Commit: true})
		case m.Apply != nil && m.Apply.Committed:
			b.mu.Lock()
			b.Applied = append(b.Applied, m.Apply.Version)
			b.mu.Unlock()
		}
	case upstream.LedgerEntries:
		b.mu.Lock()
		for _, e := range m.Entries {
			if u := e.Usage; u != nil {
				k := [2]model.UUID{u.Node, u.Boot}
				if b.Usage[k] == nil {
					b.Usage[k] = map[uint64]bool{}
				}
				b.Usage[k][u.Seq] = true
			}
			if e.Issued != nil {
				b.Issued++
			}
		}
		reconc := b.reconc
		b.reconc = false
		b.mu.Unlock()
		_ = b.link.Handle(upstream.LedgerAck{Through: m.Entries[len(m.Entries)-1].Seq})
		if reconc {
			_ = b.link.Handle(upstream.Reconcile{Phase: upstream.ReconcileCompleted})
		}
	case upstream.NodeStatusBatch:
		for _, n := range m.Nodes {
			if !n.Connected {
				continue
			}
			k := [2]model.UUID{n.Node, n.Boot}
			b.mu.Lock()
			first := !b.pushed[k]
			b.pushed[k] = true
			b.mu.Unlock()
			if first {
				_ = b.link.Handle(upstream.Leases{Node: n.Node, Leases: b.shares})
			}
		}
	case upstream.ForeignGrants:
		b.mu.Lock()
		b.Foreign += len(m.Grants)
		b.mu.Unlock()
	}
}

// completeMsg 是 Backend 自己排入队列的“完成对账”。
type completeMsg struct{ upstream.Register }

// Batches 返回后端收到的某节点某启动的计量批次数。
func (b *Backend) Batches(node, boot types.UUID) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.Usage[[2]model.UUID{toModel(node), toModel(boot)}])
}
