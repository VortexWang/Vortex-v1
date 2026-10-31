// Package agent 组装节点端 Agent（02 方案 2、3.4～3.6；AGT-T19）：嵌入的 Engine、计量（report）、授权（grants）、
// 配置协调（reconcile）与控制流（Session）。
//
// 事件循环（Run）：
//   - 每秒：DrainUsage → 落盘 → 记录计费授权消耗（final 的移出）→ 续期到期的运行授权与份额；
//   - 每 10 秒：封批并上报、磁盘水位检查、保留期清理；
//   - Engine 事件：grant_low / grant_needed 申请计费授权，其余（限制、拒绝审计、对端断开）转发给 Edge Hub；
//   - Edge Hub 消息：HubHello（实例变化时先报告持有授权，再重发未确认批次，再申请启动授权）、期望配置、
//     Commit / Abort、授权应答、计量确认、模式变化。
//
// 全部处理串行进行（一把锁）；Session.Send 不阻塞。控制连接断开时不做任何主动停止（AGT-011）。
package agent

import (
	"context"
	"sync"
	"time"

	"vortex/engine"
	"vortex/engine/types"
	"vortex/node/agent/boot"
	"vortex/node/agent/grants"
	"vortex/node/agent/probe"
	"vortex/node/agent/reconcile"
	"vortex/node/agent/report"
	"vortex/node/agent/store"
	"vortex/node/internal/clock"
)

// DefaultDrainTimeout 是维护排空的默认超时（NOD-003）。
const DefaultDrainTimeout = 300 * time.Second

type Options struct {
	Node         types.UUID
	AgentVersion string
	Engine       engine.Engine
	Clock        clock.Clock // nil = 系统时钟
	BootLog      store.Log
	UsageLog     store.Log
	ConfigLog    store.Log
	Disk         *report.DiskGuard // nil = 不检查
	// OnApplied 在配置生效（Commit 成功，含启动恢复）后调用，例如更新探测对象。在 Agent 的锁内调用，不得回调 Agent。
	OnApplied func(*types.RuntimeConfig)
}

type Agent struct {
	mu    sync.Mutex
	opt   Options
	clk   clock.Clock
	eng   engine.Engine
	boot  boot.Boot
	rep   *report.Reporter
	gr    *grants.Manager
	rec   *reconcile.Reconciler
	sess  Session
	hub   types.UUID
	mode  Mode
	rates *rateWindow
	low   bool // 磁盘低水位
}

// New 开始一次启动：生成 boot_id、打开计量（上次启动未封的部分封为其最后一批）、恢复最后生效配置并 Prepare。
func New(ctx context.Context, opt Options) (*Agent, error) {
	clk := opt.Clock
	if clk == nil {
		clk = clock.System{}
	}
	b, err := boot.Start(opt.BootLog)
	if err != nil {
		return nil, err
	}
	rep, err := report.Open(clk, opt.UsageLog, b.Current)
	if err != nil {
		return nil, err
	}
	a := &Agent{opt: opt, clk: clk, eng: opt.Engine, boot: b, rep: rep, gr: grants.New(clk, b.Current),
		rec: reconcile.New(opt.Engine, opt.ConfigLog), rates: newRateWindow(60)}
	if _, err := a.rec.Recover(ctx); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *Agent) sendLocked(m Out) {
	if a.sess != nil {
		_ = a.sess.Send(m) // 失败由连接层发现并调用 Disconnected
	}
}

// Hello 返回本次启动的 AgentHello（每条流的第一条消息）。
func (a *Agent) Hello() Hello {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.helloLocked()
}

func (a *Agent) helloLocked() Hello {
	return Hello{Node: a.opt.Node, Boot: a.boot.Current, PreviousBoot: a.boot.Previous,
		AgentVersion: a.opt.AgentVersion, AppliedConfigVersion: a.rec.AppliedVersion(), UnackedUsageFromSeq: a.rep.FirstUnacked()}
}

// Connected 在新的控制连接建立后调用：发送 AgentHello。
func (a *Agent) Connected(s Session) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sess = s
	a.sendLocked(a.helloLocked())
}

// Adopt 接管一条已发送过 Hello 的流（实例切换时由探测连接转正，HUB-014）。随后应把该流已收到的
// HubHello 交给 HandleHub。
func (a *Agent) Adopt(s Session) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sess = s
}

// Disconnected：控制连接断开。授权保留，Engine 照常转发直至授权到期（AGT-011）。
func (a *Agent) Disconnected() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sess, a.mode = nil, ModeUnspecified
	a.gr.Disconnected()
}

// HandleHub 处理一条 Edge Hub 消息。
func (a *Agent) HandleHub(ctx context.Context, m In) {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch m := m.(type) {
	case HubHello:
		a.hub, a.mode = m.Hub, m.Mode
		if held, switched := a.gr.Connected(m.Hub); switched {
			a.sendLocked(HeldGrants{held}) // HUB-014：切换后第一件事
		}
		for _, b := range a.rep.Pending() { // 上次启动的在前（AGT-009）
			a.sendLocked(UsageBatch{b})
		}
		if r := a.gr.Bootstrap(); r != nil {
			a.sendLocked(GrantRequest{*r})
		}
	case ModeChange:
		a.mode = m.Mode
	case DesiredConfig:
		r := a.rec.Desired(ctx, m.Config)
		if r.Prepare != nil || r.Err != "" {
			a.sendLocked(PrepareReport{Version: r.Version, Result: r.Prepare, Err: r.Err})
		}
	case ApplyCommand:
		r := a.rec.Command(ctx, m.Version, m.Action)
		a.sendLocked(ApplyReport{Version: r.Version, Action: r.Action, Result: r.Apply, Err: r.Err})
		if r.Apply != nil {
			a.appliedLocked()
		}
	case GrantResponse:
		changed, err := a.gr.Apply(a.hub, m.Response)
		if err != nil {
			return
		}
		if changed {
			a.eng.UpdateGrants(a.gr.EngineSet())
		}
		a.maybeCommitRecoveryLocked(ctx)
	case UsageAck:
		_ = a.rep.Ack(m.Boot, m.Through)
	case DrainCommand:
		d := m.Timeout
		if d == 0 {
			d = DefaultDrainTimeout
		}
		go func() { // 排空可能持续数分钟，不阻塞控制流
			dctx, cancel := context.WithTimeout(context.Background(), d)
			defer cancel()
			_ = a.Drain(dctx, m.Rules)
		}()
	}
}

func (a *Agent) maybeCommitRecoveryLocked(ctx context.Context) {
	run := a.gr.RunRemaining()
	r := a.rec.GrantsUpdated(ctx, func(rule types.UUID) bool { return run[rule] > 0 }, a.gr.Answered())
	if r != nil {
		a.sendLocked(ApplyReport{Version: r.Version, Action: r.Action, Result: r.Apply, Err: r.Err})
		if r.Apply != nil {
			a.appliedLocked()
		}
	}
}

func (a *Agent) appliedLocked() {
	if a.opt.OnApplied != nil {
		a.opt.OnApplied(a.rec.Applied())
	}
}

// ReportProbes 上报一轮探测结果（AGT-007）。
func (a *Agent) ReportProbes(rs []probe.Result) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sendLocked(ProbeReport{rs})
}

// Drain 维护排空（NOD-003）：停止范围内规则的新会话并等待已有会话结束，ctx 结束时关闭剩余会话。
// 不持有 Agent 的锁，排空期间计量与授权照常处理。
func (a *Agent) Drain(ctx context.Context, rules []types.UUID) error {
	return a.eng.Drain(ctx, engine.DrainScope{RuleIDs: rules})
}

// Tick 是每秒的工作。
func (a *Agent) Tick(ctx context.Context) {
	a.mu.Lock()
	defer a.mu.Unlock()
	snap := a.eng.DrainUsage()
	if err := a.rep.Add(snap); err != nil {
		// 落盘失败：这一秒的计量只在内存的消耗记录里（Engine 的累计已用量会在下一次快照中再次出现），
		// 增量本身丢失。磁盘水位保护会让 Engine 停止新增业务。
		_ = err
	}
	a.rates.add(snap)
	changed := a.gr.Observe(snap.Consumptions)
	for _, r := range a.gr.Due() {
		a.sendLocked(GrantRequest{r})
	}
	if r := a.gr.Bootstrap(); r != nil {
		a.sendLocked(GrantRequest{*r})
	}
	if changed {
		a.eng.UpdateGrants(a.gr.EngineSet())
	}
}

// SealTick 是每 10 秒的工作：封批上报、磁盘水位、保留期清理。返回封的批次（无则 nil）。
func (a *Agent) SealTick() *report.Batch {
	a.mu.Lock()
	defer a.mu.Unlock()
	b, _ := a.rep.Seal()
	if b != nil {
		a.sendLocked(UsageBatch{*b})
	}
	if a.opt.Disk != nil {
		low, _ := a.opt.Disk.Check()
		a.low = low
	}
	_, _ = a.rep.Prune()
	return b
}

// HandleEvent 处理一条 Engine 事件。
func (a *Agent) HandleEvent(ev types.Event) {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch ev.Kind {
	case types.EventGrantLow, types.EventGrantNeeded:
		r, err := a.gr.OnEvent(ev, a.rates.rate(ev.RuleID))
		if err == nil && r != nil {
			a.sendLocked(GrantRequest{*r})
		}
	default:
		a.sendLocked(EngineEvent{ev})
	}
}

// ReportState 上报 Engine 的实际状态（定期，或配置变化后）。
func (a *Agent) ReportState() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sendLocked(StateReport{a.eng.State()})
}

// Run 运行事件循环直至 ctx 结束；结束前做最后一次拉取与封批。
func (a *Agent) Run(ctx context.Context) {
	sec := time.NewTicker(time.Second)
	seal := time.NewTicker(report.SealEvery)
	state := time.NewTicker(time.Minute)
	defer sec.Stop()
	defer seal.Stop()
	defer state.Stop()
	for {
		select {
		case <-ctx.Done():
			a.Tick(context.Background())
			a.SealTick()
			return
		case ev := <-a.eng.Events():
			a.HandleEvent(ev)
		case <-sec.C:
			a.Tick(ctx)
		case <-seal.C:
			a.SealTick()
		case <-state.C:
			a.ReportState()
		}
	}
}

// Status 是 status 子命令的内容（02 方案 3.10）。
type Status struct {
	Node, Boot, PreviousBoot types.UUID
	Hub                      types.UUID
	Mode                     Mode
	Connected                bool
	Phase                    reconcile.Phase
	AppliedVersion           uint64
	ActualVersion            uint64
	RunRemaining             map[types.UUID]time.Duration
	PendingBatches           int
	DiskLow                  bool
}

func (a *Agent) Status() Status {
	a.mu.Lock()
	defer a.mu.Unlock()
	return Status{Node: a.opt.Node, Boot: a.boot.Current, PreviousBoot: a.boot.Previous, Hub: a.hub, Mode: a.mode,
		Connected: a.sess != nil, Phase: a.rec.Phase(), AppliedVersion: a.rec.AppliedVersion(),
		ActualVersion: a.eng.State().ActualVersion, RunRemaining: a.gr.RunRemaining(),
		PendingBatches: a.rep.Stats().Pending, DiskLow: a.low}
}

// ---------------------------------------------------------------- 速率窗口

// rateWindow 记录执行点规则近 60 秒的原始字节（计费授权申请的额度依据，ENG-037）。
type rateWindow struct {
	n     int
	slots []map[types.UUID]uint64
	i     int
}

func newRateWindow(n int) *rateWindow {
	w := &rateWindow{n: n, slots: make([]map[types.UUID]uint64, n)}
	for i := range w.slots {
		w.slots[i] = map[types.UUID]uint64{}
	}
	return w
}

func (w *rateWindow) add(s types.UsageSnapshot) {
	w.i = (w.i + 1) % w.n
	w.slots[w.i] = map[types.UUID]uint64{}
	for _, d := range s.Deltas {
		if d.BillingGrantID != (types.UUID{}) { // 只计执行点的计费流量
			w.slots[w.i][d.RuleID] += d.RawBytes
		}
	}
}

// rate 返回近 60 秒的平均速率（bit/s）。
func (w *rateWindow) rate(rule types.UUID) uint64 {
	var sum uint64
	for _, s := range w.slots {
		sum += s[rule]
	}
	return sum * 8 / uint64(w.n)
}
