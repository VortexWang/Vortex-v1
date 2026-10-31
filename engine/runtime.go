package engine

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"vortex/engine/gate"
	"vortex/engine/internal/lifecycle"
	"vortex/engine/pipe"
	"vortex/engine/route"
	"vortex/engine/types"
)

// 配置应用错误（ENG-030～ENG-033）。校验错误使 Prepare 整体失败、不留下未决版本；
// 资源错误（端口占用等）写入 PrepareResult.Errors，由调用方决定 Commit（部分应用）或 Abort（ENG-034）。
var (
	ErrStaleVersion   = errors.New("engine: version not newer than the active one")
	ErrPendingVersion = errors.New("engine: another version is already prepared")
	ErrNoPending      = errors.New("engine: version is not the prepared one")
	ErrInvalidConfig  = errors.New("engine: invalid runtime config")
	ErrDrainAborted   = errors.New("engine: drain deadline reached, remaining sessions closed")
)

// 资源错误码（runtime.v1.ResourceError.code）。
const (
	CodePortInUse      = "PORT_IN_USE"
	CodeBindFailed     = "BIND_FAILED"
	CodeNotImplemented = "NOT_IMPLEMENTED" // M0 只有直接转发；下一跳（隧道）在 M1 实现
)

// Options 是运行时的构造参数。
type Options struct {
	SelfAddrs   []netip.Addr      // 本机地址，供目标检查拒绝回环到自身（SEC-011）
	Lookup      route.LookupFunc  // nil = 系统解析器
	Gate        gate.Options      // Emit 由运行时接管
	EventBuffer int               // Events 通道容量，默认 4096；满时丢弃最新事件并计数
	Backlog     int               // TCP listen backlog，默认 4096
	DialTimeout time.Duration     // 默认 5 秒
	Healthy     func(string) bool // 目标探测结果；nil 表示全部健康
}

type listenerKey struct {
	network types.Network
	bind    netip.AddrPort
}

func keyOf(l types.Listener) listenerKey { return listenerKey{l.Network, l.Bind} }

func (k listenerKey) id() string {
	n := "tcp"
	if k.network == types.NetworkUDP {
		n = "udp"
	}
	return n + "/" + k.bind.String()
}

// activeListener 是一个已提交的监听及其入口；Scope 拥有 socket 与全部会话。
type activeListener struct {
	key     listenerKey
	rule    types.UUID
	version uint64 // 最后一次被应用（新建或改绑）的配置版本
	scope   *lifecycle.Scope
	addr    netip.AddrPort
	tcp     *route.DirectEntry
	udp     *route.UDPEntry
}

func (a *activeListener) sessions() int {
	if a.tcp != nil {
		return int(a.tcp.Active.Load())
	}
	return a.udp.Sessions()
}

func (a *activeListener) setRule(id types.UUID) {
	a.rule = id
	if a.tcp != nil {
		a.tcp.SetRule(id)
	} else {
		a.udp.SetRule(id)
	}
}

type pending struct {
	cfg      *types.RuntimeConfig
	reserved map[listenerKey]*route.Reserved
	failed   map[listenerKey]types.ResourceError
	rebind   map[listenerKey]types.UUID
	stopL    []listenerKey
	revoke   map[types.UUID]bool // 删除或权限收紧的规则：已有会话在提交时关闭（ENG-032）
	unsupp   map[types.UUID]bool
}

// Runtime 是 Engine 的进程内实现：入口（route）→ 门控（gate，含计量）→ 管道（pipe）。
type Runtime struct {
	opt      Options
	root     *lifecycle.Scope
	tables   route.Tables
	gate     *gate.Gate
	resolver *route.Resolver
	guard    atomic.Pointer[route.Guard]
	events   chan types.Event
	dropped  atomic.Uint64

	mu        sync.Mutex // 串行化 Prepare / Commit / Abort / Drain
	cfg       *types.RuntimeConfig
	ruleVer   map[types.UUID]uint64 // 规则 → 最后应用的配置版本
	unsupp    map[types.UUID]bool
	listeners map[listenerKey]*activeListener
	failed    map[listenerKey]types.ResourceError
	pend      *pending

	adm admission
}

var _ Engine = (*Runtime)(nil)

// New 构造运行时；Close 停止全部监听与会话。
func New(opt Options) *Runtime {
	if opt.EventBuffer <= 0 {
		opt.EventBuffer = 4096
	}
	if opt.Backlog <= 0 {
		opt.Backlog = 4096
	}
	rt := &Runtime{opt: opt, root: lifecycle.NewRoot(context.Background()), events: make(chan types.Event, opt.EventBuffer),
		resolver: route.NewResolver(opt.Lookup, 30*time.Second),
		ruleVer:  map[types.UUID]uint64{}, unsupp: map[types.UUID]bool{},
		listeners: map[listenerKey]*activeListener{}, failed: map[listenerKey]types.ResourceError{}}
	gopt := opt.Gate
	gopt.Emit = rt.emit
	rt.gate = gate.New(gopt)
	rt.adm = admission{next: rt.gate, live: map[*types.Rule]map[*admitted]struct{}{},
		revoked: map[*types.Rule]time.Time{}, draining: map[types.UUID]bool{}}
	rt.guard.Store(route.NewGuard(types.TargetPolicy{}, opt.SelfAddrs, nil))
	return rt
}

// Close 停止全部监听与会话并等待其 goroutine 退出。
func (rt *Runtime) Close() error {
	rt.mu.Lock()
	if p := rt.pend; p != nil {
		for _, r := range p.reserved {
			_ = r.Abort()
		}
		rt.pend = nil
	}
	rt.mu.Unlock()
	rt.root.Shutdown()
	rt.root.Wait()
	return nil
}

func (rt *Runtime) emit(ev types.Event) {
	if ev.At == 0 {
		ev.At = rt.gate.Now()
	}
	select {
	case rt.events <- ev:
	default:
		rt.dropped.Add(1) // 事件只是提示（低水位、需要授权等），Agent 还会定期拉取用量与状态
	}
}

// DroppedEvents 返回因通道已满而丢弃的事件数。
func (rt *Runtime) DroppedEvents() uint64 { return rt.dropped.Load() }

func (rt *Runtime) Events() <-chan types.Event       { return rt.events }
func (rt *Runtime) UpdateGrants(set *types.GrantSet) { rt.gate.UpdateGrants(set) }
func (rt *Runtime) DrainUsage() types.UsageSnapshot  { return rt.gate.UsageSnapshot() }

// ListenerAddr 返回监听实际绑定的地址（配置端口为 0 时由内核分配）；未生效时返回零值。
func (rt *Runtime) ListenerAddr(l types.Listener) netip.AddrPort {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if a := rt.listeners[keyOf(l)]; a != nil {
		return a.addr
	}
	return netip.AddrPort{}
}

// ---------------------------------------------------------------- Prepare

func validate(cfg *types.RuntimeConfig) error {
	rules := map[types.UUID]*types.Rule{}
	for i := range cfg.Rules {
		r := &cfg.Rules[i]
		if _, dup := rules[r.RuleID]; dup {
			return fmt.Errorf("%w: duplicate rule %x", ErrInvalidConfig, r.RuleID)
		}
		if (r.NextHop == nil) == (len(r.Targets) == 0) {
			return fmt.Errorf("%w: rule %x must have exactly one of next_hop / targets", ErrInvalidConfig, r.RuleID)
		}
		rules[r.RuleID] = r
	}
	seen := map[listenerKey]bool{}
	for _, l := range cfg.Listeners {
		k := keyOf(l)
		if seen[k] {
			return fmt.Errorf("%w: duplicate listener %s", ErrInvalidConfig, k.id())
		}
		seen[k] = true
		if l.Network != types.NetworkTCP && l.Network != types.NetworkUDP {
			return fmt.Errorf("%w: listener %s has no network", ErrInvalidConfig, k.id())
		}
		r := rules[l.RuleID]
		if r == nil {
			return fmt.Errorf("%w: listener %s references unknown rule %x", ErrInvalidConfig, k.id(), l.RuleID)
		}
		if !slices.Contains(r.Networks, l.Network) {
			return fmt.Errorf("%w: listener %s network not enabled on rule %x", ErrInvalidConfig, k.id(), l.RuleID)
		}
	}
	return nil
}

func targetKeys(r *types.Rule) map[string]bool {
	m := make(map[string]bool, len(r.Targets))
	for _, t := range r.Targets {
		m[net.JoinHostPort(t.Host, fmt.Sprint(t.Port))] = true
	}
	return m
}

// tightened 判断规则变更是否属于权限收紧（ENG-032）：租户、执行点身份、允许上游变化，或移除了目标。
// 其余字段（限速、连接数、调度、空闲超时、PROXY protocol、新增目标等）为 hot：已有会话保持旧快照。
func tightened(old, cur *types.Rule) bool {
	if old.TenantID != cur.TenantID || old.EnforcementPoint != cur.EnforcementPoint || old.Role != cur.Role {
		return true
	}
	if !slices.Equal(old.AllowedUpstreams, cur.AllowedUpstreams) {
		return true
	}
	nk := targetKeys(cur)
	for k := range targetKeys(old) {
		if !nk[k] {
			return true
		}
	}
	for _, n := range old.Networks {
		if !slices.Contains(cur.Networks, n) {
			return true
		}
	}
	return false
}

func ruleEqual(a, b *types.Rule) bool {
	return fmt.Sprintf("%#v", *a) == fmt.Sprintf("%#v", *b)
}

// Prepare 校验 → 差异分类 → 为新增监听执行 bind（不 listen / 不读取）（ENG-030、ENG-031）。
func (rt *Runtime) Prepare(ctx context.Context, cfg *types.RuntimeConfig) (*types.PrepareResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validate(cfg); err != nil {
		return nil, err
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.root.Closed() {
		return nil, lifecycle.ErrClosed
	}
	if rt.pend != nil {
		return nil, ErrPendingVersion
	}
	if rt.cfg != nil && cfg.Version <= rt.cfg.Version {
		return nil, ErrStaleVersion
	}

	p := &pending{cfg: cfg, reserved: map[listenerKey]*route.Reserved{}, failed: map[listenerKey]types.ResourceError{},
		rebind: map[listenerKey]types.UUID{}, revoke: map[types.UUID]bool{}, unsupp: map[types.UUID]bool{}}
	res := &types.PrepareResult{Version: cfg.Version}
	change := func(kind types.ObjectKind, id string, c types.ChangeClass, n int) {
		res.Changes = append(res.Changes, types.ObjectChange{Object: types.ObjectRef{Kind: kind, ID: id}, Class: c, AffectedSessions: uint32(n)})
	}

	// 规则
	old := map[types.UUID]*types.Rule{}
	if rt.cfg != nil {
		for i := range rt.cfg.Rules {
			old[rt.cfg.Rules[i].RuleID] = &rt.cfg.Rules[i]
		}
	}
	for i := range cfg.Rules {
		r := &cfg.Rules[i]
		id := uuidString(r.RuleID)
		if r.NextHop != nil {
			p.unsupp[r.RuleID] = true
			res.Errors = append(res.Errors, types.ResourceError{Object: types.ObjectRef{Kind: types.ObjectRule, ID: id},
				Code: CodeNotImplemented, Detail: "next_hop forwarding is not available in this engine build"})
		}
		o := old[r.RuleID]
		switch {
		case o == nil:
			change(types.ObjectRule, id, types.ChangeHot, 0)
		case ruleEqual(o, r):
			change(types.ObjectRule, id, types.ChangeNone, 0)
		case tightened(o, r):
			p.revoke[r.RuleID] = true
			change(types.ObjectRule, id, types.ChangeStop, rt.adm.count(r.RuleID))
		default:
			change(types.ObjectRule, id, types.ChangeHot, 0)
		}
		delete(old, r.RuleID)
	}
	for rid := range old {
		p.revoke[rid] = true
		change(types.ObjectRule, uuidString(rid), types.ChangeStop, rt.adm.count(rid))
	}

	// 监听
	want := map[listenerKey]types.Listener{}
	for _, l := range cfg.Listeners {
		want[keyOf(l)] = l
	}
	for k, a := range rt.listeners {
		l, ok := want[k]
		switch {
		case !ok:
			p.stopL = append(p.stopL, k)
			change(types.ObjectListener, k.id(), types.ChangeStop, a.sessions())
		case l.RuleID != a.rule:
			// 改绑规则：socket 保留，新会话使用新规则；已有会话保持旧快照（ENG-032 rebuild）
			p.rebind[k] = l.RuleID
			change(types.ObjectListener, k.id(), types.ChangeRebuild, a.sessions())
		default:
			change(types.ObjectListener, k.id(), types.ChangeNone, 0)
		}
	}
	keys := make([]listenerKey, 0, len(want))
	for k := range want {
		if rt.listeners[k] == nil {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].id() < keys[j].id() })
	for _, k := range keys {
		var (
			r   *route.Reserved
			err error
		)
		if k.network == types.NetworkTCP {
			r, err = route.ReserveTCP(k.bind)
		} else {
			r, err = route.ReserveUDP(k.bind)
		}
		if err != nil {
			code := CodeBindFailed
			if errors.Is(err, route.ErrPortInUse) {
				code = CodePortInUse
			}
			e := types.ResourceError{Object: types.ObjectRef{Kind: types.ObjectListener, ID: k.id()}, Code: code, Detail: err.Error()}
			p.failed[k] = e
			res.Errors = append(res.Errors, e)
			continue
		}
		p.reserved[k] = r
		change(types.ObjectListener, k.id(), types.ChangeHot, 0)
	}
	rt.pend = p
	return res, nil
}

// ---------------------------------------------------------------- Commit / Abort

// Commit 开始监听与接收，整体原子切换规则表；删除与权限收紧的规则关闭已有会话（ENG-032）。
// 预留失败的对象报告为 failed，其余照常生效（部分应用由后端编排，ENG-034）。
func (rt *Runtime) Commit(ctx context.Context, version uint64) (*types.ApplyResult, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	p := rt.pend
	if p == nil || p.cfg.Version != version {
		return nil, ErrNoPending
	}
	rt.pend = nil
	cfg := p.cfg

	// 目标策略先于规则表生效：新规则的会话不会按旧策略检查目标
	var selfPorts []uint16
	for _, l := range cfg.Listeners {
		selfPorts = append(selfPorts, l.Bind.Port())
	}
	for k, r := range p.reserved {
		if k.bind.Port() == 0 {
			selfPorts = append(selfPorts, r.Bind.Port())
		}
	}
	rt.guard.Store(route.NewGuard(cfg.TargetPolicy, rt.opt.SelfAddrs, selfPorts))

	prev, ok := rt.tables.Swap(route.NewTable(cfg.Version, cfg.Rules))
	if !ok {
		for _, r := range p.reserved {
			_ = r.Abort()
		}
		return nil, ErrStaleVersion
	}

	// 删除与收紧的规则：先吊销旧快照（之后用旧快照的准入被拒），再关闭已有会话
	rt.adm.revokeRules(p.revoke, prev)

	for _, k := range p.stopL {
		a := rt.listeners[k]
		delete(rt.listeners, k)
		a.scope.Shutdown() // 停止接入并关闭该监听的全部会话
	}
	for k, id := range p.rebind {
		a := rt.listeners[k]
		a.setRule(id)
		a.version = cfg.Version
	}
	rt.failed = p.failed
	for k, r := range p.reserved {
		a, err := rt.start(k, r, cfg)
		if err != nil {
			rt.failed[k] = types.ResourceError{Object: types.ObjectRef{Kind: types.ObjectListener, ID: k.id()}, Code: CodeBindFailed, Detail: err.Error()}
			continue
		}
		rt.listeners[k] = a
	}

	for i := range cfg.Rules {
		r := &cfg.Rules[i]
		var o *types.Rule
		if rt.cfg != nil {
			for j := range rt.cfg.Rules {
				if rt.cfg.Rules[j].RuleID == r.RuleID {
					o = &rt.cfg.Rules[j]
				}
			}
		}
		if o == nil || !ruleEqual(o, r) {
			rt.ruleVer[r.RuleID] = cfg.Version
		}
	}
	for id := range rt.ruleVer {
		if !slices.ContainsFunc(cfg.Rules, func(r types.Rule) bool { return r.RuleID == id }) {
			delete(rt.ruleVer, id)
		}
	}
	rt.unsupp = p.unsupp
	rt.cfg = cfg
	return &types.ApplyResult{Version: cfg.Version, Objects: rt.objectsLocked()}, nil
}

func (rt *Runtime) start(k listenerKey, r *route.Reserved, cfg *types.RuntimeConfig) (*activeListener, error) {
	var rule types.UUID
	for _, l := range cfg.Listeners {
		if keyOf(l) == k {
			rule = l.RuleID
		}
	}
	a := &activeListener{key: k, rule: rule, version: cfg.Version, scope: rt.root.Child(), addr: r.Bind}
	if k.network == types.NetworkTCP {
		ln, err := r.CommitTCP(rt.opt.Backlog)
		if err != nil {
			a.scope.Shutdown()
			return nil, err
		}
		a.tcp = &route.DirectEntry{RuleID: rule, Tables: &rt.tables, Listener: ln, Scope: a.scope, Admission: &rt.adm,
			Resolver: rt.resolver, GuardRef: &rt.guard, Healthy: rt.opt.Healthy, Events: rt.emit, DialTimeout: rt.opt.DialTimeout}
		if err := a.tcp.Start(); err != nil {
			_ = ln.Close()
			a.scope.Shutdown()
			return nil, err
		}
		return a, nil
	}
	conn, _, err := r.CommitUDP()
	if err != nil {
		a.scope.Shutdown()
		return nil, err
	}
	a.udp = &route.UDPEntry{RuleID: rule, Tables: &rt.tables, Conn: conn, Scope: a.scope, Admission: &rt.adm,
		Resolver: rt.resolver, GuardRef: &rt.guard, Healthy: rt.opt.Healthy, Events: rt.emit}
	if err := a.udp.Start(); err != nil {
		_ = r.ReleaseUDP(conn)
		a.scope.Shutdown()
		return nil, err
	}
	return a, nil
}

// Abort 只释放本次预留，不影响已生效对象（ENG-033）。
func (rt *Runtime) Abort(_ context.Context, version uint64) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	p := rt.pend
	if p == nil || p.cfg.Version != version {
		return ErrNoPending
	}
	rt.pend = nil
	var first error
	for _, r := range p.reserved {
		if err := r.Abort(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// ---------------------------------------------------------------- Drain / State

// Drain 停止范围内规则的新会话并等待已有会话结束；ctx 结束时关闭剩余会话并返回 ErrDrainAborted（NOD-003）。
// 范围为空表示全部规则。排空状态保持到进程结束（维护排空之后是停止或升级）。
func (rt *Runtime) Drain(ctx context.Context, scope DrainScope) error {
	rt.adm.drain(scope.RuleIDs)
	t := time.NewTicker(20 * time.Millisecond)
	defer t.Stop()
	for {
		if rt.adm.countScope(scope.RuleIDs) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			rt.adm.killScope(scope.RuleIDs)
			return ErrDrainAborted
		case <-t.C:
		}
	}
}

// State 如实报告每个对象的实际版本与状态（ENG-034）。
func (rt *Runtime) State() types.RuntimeState {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	var v uint64
	if rt.cfg != nil {
		v = rt.cfg.Version
	}
	return types.RuntimeState{ActualVersion: v, Objects: rt.objectsLocked()}
}

func (rt *Runtime) objectsLocked() []types.ObjectState {
	var out []types.ObjectState
	if rt.cfg != nil {
		for _, r := range rt.cfg.Rules {
			st := types.ObjectState{Object: types.ObjectRef{Kind: types.ObjectRule, ID: uuidString(r.RuleID)},
				ActualVersion: rt.ruleVer[r.RuleID], Status: types.StatusActive}
			if rt.unsupp[r.RuleID] {
				st.Status, st.ErrorCode = types.StatusFailed, CodeNotImplemented
			}
			out = append(out, st)
		}
	}
	for k, a := range rt.listeners {
		out = append(out, types.ObjectState{Object: types.ObjectRef{Kind: types.ObjectListener, ID: k.id()},
			ActualVersion: a.version, Status: types.StatusActive})
	}
	for _, e := range rt.failed {
		out = append(out, types.ObjectState{Object: e.Object, Status: types.StatusFailed, ErrorCode: e.Code})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Object.Kind != out[j].Object.Kind {
			return out[i].Object.Kind < out[j].Object.Kind
		}
		return out[i].Object.ID < out[j].Object.ID
	})
	return out
}

func uuidString(u types.UUID) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
}

// ---------------------------------------------------------------- 准入包装

// admission 包在门控外面，按规则快照登记会话，用于：Prepare 统计受影响会话数、Commit 关闭被删除或收紧的
// 规则的会话、Drain 停止新接入。
type admission struct {
	next route.Admission

	mu       sync.Mutex
	live     map[*types.Rule]map[*admitted]struct{}
	revoked  map[*types.Rule]time.Time // 被吊销的规则快照；拿着它的准入一律拒绝
	draining map[types.UUID]bool
	drainAll bool
}

type admitted struct{ kill func() }

var (
	errRevoked  = errors.New("no_route: rule snapshot revoked")
	errDraining = errors.New("draining")
)

func (a *admission) Admit(ctx context.Context, rule *types.Rule, client netip.AddrPort, kill func()) (pipe.Hook, func(), error) {
	a.mu.Lock()
	if _, bad := a.revoked[rule]; bad {
		a.mu.Unlock()
		return nil, nil, errRevoked
	}
	if a.drainAll || a.draining[rule.RuleID] {
		a.mu.Unlock()
		return nil, nil, errDraining
	}
	// 先登记再交给门控：Commit 的吊销与关闭和这里持同一把锁，登记之后发生的吊销一定能关闭本会话
	s := &admitted{kill: kill}
	m := a.live[rule]
	if m == nil {
		m = map[*admitted]struct{}{}
		a.live[rule] = m
	}
	m[s] = struct{}{}
	a.mu.Unlock()

	unregister := func() {
		a.mu.Lock()
		if m := a.live[rule]; m != nil {
			delete(m, s)
			if len(m) == 0 {
				delete(a.live, rule)
			}
		}
		a.mu.Unlock()
	}
	hook, release, err := a.next.Admit(ctx, rule, client, kill)
	if err != nil {
		unregister()
		return nil, nil, err
	}
	var once sync.Once
	return hook, func() { once.Do(func() { release(); unregister() }) }, nil
}

func (a *admission) count(id types.UUID) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for r, m := range a.live {
		if r.RuleID == id {
			n += len(m)
		}
	}
	return n
}

func inScope(ids []types.UUID, id types.UUID) bool { return len(ids) == 0 || slices.Contains(ids, id) }

func (a *admission) countScope(ids []types.UUID) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for r, m := range a.live {
		if inScope(ids, r.RuleID) {
			n += len(m)
		}
	}
	return n
}

func (a *admission) killScope(ids []types.UUID) {
	a.mu.Lock()
	var kills []func()
	for r, m := range a.live {
		if inScope(ids, r.RuleID) {
			for s := range m {
				kills = append(kills, s.kill)
			}
		}
	}
	a.mu.Unlock()
	for _, k := range kills {
		k()
	}
}

func (a *admission) drain(ids []types.UUID) {
	a.mu.Lock()
	if len(ids) == 0 {
		a.drainAll = true
	}
	for _, id := range ids {
		a.draining[id] = true
	}
	a.mu.Unlock()
}

// revokeRules 吊销这些规则的全部现存快照并关闭其会话。准入在读取快照后立即进行，
// 一分钟后仍未完成准入的旧快照不存在，因此吊销记录保留一分钟即可清理。
func (a *admission) revokeRules(ids map[types.UUID]bool, prev *route.Table) {
	now := time.Now()
	a.mu.Lock()
	for r, t := range a.revoked {
		if now.Sub(t) > time.Minute {
			delete(a.revoked, r)
		}
	}
	for id := range ids {
		if r := prev.Rule(id); r != nil { // 切换前读到旧快照、尚未准入的会话
			a.revoked[r] = now
		}
	}
	var kills []func()
	for r, m := range a.live {
		if ids[r.RuleID] {
			a.revoked[r] = now
			for s := range m {
				kills = append(kills, s.kill)
			}
		}
	}
	a.mu.Unlock()
	for _, k := range kills {
		k()
	}
}
