// Package mode 是 Edge Hub 的在线 / 自治状态机（HUB-007、HUB-008、HUB-014，03 方案 3.1）：
//
//	online ──(与后端连续 LostAfter 无法通信)──▶ autonomous
//	autonomous ──(恢复通信)──▶ reconciling ──(对账完成)──▶ online
//	reconciling ──(再次失联 LostAfter)──▶ autonomous（起算时刻不变）
//	autonomous / reconciling ──(自治满 MaxAutonomy)──▶ 自治到期：停止续期运行授权，恢复通信后仍须完成对账
//
// 起算时刻在进入自治时写入账本；进程重启后按账本恢复，不重新计时（HUB-008）。
package mode

import (
	"encoding/json"
	"sync"
	"time"

	"vortex/edgehub/clock"
	"vortex/edgehub/ledger"
	"vortex/edgehub/model"
)

const (
	DefaultLostAfter   = 30 * time.Second
	DefaultMaxAutonomy = 12 * time.Hour // Q-01
	KindMode           = "mode"
)

type record struct {
	Mode    model.Mode `json:"mode"`
	Started time.Time  `json:"autonomy_started_at,omitempty"`
	Expired bool       `json:"expired,omitempty"`
}

// Machine 是模式状态机。Contact / Tick / ReconcileCompleted 驱动转换；每次转换写账本并通知监听者
// （由下游服务向全部 Agent 声明，HUB-014）。
type Machine struct {
	LostAfter   time.Duration
	MaxAutonomy time.Duration

	mu          sync.Mutex
	clk         clock.Clock
	l           *ledger.Ledger
	mode        model.Mode
	started     time.Time // 自治起算时刻
	expired     bool      // 本次自治已到期（对账完成前保持）
	lastContact time.Time
	listeners   []func(model.Mode)
}

// New 创建状态机（初始为 online）；恢复时先 Replay 账本条目再 Bind。
func New(clk clock.Clock) *Machine {
	return &Machine{LostAfter: DefaultLostAfter, MaxAutonomy: DefaultMaxAutonomy, clk: clk, mode: model.ModeOnline,
		lastContact: clk.Now()}
}

// Replay 应用一条模式记录。
func (m *Machine) Replay(e ledger.Entry) (bool, error) {
	if e.Kind != KindMode {
		return false, nil
	}
	var r record
	if err := json.Unmarshal(e.Data, &r); err != nil {
		return true, err
	}
	m.mu.Lock()
	m.mode, m.started, m.expired = r.Mode, r.Started, r.Expired
	m.mu.Unlock()
	return true, nil
}

// Bind 在重放完成后绑定账本。重启后的失联计时从此刻开始：给后端 LostAfter 的时间重新建立连接；
// 若重启前已在自治中，保持自治并沿用原起算时刻。
func (m *Machine) Bind(l *ledger.Ledger) {
	m.mu.Lock()
	m.l = l
	m.lastContact = m.clk.Now()
	if m.mode == model.ModeReconciling { // 重启丢失了对账进度：回到自治，恢复通信后重新对账
		m.mode = model.ModeAutonomous
	}
	m.mu.Unlock()
}

// OnChange 登记模式变化的监听者。
func (m *Machine) OnChange(f func(model.Mode)) {
	m.mu.Lock()
	m.listeners = append(m.listeners, f)
	m.mu.Unlock()
}

// Mode 返回当前声明的模式：自治到期且尚未恢复通信时为 AutonomyExpired。
func (m *Machine) Mode() model.Mode {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.declaredLocked()
}

func (m *Machine) declaredLocked() model.Mode {
	if m.mode == model.ModeAutonomous && m.expired {
		return model.ModeAutonomyExpired
	}
	return m.mode
}

// AutonomyStartedAt 返回自治起算时刻（HubRegister.autonomy_started_at）；在线时为零值。
func (m *Machine) AutonomyStartedAt() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mode == model.ModeOnline {
		return time.Time{}
	}
	return m.started
}

// CanRenewRun：自治到期后（直到对账完成回到在线）停止续期运行授权（HUB-009）。
func (m *Machine) CanRenewRun() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return !m.expired
}

// AcceptsConfigChanges：只有在线时接受配置变更、注册、端口与 DNS 指令（HUB-007、HUB-T14）。
func (m *Machine) AcceptsConfigChanges() bool { return m.Mode() == model.ModeOnline }

// Contact 记录一次与后端的成功通信；自治中则转入对账。
func (m *Machine) Contact() error {
	now := m.clk.Now()
	m.mu.Lock()
	m.lastContact = now
	if m.mode != model.ModeAutonomous {
		m.mu.Unlock()
		return nil
	}
	return m.transitionLocked(model.ModeReconciling, m.started, m.expired)
}

// Tick 检查失联与自治到期；由定时器每秒调用。
func (m *Machine) Tick() error {
	now := m.clk.Now()
	m.mu.Lock()
	switch m.mode {
	case model.ModeOnline:
		if now.Sub(m.lastContact) >= m.LostAfter {
			return m.transitionLocked(model.ModeAutonomous, now, false)
		}
	case model.ModeReconciling:
		if now.Sub(m.lastContact) >= m.LostAfter {
			return m.transitionLocked(model.ModeAutonomous, m.started, m.expired)
		}
		if !m.expired && now.Sub(m.started) >= m.MaxAutonomy {
			return m.transitionLocked(model.ModeReconciling, m.started, true)
		}
	case model.ModeAutonomous:
		if !m.expired && now.Sub(m.started) >= m.MaxAutonomy {
			return m.transitionLocked(model.ModeAutonomous, m.started, true)
		}
	}
	m.mu.Unlock()
	return nil
}

// ReconcileCompleted：后端通知对账完成（ReconcileDirective COMPLETED），回到在线。
func (m *Machine) ReconcileCompleted() error {
	m.mu.Lock()
	if m.mode != model.ModeReconciling {
		m.mu.Unlock()
		return nil
	}
	return m.transitionLocked(model.ModeOnline, time.Time{}, false)
}

// transitionLocked 写账本后切换并通知（调用时持有 m.mu，返回前释放）。
func (m *Machine) transitionLocked(to model.Mode, started time.Time, expired bool) error {
	before := m.declaredLocked()
	if m.l != nil {
		if _, err := m.l.Append(KindMode, record{Mode: to, Started: started, Expired: expired}); err != nil {
			m.mu.Unlock()
			return err
		}
	}
	m.mode, m.started, m.expired = to, started, expired
	after := m.declaredLocked()
	ls := append([]func(model.Mode){}, m.listeners...)
	m.mu.Unlock()
	if after != before {
		for _, f := range ls {
			f(after)
		}
	}
	return nil
}
