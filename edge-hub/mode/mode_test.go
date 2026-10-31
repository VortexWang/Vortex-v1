package mode

import (
	"testing"
	"time"

	"vortex/edgehub/clock"
	"vortex/edgehub/ledger"
	"vortex/edgehub/model"
)

func newMachine(t *testing.T, clk *clock.Fake, st ledger.Store) *Machine {
	m := New(clk)
	l, err := ledger.Open(st, clk, func(e ledger.Entry) error { _, err := m.Replay(e); return err })
	if err != nil {
		t.Fatal(err)
	}
	m.Bind(l)
	return m
}

func TestModeTransitions(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC))
	st := &ledger.MemStore{}
	m := newMachine(t, clk, st)
	var seen []model.Mode
	m.OnChange(func(md model.Mode) { seen = append(seen, md) })

	clk.Advance(29 * time.Second)
	_ = m.Tick()
	if m.Mode() != model.ModeOnline || !m.AcceptsConfigChanges() {
		t.Fatal("must stay online before 30 s without contact")
	}
	clk.Advance(time.Second)
	_ = m.Tick()
	start := clk.Now()
	if m.Mode() != model.ModeAutonomous || !m.AutonomyStartedAt().Equal(start) || m.AcceptsConfigChanges() {
		t.Fatalf("after 30 s: %v %v", m.Mode(), m.AutonomyStartedAt())
	}
	clk.Advance(time.Hour)
	_ = m.Contact()
	if m.Mode() != model.ModeReconciling || m.AcceptsConfigChanges() {
		t.Fatal("contact during autonomy must start reconciliation (config changes still held)")
	}
	clk.Advance(30 * time.Second) // 对账中再次失联
	_ = m.Tick()
	if m.Mode() != model.ModeAutonomous || !m.AutonomyStartedAt().Equal(start) {
		t.Fatal("losing contact while reconciling returns to autonomy with the original start")
	}
	_ = m.Contact()
	_ = m.ReconcileCompleted()
	if m.Mode() != model.ModeOnline || !m.AutonomyStartedAt().IsZero() || !m.AcceptsConfigChanges() {
		t.Fatal("reconcile completed must return online")
	}
	want := []model.Mode{model.ModeAutonomous, model.ModeReconciling, model.ModeAutonomous, model.ModeReconciling, model.ModeOnline}
	if len(seen) != len(want) {
		t.Fatalf("notifications %v", seen)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("notifications %v", seen)
		}
	}
}

// HUB-008：起算时刻持久化，重启不重新计时；自治满 12 小时停止续期运行授权，直到对账完成。
func TestAutonomyLimitSurvivesRestart(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC))
	st := &ledger.MemStore{}
	m := newMachine(t, clk, st)
	clk.Advance(30 * time.Second)
	_ = m.Tick()
	start := m.AutonomyStartedAt()

	clk.Advance(11 * time.Hour)
	m2 := newMachine(t, clk, st) // 进程重启
	if m2.Mode() != model.ModeAutonomous || !m2.AutonomyStartedAt().Equal(start) || !m2.CanRenewRun() {
		t.Fatalf("after restart: %v started %v", m2.Mode(), m2.AutonomyStartedAt())
	}
	clk.Advance(59 * time.Minute)
	_ = m2.Tick()
	if !m2.CanRenewRun() {
		t.Fatal("expired before 12 h")
	}
	clk.Advance(time.Minute) // 起算后正好 12 小时 + 30 秒前进入自治
	_ = m2.Tick()
	if m2.Mode() != model.ModeAutonomyExpired || m2.CanRenewRun() {
		t.Fatalf("12 h after autonomy started (measured from the persisted start): %v", m2.Mode())
	}
	m3 := newMachine(t, clk, st) // 到期后重启仍是到期
	if m3.CanRenewRun() || m3.Mode() != model.ModeAutonomyExpired {
		t.Fatal("expiry must survive restart")
	}
	_ = m3.Contact()
	if m3.Mode() != model.ModeReconciling || m3.CanRenewRun() {
		t.Fatal("run renewals resume only after reconciliation completes")
	}
	_ = m3.ReconcileCompleted()
	if m3.Mode() != model.ModeOnline || !m3.CanRenewRun() {
		t.Fatal("online after reconciliation")
	}
}

func TestRestartDuringReconcilingResumesAutonomy(t *testing.T) {
	clk := clock.NewFake(time.Unix(0, 0))
	st := &ledger.MemStore{}
	m := newMachine(t, clk, st)
	clk.Advance(30 * time.Second)
	_ = m.Tick()
	_ = m.Contact()
	m2 := newMachine(t, clk, st)
	if m2.Mode() != model.ModeAutonomous {
		t.Fatalf("restart while reconciling: %v (reconciliation must restart)", m2.Mode())
	}
}
