package clock

import (
	"testing"
	"time"
)

func TestSkewEstimate(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	cases := []struct {
		sent, recv, remote time.Duration // 相对 base
		off, unc           time.Duration
		alert              bool
	}{
		{0, 2 * time.Second, time.Second, 0, time.Second, false},                       // 恰在中点
		{0, 2 * time.Second, 8 * time.Second, 7 * time.Second, time.Second, true},      // 对端快 7 秒 ±1
		{0, 2 * time.Second, -6 * time.Second, -7 * time.Second, time.Second, true},    // 对端慢
		{0, 4 * time.Second, 9 * time.Second, 7 * time.Second, 2 * time.Second, false}, // 扣除不确定度后 5 秒：不算超过
		{0, 0, 5 * time.Second, 5 * time.Second, 0, false},                             // 恰好 5 秒不告警
		{0, 0, 5*time.Second + time.Millisecond, 5*time.Second + time.Millisecond, 0, true},
		{time.Second, 0, time.Second, 0, 0, false}, // 本地时钟回退：往返按 0
	}
	for i, c := range cases {
		s := Sample{Sent: base.Add(c.sent), Received: base.Add(c.recv), Remote: base.Add(c.remote)}
		off, unc := s.Skew()
		if off != c.off || unc != c.unc {
			t.Errorf("case %d: skew %v ±%v, want %v ±%v", i, off, unc, c.off, c.unc)
		}
		if a := Exceeds(off, unc, SkewLimit); a != c.alert {
			t.Errorf("case %d: alert %v, want %v", i, a, c.alert)
		}
	}
}

// 本地时刻带单调读数时，比较仍按墙钟进行。
func TestSkewUsesWallClock(t *testing.T) {
	now := time.Now() // 带单调读数
	remote := now.Round(0).Add(10 * time.Second)
	if off, _ := (Sample{Sent: now, Received: now, Remote: remote}).Skew(); off != 10*time.Second {
		t.Fatalf("offset %v", off)
	}
}
