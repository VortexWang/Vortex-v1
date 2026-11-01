package clock

import "time"

// SkewLimit 是告警阈值：本地墙钟与 Edge Hub 相差超过 5 秒只告警，不影响授权（授权按相对时长与单调时钟计算，AGT-005）。
const SkewLimit = 5 * time.Second

// Sample 是一次墙钟比对：本地在 [Sent, Received] 之间收到对端在 Remote 时刻（对端墙钟）发出的时间。
type Sample struct {
	Sent, Received time.Time // 本地时刻（Received 晚于 Sent）
	Remote         time.Time // 对端墙钟
}

// Skew 返回对端相对本地的偏差估计（正值表示对端快）与不确定度：以往返中点为对端时刻的估计，
// 不确定度为往返时间的一半。
func (s Sample) Skew() (offset, uncertainty time.Duration) {
	rtt := s.Received.Sub(s.Sent)
	if rtt < 0 {
		rtt = 0
	}
	mid := s.Sent.Add(rtt / 2)
	return s.Remote.Sub(mid), rtt / 2 // Remote 不带单调读数，相减按墙钟进行
}

// Exceeds 报告偏差是否确定超过 limit（扣除不确定度后仍超过才算，避免慢速往返造成误报）。
func Exceeds(offset, uncertainty, limit time.Duration) bool {
	if offset < 0 {
		offset = -offset
	}
	return offset-uncertainty > limit
}
