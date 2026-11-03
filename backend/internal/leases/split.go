package leases

import (
	"math/bits"
	"time"
)

const (
	// FloorRatio：份额保底比例 50%（LIM-009、附录 A）。
	FloorRatioPercent = 50
	// AutonomyCap：自治上限 12 小时（HUB-008），单实例区域额度上限 = 近 1 小时计费速率 × 自治上限（HUB-006）。
	AutonomyCap = 12 * time.Hour
	// BudgetFloor：有已激活规则但近期无用量的实例至少分得 min(64 MiB, 剩余余额 ÷ 实例数)（HUB-006）。
	BudgetFloor = 64 << 20
)

// SplitShare 把用户上限切分给 n 个活跃执行点（LIM-009）：保底 = 上限 × 50% ÷ n；其余按需求信号
// （近 60 秒用量 × (1 + 令牌耗尽占比)）分配，全部为 0 时均分。结果之和恰好等于 limit。
func SplitShare(limit uint64, demand []uint64) []uint64 {
	n := uint64(len(demand))
	out := make([]uint64, n)
	if n == 0 {
		return out
	}
	floor := limit * FloorRatioPercent / 100 / n
	if limit > (1<<63)/100 { // 避免乘法溢出
		floor = limit / 100 * FloorRatioPercent / n
	}
	rest := limit - floor*n
	var dsum uint64
	for _, d := range demand {
		dsum = satAdd(dsum, d)
	}
	var sum uint64
	for i := range out {
		out[i] = floor
		if dsum > 0 {
			out[i] += mulDiv(rest, demand[i], dsum)
		} else {
			out[i] += rest / n
		}
		sum += out[i]
	}
	// 取整余数：均分时给靠前的执行点各 1；按需求分配时全部给需求最大者（相同则靠前者）。总和恰好等于 limit。
	left := limit - sum
	if dsum == 0 {
		for i := uint64(0); i < left; i++ {
			out[i]++
		}
		return out
	}
	best := 0
	for i := range demand {
		if demand[i] > demand[best] {
			best = i
		}
	}
	out[best] += left
	return out
}

// SplitBudget 计算各在线实例的区域额度目标（HUB-006）。rates 是各实例近 1 小时平均计费速率（bit/s，0 表示近期无用量）。
// 上限 = 速率 × 自治上限，不低于 min(64 MiB, avail ÷ 实例数)；上限之和不超过 avail 时各得上限，否则无用量的实例
// 得保底、其余按用量比例切分（不超过各自上限）。结果之和不超过 avail。
func SplitBudget(avail uint64, rates []uint64) []uint64 {
	n := uint64(len(rates))
	out := make([]uint64, n)
	if n == 0 || avail == 0 {
		return out
	}
	floor := min(uint64(BudgetFloor), avail/n)
	caps := make([]uint64, n)
	var sum uint64
	for i, r := range rates {
		caps[i] = max(satMul(r/8, uint64(AutonomyCap/time.Second)), floor)
		sum = satAdd(sum, caps[i])
	}
	if sum <= avail {
		return caps
	}
	rest := avail
	var rsum uint64
	for i, r := range rates {
		if r == 0 {
			out[i] = floor
			rest -= floor
		} else {
			rsum = satAdd(rsum, r)
		}
	}
	for i, r := range rates {
		if r > 0 {
			out[i] = min(mulDiv(rest, r, rsum), caps[i])
		}
	}
	return out
}

func satAdd(a, b uint64) uint64 {
	if s, c := bits.Add64(a, b, 0); c == 0 {
		return s
	}
	return ^uint64(0)
}

func satMul(a, b uint64) uint64 {
	if hi, lo := bits.Mul64(a, b); hi == 0 {
		return lo
	}
	return ^uint64(0)
}

// mulDiv = ⌊a × b ÷ c⌋（c > 0；结果溢出时返回 a）。
func mulDiv(a, b, c uint64) uint64 {
	hi, lo := bits.Mul64(a, b)
	if hi >= c {
		return a
	}
	q, _ := bits.Div64(hi, lo, c)
	return q
}
