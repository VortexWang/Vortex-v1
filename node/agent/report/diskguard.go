package report

import (
	"sync"
	"syscall"
)

// DiskGuard 是磁盘水位保护（AGT-006，AGT-T11）：数据目录所在文件系统的可用空间低于 Low 时调用 OnLow
// （Agent 据此让 Engine 停止接受新业务，已有会话继续，计量照常落盘），回到 High 以上时调用 OnOK。
// 两个阈值之间不切换，避免在边界上反复。
type DiskGuard struct {
	Path      string
	Low, High uint64 // 字节；High ≥ Low
	OnLow     func(free uint64)
	OnOK      func(free uint64)
	Free      func(path string) (uint64, error) // 默认 statfs；测试注入

	mu  sync.Mutex
	low bool
}

func statfsFree(path string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, err
	}
	return st.Bavail * uint64(st.Bsize), nil
}

// Check 检查一次（Agent 每 10 秒随封批调用）。返回当前是否处于低水位。
func (g *DiskGuard) Check() (bool, error) {
	free := g.Free
	if free == nil {
		free = statfsFree
	}
	n, err := free(g.Path)
	g.mu.Lock()
	defer g.mu.Unlock()
	if err != nil {
		return g.low, err
	}
	switch {
	case !g.low && n < g.Low:
		g.low = true
		if g.OnLow != nil {
			g.OnLow(n)
		}
	case g.low && n >= max(g.High, g.Low):
		g.low = false
		if g.OnOK != nil {
			g.OnOK(n)
		}
	}
	return g.low, nil
}
