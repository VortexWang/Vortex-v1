// Package boot 管理启动标识（AGT-009）：每次启动生成新的 boot_id，并记住上一次的，供 AgentHello.previous_boot_id
// 使用（签发方据此执行 LIM-010 转移）。新 boot_id 先落盘再使用：即使启动后立刻崩溃，下次启动也不会复用它。
package boot

import (
	"crypto/rand"
	"encoding/json"

	"vortex/engine/types"
	"vortex/node/agent/store"
)

const kindBoot = "boot"

// Keep 是保留的历史启动数（清理时）。
const Keep = 16

type Boot struct {
	Current  types.UUID
	Previous types.UUID // 首次启动为零值
	History  []types.UUID
}

func newID() types.UUID {
	var u types.UUID
	_, _ = rand.Read(u[:])
	u[6] = u[6]&0x0f | 0x40
	u[8] = u[8]&0x3f | 0x80
	return u
}

// Start 读取启动历史，生成并持久化新的 boot_id。
func Start(log store.Log) (Boot, error) {
	var hist []types.UUID
	err := log.Load(func(r store.Record) error {
		if r.Kind != kindBoot {
			return nil
		}
		var id types.UUID
		if err := json.Unmarshal(r.Data, &id); err != nil {
			return err
		}
		hist = append(hist, id)
		return nil
	})
	if err != nil {
		return Boot{}, err
	}
	b := Boot{Current: newID()}
	if len(hist) > 0 {
		b.Previous = hist[len(hist)-1]
	}
	hist = append(hist, b.Current)
	if len(hist) > Keep*2 { // 压缩
		hist = hist[len(hist)-Keep:]
		var recs []store.Record
		for _, id := range hist[:len(hist)-1] {
			r, _ := store.Make(kindBoot, id)
			recs = append(recs, r)
		}
		if err := log.Rewrite(recs); err != nil {
			return Boot{}, err
		}
	}
	r, _ := store.Make(kindBoot, b.Current)
	if err := log.Append(r); err != nil {
		return Boot{}, err
	}
	b.History = hist
	return b, nil
}
