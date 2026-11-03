// Package ports 是业务端口的分配（BE-T12；PRT-001～PRT-009）：同一规则在入口组全部成员上使用同一端口（PRT-003），
// tcp_udp 规则两种协议用同一端口号、分别预留（PRT-006）。共同可用端口 = 各成员 service 范围的交集，去掉 link /
// control / metrics 范围与已被占用的端口；在事务中锁住成员节点行后计算并写入预留（PRT-004、PRT-005）。
// 端口状态：reserved（已预留）→ occupied（节点确认已监听）；不再需要时 releasing，节点确认不再监听后 released（PRT-009）。
package ports

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"

	"vortex/backend/internal/db"
	p "vortex/backend/internal/platform"
)

// 未配置 service 范围的节点默认允许 1024～65535（PRT-001）。
const (
	DefaultMin = 1024
	DefaultMax = 65535
)

// UnavailableError：指定的端口不可用（PRT-003：创建失败并提示可用端口）。
type UnavailableError struct {
	Port      uint16
	Available []uint16
}

func (e *UnavailableError) Error() string {
	return fmt.Sprintf("ports: port %d is not available on every entry member (available: %v)", e.Port, e.Available)
}

var ErrExhausted = errors.New("ports: no common port available on the entry members")

func networks(n string) []string {
	if n == "tcp_udp" {
		return []string{"tcp", "udp"}
	}
	return []string{n}
}

type rng struct{ min, max int32 }

// Reserve 为规则在入口组当前成员上预留端口（want 为 0 时自动选择；修改规则时保留已有端口）。返回选定的端口。
// 已有预留中不再需要的（成员退出、协议或端口变化）转为 releasing。
func Reserve(ctx context.Context, q *db.Queries, tenant, rule, entryGroup p.UUID, network string, want uint16) (uint16, error) {
	nodeRows, err := q.EntryMembersOfGroup(ctx, entryGroup.PG())
	if err != nil {
		return 0, err
	}
	ids := make([]pgtype.UUID, len(nodeRows))
	copy(ids, nodeRows)
	if _, err := q.LockNodes(ctx, ids); err != nil {
		return 0, err
	}
	existing, err := q.ListRuleAllocations(ctx, rule.PG())
	if err != nil {
		return 0, err
	}
	service := map[p.UUID][]rng{}
	blocked := map[p.UUID][]rng{}
	rows, err := q.ListServiceRanges(ctx, ids)
	if err != nil {
		return 0, err
	}
	for _, r := range rows {
		n := p.FromPG(r.NodeID)
		if r.Kind == "service" {
			service[n] = append(service[n], rng{r.PortMin, r.PortMax})
		} else {
			blocked[n] = append(blocked[n], rng{r.PortMin, r.PortMax})
		}
	}
	used := map[[2]any]bool{} // (node, network, port) 被其他规则占用
	act, err := q.ListActivePortsOnNodes(ctx, ids)
	if err != nil {
		return 0, err
	}
	for _, a := range act {
		if p.FromPG(a.RuleID) != rule {
			used[[2]any{p.FromPG(a.NodeID), fmt.Sprint(a.Network, "/", a.Port)}] = true
		}
	}
	nets := networks(network)
	free := func(port int32) bool {
		for _, nr := range nodeRows {
			n := p.FromPG(nr)
			rs := service[n]
			if len(rs) == 0 {
				rs = []rng{{DefaultMin, DefaultMax}}
			}
			ok := false
			for _, r := range rs {
				if port >= r.min && port <= r.max {
					ok = true
				}
			}
			for _, r := range blocked[n] {
				if port >= r.min && port <= r.max {
					ok = false
				}
			}
			for _, net := range nets {
				if used[[2]any{n, fmt.Sprint(net, "/", port)}] {
					ok = false
				}
			}
			if !ok {
				return false
			}
		}
		return true
	}
	port := int32(want)
	if port == 0 && len(existing) > 0 {
		port = existing[0].Port // 修改规则：保持已分配的端口
	}
	switch {
	case port != 0 && !free(port):
		e := &UnavailableError{Port: uint16(port)}
		for c := int32(DefaultMin); c <= DefaultMax && len(e.Available) < 5; c++ {
			if free(c) {
				e.Available = append(e.Available, uint16(c))
			}
		}
		return 0, e
	case port == 0:
		for c := int32(DefaultMin); c <= DefaultMax; c++ {
			if free(c) {
				port = c
				break
			}
		}
		if port == 0 {
			return 0, ErrExhausted
		}
	}
	// 与已有预留做差异：保留仍需要的（releasing 的恢复为 reserved），其余转为 releasing，缺的新增
	need := map[[2]any]bool{}
	for _, nr := range nodeRows {
		for _, net := range nets {
			need[[2]any{p.FromPG(nr), net}] = true
		}
	}
	for _, a := range existing {
		k := [2]any{p.FromPG(a.NodeID), a.Network}
		switch {
		case need[k] && a.Port == port:
			delete(need, k)
			if a.State == "releasing" {
				if err := q.SetAllocationState(ctx, db.SetAllocationStateParams{ID: a.ID, State: "reserved"}); err != nil {
					return 0, err
				}
			}
		case a.State != "releasing":
			if err := q.SetAllocationState(ctx, db.SetAllocationStateParams{ID: a.ID, State: "releasing"}); err != nil {
				return 0, err
			}
		}
	}
	for _, nr := range nodeRows {
		for _, net := range nets {
			if !need[[2]any{p.FromPG(nr), net}] {
				continue
			}
			if err := q.InsertPortAllocation(ctx, db.InsertPortAllocationParams{ID: p.NewUUIDv7().PG(), TenantID: tenant.PG(), RuleID: rule.PG(),
				NodeID: nr, Network: net, Port: port}); err != nil {
				return 0, err
			}
		}
	}
	if err := q.SetRuleServicePort(ctx, db.SetRuleServicePortParams{ID: rule.PG(), ServicePort: &port}); err != nil {
		return 0, err
	}
	return uint16(port), nil
}

// Release 把规则的全部预留转为 releasing（删除或停用规则时）；节点确认不再监听后由编排转为 released。
func Release(ctx context.Context, q *db.Queries, rule p.UUID) error {
	as, err := q.ListRuleAllocations(ctx, rule.PG())
	if err != nil {
		return err
	}
	for _, a := range as {
		if a.State != "releasing" {
			if err := q.SetAllocationState(ctx, db.SetAllocationStateParams{ID: a.ID, State: "releasing"}); err != nil {
				return err
			}
		}
	}
	return nil
}
