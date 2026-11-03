package ports

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"

	"vortex/backend/apptest"
	"vortex/backend/internal/db"
	p "vortex/backend/internal/platform"
)

type fx struct {
	t      *testing.T
	dsn    string
	exec   func(fn func(tx pgx.Tx) error)
	run    func(f func(q *db.Queries) error) error
	tenant p.UUID
	group  p.UUID
	n1, n2 p.UUID
}

func setup(t *testing.T) *fx {
	dsn := apptest.Database(t)
	pool, err := p.OpenDB(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	f := &fx{t: t, dsn: dsn, tenant: apptest.Tenant(t, dsn), group: p.NewUUIDv7(), n1: p.NewUUIDv7(), n2: p.NewUUIDv7()}
	f.exec = func(fn func(tx pgx.Tx) error) {
		if err := p.InTx(context.Background(), pool, fn); err != nil {
			t.Fatal(err)
		}
	}
	f.run = func(fn func(q *db.Queries) error) error {
		return p.InTx(context.Background(), pool, func(tx pgx.Tx) error { return fn(db.New(tx)) })
	}
	for i, n := range []p.UUID{f.n1, f.n2} {
		apptest.Exec(t, dsn, "INSERT INTO nodes (id, name, region, can_entry, public_addresses) VALUES ($1, 'n', 'eu', true, $2::inet[])",
			n.PG(), []string{"203.0.113." + string(rune('1'+i))})
	}
	// n1：service 30000～30010；n2：service 30005～30020，其中 30006 是 link 端口
	apptest.Exec(t, dsn, `INSERT INTO node_port_ranges (node_id, kind, port_min, port_max) VALUES
		($1, 'service', 30000, 30010), ($2, 'service', 30005, 30020), ($2, 'link', 30006, 30006)`, f.n1.PG(), f.n2.PG())
	apptest.Exec(t, dsn, "INSERT INTO node_groups (id, name, purpose) VALUES ($1, 'e', 'entry')", f.group.PG())
	apptest.Exec(t, dsn, "INSERT INTO node_group_members (node_group_id, node_id) VALUES ($1, $2), ($1, $3)", f.group.PG(), f.n1.PG(), f.n2.PG())
	return f
}

// rule 插入一条规则行（端口分配只需要规则存在）。
func (f *fx) rule(network string) p.UUID {
	id := p.NewUUIDv7()
	f.exec(func(tx pgx.Tx) error {
		ctx := context.Background()
		if _, err := tx.Exec(ctx, `INSERT INTO rules (id, tenant_id, name, network, forward_mode, entry_group_id) VALUES ($1, $2, 'r', $3, 'direct', $4)`,
			id.PG(), f.tenant.PG(), network, f.group.PG()); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO rule_targets (rule_id, position, host, port) VALUES ($1, 1, '198.51.100.1', 80)`, id.PG())
		return err
	})
	return id
}

func (f *fx) reserve(rule p.UUID, network string, want uint16) (uint16, error) {
	var got uint16
	err := f.run(func(q *db.Queries) error {
		var err error
		got, err = Reserve(context.Background(), q, f.tenant, rule, f.group, network, want)
		return err
	})
	return got, err
}

func (f *fx) states(rule p.UUID) map[string]int {
	out := map[string]int{}
	_ = f.run(func(q *db.Queries) error {
		as, _ := q.ListRuleAllocations(context.Background(), rule.PG())
		for _, a := range as {
			out[a.State]++
		}
		return nil
	})
	return out
}

func TestReserve(t *testing.T) {
	f := setup(t)
	// 自动分配：两节点 service 范围的交集 30005～30010，去掉 n2 的 link 端口 30006 → 30005；tcp_udp 两种协议各一行
	r1 := f.rule("tcp_udp")
	got, err := f.reserve(r1, "tcp_udp", 0)
	if err != nil || got != 30005 {
		t.Fatalf("auto: %d %v", got, err)
	}
	if st := f.states(r1); st["reserved"] != 4 {
		t.Fatalf("allocations %v", st)
	}
	// 同端口冲突：提示可用端口；link 端口与范围外端口不可用
	r2 := f.rule("tcp")
	for _, port := range []uint16{30005, 30006, 30011, 30001} {
		var ue *UnavailableError
		if _, err := f.reserve(r2, "tcp", port); !errors.As(err, &ue) || len(ue.Available) == 0 || ue.Available[0] != 30007 {
			t.Fatalf("port %d: %v", port, err)
		}
	}
	if got, err := f.reserve(r2, "tcp", 30008); err != nil || got != 30008 {
		t.Fatalf("explicit: %d %v", got, err)
	}
	// 修改规则（自动）保留已分配端口；只改协议时多出的协议新增、去掉的协议转为 releasing
	if got, err := f.reserve(r2, "tcp", 0); err != nil || got != 30008 {
		t.Fatalf("keep: %d %v", got, err)
	}
	if got, err := f.reserve(r1, "udp", 0); err != nil || got != 30005 {
		t.Fatalf("narrow: %d %v", got, err)
	}
	if st := f.states(r1); st["reserved"] != 2 || st["releasing"] != 2 {
		t.Fatalf("after narrowing %v", st)
	}
	// 释放中的端口在节点确认前仍占用（PRT-009）
	if err := f.run(func(q *db.Queries) error { return Release(context.Background(), q, r2) }); err != nil {
		t.Fatal(err)
	}
	r3 := f.rule("tcp")
	var ue *UnavailableError
	if _, err := f.reserve(r3, "tcp", 30008); !errors.As(err, &ue) {
		t.Fatalf("releasing port reused: %v", err)
	}
	// 恢复为同一端口：releasing 的预留回到 reserved
	if got, err := f.reserve(r2, "tcp", 30008); err != nil || got != 30008 || f.states(r2)["reserved"] != 2 {
		t.Fatalf("restore: %d %v %v", got, err, f.states(r2))
	}
	// 耗尽
	for i := 0; ; i++ {
		r := f.rule("tcp_udp")
		if _, err := f.reserve(r, "tcp_udp", 0); err != nil {
			if !errors.Is(err, ErrExhausted) {
				t.Fatalf("exhaustion: %v", err)
			}
			break
		}
		if i > 10 {
			t.Fatal("never exhausted")
		}
	}
}
