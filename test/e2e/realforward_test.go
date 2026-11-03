package e2e

import (
	"net/netip"
	"strconv"
	"testing"
	"time"

	backendapp "vortex/backend/app"
	"vortex/backend/compiler"
	edgehubv1 "vortex/proto/gen/go/vortex/edgehub/v1"
)

// 真实后端驱动转发（M4 第四批）：套餐（速率、连接数、基础流量）→ 后端为执行点生成份额租约、为实例委托区域额度、
// 下发限制快照 → Edge Hub 签发运行授权与计费授权 → 平台入口经自托管出口转发到本机目标。下调套餐速率后，执行点
// 收到新的份额租约，旧租约作废（LIM-006）。
func TestRealBackendForwarding(t *testing.T) {
	st := startRealStack(t, backendapp.Options{ReconcileEvery: 300 * time.Millisecond, LeaseEvery: 300 * time.Millisecond})
	tenant := st.tenant
	exitLink := freePort(t)
	entry := st.enroll("entry", [16]byte{}, &edgehubv1.NodeCapabilities{CanEntry: true}, "")
	exit := st.enroll("exit", tenant, &edgehubv1.NodeCapabilities{CanExit: true}, exitLink.String()) // 自托管出口：可访问本机（SEC-011）
	eid, xid := entry.id(), exit.id()

	// 套餐：100 Mbit/s、50 个连接、1 GiB 基础流量；计费版本 0 中入口组倍率 1.0
	plan := [16]byte{0x9e, 1}
	st.exec(`INSERT INTO plans (id, name, duration_days, base_traffic_bytes, traffic_reset, max_rules, rate_bps, max_conns)
		VALUES ($1, 'basic', 30, $2, 'monthly', 10, 100000000, 50)`, plan[:], int64(1<<30))
	st.exec(`INSERT INTO subscriptions (tenant_id, plan_id, started_at, expires_at, base_period_end, base_remaining_bytes)
		VALUES ($1, $2, now() - interval '1 day', now() + interval '29 days', now() + interval '29 days', $3)`, tenant[:], plan[:], int64(1<<30))
	gEntry, gExit := [16]byte{0x9f, 1}, [16]byte{0x9f, 2}
	st.exec("UPDATE nodes SET public_addresses = '{127.0.0.1}' WHERE id IN ($1, $2)", eid[:], xid[:])
	st.exec("INSERT INTO node_port_ranges (node_id, kind, port_min, port_max) VALUES ($1, 'link', $2, $2)", xid[:], int32(exitLink.Port()))
	st.exec("INSERT INTO node_groups (id, name, purpose) VALUES ($1, 'entry', 'entry')", gEntry[:])
	st.exec("INSERT INTO node_groups (id, tenant_id, name, purpose) VALUES ($1, $2, 'mine', 'exit')", gExit[:], tenant[:])
	st.exec("INSERT INTO node_group_members (node_group_id, node_id) VALUES ($1, $2), ($3, $4)", gEntry[:], eid[:], gExit[:], xid[:])
	st.exec("INSERT INTO segment_permits (upstream_group_id, downstream_group_id, created_by) VALUES ($1, $2, $3)", gEntry[:], gExit[:], st.admin[:])
	st.exec(`INSERT INTO group_multipliers (billing_version_id, node_group_id, multiplier_milli)
		SELECT id, $1, 1000 FROM billing_versions WHERE version = 0`, gEntry[:])
	svc := freePort(t)
	st.exec("INSERT INTO node_port_ranges (node_id, kind, port_min, port_max) VALUES ($1, 'service', $2, $2)", eid[:], int32(svc.Port()))

	tg := target(t)
	spec := backendapp.RuleSpec{Name: "web", Network: "tcp", Mode: "tunnel", EntryGroup: compiler.UUID(uuidText(gEntry)),
		Path: []compiler.Segment{{Group: compiler.UUID(uuidText(gExit))}}, Targets: []compiler.Target{{Host: tg.Addr().String(), Port: tg.Port(), Weight: 1}},
		TargetSchedule: "weighted_round_robin", ProxyProtocol: "off", Enabled: true}
	saved, err := st.be.CreateRule(st.ctx, tenant, spec)
	if err != nil {
		t.Fatal(err)
	}
	st.waitSQL("rule applied", "SELECT sync_status FROM rules WHERE id = $1", []any{saved.ID[:]}, "applied")

	// 入口是唯一执行点：持有全部速率与连接份额（LIM-002、LIM-003）；实例分得区域额度 min(64 MiB, 余额)（HUB-006）
	live := "SELECT coalesce(string_agg(kind || ':' || amount, ',' ORDER BY kind), '') FROM share_leases WHERE node_id = $1 AND released_at IS NULL"
	st.waitSQL("entry shares", live, []any{eid[:]}, "conn:50,rate:100000000")
	st.waitSQL("exit holds no shares", live, []any{xid[:]}, "")
	st.waitSQL("regional budget", "SELECT delegated::text FROM regional_budgets WHERE tenant_id = $1", []any{tenant[:]}, strconv.Itoa(64<<20))

	addr := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), svc.Port())
	eventually(t, "forwarding through the real backend", func() bool { return ok(addr) })
	// 计费授权由实例从区域额度签发并入账本，账本上报到后端（HUB-012）
	st.waitSQL("grant issued in ledger", "SELECT (count(*) > 0)::text FROM hub_ledger_entries WHERE kind = 'grant_issued' AND tenant_id = $1",
		[]any{tenant[:]}, "true")

	// 下调套餐速率：下一次重算生成新租约，旧租约作废
	st.exec("UPDATE plans SET rate_bps = 40000000 WHERE id = $1", plan[:])
	st.waitSQL("rate share lowered", live, []any{eid[:]}, "conn:50,rate:40000000")
	if !ok(addr) {
		t.Fatal("forwarding stopped after the share update")
	}
}
