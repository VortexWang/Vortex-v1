package e2e

import (
	"context"
	"encoding/pem"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	backendapp "vortex/backend/app"
	"vortex/backend/apptest"
	"vortex/backend/compiler"
	hubapp "vortex/edgehub/app"
	"vortex/node/agent/localapi"
	nodeapp "vortex/node/app"
	edgehubv1 "vortex/proto/gen/go/vortex/edgehub/v1"
)

// 真实后端驱动配置（M4 第三批）：后端保存规则（端口自动分配）→ 编译 → 节点配置版本化并经 Outbox 投递到实例 →
// 两个真实节点 Prepare → 后端确认全部准备好后下发 Commit → 节点生效 → 规则 applied、端口 occupied；
// 入口在分配的端口上监听。修改端口后同样收敛；删除后端口在节点确认不再监听后释放。
// （转发由 TestRealBackendForwarding 检查。）
// realStack 是真实后端 + 真实 Edge Hub + 真实节点的测试环境。
type realStack struct {
	t      *testing.T
	ctx    context.Context
	dir    string
	dsn    string
	caFile string
	admin  [16]byte
	tenant [16]byte
	be     *backendapp.Backend
	hub    string // 节点注册经由的实例
	beAddr string
	hubs   []*realHub
	log    *slog.Logger
	nodes  []*realNode
}

type realHub struct {
	id   [16]byte
	addr string
	stop func() // 停止并等待退出（可重复调用）
}

// addHub 引导并启动一个 Edge Hub。
func (st *realStack) addHub(name string) *realHub {
	t := st.t
	addr := freePort(t).String()
	tok, _ := st.be.CreateHubBootstrapToken(st.ctx, "test", time.Hour)
	cfg, err := hubapp.Enroll(st.ctx, hubapp.EnrollParams{Token: tok, Backend: st.beAddr, CAFile: st.caFile, Region: "test", Listen: addr,
		DataDir: filepath.Join(st.dir, name), ConfigPath: filepath.Join(st.dir, name+".json")})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(st.ctx)
	done := make(chan error, 1)
	go func() { done <- hubapp.Run(ctx, cfg, st.log, hubapp.Options{}) }()
	var once sync.Once
	h := &realHub{addr: addr, stop: func() { once.Do(func() { cancel(); <-done }) }}
	id, _ := nodeapp.ParseUUID(cfg.HubID)
	h.id = id
	st.hubs = append(st.hubs, h)
	return h
}

type realNode struct {
	cfg  *nodeapp.Config
	done chan error
}

func startRealStack(t *testing.T, opt backendapp.Options) *realStack {
	dsn := apptest.Database(t)
	dir := t.TempDir()
	caCfg, ca := apptest.CA(t, dir)
	caFile := filepath.Join(dir, "platform-ca.crt")
	_ = os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Root.Raw}), 0o644)
	st := &realStack{t: t, dir: dir, dsn: dsn, caFile: caFile, admin: apptest.Admin(t, dsn), tenant: apptest.Tenant(t, dsn)}
	st.log = slog.New(slog.NewTextHandler(io.Discard, nil))
	if os.Getenv("STACK_DEBUG") != "" {
		st.log = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	ctx, cancel := context.WithCancel(context.Background())
	st.ctx = ctx

	ready := make(chan *backendapp.Backend, 1)
	beAddr := freePort(t).String()
	beDone := make(chan error, 1)
	opt.OnReady = func(b *backendapp.Backend) { ready <- b }
	go func() {
		beDone <- backendapp.Run(ctx, &backendapp.Config{DatabaseURL: dsn, GRPCListen: beAddr, CA: caCfg}, st.log, opt)
	}()
	select {
	case st.be = <-ready:
	case err := <-beDone:
		cancel()
		t.Fatal(err)
	}
	st.beAddr = beAddr
	t.Cleanup(func() {
		cancel()
		for _, n := range st.nodes {
			if n.done != nil {
				<-n.done
			}
		}
		for _, h := range st.hubs {
			h.stop()
		}
		<-beDone
	})
	st.hub = st.addHub("hub").addr
	return st
}

// enroll 注册一个节点（tenant 为零值时是平台节点，否则是该租户的自托管节点）并启动它。
func (st *realStack) enroll(name string, tenant [16]byte, caps *edgehubv1.NodeCapabilities, link string) *realNode {
	t := st.t
	ntok, err := st.be.CreateEnrollToken(st.ctx, tenant, st.admin)
	if err != nil {
		t.Fatal(err)
	}
	np := nodeapp.EnrollParams{Token: ntok, Hub: st.hub, CAFile: st.caFile, DataDir: filepath.Join(st.dir, name),
		ConfigPath: filepath.Join(st.dir, name+".json"), Socket: filepath.Join(st.dir, name+".sock"), Caps: caps, LinkListen: link}
	n := &realNode{}
	eventually(t, name+" enrolled", func() bool { var err error; n.cfg, err = nodeapp.Enroll(st.ctx, np); return err == nil })
	d, err := nodeapp.Dialer(n.cfg)
	if err != nil {
		t.Fatal(err)
	}
	n.done = make(chan error, 1)
	go func() { n.done <- nodeapp.Run(st.ctx, n.cfg, d, st.log) }()
	st.nodes = append(st.nodes, n)
	return n
}

func (n *realNode) id() [16]byte { id, _ := nodeapp.ParseUUID(n.cfg.NodeID); return id }

func (st *realStack) exec(sql string, args ...any) {
	st.t.Helper()
	apptest.Exec(st.t, st.dsn, sql, args...)
}

func (st *realStack) waitSQL(what, sql string, args []any, want string) {
	t := st.t
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var got string
		apptest.QueryRow(t, st.dsn, sql, args, &got)
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: got %q, want %q", what, got, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestRealBackendApply(t *testing.T) {
	st := startRealStack(t, backendapp.Options{ReconcileEvery: 300 * time.Millisecond})
	ctx, dsn, admin, tenant, be := st.ctx, st.dsn, st.admin, st.tenant, st.be
	exitLink := freePort(t)
	entry := st.enroll("entry", [16]byte{}, &edgehubv1.NodeCapabilities{CanEntry: true}, "")
	exit := st.enroll("exit", [16]byte{}, &edgehubv1.NodeCapabilities{CanExit: true}, exitLink.String())

	// 平台组、成员、段许可与链路端口（管理接口在 BE-T09 / BE-T27；这里直接写库）。本机地址作为“公网地址”。
	eid, xid := entry.id(), exit.id()
	gEntry, gExit := [16]byte{0x9f, 1}, [16]byte{0x9f, 2}
	apptest.Exec(t, dsn, "UPDATE nodes SET public_addresses = '{127.0.0.1}' WHERE id IN ($1, $2)", eid[:], xid[:])
	apptest.Exec(t, dsn, "INSERT INTO node_port_ranges (node_id, kind, port_min, port_max) VALUES ($1, 'link', $2, $2)", xid[:], int32(exitLink.Port()))
	apptest.Exec(t, dsn, "INSERT INTO node_groups (id, name, purpose) VALUES ($1, 'entry', 'entry'), ($2, 'exit', 'exit')", gEntry[:], gExit[:])
	apptest.Exec(t, dsn, "INSERT INTO node_group_members (node_group_id, node_id) VALUES ($1, $2), ($3, $4)", gEntry[:], eid[:], gExit[:], xid[:])
	apptest.Exec(t, dsn, "INSERT INTO segment_permits (upstream_group_id, downstream_group_id, created_by) VALUES ($1, $2, $3)", gEntry[:], gExit[:], admin[:])
	// 入口在 service 范围内自动分配端口：给一个确定可用的范围
	svc := freePort(t)
	apptest.Exec(t, dsn, "INSERT INTO node_port_ranges (node_id, kind, port_min, port_max) VALUES ($1, 'service', $2, $2)", eid[:], int32(svc.Port()))

	apptest.Entitle(t, dsn, tenant)
	tg := target(t)
	spec := backendapp.RuleSpec{Name: "web", Network: "tcp", Mode: "tunnel", EntryGroup: compiler.UUID(uuidText(gEntry)),
		Path: []compiler.Segment{{Group: compiler.UUID(uuidText(gExit))}}, Targets: []compiler.Target{{Host: tg.Addr().String(), Port: tg.Port(), Weight: 1}},
		TargetSchedule: "weighted_round_robin", ProxyProtocol: "off", Enabled: true}
	// 平台出口访问本机目标会被 SEC-011 拒绝：保存失败、不留任何东西
	if _, err := be.CreateRule(ctx, tenant, spec); err == nil || !strings.Contains(err.Error(), "RULE_TARGET_FORBIDDEN") {
		t.Fatalf("loopback target on a platform exit: %v", err)
	}
	spec.Targets = []compiler.Target{{Host: "198.51.100.7", Port: 443, Weight: 1}}
	saved, err := be.CreateRule(ctx, tenant, spec)
	if err != nil {
		t.Fatal(err)
	}
	waitSQL := st.waitSQL
	waitSQL("rule applied", "SELECT sync_status FROM rules WHERE id = $1", []any{saved.ID[:]}, "applied")
	waitSQL("entry port occupied", "SELECT string_agg(state || ':' || port, ',') FROM port_allocations WHERE rule_id = $1", []any{saved.ID[:]},
		"occupied:"+strconv.Itoa(int(svc.Port())))
	for _, n := range []*realNode{entry, exit} {
		st, err := localapi.GetStatus(ctx, n.cfg.Socket)
		if err != nil || st.Phase != "applied" || st.DesiredVersion == 0 {
			t.Fatalf("%s status %+v %v", n.cfg.NodeID, st, err)
		}
	}
	if c, err := net.DialTimeout("tcp", netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), svc.Port()).String(), time.Second); err != nil {
		t.Fatalf("entry not listening on the allocated port: %v", err)
	} else {
		c.Close()
	}

	// 删除：节点确认不再监听后端口释放（PRT-009）
	if err := be.DeleteRule(ctx, tenant, saved.ID); err != nil {
		t.Fatal(err)
	}
	waitSQL("port released", "SELECT string_agg(state, ',') FROM port_allocations WHERE rule_id = $1", []any{saved.ID[:]}, "released")
	eventually(t, "entry stopped listening", func() bool {
		c, err := net.DialTimeout("tcp", netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), svc.Port()).String(), 200*time.Millisecond)
		if err == nil {
			c.Close()
		}
		return err != nil
	})
}

func uuidText(u [16]byte) string {
	const h = "0123456789abcdef"
	out := make([]byte, 0, 36)
	for i, c := range u {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			out = append(out, '-')
		}
		out = append(out, h[c>>4], h[c&15])
	}
	return string(out)
}
