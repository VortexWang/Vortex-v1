package grpcgw

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"vortex/backend/internal/db"
	"vortex/backend/internal/metering"
	p "vortex/backend/internal/platform"
	backendv1 "vortex/proto/gen/go/vortex/backend/v1"
	commonv1 "vortex/proto/gen/go/vortex/common/v1"
	runtimev1 "vortex/proto/gen/go/vortex/runtime/v1"
)

// SendQueue 是每条控制流的下行队列容量；满时关闭该流（Edge Hub 重连后重新注册）。
const SendQueue = 1024

var modeNames = map[commonv1.HubMode]string{commonv1.HubMode_HUB_MODE_ONLINE: "online", commonv1.HubMode_HUB_MODE_AUTONOMOUS: "autonomous",
	commonv1.HubMode_HUB_MODE_RECONCILING: "reconciling", commonv1.HubMode_HUB_MODE_AUTONOMY_EXPIRED: "autonomy_expired"}

// Connect 是控制主通道（C2 第 7 节）。本批实现：注册（实例状态入库，下发计费版本、吊销列表与账本确认位置；
// 对账中先下发限制快照与 STARTED）、节点状态入库（HUB-004）、账本条目按 (hub_id, seq) 幂等落库并累计确认
// （HUB-012；入账与额度在 BE-T18～T23）。其余上行消息暂只记录。
func (s *Server) Connect(stream grpc.BidiStreamingServer[backendv1.HubUpMessage, backendv1.BackendMessage]) error {
	hub, err := s.hubIdentity(stream.Context())
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	q := db.New(s.Fleet.DB)

	out := make(chan *backendv1.BackendMessage, SendQueue)
	var overflow sync.Once
	send := func(m *backendv1.BackendMessage) {
		select {
		case out <- m:
		default:
			overflow.Do(cancel)
		}
	}
	conn := s.attach(hub, send)
	defer func() {
		if !s.detach(hub, conn) {
			return // 已被同一实例的新流取代
		}
		_ = q.SetHubOffline(context.Background(), hub.PG())
		if s.Leases != nil { // 失联即冻结该实例的区域额度（HUB-015）
			if err := s.Leases.HubDisconnected(context.Background(), hub); err != nil {
				s.log().Warn("freeze budgets", "hub", hub.String(), "err", err)
			}
		}
	}()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case m := <-out:
				if err := stream.Send(m); err != nil {
					cancel()
					return
				}
			}
		}
	}()
	in := make(chan *backendv1.HubUpMessage)
	recvErr := make(chan error, 1)
	go func() {
		for {
			m, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			select {
			case in <- m:
			case <-ctx.Done():
				return
			}
		}
	}()
	quiet := s.ReconcileQuiet
	if quiet <= 0 {
		quiet = time.Second
	}
	var reconcileDue <-chan time.Time
	var reconcileTimer *time.Timer
	reconciling := false
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-recvErr:
			return err
		case <-reconcileDue:
			reconciling, reconcileDue = false, nil
			_ = q.SetHubMode(ctx, db.SetHubModeParams{ID: hub.PG(), Mode: "online"}) // 先入库，再通知实例
			send(&backendv1.BackendMessage{Body: &backendv1.BackendMessage_Reconcile{Reconcile: &backendv1.ReconcileDirective{
				Phase: backendv1.ReconcilePhase_RECONCILE_PHASE_COMPLETED}}})
			s.log().Info("hub reconcile completed", "hub", hub.String())
			if s.Leases != nil {
				if err := s.Leases.HubOnline(ctx, hub); err != nil {
					s.log().Warn("budgets after reconcile", "hub", hub.String(), "err", err)
				}
			}
		case m := <-in:
			_ = q.TouchHub(ctx, hub.PG())
			switch b := m.Body.(type) {
			case *backendv1.HubUpMessage_Register:
				if err := s.onRegister(ctx, hub, b.Register, send); err != nil {
					return err
				}
				s.pushHubNodes(ctx, hub, send) // 本实例所辖节点的最新配置与未完成的提交
				if b.Register.GetMode() == commonv1.HubMode_HUB_MODE_RECONCILING {
					reconciling = true
					reconcileTimer = time.NewTimer(quiet)
					reconcileDue = reconcileTimer.C
				}
			case *backendv1.HubUpMessage_NodeStatus:
				s.onNodeStatus(ctx, hub, b.NodeStatus)
				for _, n := range b.NodeStatus.GetNodes() { // 节点接入（含切换实例）：发给它当前所在的实例
					if id, err := fromMsg(n.GetNodeId()); err == nil && n.GetConnected() {
						s.pushNode(ctx, id, send, true)
						if s.Leases != nil { // 新启动的份额（LIM-010）与该实例的区域额度
							if err := s.Leases.NodeConnected(ctx, id); err != nil {
								s.log().Warn("leases on node connect", "node", id.String(), "err", err)
							}
						}
					}
				}
			case *backendv1.HubUpMessage_ForeignGrants:
				if n, err := fromMsg(b.ForeignGrants.GetNodeId()); err == nil && s.Leases != nil {
					s.Leases.ReportForeign(hub, n, b.ForeignGrants.GetGrants())
				}
			case *backendv1.HubUpMessage_BudgetRequest:
				if t, err := fromMsg(b.BudgetRequest.GetTenantId()); err == nil && s.Leases != nil {
					s.Leases.ReportRate(t, hub, b.BudgetRequest.GetRateBillingBps_1H())
					if err := s.Leases.Rebudget(ctx, t, hub); err != nil {
						s.log().Warn("budget request", "tenant", t.String(), "err", err)
					}
				}
			case *backendv1.HubUpMessage_ApplyProgress:
				s.onApplyProgress(ctx, b.ApplyProgress)
			case *backendv1.HubUpMessage_Ledger:
				if err := s.onLedger(ctx, hub, b.Ledger, send); err != nil {
					return err
				}
				if reconciling && reconcileTimer != nil {
					reconcileTimer.Reset(quiet)
				}
			}
		}
	}
}

func (s *Server) onRegister(ctx context.Context, hub p.UUID, r *backendv1.HubRegister, send func(*backendv1.BackendMessage)) error {
	q := db.New(s.Fleet.DB)
	mode := modeNames[r.GetMode()]
	if mode == "" {
		mode = "online"
	}
	v := r.GetHubVersion()
	auto := p.TS(r.GetAutonomyStartedAt().AsTime())
	auto.Valid = r.GetAutonomyStartedAt() != nil
	if err := q.UpdateHubRegister(ctx, db.UpdateHubRegisterParams{ID: hub.PG(), Mode: mode, HubVersion: &v, AutonomyStartedAt: auto}); err != nil {
		return err
	}
	s.log().Info("hub registered", "hub", hub.String(), "mode", mode, "ledger_last_seq", r.GetLedgerLastSeq(), "version", v)
	// 吊销列表最先（HUB-010），然后计费版本（HUB-018）与账本确认位置
	ver, serials, err := s.Fleet.Revocations(ctx)
	if err != nil {
		return err
	}
	send(&backendv1.BackendMessage{Body: &backendv1.BackendMessage_Revocations{Revocations: &runtimev1.RevocationList{Serial: uint64(ver), CertSerials: serials}}})
	bvs, err := q.ListBillingVersions(ctx)
	if err != nil {
		return err
	}
	sync := &backendv1.BillingVersionSync{}
	for _, bv := range bvs {
		sync.Versions = append(sync.Versions, &backendv1.BillingVersion{Version: uint64(bv.Version), EffectiveAt: timestamppb.New(bv.EffectiveAt.Time)})
	}
	send(&backendv1.BackendMessage{Body: &backendv1.BackendMessage_BillingVersions{BillingVersions: sync}})
	if acked, err := q.MaxLedgerSeq(ctx, hub.PG()); err == nil && acked > 0 {
		send(&backendv1.BackendMessage{Body: &backendv1.BackendMessage_LedgerAck{LedgerAck: &backendv1.LedgerAck{AckedThroughSeq: uint64(acked)}}})
	}
	reconciling := r.GetMode() == commonv1.HubMode_HUB_MODE_RECONCILING
	var revoked []p.UUID
	// 限制快照（HUB-009）；在线时随后下发区域额度（HUB-006），对账中在 COMPLETED 之后下发
	if s.Leases != nil {
		reported := map[p.UUID]uint64{}
		for _, bv := range r.GetBudgetVersions() {
			if t, err := fromMsg(bv.GetTenantId()); err == nil {
				reported[t] = bv.GetBudgetVersion()
			}
		}
		var err error
		if revoked, err = s.Leases.HubRegistered(ctx, hub, reported, mode == "online"); err != nil {
			return err
		}
	} else if reconciling {
		send(&backendv1.BackendMessage{Body: &backendv1.BackendMessage_Restrictions{Restrictions: &backendv1.RestrictionSnapshot{
			SnapshotVersion: uint64(time.Now().UnixNano())}}})
	}
	if reconciling {
		// 恢复对账（HUB-013）：先下发吊销与限制，再 STARTED；Edge Hub 随后上报账本
		// HUB-017：冻结期间额度被回收的租户，对账完成前不得再签发
		d := &backendv1.ReconcileDirective{Phase: backendv1.ReconcilePhase_RECONCILE_PHASE_STARTED}
		for _, t := range revoked {
			d.BudgetRevokedTenantIds = append(d.BudgetRevokedTenantIds, uuidMsg(t))
		}
		send(&backendv1.BackendMessage{Body: &backendv1.BackendMessage_Reconcile{Reconcile: d}})
	}
	return nil
}

func (s *Server) onNodeStatus(ctx context.Context, hub p.UUID, b *backendv1.NodeStatusBatch) {
	q := db.New(s.Fleet.DB)
	for _, n := range b.GetNodes() {
		id, err := fromMsg(n.GetNodeId())
		if err != nil {
			continue
		}
		boot := p.UUID{}.PG()
		boot.Valid = false
		if b, err := fromMsg(n.GetBootId()); err == nil && b != (p.UUID{}) {
			boot = b.PG()
		}
		var ver *string
		if v := n.GetAgentVersion(); v != "" {
			ver = &v
		}
		if err := q.UpdateNodeStatus(ctx, db.UpdateNodeStatusParams{ID: id.PG(), EdgeHubID: hub.PG(), Connected: n.GetConnected(),
			AgentVersion: ver, BootID: boot}); err != nil {
			s.log().Warn("node status", "node", id.String(), "err", err)
		}
	}
}

func (s *Server) onLedger(ctx context.Context, hub p.UUID, b *backendv1.LedgerEntries, send func(*backendv1.BackendMessage)) error {
	es := b.GetEntries()
	if len(es) == 0 {
		return nil
	}
	var eff *metering.Effect
	err := p.InTx(ctx, s.Fleet.DB, func(tx pgx.Tx) error {
		eff = &metering.Effect{} // 重试时重新收集
		q := db.New(tx)
		for _, e := range es {
			kind, tenant := entryKind(e)
			if kind == "" || e.GetSeq() == 0 {
				continue // 未知或空条目不入库，但仍随累计确认越过（契约补充 L3）
			}
			raw, err := proto.Marshal(e)
			if err != nil {
				return err
			}
			t := tenant.PG()
			t.Valid = tenant != (p.UUID{})
			n, err := q.InsertLedgerEntry(ctx, db.InsertLedgerEntryParams{HubID: hub.PG(), Seq: int64(e.GetSeq()), Kind: kind, TenantID: t, Payload: raw})
			if err != nil {
				return err
			}
			if n == 1 && s.Metering != nil { // 首次落库的条目在同一事务内入账（BE-T18）
				if err := s.Metering.Apply(ctx, q, hub, e, eff); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	send(&backendv1.BackendMessage{Body: &backendv1.BackendMessage_LedgerAck{LedgerAck: &backendv1.LedgerAck{AckedThroughSeq: es[len(es)-1].GetSeq()}}})
	if s.Leases != nil {
		eff.Commit(s.Leases)
		for _, r := range eff.Budgets { // 结算确认（MTR-004）：实例据此把待入账量归零
			s.Leases.SendBudget(r)
		}
		exhausted := false
		for _, t := range sortedTenants(eff.Tenants) { // 结算后补充区域额度（HUB-006）
			if err := s.Leases.Rebudget(ctx, t, hub); err != nil {
				s.log().Warn("budget after settlement", "tenant", t.String(), "err", err)
			}
			if bal, err := db.New(s.Fleet.DB).TenantBalance(ctx, t.PG()); err == nil && bal <= 0 {
				exhausted = true
			}
		}
		if exhausted { // 流量耗尽立即广播限制（不等下一个续租周期）
			if err := s.Leases.BroadcastRestrictions(ctx); err != nil {
				s.log().Warn("restrictions after settlement", "err", err)
			}
		}
	}
	return nil
}

func sortedTenants(m map[p.UUID]bool) []p.UUID {
	out := make([]p.UUID, 0, len(m))
	for t := range m {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i][:], out[j][:]) < 0 })
	return out
}

func entryKind(e *backendv1.LedgerEntry) (string, p.UUID) {
	var t p.UUID
	switch b := e.Body.(type) {
	case *backendv1.LedgerEntry_GrantIssued:
		t, _ = fromMsg(b.GrantIssued.GetTenantId())
		return "grant_issued", t
	case *backendv1.LedgerEntry_GrantSettled:
		t, _ = fromMsg(b.GrantSettled.GetTenantId())
		return "grant_settled", t
	case *backendv1.LedgerEntry_NodeUsage:
		return "node_usage", t
	case *backendv1.LedgerEntry_GrantTransferred:
		return "grant_transferred", t
	}
	return "", t
}
