package grpcgw

import (
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"

	"vortex/backend/internal/apply"
	"vortex/backend/internal/db"
	p "vortex/backend/internal/platform"
	backendv1 "vortex/proto/gen/go/vortex/backend/v1"
	edgehubv1 "vortex/proto/gen/go/vortex/edgehub/v1"
	runtimev1 "vortex/proto/gen/go/vortex/runtime/v1"
)

// 下发（BE-T11 剩余）：Outbox 中的期望配置与提交指令按节点当前所在的实例投递；实例注册时补发其所辖节点的最新配置
// 与未完成的提交。实例按节点只保留最新期望版本、节点报告准备成功后才转发提交（HUB-T04），因此重复投递无害。

// hubConn 是一条已注册的控制流。
type hubConn struct {
	send func(*backendv1.BackendMessage)
}

func (s *Server) attach(hub p.UUID, send func(*backendv1.BackendMessage)) *hubConn {
	c := &hubConn{send: send}
	s.hubsMu.Lock()
	if s.hubs == nil {
		s.hubs = map[p.UUID]*hubConn{}
	}
	s.hubs[hub] = c
	s.hubsMu.Unlock()
	return c
}

// detach 移除这条流；同一实例的新流可能已经取代了它，此时不动。
func (s *Server) detach(hub p.UUID, c *hubConn) {
	s.hubsMu.Lock()
	if s.hubs[hub] == c {
		delete(s.hubs, hub)
	}
	s.hubsMu.Unlock()
}

func (s *Server) hubSender(hub p.UUID) func(*backendv1.BackendMessage) {
	s.hubsMu.Lock()
	defer s.hubsMu.Unlock()
	if c := s.hubs[hub]; c != nil {
		return c.send
	}
	return nil
}

// Wake 唤醒投递（apply.Service.Notify）。
func (s *Server) Wake() {
	s.hubsMu.Lock()
	if s.wake == nil {
		s.wake = make(chan struct{}, 1)
	}
	w := s.wake
	s.hubsMu.Unlock()
	select {
	case w <- struct{}{}:
	default:
	}
}

// RunDelivery 投递 Outbox，直到 ctx 结束（每秒兜底一次）。
func (s *Server) RunDelivery(ctx context.Context) {
	s.Wake()
	s.hubsMu.Lock()
	w := s.wake
	s.hubsMu.Unlock()
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-w:
		case <-t.C:
		}
		if err := s.deliver(ctx); err != nil && ctx.Err() == nil {
			s.log().Warn("outbox delivery", "err", err)
		}
	}
}

func (s *Server) deliver(ctx context.Context) error {
	return p.InTx(ctx, s.Fleet.DB, func(tx pgx.Tx) error {
		q := db.New(tx)
		rows, err := q.ClaimOutbox(ctx)
		if err != nil {
			return err
		}
		for _, r := range rows {
			if len(r.Payload) >= 16 && (r.Topic == apply.TopicDesired || r.Topic == apply.TopicCommit) {
				var n p.UUID
				copy(n[:], r.Payload)
				node, err := q.GetNode(ctx, n.PG())
				if err == nil && node.EdgeHubID.Valid {
					if send := s.hubSender(p.FromPG(node.EdgeHubID)); send != nil {
						if r.Topic == apply.TopicDesired {
							s.sendConfig(ctx, q, n, send)
						} else if len(r.Payload) == 24 {
							send(commitMsg(n, binary.BigEndian.Uint64(r.Payload[16:])))
						}
					}
				}
			}
			// 实例不在线时同样标记：实例注册时补发其所辖节点的最新配置与未完成的提交
			if err := q.MarkOutboxPublished(ctx, r.ID); err != nil {
				return err
			}
		}
		return nil
	})
}

func commitMsg(n p.UUID, v uint64) *backendv1.BackendMessage {
	return &backendv1.BackendMessage{Body: &backendv1.BackendMessage_Command{Command: &backendv1.NodeApplyCommand{NodeId: uuidMsg(n),
		Command: &edgehubv1.ApplyCommand{Version: v, Action: edgehubv1.ApplyAction_APPLY_ACTION_COMMIT}}}}
}

func (s *Server) sendConfig(ctx context.Context, q *db.Queries, n p.UUID, send func(*backendv1.BackendMessage)) {
	nc, err := q.GetNodeConfig(ctx, n.PG())
	if err != nil {
		return
	}
	var c runtimev1.RuntimeConfig
	if proto.Unmarshal(nc.Config, &c) != nil {
		return
	}
	send(&backendv1.BackendMessage{Body: &backendv1.BackendMessage_Desired{Desired: &backendv1.NodeDesiredConfig{NodeId: uuidMsg(n), Config: &c}}})
}

// pushNode 发送节点的最新配置；withCommit 时若已下发提交而节点尚未生效，再发一次提交。
func (s *Server) pushNode(ctx context.Context, n p.UUID, send func(*backendv1.BackendMessage), withCommit bool) {
	q := db.New(s.Fleet.DB)
	nc, err := q.GetNodeConfig(ctx, n.PG())
	if errors.Is(err, pgx.ErrNoRows) || err != nil {
		return
	}
	s.sendConfig(ctx, q, n, send)
	if withCommit && nc.CommitSent != nil && (nc.AppliedVersion == nil || *nc.AppliedVersion < *nc.CommitSent) {
		send(commitMsg(n, uint64(*nc.CommitSent)))
	}
}

func (s *Server) pushHubNodes(ctx context.Context, hub p.UUID, send func(*backendv1.BackendMessage)) {
	ids, err := db.New(s.Fleet.DB).ListHubNodes(ctx, hub.PG())
	if err != nil {
		return
	}
	for _, id := range ids {
		s.pushNode(ctx, p.FromPG(id), send, true)
	}
}

// onApplyProgress 记录节点的 Prepare / Commit 结果（引擎错误的表示见契约补充 L4）并推进编排。
func (s *Server) onApplyProgress(ctx context.Context, ap *backendv1.NodeApplyProgress) {
	if s.Apply == nil {
		return
	}
	n, err := fromMsg(ap.GetNodeId())
	if err != nil {
		return
	}
	switch r := ap.Report.(type) {
	case *backendv1.NodeApplyProgress_Prepare:
		res := r.Prepare.GetResult()
		var errs []string
		for _, e := range res.GetErrors() {
			errs = append(errs, e.GetCode()+": "+e.GetDetail())
		}
		err = s.Apply.Prepared(ctx, n, res.GetVersion(), errs)
	case *backendv1.NodeApplyProgress_Apply:
		if r.Apply.GetAction() != edgehubv1.ApplyAction_APPLY_ACTION_COMMIT {
			return
		}
		res := r.Apply.GetResult()
		var failed []string
		for _, o := range res.GetObjects() {
			if o.GetStatus() == runtimev1.ObjectStatus_OBJECT_STATUS_FAILED {
				failed = append(failed, o.GetErrorCode())
			}
		}
		err = s.Apply.Applied(ctx, n, res.GetVersion(), len(failed) == 0, strings.Join(failed, "; "))
	}
	if err != nil {
		s.log().Warn("apply progress", "node", n.String(), "err", err)
		return
	}
	if _, err := s.Apply.Advance(ctx); err != nil {
		s.log().Warn("advance", "err", err)
	}
}
