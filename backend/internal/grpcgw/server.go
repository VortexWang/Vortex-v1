// Package grpcgw 是面向 Edge Hub 的 HubControl 服务（BE-T11；契约 C2）：实例引导、代理节点注册与续签、实例证书
// 续签（CC-02）与控制主通道。组件间一律 mTLS（SEC-001）：除 EnrollHub 外，调用方须出示本平台签发、未吊销的
// Edge Hub 证书，身份只取自证书 SAN URI。
package grpcgw

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"vortex/backend/internal/apply"
	"vortex/backend/internal/fleet"
	"vortex/backend/internal/leases"
	"vortex/backend/internal/metering"
	"vortex/backend/internal/pki"
	p "vortex/backend/internal/platform"
	backendv1 "vortex/proto/gen/go/vortex/backend/v1"
	commonv1 "vortex/proto/gen/go/vortex/common/v1"
	edgehubv1 "vortex/proto/gen/go/vortex/edgehub/v1"
)

type Server struct {
	backendv1.UnimplementedHubControlServer
	Fleet *fleet.Service
	Log   *slog.Logger
	// ReconcileQuiet：对账中（STARTED 之后）多久没有新的账本条目即视为上报完毕并下发 COMPLETED。
	// 暂行规则，BE-T23 实现结清与重算后替换；默认 1 秒。
	ReconcileQuiet time.Duration
	// Apply 处理节点的 Prepare / Commit 报告并推进编排（BE-T15）；nil 时只记录。
	Apply *apply.Service
	// Leases 下发限制快照、份额与区域额度（BE-T20、BE-T21）；nil 时不下发。
	Leases *leases.Service
	// Metering 入账首次落库的账本条目（BE-T18）；nil 时只落库。
	Metering *metering.Service

	certMu sync.Mutex
	cert   *tls.Certificate

	hubsMu sync.Mutex
	hubs   map[p.UUID]*hubConn // 当前连接的实例 → 下行发送
	wake   chan struct{}
}

// TLSConfig 返回 HubControl 的服务端 TLS：后端证书由平台 CA 自签发（spiffe://vortex/backend），剩余 1/3 时更换；
// 客户端证书“提供了就校验”（EnrollHub 允许无证书），校验链到平台根。
func (s *Server) TLSConfig() *tls.Config {
	return &tls.Config{MinVersion: tls.VersionTLS13, ClientCAs: s.Fleet.CA.Pool, ClientAuth: tls.VerifyClientCertIfGiven,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return s.serverCert() }}
}

func (s *Server) serverCert() (*tls.Certificate, error) {
	s.certMu.Lock()
	defer s.certMu.Unlock()
	if c := s.cert; c != nil {
		life := c.Leaf.NotAfter.Sub(c.Leaf.NotBefore)
		if c.Leaf.NotAfter.Sub(time.Now()) > life/3 {
			return c, nil
		}
	}
	c, err := s.Fleet.CA.ServerCert()
	if err != nil {
		return nil, err
	}
	s.cert = &c
	return s.cert, nil
}

func (s *Server) log() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.Default()
}

// hubIdentity 取出调用方的 Edge Hub 身份：证书链已在握手中校验到平台根；这里要求 SAN URI 为实例身份、
// 证书由本平台签发给该实例且未吊销。
func (s *Server) hubIdentity(ctx context.Context) (p.UUID, error) {
	pr, ok := peer.FromContext(ctx)
	if !ok {
		return p.UUID{}, status.Error(codes.Unauthenticated, "no peer")
	}
	ti, ok := pr.AuthInfo.(credentials.TLSInfo)
	if !ok || len(ti.State.VerifiedChains) == 0 {
		return p.UUID{}, status.Error(codes.Unauthenticated, "an Edge Hub client certificate is required")
	}
	leaf := ti.State.VerifiedChains[0][0]
	kind, id, err := pki.ParseIdentity(leaf)
	if err != nil || kind != "hub" {
		return p.UUID{}, status.Error(codes.PermissionDenied, "not an Edge Hub identity")
	}
	if !s.Fleet.CertUsable(ctx, "hub", id, leaf.SerialNumber.Bytes()) {
		return p.UUID{}, status.Error(codes.PermissionDenied, "certificate unknown or revoked")
	}
	return id, nil
}

func grpcErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, fleet.ErrToken), errors.Is(err, fleet.ErrRegion), errors.Is(err, fleet.ErrNotPermitted):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, pki.ErrCSR), errors.Is(err, fleet.ErrAddress):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, fleet.ErrUnknown):
		return status.Error(codes.NotFound, err.Error())
	}
	return status.Error(codes.Internal, "internal error")
}

func uuidMsg(u p.UUID) *commonv1.Uuid { return &commonv1.Uuid{Value: append([]byte(nil), u[:]...)} }

func fromMsg(u *commonv1.Uuid) (p.UUID, error) {
	var id p.UUID
	if len(u.GetValue()) != 16 {
		return id, status.Error(codes.InvalidArgument, "bad uuid")
	}
	copy(id[:], u.GetValue())
	return id, nil
}

// EnrollHub：一次性引导令牌 + CSR → hub_id 与实例证书（HUB-T19；允许无客户端证书）。
func (s *Server) EnrollHub(ctx context.Context, r *backendv1.EnrollHubRequest) (*backendv1.EnrollHubResponse, error) {
	e, err := s.Fleet.EnrollHub(ctx, r.GetBootstrapToken(), r.GetCsrDer(), r.GetRegion(), r.GetHubVersion(), r.GetAdvertiseAddress())
	if err != nil {
		s.log().Warn("hub enrollment refused", "region", r.GetRegion(), "err", err)
		return nil, grpcErr(err)
	}
	s.log().Info("hub enrolled", "hub", e.HubID.String(), "region", r.GetRegion(), "advertise", r.GetAdvertiseAddress())
	return &backendv1.EnrollHubResponse{HubId: uuidMsg(e.HubID), CertificateChainDer: e.Issued.Chain, CaBundleDer: s.Fleet.CA.Bundle()}, nil
}

// EnrollNode：经 Edge Hub 代理的节点注册（AGT-001）。
func (s *Server) EnrollNode(ctx context.Context, r *backendv1.EnrollNodeRequest) (*edgehubv1.EnrollResponse, error) {
	hub, err := s.hubIdentity(ctx)
	if err != nil {
		return nil, err
	}
	req := r.GetRequest()
	c := req.GetCapabilities()
	e, err := s.Fleet.EnrollNode(ctx, hub, req.GetEnrollToken(), req.GetCsrDer(), fleet.Caps{Entry: c.GetCanEntry(), Relay: c.GetCanRelay(),
		Exit: c.GetCanExit(), PublicAddresses: c.GetPublicAddresses(), AgentVersion: req.GetAgentVersion()})
	if err != nil {
		s.log().Warn("node enrollment refused", "via", hub.String(), "remote", r.GetObservedRemoteAddress(), "err", err)
		return nil, grpcErr(err)
	}
	resp := &edgehubv1.EnrollResponse{NodeId: uuidMsg(e.NodeID), CertificateChainDer: e.Issued.Chain, CaBundleDer: s.Fleet.CA.Bundle()}
	for _, h := range e.Hubs {
		if h.Address == "" {
			continue
		}
		resp.EdgeHubs = append(resp.EdgeHubs, &edgehubv1.HubEndpoint{HubId: uuidMsg(p.FromPG(h.ID)), Address: h.Address, Region: h.Region})
	}
	s.log().Info("node enrolled", "node", e.NodeID.String(), "via", hub.String())
	return resp, nil
}

// RenewNodeCert：经 Edge Hub 代理的节点证书续签（AGT-012）；node_id 由 Edge Hub 取自节点的 mTLS 证书。
func (s *Server) RenewNodeCert(ctx context.Context, r *backendv1.RenewNodeCertRequest) (*edgehubv1.RenewCertResponse, error) {
	hub, err := s.hubIdentity(ctx)
	if err != nil {
		return nil, err
	}
	node, err := fromMsg(r.GetNodeId())
	if err != nil {
		return nil, err
	}
	is, err := s.Fleet.RenewNodeCert(ctx, hub, node, r.GetCsrDer())
	if err != nil {
		return nil, grpcErr(err)
	}
	return &edgehubv1.RenewCertResponse{CertificateChainDer: is.Chain}, nil
}

// RenewHubCert：实例证书续签（CC-02），身份取自当前 mTLS 证书。
func (s *Server) RenewHubCert(ctx context.Context, r *edgehubv1.RenewCertRequest) (*edgehubv1.RenewCertResponse, error) {
	hub, err := s.hubIdentity(ctx)
	if err != nil {
		return nil, err
	}
	is, err := s.Fleet.RenewHubCert(ctx, hub, r.GetCsrDer())
	if err != nil {
		return nil, grpcErr(err)
	}
	s.log().Info("hub certificate renewed", "hub", hub.String())
	return &edgehubv1.RenewCertResponse{CertificateChainDer: is.Chain}, nil
}
