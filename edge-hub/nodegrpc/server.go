// Package nodegrpc 是面向 Agent 的 NodeControl gRPC 服务（HUB-001、HUB-T02、SEC-001、SEC-002）。
//
// 传输：TLS 1.3 双向认证。节点身份只取自客户端证书 SAN URI `spiffe://vortex/node/<node_id>`（与 VLink
// 相同，契约 C1 第 1 节）；Enroll 允许无客户端证书（一次性令牌换证书），因此握手层只要求“提供了就校验”，
// Connect 在应用层要求证书。吊销的节点与证书序列号拒绝建立控制流（SEC-002）。
//
// Connect：每条流一个会话（session.Session）；会话的发送不阻塞——消息进入有界队列，由写协程发送；
// 队列满说明客户端读得太慢，关闭这条流（Agent 会重连并按序重发）。
package nodegrpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"vortex/edgehub/issuer"
	"vortex/edgehub/model"
	"vortex/edgehub/session"
	"vortex/edgehub/wire"
	edgehubv1 "vortex/proto/gen/go/vortex/edgehub/v1"
)

// SendQueue 是每条流的待发消息上限。
const SendQueue = 4096

var ErrNoNodeURI = errors.New("nodegrpc: client certificate has no vortex node URI")

// NodeIDFromCert 从证书 SAN URI spiffe://vortex/node/<uuid> 取 node_id（恰好一个）。
func NodeIDFromCert(c *x509.Certificate) (model.UUID, error) {
	var id model.UUID
	found := 0
	for _, u := range c.URIs {
		if u.Scheme != "spiffe" || u.Host != "vortex" || !strings.HasPrefix(u.Path, "/node/") {
			continue
		}
		v, err := ParseUUID(strings.TrimPrefix(u.Path, "/node/"))
		if err != nil {
			return id, fmt.Errorf("%w: %v", ErrNoNodeURI, err)
		}
		id = v
		found++
	}
	if found != 1 {
		return id, ErrNoNodeURI
	}
	return id, nil
}

// ParseUUID 解析 8-4-4-4-12 形式。
func ParseUUID(s string) (model.UUID, error) {
	var u model.UUID
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return u, fmt.Errorf("bad uuid %q", s)
	}
	h := strings.ReplaceAll(s, "-", "")
	return u, u.UnmarshalText([]byte(h))
}

// HubURI 是 Edge Hub 证书的 SAN URI（契约补充 L5）。
func HubURI(id model.UUID) string { return "spiffe://vortex/hub/" + id.String() }

// ServerTLS 返回服务端 TLS 配置：TLS 1.3，客户端证书提供了就按平台 CA 校验。
func ServerTLS(cert tls.Certificate, roots *x509.CertPool) *tls.Config {
	return &tls.Config{Certificates: []tls.Certificate{cert}, ClientCAs: roots, ClientAuth: tls.VerifyClientCertIfGiven,
		MinVersion: tls.VersionTLS13}
}

// Server 实现 NodeControl。
type Server struct {
	edgehubv1.UnimplementedNodeControlServer
	Hub    *session.Hub
	Region string
	// Revoked 判断节点或证书是否已吊销（吊销列表随后端同步，SEC-002）；nil 表示没有吊销。
	Revoked func(node model.UUID, serial []byte) bool

	mu      sync.Mutex
	streams int
}

// Streams 返回当前控制流数。
func (s *Server) Streams() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.streams
}

func nodeFromContext(ctx context.Context) (model.UUID, *x509.Certificate, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return model.UUID{}, nil, status.Error(codes.Unauthenticated, "no peer")
	}
	ti, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(ti.State.VerifiedChains) == 0 || len(ti.State.VerifiedChains[0]) == 0 {
		return model.UUID{}, nil, status.Error(codes.Unauthenticated, "client certificate required")
	}
	leaf := ti.State.VerifiedChains[0][0]
	id, err := NodeIDFromCert(leaf)
	if err != nil {
		return model.UUID{}, nil, status.Error(codes.Unauthenticated, err.Error())
	}
	return id, leaf, nil
}

// Connect 实现控制主通道。
func (s *Server) Connect(stream grpc.BidiStreamingServer[edgehubv1.AgentMessage, edgehubv1.HubMessage]) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	node, cert, err := nodeFromContext(ctx)
	if err != nil {
		return err
	}
	if s.Revoked != nil && s.Revoked(node, cert.SerialNumber.Bytes()) {
		return status.Error(codes.PermissionDenied, "certificate or node revoked")
	}
	s.mu.Lock()
	s.streams++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.streams--
		s.mu.Unlock()
	}()

	out := make(chan *edgehubv1.HubMessage, SendQueue)
	var overflow sync.Once
	sess := s.Hub.NewSession(node, func(m session.HubMsg) {
		pm, err := wire.ToAgent(s.Hub.ID, s.Region, m)
		if err != nil {
			return
		}
		select {
		case out <- pm:
		default:
			overflow.Do(cancel) // 读得太慢：关闭这条流，Agent 重连后按序重发
		}
	})
	defer sess.Close()

	werr := make(chan error, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				werr <- nil
				return
			case m := <-out:
				if err := stream.Send(m); err != nil {
					werr <- err
					cancel()
					return
				}
			}
		}
	}()
	rerr := make(chan error, 1)
	go func() {
		for {
			pm, err := stream.Recv()
			if err != nil {
				rerr <- err
				return
			}
			am, err := wire.FromAgent(pm)
			if err != nil {
				rerr <- status.Error(codes.InvalidArgument, err.Error())
				return
			}
			if am == nil {
				continue
			}
			if err := sess.Handle(am); err != nil {
				rerr <- handleErr(err)
				return
			}
		}
	}()
	select {
	case err = <-rerr:
	case <-ctx.Done():
		err = status.Error(codes.ResourceExhausted, "send queue overflow or stream cancelled")
	}
	cancel()
	<-werr
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func handleErr(err error) error {
	switch {
	case errors.Is(err, session.ErrNoHello):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, session.ErrNodeMismatch):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, session.ErrReplaced):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, issuer.ErrStaleBoot):
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}
