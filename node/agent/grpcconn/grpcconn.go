// Package grpcconn 是 Agent 控制连接的 gRPC 传输（NodeControl.Connect，AGT-002、SEC-001）：实现
// control.Dialer / control.Conn。
//
// TLS 1.3 双向认证：Agent 出示节点证书；Edge Hub 证书按平台 CA 校验，身份取自 SAN URI
// `spiffe://vortex/hub/<hub_id>`（契约补充 L5），必须与实例列表中的 hub_id 一致；不做主机名校验
// （与 VLink 相同，地址可能是 IP 或经负载均衡的域名，身份以证书 URI 为准）。
package grpcconn

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"vortex/engine/types"
	"vortex/engine/vlink"
	"vortex/node/agent"
	"vortex/node/agent/control"
	"vortex/node/agent/wire"
	edgehubv1 "vortex/proto/gen/go/vortex/edgehub/v1"
)

var ErrHubIdentity = errors.New("grpcconn: edge hub certificate does not match the expected hub_id")

// HubURI 是 Edge Hub 证书的 SAN URI。
func HubURI(id types.UUID) string { return "spiffe://vortex/hub/" + vlink.FormatUUID(id) }

// ClientTLS 返回连接某个 Edge Hub 的 TLS 配置。
func ClientTLS(cert tls.Certificate, roots *x509.CertPool, hub types.UUID) *tls.Config {
	want := HubURI(hub)
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
		// 主机名校验关闭，改为下面的链与身份校验（不是跳过校验）
		InsecureSkipVerify: true,
		VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
			if len(raw) == 0 {
				return ErrHubIdentity
			}
			certs := make([]*x509.Certificate, len(raw))
			for i, r := range raw {
				c, err := x509.ParseCertificate(r)
				if err != nil {
					return err
				}
				certs[i] = c
			}
			inter := x509.NewCertPool()
			for _, c := range certs[1:] {
				inter.AddCert(c)
			}
			if _, err := certs[0].Verify(x509.VerifyOptions{Roots: roots, Intermediates: inter,
				KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err != nil {
				return err
			}
			for _, u := range certs[0].URIs {
				if u.String() == want {
					return nil
				}
			}
			return fmt.Errorf("%w: want %s", ErrHubIdentity, want)
		},
	}
}

// Dialer 建立到 Edge Hub 的控制流。
type Dialer struct {
	Cert        tls.Certificate
	Roots       *x509.CertPool
	DialTimeout time.Duration // 默认 10 秒
}

type conn struct {
	cc     *grpc.ClientConn
	stream grpc.BidiStreamingClient[edgehubv1.AgentMessage, edgehubv1.HubMessage]
	cancel context.CancelFunc
	smu    sync.Mutex
	once   sync.Once
}

func (d Dialer) Dial(ctx context.Context, ep control.Endpoint) (control.Conn, error) {
	timeout := d.DialTimeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	cc, err := grpc.NewClient(ep.Address,
		grpc.WithTransportCredentials(credentials.NewTLS(ClientTLS(d.Cert, d.Roots, ep.ID))),
		// 传输层保活：连接中断尽快发现（控制流的应用层失联判定仍由 Edge Hub 的模式与授权有效期承担）
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 20 * time.Second, Timeout: 10 * time.Second}))
	if err != nil {
		return nil, err
	}
	sctx, cancel := context.WithCancel(context.Background())
	dctx, dcancel := context.WithTimeout(ctx, timeout)
	defer dcancel()
	type res struct {
		s   grpc.BidiStreamingClient[edgehubv1.AgentMessage, edgehubv1.HubMessage]
		err error
	}
	ch := make(chan res, 1)
	go func() {
		s, err := edgehubv1.NewNodeControlClient(cc).Connect(sctx, grpc.WaitForReady(true))
		ch <- res{s, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			cancel()
			cc.Close()
			return nil, r.err
		}
		return &conn{cc: cc, stream: r.s, cancel: cancel}, nil
	case <-dctx.Done():
		cancel()
		cc.Close()
		return nil, dctx.Err()
	}
}

func (c *conn) Send(m agent.Out) error {
	pm, err := wire.AgentMessage(m)
	if err != nil || pm == nil {
		return err
	}
	c.smu.Lock()
	defer c.smu.Unlock()
	return c.stream.Send(pm)
}

func (c *conn) Recv(ctx context.Context) (agent.In, error) {
	for {
		type res struct {
			m   *edgehubv1.HubMessage
			err error
		}
		ch := make(chan res, 1)
		go func() {
			m, err := c.stream.Recv()
			ch <- res{m, err}
		}()
		var r res
		select {
		case r = <-ch:
		case <-ctx.Done():
			c.Close()
			return nil, ctx.Err()
		}
		if r.err != nil {
			return nil, r.err
		}
		in, err := wire.HubMessage(r.m)
		if err != nil {
			return nil, err
		}
		if in != nil {
			return in, nil
		}
	}
}

func (c *conn) Close() error {
	c.once.Do(func() {
		c.smu.Lock()
		_ = c.stream.CloseSend()
		c.smu.Unlock()
		c.cancel()
		c.cc.Close()
	})
	return nil
}
