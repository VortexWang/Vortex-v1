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
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"vortex/engine/types"
	"vortex/engine/vlink"
	"vortex/node/agent"
	"vortex/node/agent/certs"
	"vortex/node/agent/control"
	"vortex/node/agent/wire"
	"vortex/node/internal/clock"
	edgehubv1 "vortex/proto/gen/go/vortex/edgehub/v1"
)

var ErrHubIdentity = errors.New("grpcconn: edge hub certificate does not match the expected hub_id")

// HubURI 是 Edge Hub 证书的 SAN URI。
func HubURI(id types.UUID) string { return "spiffe://vortex/hub/" + vlink.FormatUUID(id) }

// ClientTLS 返回连接某个 Edge Hub 的 TLS 配置。
func ClientTLS(cert tls.Certificate, roots *x509.CertPool, hub types.UUID) *tls.Config {
	c := hubTLS(roots, func(u string) bool { return u == HubURI(hub) })
	c.Certificates = []tls.Certificate{cert}
	return c
}

// hubTLS：Edge Hub 证书按平台 CA 校验链，SAN URI 须满足 ok。客户端证书由调用方设置。
func hubTLS(roots *x509.CertPool, ok func(uri string) bool) *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
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
				if ok(u.String()) {
					return nil
				}
			}
			return ErrHubIdentity
		},
	}
}

// Dialer 建立到 Edge Hub 的控制流。Holder 非空时从中取当前证书（续签后之后的连接自动使用新证书），否则用 Cert。
type Dialer struct {
	Cert        tls.Certificate
	Holder      *certs.Holder
	Roots       *x509.CertPool
	DialTimeout time.Duration // 默认 10 秒
}

func (d Dialer) tls(hub types.UUID) *tls.Config {
	if d.Holder == nil {
		return ClientTLS(d.Cert, d.Roots, hub)
	}
	c := hubTLS(d.Roots, func(u string) bool { return u == HubURI(hub) })
	c.GetClientCertificate = d.Holder.GetClientCertificate
	return c
}

type conn struct {
	cc     *grpc.ClientConn
	stream grpc.BidiStreamingClient[edgehubv1.AgentMessage, edgehubv1.HubMessage]
	cancel context.CancelFunc
	smu    sync.Mutex
	once   sync.Once

	sent   time.Time     // Connect 返回（请求头已发出）的本地时刻
	header chan struct{} // 收到响应头（或流结束）后关闭
	sample clock.Sample
	hasT   bool
}

// HubTimeHeader 是 Edge Hub 在 Connect 响应头中写入其墙钟的键（契约补充 L6）。
const HubTimeHeader = "vortex-hub-time-unix-nano"

// watchHeader 等待响应头并记录墙钟比对样本。响应头早于任何消息到达，不阻塞收发。
func (c *conn) watchHeader() {
	defer close(c.header)
	md, err := c.stream.Header()
	recv := time.Now()
	if err != nil {
		return
	}
	v := md.Get(HubTimeHeader)
	if len(v) != 1 {
		return
	}
	ns, err := strconv.ParseInt(v[0], 10, 64)
	if err != nil {
		return
	}
	c.sample, c.hasT = clock.Sample{Sent: c.sent, Received: recv, Remote: time.Unix(0, ns)}, true
}

// ClockSample 返回本连接的墙钟比对样本（control.ClockSampler）；Edge Hub 未提供时 ok 为 false。
func (c *conn) ClockSample(ctx context.Context) (clock.Sample, bool) {
	select {
	case <-c.header:
		return c.sample, c.hasT
	case <-ctx.Done():
		return clock.Sample{}, false
	}
}

func (d Dialer) Dial(ctx context.Context, ep control.Endpoint) (control.Conn, error) {
	timeout := d.DialTimeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	cc, err := grpc.NewClient(ep.Address,
		grpc.WithTransportCredentials(credentials.NewTLS(d.tls(ep.ID))),
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
		c := &conn{cc: cc, stream: r.s, cancel: cancel, sent: time.Now(), header: make(chan struct{})}
		go c.watchHeader()
		return c, nil
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

// ---------------------------------------------------------------- 注册与续签（AGT-001、AGT-012）

func unary(ctx context.Context, addr string, tc *tls.Config, f func(edgehubv1.NodeControlClient) error) error {
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(tc)))
	if err != nil {
		return err
	}
	defer cc.Close()
	return f(edgehubv1.NewNodeControlClient(cc))
}

// Enroll 用一次性注册令牌经 Edge Hub 换取节点证书（不出示客户端证书）。注册时还不知道实例 ID，
// Edge Hub 证书只要求由平台 CA 签发且带 Edge Hub 身份（spiffe://vortex/hub/…）。
func Enroll(ctx context.Context, addr string, roots *x509.CertPool, req *edgehubv1.EnrollRequest) (*edgehubv1.EnrollResponse, error) {
	var resp *edgehubv1.EnrollResponse
	err := unary(ctx, addr, hubTLS(roots, func(u string) bool { return strings.HasPrefix(u, "spiffe://vortex/hub/") }),
		func(c edgehubv1.NodeControlClient) error {
			var err error
			resp, err = c.Enroll(ctx, req)
			return err
		})
	return resp, err
}

// RenewCert 用当前证书经 Edge Hub 续签（本地新私钥，只上传 CSR）。
func (d Dialer) RenewCert(ctx context.Context, ep control.Endpoint, csr []byte) ([][]byte, error) {
	var chain [][]byte
	err := unary(ctx, ep.Address, d.tls(ep.ID), func(c edgehubv1.NodeControlClient) error {
		resp, err := c.RenewCert(ctx, &edgehubv1.RenewCertRequest{CsrDer: csr})
		if err == nil {
			chain = resp.GetCertificateChainDer()
		}
		return err
	})
	return chain, err
}
