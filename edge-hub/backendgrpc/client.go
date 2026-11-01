package backendgrpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"vortex/edgehub/upstream"
	backendv1 "vortex/proto/gen/go/vortex/backend/v1"
)

// BackendURI 是后端证书的 SAN URI（契约补充 L5）。
const BackendURI = "spiffe://vortex/backend"

// SendQueue 是上行消息队列上限；满了说明后端读得太慢，断开重连（账本从已确认位置重报）。
const SendQueue = 8192

var ErrBackendIdentity = errors.New("backendgrpc: backend certificate does not carry the backend URI")

// ClientTLS：出示 Edge Hub 证书；后端证书按平台 CA 校验链并要求 SAN URI 为 BackendURI，不做主机名校验。
func ClientTLS(cert tls.Certificate, roots *x509.CertPool) *tls.Config {
	return &tls.Config{
		Certificates:       []tls.Certificate{cert},
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: true, // 主机名校验关闭，由下面的链与身份校验代替
		VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
			if len(raw) == 0 {
				return ErrBackendIdentity
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
				if u.String() == BackendURI {
					return nil
				}
			}
			return ErrBackendIdentity
		},
	}
}

// Client 维持到后端的 HubControl.Connect 流。
type Client struct {
	Link    *upstream.Link
	Addr    string
	TLS     *tls.Config
	Region  string
	Version string

	BackoffMin, BackoffMax time.Duration // 默认 1 秒、30 秒（±20% 抖动）
	OnError                func(error)
}

// Run 连接并在断线后退避重连，直至 ctx 结束。失联判定（30 秒进入自治）由模式状态机按最后一次通信计算。
func (c *Client) Run(ctx context.Context) {
	lo, hi := c.BackoffMin, c.BackoffMax
	if lo == 0 {
		lo = time.Second
	}
	if hi == 0 {
		hi = 30 * time.Second
	}
	backoff := lo
	for ctx.Err() == nil {
		start := time.Now()
		err := c.once(ctx)
		if err != nil && c.OnError != nil && ctx.Err() == nil {
			c.OnError(err)
		}
		if time.Since(start) > 10*time.Second {
			backoff = lo // 连接曾稳定：重新从最短退避开始
		}
		d := time.Duration(float64(backoff) * (0.8 + 0.4*rand.Float64()))
		select {
		case <-ctx.Done():
			return
		case <-time.After(d):
		}
		backoff = min(backoff*2, hi)
	}
}

func (c *Client) once(ctx context.Context) error {
	cc, err := grpc.NewClient(c.Addr, grpc.WithTransportCredentials(credentials.NewTLS(c.TLS)),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 10 * time.Second, Timeout: 5 * time.Second}))
	if err != nil {
		return err
	}
	defer cc.Close()
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := backendv1.NewHubControlClient(cc).Connect(sctx)
	if err != nil {
		return err
	}
	out := make(chan *backendv1.HubUpMessage, SendQueue)
	send := func(m upstream.Up) {
		pm, err := Up(m, c.Region, c.Version, time.Now())
		if err != nil || pm == nil {
			if err != nil && c.OnError != nil {
				c.OnError(err)
			}
			return
		}
		select {
		case out <- pm:
		default:
			cancel()
		}
	}
	werr := make(chan error, 1)
	go func() {
		for {
			select {
			case <-sctx.Done():
				werr <- sctx.Err()
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
	defer c.Link.Disconnected()
	// 首条消息由 Connected 写入队列（HubRegister）；后端的任何消息都算一次成功通信
	if err := c.Link.Connected(send); err != nil {
		cancel()
		<-werr
		return err
	}
	var rerr error
	for {
		pm, err := stream.Recv()
		if err != nil {
			rerr = err
			break
		}
		d, err := Down(pm)
		if err != nil {
			rerr = fmt.Errorf("backend message: %w", err)
			break
		}
		if d == nil {
			continue
		}
		if err := c.Link.Handle(d); err != nil {
			rerr = err
			break
		}
	}
	cancel()
	<-werr
	return rerr
}
