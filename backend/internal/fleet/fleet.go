// Package fleet 的这一部分是实例与节点的身份（BE-T08、BE-T10）：一次性令牌、实例引导、节点注册、证书续签与吊销判定。
// 令牌只存 SHA-256 哈希（SEC-004）；全部签发在同一事务内记录证书并消耗令牌。
package fleet

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"vortex/backend/internal/db"
	"vortex/backend/internal/pki"
	p "vortex/backend/internal/platform"
)

// EnrollTokenTTL 是节点注册令牌的有效期（AGT-001）。
const EnrollTokenTTL = 30 * time.Minute

var (
	ErrToken        = errors.New("fleet: invalid, used or expired token")
	ErrRegion       = errors.New("fleet: region does not match the token")
	ErrAddress      = errors.New("fleet: invalid address")
	ErrUnknown      = errors.New("fleet: unknown or deleted subject")
	ErrNotPermitted = errors.New("fleet: not permitted")
)

type Service struct {
	DB  *pgxpool.Pool
	CA  *pki.CA
	Now func() time.Time
	// 证书有效期（测试用较短的值）；0 = pki.NodeLifetime / pki.HubLifetime
	NodeLife, HubLife time.Duration
}

func (s *Service) nodeLife() time.Duration {
	if s.NodeLife > 0 {
		return s.NodeLife
	}
	return pki.NodeLifetime
}

func (s *Service) hubLife() time.Duration {
	if s.HubLife > 0 {
		return s.HubLife
	}
	return pki.HubLifetime
}

func (s *Service) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func newToken() (string, []byte) {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	t := base64.RawURLEncoding.EncodeToString(b)
	return t, hashToken(t)
}

func hashToken(t string) []byte { h := sha256.Sum256([]byte(t)); return h[:] }

// CreateHubBootstrapToken 签发实例引导令牌（明文只返回这一次）。
func (s *Service) CreateHubBootstrapToken(ctx context.Context, region string, ttl time.Duration) (string, error) {
	if region == "" {
		return "", ErrRegion
	}
	tok, h := newToken()
	err := db.New(s.DB).CreateHubBootstrapToken(ctx, db.CreateHubBootstrapTokenParams{ID: p.NewUUIDv7().PG(), TokenHash: h, Region: region,
		ExpiresAt: p.TS(s.now().Add(ttl))})
	return tok, err
}

// CreateEnrollToken 签发节点注册令牌（30 分钟，一次性）；tenant 为零值表示平台节点。
func (s *Service) CreateEnrollToken(ctx context.Context, tenant, createdBy p.UUID) (string, error) {
	tok, h := newToken()
	t := tenant.PG()
	t.Valid = tenant != (p.UUID{})
	now := s.now()
	err := db.New(s.DB).CreateEnrollToken(ctx, db.CreateEnrollTokenParams{ID: p.NewUUIDv7().PG(), TenantID: t, TokenHash: h,
		ExpiresAt: p.TS(now.Add(EnrollTokenTTL)), CreatedBy: createdBy.PG()})
	return tok, err
}

// usable：令牌未使用且未过期。
func usable(used bool, expiresAt, now time.Time) bool { return !used && now.Before(expiresAt) }

// HubEnrollment 是 EnrollHub 的结果。
type HubEnrollment struct {
	HubID  p.UUID
	Issued *pki.Issued
}

// EnrollHub 用引导令牌换取实例身份与证书（HUB-T19）。区域以令牌为准，请求中的区域须一致；advertise 为节点连接本实例的地址（CC-03）。
func (s *Service) EnrollHub(ctx context.Context, token string, csr []byte, region, version, advertise string) (*HubEnrollment, error) {
	if h, port, err := net.SplitHostPort(advertise); err != nil || h == "" || port == "" || port == "0" {
		return nil, fmt.Errorf("%w: advertise %q", ErrAddress, advertise)
	}
	var out *HubEnrollment
	err := p.InTx(ctx, s.DB, func(tx pgx.Tx) error {
		q := db.New(tx)
		t, err := q.LockHubBootstrapToken(ctx, hashToken(token))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrToken
		} else if err != nil {
			return err
		}
		if !usable(t.UsedAt.Valid, t.ExpiresAt.Time, s.now()) {
			return ErrToken
		}
		if region != t.Region {
			return ErrRegion
		}
		id := p.NewUUIDv7()
		is, err := s.CA.Issue(csr, pki.HubURI(id), s.hubLife(), x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth)
		if err != nil {
			return err
		}
		v := version
		if err := q.CreateEdgeHub(ctx, db.CreateEdgeHubParams{ID: id.PG(), Region: t.Region, Address: advertise, HubVersion: &v}); err != nil {
			return err
		}
		if err := s.recordCert(ctx, q, "hub", id, is); err != nil {
			return err
		}
		if err := q.UseHubBootstrapToken(ctx, db.UseHubBootstrapTokenParams{ID: t.ID, UsedByHub: id.PG()}); err != nil {
			return err
		}
		out = &HubEnrollment{HubID: id, Issued: is}
		return nil
	})
	return out, err
}

func (s *Service) recordCert(ctx context.Context, q *db.Queries, kind string, id p.UUID, is *pki.Issued) error {
	return q.InsertCertificate(ctx, db.InsertCertificateParams{ID: p.NewUUIDv7().PG(), SubjectKind: kind, SubjectID: id.PG(), Serial: is.Serial,
		NotBefore: p.TS(is.Leaf.NotBefore), NotAfter: p.TS(is.Leaf.NotAfter)})
}

// NodeEnrollment 是 EnrollNode 的结果。
type NodeEnrollment struct {
	NodeID p.UUID
	Issued *pki.Issued
	Hubs   []db.EdgeHub // 同区域 Edge Hub（AGT-002）
}

// Caps 是节点申报的能力（edgehub.v1.NodeCapabilities）。
type Caps struct {
	Entry, Relay, Exit bool
	PublicAddresses    []string
	AgentVersion       string
}

// EnrollNode 用注册令牌换取节点身份与证书（AGT-001）：节点归属取自令牌（平台节点或某租户的自托管节点），区域为经由的实例所在区域。
func (s *Service) EnrollNode(ctx context.Context, via p.UUID, token string, csr []byte, caps Caps) (*NodeEnrollment, error) {
	var addrs []netip.Addr
	for _, a := range caps.PublicAddresses {
		ad, err := netip.ParseAddr(a)
		if err != nil || !ad.IsGlobalUnicast() || ad.IsPrivate() {
			return nil, fmt.Errorf("%w: public address %q", ErrAddress, a)
		}
		addrs = append(addrs, ad)
	}
	if addrs == nil {
		addrs = []netip.Addr{}
	}
	var out *NodeEnrollment
	err := p.InTx(ctx, s.DB, func(tx pgx.Tx) error {
		q := db.New(tx)
		hub, err := q.GetEdgeHub(ctx, via.PG())
		if err != nil {
			return ErrUnknown
		}
		t, err := q.LockEnrollToken(ctx, hashToken(token))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrToken
		} else if err != nil {
			return err
		}
		if !usable(t.UsedAt.Valid, t.ExpiresAt.Time, s.now()) {
			return ErrToken
		}
		id := p.NewUUIDv7()
		is, err := s.CA.Issue(csr, pki.NodeURI(id), s.nodeLife(), x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth)
		if err != nil {
			return err
		}
		ver := caps.AgentVersion
		if err := q.CreateNode(ctx, db.CreateNodeParams{ID: id.PG(), TenantID: t.TenantID, Name: "node-" + id.String()[:8], Region: hub.Region,
			EdgeHubID: via.PG(), CanEntry: caps.Entry, CanRelay: caps.Relay, CanExit: caps.Exit, PublicAddresses: addrs, AgentVersion: &ver}); err != nil {
			return err
		}
		if err := s.recordCert(ctx, q, "node", id, is); err != nil {
			return err
		}
		if err := q.UseEnrollToken(ctx, db.UseEnrollTokenParams{ID: t.ID, UsedByNode: id.PG()}); err != nil {
			return err
		}
		hubs, err := q.ListRegionHubs(ctx, hub.Region)
		if err != nil {
			return err
		}
		out = &NodeEnrollment{NodeID: id, Issued: is, Hubs: hubs}
		return nil
	})
	return out, err
}

// RenewNodeCert 续签节点证书（AGT-012）：节点须存在且未删除、未停用，且与经由的实例在同一区域；
// 节点 ID 来自经由实例转发的节点 mTLS 证书。
func (s *Service) RenewNodeCert(ctx context.Context, via, node p.UUID, csr []byte) (*pki.Issued, error) {
	var out *pki.Issued
	err := p.InTx(ctx, s.DB, func(tx pgx.Tx) error {
		q := db.New(tx)
		n, err := q.GetNode(ctx, node.PG())
		if err != nil || n.DeletedAt.Valid {
			return ErrUnknown
		}
		hub, err := q.GetEdgeHub(ctx, via.PG())
		if err != nil {
			return ErrUnknown
		}
		if hub.Region != n.Region {
			return ErrRegion
		}
		if n.Lifecycle == "disabled" {
			return ErrNotPermitted
		}
		is, err := s.CA.Issue(csr, pki.NodeURI(node), s.nodeLife(), x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth)
		if err != nil {
			return err
		}
		out = is
		return s.recordCert(ctx, q, "node", node, is)
	})
	return out, err
}

// RenewHubCert 续签实例证书（CC-02）。
func (s *Service) RenewHubCert(ctx context.Context, hub p.UUID, csr []byte) (*pki.Issued, error) {
	var out *pki.Issued
	err := p.InTx(ctx, s.DB, func(tx pgx.Tx) error {
		q := db.New(tx)
		if _, err := q.GetEdgeHub(ctx, hub.PG()); err != nil {
			return ErrUnknown
		}
		is, err := s.CA.Issue(csr, pki.HubURI(hub), s.hubLife(), x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth)
		if err != nil {
			return err
		}
		out = is
		return s.recordCert(ctx, q, "hub", hub, is)
	})
	return out, err
}

// CertUsable 报告某证书是否由本平台签发、属于该主体且未吊销（SEC-002）。
func (s *Service) CertUsable(ctx context.Context, kind string, subject p.UUID, serial []byte) bool {
	c, err := db.New(s.DB).GetCertificateBySerial(ctx, serial)
	return err == nil && c.SubjectKind == kind && p.FromPG(c.SubjectID) == subject && !c.RevokedAt.Valid
}

// Revoke 吊销证书并推进吊销列表版本。
func (s *Service) Revoke(ctx context.Context, serial []byte, reason string) error {
	return p.InTx(ctx, s.DB, func(tx pgx.Tx) error {
		q := db.New(tx)
		n, err := q.RevokeCertificate(ctx, db.RevokeCertificateParams{Serial: serial, RevokeReason: &reason})
		if err != nil {
			return err
		}
		if n == 0 {
			return ErrUnknown
		}
		_, err = q.BumpRevocationList(ctx, reason)
		return err
	})
}

// Revocations 返回吊销列表（版本、未过期的已吊销序列号）。
func (s *Service) Revocations(ctx context.Context) (int64, [][]byte, error) {
	q := db.New(s.DB)
	v, err := q.CurrentRevocationSerial(ctx)
	if err != nil {
		return 0, nil, err
	}
	ss, err := q.ListRevokedSerials(ctx)
	return v, ss, err
}
