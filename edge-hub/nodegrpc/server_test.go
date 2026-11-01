package nodegrpc

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"vortex/edgehub/clock"
	"vortex/edgehub/ledger"
	"vortex/edgehub/model"
	"vortex/edgehub/session"
	edgehubv1 "vortex/proto/gen/go/vortex/edgehub/v1"
)

type stubBackend struct{ enrolls, renews int }

func (b *stubBackend) EnrollNode(context.Context, *edgehubv1.EnrollRequest, string) (*edgehubv1.EnrollResponse, error) {
	b.enrolls++
	return &edgehubv1.EnrollResponse{}, nil
}

func (b *stubBackend) RenewNodeCert(context.Context, model.UUID, []byte) (*edgehubv1.RenewCertResponse, error) {
	b.renews++
	return &edgehubv1.RenewCertResponse{}, nil
}

// 注册：在线时转发；令牌或 CSR 缺失拒绝；自治时拒绝（HUB-007）；未配置后端时 Unimplemented。
// 续签：没有已校验的客户端证书时拒绝。
func TestEnrollAndRenewForwarding(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC))
	h, err := session.Open(session.Options{HubID: model.UUID{1}, Clock: clk, Store: &ledger.MemStore{}})
	if err != nil {
		t.Fatal(err)
	}
	b := &stubBackend{}
	s := &Server{Hub: h, Backend: b}
	ctx := context.Background()
	req := &edgehubv1.EnrollRequest{EnrollToken: "t", CsrDer: []byte{1}}
	if _, err := s.Enroll(ctx, req); err != nil || b.enrolls != 1 {
		t.Fatalf("online enroll %v", err)
	}
	if _, err := s.Enroll(ctx, &edgehubv1.EnrollRequest{CsrDer: []byte{1}}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing token: %v", err)
	}
	if _, err := s.RenewCert(ctx, &edgehubv1.RenewCertRequest{CsrDer: []byte{1}}); status.Code(err) != codes.Unauthenticated || b.renews != 0 {
		t.Fatalf("renew without a certificate: %v", err)
	}
	clk.Advance(31 * time.Second)
	_ = h.Tick()
	if _, err := s.Enroll(ctx, req); status.Code(err) != codes.Unavailable || b.enrolls != 1 {
		t.Fatalf("enroll while autonomous: %v", err)
	}
	if _, err := (&Server{Hub: h}).Enroll(ctx, req); status.Code(err) != codes.Unimplemented {
		t.Fatalf("no backend: %v", err)
	}
}
