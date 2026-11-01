package app

import (
	"math/big"
	"testing"

	"google.golang.org/protobuf/proto"

	"vortex/edgehub/ledger"
	"vortex/edgehub/model"
	"vortex/edgehub/session"
	"vortex/edgehub/upstream"
	"vortex/edgehub/wire"
	commonv1 "vortex/proto/gen/go/vortex/common/v1"
	runtimev1 "vortex/proto/gen/go/vortex/runtime/v1"
)

// SEC-002：吊销列表中的节点或证书序列号拒绝建立控制流；没有吊销列表时都放行。
func TestRevoked(t *testing.T) {
	h, err := session.Open(session.Options{HubID: model.UUID{1}, Store: &ledger.MemStore{}})
	if err != nil {
		t.Fatal(err)
	}
	l := upstream.New(h)
	check := revoked(l)
	bad, good := model.UUID{0xba}, model.UUID{0x90}
	if check(bad, nil) {
		t.Fatal("revoked without a list")
	}
	raw, _ := proto.Marshal(&runtimev1.RevocationList{Serial: 2, NodeIds: []*commonv1.Uuid{wire.UUID(bad)}, CertSerials: [][]byte{big.NewInt(77).Bytes()}})
	_ = l.Handle(upstream.Revocations{Serial: 2, Payload: raw})
	if !check(bad, big.NewInt(1).Bytes()) || check(good, big.NewInt(1).Bytes()) || !check(good, append([]byte{0}, big.NewInt(77).Bytes()...)) {
		t.Fatal("revocation decision")
	}
}
