package vlink

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"vortex/engine/types"
)

// HelloTimeout：TLS 完成后 5 秒内必须收到对端 HELLO（R2）。
const HelloTimeout = 5 * time.Second

// Local 是本端 HELLO 的内容与校验对端所需的本地信息。
type Local struct {
	Identity     *Identity
	Trust        *Trust
	BootID       types.UUID
	Generation   uint64
	Capabilities uint64
	Mode         LinkMode
	// KnownGeneration 返回本地已知的对端最低代次；对端 HELLO 中的代次低于它即拒绝（防止旧启动重放）。nil 表示 0。
	KnownGeneration func(peer types.UUID) uint64
	Timeout         time.Duration // TLS + HELLO 总时限，默认 HelloTimeout
}

// Expect 是本次建链对对端的预期。
type Expect struct {
	NodeID types.UUID // 发起方填写要连接的节点；接受方为零值
	// Served：本端是反向链路的接受方（上游）时，对端应服务的规则集。非 nil 时与对端 HELLO 的摘要比对（R1）。
	Served []ServedRule
	// SendServed：本端是反向建链发起方时，自己服务的规则集（写入 HELLO 的 served_digest）。
	SendServed []ServedRule
	Reverse    bool // 本端是反向建链发起方
}

// PeerInfo 是握手得到的对端信息。
type PeerInfo struct {
	NodeID         types.UUID
	Hello          *Hello
	Version        uint16 // 协商结果 min(双方版本)
	Capabilities   uint64 // 双方能力位的交集
	ServedMismatch bool   // 摘要不一致：记录事件并照常建链，每条流仍由 OPEN 校验把关（ENG-020）
}

// Established 是完成 TLS 与 HELLO 的链路。后续帧必须继续使用 Reader（它可能已缓冲 HELLO 之后的数据）。
type Established struct {
	TLS    *tls.Conn
	Reader *Reader
	Peer   PeerInfo
}

// HandshakeError 携带发送给对端的 GOAWAY 码（TLS 层失败时 Sent 为 false）。
type HandshakeError struct {
	Code GoawayCode
	Sent bool
	Err  error
}

func (e *HandshakeError) Error() string { return fmt.Sprintf("vlink handshake: %v", e.Err) }
func (e *HandshakeError) Unwrap() error { return e.Err }

var ErrStaleGeneration = errors.New("vlink: peer generation lower than known")
var ErrVersion = errors.New("vlink: protocol version unsupported")

// Handshake 在已建立的 TCP 连接（包成 tls.Conn）上完成 TLS 1.3 握手与 HELLO 交换（C1 第 1、4 节）：
// 双方在 TLS 完成后立即各自发送 HELLO，不等待对方（R6）；HELLO 必须是第一帧。失败时关闭连接，
// HELLO 阶段的失败先发送 GOAWAY。
func Handshake(ctx context.Context, tc *tls.Conn, l *Local, ex Expect) (*Established, error) {
	timeout := l.Timeout
	if timeout <= 0 {
		timeout = HelloTimeout
	}
	deadline := time.Now().Add(timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = tc.SetDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { _ = tc.SetDeadline(time.Unix(1, 0)) })
	defer stop()

	if err := tc.HandshakeContext(ctx); err != nil {
		_ = tc.Close()
		return nil, &HandshakeError{Err: err}
	}
	cs := tc.ConnectionState()
	peerID, err := l.Trust.VerifyPeer(&cs, ex.NodeID) // VerifyConnection 已校验；这里取 node_id
	if err != nil {
		_ = tc.Close()
		return nil, &HandshakeError{Err: err}
	}

	signer, err := l.Identity.Signer()
	if err != nil {
		_ = tc.Close()
		return nil, &HandshakeError{Err: err}
	}
	mine := &Hello{Version: ProtocolVersion, NodeID: l.Identity.NodeID, BootID: l.BootID, Generation: l.Generation,
		Capabilities: l.Capabilities, LinkMode: l.Mode}
	if ex.Reverse {
		mine.HasServed = true
		mine.ServedCount, mine.ServedDigest = ServedDigest(ex.SendServed)
	}
	if err := mine.Sign(&cs, signer); err != nil {
		_ = tc.Close()
		return nil, &HandshakeError{Err: err}
	}
	frame := AppendFrame(nil, TypeHello, 0, mine.AppendPayload(nil))
	wrote := make(chan error, 1)
	go func() { _, err := tc.Write(frame); wrote <- err }()

	fail := func(code GoawayCode, err error) (*Established, error) {
		sent := false
		if <-wrote == nil { // GOAWAY 排在本端 HELLO 之后
			_ = tc.SetWriteDeadline(time.Now().Add(time.Second))
			_, werr := tc.Write(AppendFrame(nil, TypeGoaway, 0, AppendGoawayPayload(nil, 0, code)))
			sent = werr == nil
		}
		_ = tc.Close()
		return nil, &HandshakeError{Code: code, Sent: sent, Err: err}
	}

	r := NewReader(tc)
	h, p, err := r.Next()
	if err != nil {
		return fail(GoawayProtocolError, err)
	}
	if h.Type != TypeHello {
		return fail(GoawayProtocolError, fmt.Errorf("%w: first frame is %v, not HELLO", ErrProtocol, h.Type))
	}
	ph, err := ParseHello(p)
	if err != nil {
		return fail(GoawayProtocolError, err)
	}
	if ph.NodeID != peerID {
		return fail(GoawayProtocolError, fmt.Errorf("%w: HELLO node_id %s != certificate %s", ErrProtocol, FormatUUID(ph.NodeID), FormatUUID(peerID)))
	}
	if err := ph.verifySig(&cs); err != nil {
		return fail(GoawayProtocolError, err)
	}
	if l.KnownGeneration != nil && ph.Generation < l.KnownGeneration(peerID) {
		return fail(GoawayProtocolError, ErrStaleGeneration)
	}
	ver := min(ph.Version, ProtocolVersion)
	if ver < MinVersion {
		return fail(GoawayVersionUnsupported, fmt.Errorf("%w: peer version %d", ErrVersion, ph.Version))
	}
	info := PeerInfo{NodeID: peerID, Hello: ph, Version: ver, Capabilities: ph.Capabilities & l.Capabilities}
	if ex.Served != nil {
		n, d := ServedDigest(ex.Served)
		info.ServedMismatch = !ph.HasServed || ph.ServedCount != n || ph.ServedDigest != d
	}
	if err := <-wrote; err != nil {
		_ = tc.Close()
		return nil, &HandshakeError{Err: err}
	}
	_ = tc.SetDeadline(time.Time{})
	return &Established{TLS: tc, Reader: r, Peer: info}, nil
}
