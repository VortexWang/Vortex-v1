package certs

import (
	"context"
	"crypto/x509"
	"errors"
	"time"

	"vortex/engine/types"
	"vortex/engine/vlink"
)

// Renewer 在证书剩余有效期少于 1/3 时续签（AGT-012）：本地重新生成私钥与 CSR，依次经各 Edge Hub 提交，
// 校验签发结果（公钥、链、节点 ID 不变）后原子写入文件并替换 Holder 中的证书；失败告警并退避重试。
// 证书过期后 Holder 不再提供证书，新的控制连接无法建立（已有连接不受影响）。
type Renewer struct {
	Holder   *Holder
	Roots    *x509.CertPool
	CertFile string
	KeyFile  string
	// Submit 把 CSR 提交给某个 Edge Hub，返回签发的证书链（叶证书在前）；依次尝试直到成功。
	Submit []func(ctx context.Context, csr []byte) ([][]byte, error)

	Every   time.Duration // 检查周期，默认 1 小时
	Retry   time.Duration // 失败后的重试间隔，默认 1 分钟（每次翻倍，不超过 Every）
	OnEvent func(renewed bool, err error)
	Now     func() time.Time
}

func (r *Renewer) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// Check 检查并在需要时续签。返回是否续签。
func (r *Renewer) Check(ctx context.Context) (bool, error) {
	leaf, err := r.Holder.Leaf()
	if err != nil {
		return false, err
	}
	if !RenewDue(leaf, r.now()) {
		return false, nil
	}
	id, err := nodeID(leaf)
	if err != nil {
		return false, err
	}
	key, err := NewKey()
	if err != nil {
		return false, err
	}
	csr, err := CSR(key)
	if err != nil {
		return false, err
	}
	var errs []error
	for _, submit := range r.Submit {
		chain, err := submit(ctx, csr)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if _, _, err := Verify(chain, key, r.Roots, id); err != nil {
			errs = append(errs, err)
			continue
		}
		if err := Save(r.CertFile, r.KeyFile, chain, key); err != nil {
			return false, err
		}
		c, err := Load(r.CertFile, r.KeyFile)
		if err != nil {
			return false, err
		}
		r.Holder.Set(c)
		return true, nil
	}
	if len(errs) == 0 {
		return false, errors.New("certs: no edge hub to submit the renewal to")
	}
	return false, errors.Join(errs...)
}

// Run 周期检查，直至 ctx 结束。
func (r *Renewer) Run(ctx context.Context) {
	every, retry := r.Every, r.Retry
	if every == 0 {
		every = time.Hour
	}
	if retry == 0 {
		retry = time.Minute
	}
	wait := time.Duration(0)
	backoff := retry
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		renewed, err := r.Check(ctx)
		if r.OnEvent != nil && (renewed || err != nil) {
			r.OnEvent(renewed, err)
		}
		if err != nil {
			wait, backoff = backoff, min(backoff*2, every)
			continue
		}
		wait, backoff = every, retry
	}
}

func nodeID(leaf *x509.Certificate) (types.UUID, error) { return vlink.NodeIDFromCert(leaf) }
