// Package localapi 是节点本地运维接口（AGT-T16，02 方案 3.10）：Unix socket 上的 HTTP，供 vortex-node 的
// status / drain 子命令使用。socket 权限 0600，只有运行 Agent 的用户（通常为 root）可以访问。
//
//	GET  /v1/status                   → Status（JSON）
//	POST /v1/drain {"timeout_s","rules"} → 排空完成后返回 {"ok":true} 或 {"error":"..."}
package localapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"
)

// Status 是 status 子命令的输出（02 方案 3.10）。
type Status struct {
	NodeID          string           `json:"node_id"`
	BootID          string           `json:"boot_id"`
	PreviousBootID  string           `json:"previous_boot_id,omitempty"`
	Hub             string           `json:"hub,omitempty"`
	HubMode         string           `json:"hub_mode"`
	Connected       bool             `json:"connected"`
	Phase           string           `json:"phase"`
	DesiredVersion  uint64           `json:"applied_config_version"`
	ActualVersion   uint64           `json:"actual_version"`
	GrantRemainingS map[string]int64 `json:"run_grant_remaining_s"`
	PendingBatches  int              `json:"pending_usage_batches"`
	DiskFreeBytes   uint64           `json:"disk_free_bytes"`
	DiskLow         bool             `json:"disk_low"`
	// 与所连 Edge Hub 的墙钟偏差（正值表示 Edge Hub 快）；尚未比对时省略。超过 5 秒只告警（AGT-005）。
	ClockSkewMs    *int64 `json:"clock_skew_ms,omitempty"`
	ClockSkewAlert bool   `json:"clock_skew_alert"`
}

type DrainRequest struct {
	TimeoutS int      `json:"timeout_s"` // 0 = 300
	Rules    []string `json:"rules,omitempty"`
}

type drainResponse struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// Server 提供本地接口。
type Server struct {
	Status func() Status
	Drain  func(ctx context.Context, req DrainRequest) error

	srv *http.Server
	ln  net.Listener
}

// Listen 在 path 上创建 socket（已存在的旧 socket 先删除）。
func (s *Server) Listen(path string) error {
	if fi, err := os.Lstat(path); err == nil && fi.Mode()&os.ModeSocket != 0 {
		_ = os.Remove(path)
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		ln.Close()
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.Status())
	})
	mux.HandleFunc("POST /v1/drain", func(w http.ResponseWriter, r *http.Request) {
		var req DrainRequest
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		timeout := time.Duration(req.TimeoutS) * time.Second
		if timeout <= 0 {
			timeout = 300 * time.Second
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		resp := drainResponse{OK: true}
		if err := s.Drain(ctx, req); err != nil {
			resp = drainResponse{Error: err.Error()}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
	s.ln = ln
	s.srv = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = s.srv.Serve(ln) }()
	return nil
}

func (s *Server) Close() error {
	if s.srv == nil {
		return nil
	}
	return s.srv.Close()
}

// ---------------------------------------------------------------- 客户端

func client(path string) *http.Client {
	return &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", path)
	}}}
}

// GetStatus 读取状态。
func GetStatus(ctx context.Context, path string) (Status, error) {
	var st Status
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://local/v1/status", nil)
	resp, err := client(path).Do(req)
	if err != nil {
		return st, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return st, fmt.Errorf("status: %s", resp.Status)
	}
	return st, json.NewDecoder(resp.Body).Decode(&st)
}

// Drain 发起排空并等待完成。
func Drain(ctx context.Context, path string, req DrainRequest) error {
	b, _ := json.Marshal(req)
	hr, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://local/v1/drain", bytes.NewReader(b))
	hr.Header.Set("Content-Type", "application/json")
	resp, err := client(path).Do(hr)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("drain: %s: %s", resp.Status, msg)
	}
	var dr drainResponse
	if err := json.NewDecoder(resp.Body).Decode(&dr); err != nil {
		return err
	}
	if !dr.OK {
		return errors.New(dr.Error)
	}
	return nil
}
