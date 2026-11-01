// Package upgrade 实现节点升级与回退（AGT-008，02 方案 3.8）：
//
//	收到指令 → 下载（大小上限）→ 校验 SHA-256 与平台发布签名 → 执行新程序 `version -json` 取兼容信息并检查
//	→ 检查磁盘 → 排空 → 保留当前程序为 .prev、原子替换 → 记录升级状态 → 退出由 systemd 重启
//	→ 新版本启动后 60 秒内健康（已连接 Edge Hub、配置已生效）即完成；否则回退到 .prev 并再次重启。
//
// 每次启动最多给 60 秒；连续 3 次启动仍未健康（例如新版本启动后崩溃）也回退。带“不可逆迁移”标记的版本
// 不自动回退，只报告失败。回退只恢复程序：非不可逆的版本必须能被旧版本读取其写入的本地数据（兼容检查保证），
// 因此数据不回滚，升级期间的计量不会丢失。
//
// 发布签名（契约补充 L7）：Ed25519，签名内容为 "vortex-node-release-v1\x00" + version + "\x00" + sha256（32 字节）
// + 不可逆迁移标记（1 字节，0 或 1）。标记也是发布的属性，一并签名，Edge Hub 或控制通道无法改动它。公钥随安装包分发。
package upgrade

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

// Phase 与 edgehub.v1.UpgradePhase 的编号相同。
type Phase int

const (
	PhaseUnspecified Phase = iota
	PhaseDownloading
	PhaseDraining
	PhaseSwitched
	PhaseHealthy
	PhaseRolledBack
	PhaseFailed
)

func (p Phase) String() string {
	return [...]string{"", "downloading", "draining", "switched", "healthy", "rolled_back", "failed"}[p]
}

// Command 是升级指令（edgehub.v1.UpgradeCommand）。
type Command struct {
	Version      string
	URL          string
	SHA256       []byte
	Signature    []byte
	Irreversible bool // 不可逆迁移：禁止自动回退
}

// Status 是上报的升级进度（edgehub.v1.UpgradeStatus）。
type Status struct {
	Target string
	Phase  Phase
	Error  string
}

// Manifest 是 `vortex-node version -json` 的输出：版本与本地数据（store）格式的兼容范围。
type Manifest struct {
	Version     string `json:"version"`
	StoreSchema int    `json:"store_schema"`     // 本版本写入的格式
	StoreMin    int    `json:"store_schema_min"` // 本版本能读取的最低格式
	StoreMax    int    `json:"store_schema_max"` // 本版本能读取的最高格式
}

const signContext = "vortex-node-release-v1"

// SignedMessage 是发布签名覆盖的内容。
func SignedMessage(version string, sum []byte, irreversible bool) []byte {
	var b bytes.Buffer
	b.WriteString(signContext)
	b.WriteByte(0)
	b.WriteString(version)
	b.WriteByte(0)
	b.Write(sum)
	if irreversible {
		b.WriteByte(1)
	} else {
		b.WriteByte(0)
	}
	return b.Bytes()
}

var (
	ErrBusy         = errors.New("upgrade: another upgrade is in progress")
	ErrDigest       = errors.New("upgrade: sha256 mismatch")
	ErrSignature    = errors.New("upgrade: bad release signature")
	ErrTooLarge     = errors.New("upgrade: package too large")
	ErrIncompatible = errors.New("upgrade: incompatible version")
	ErrDisk         = errors.New("upgrade: not enough disk space")
)

// State 是持久化的升级状态（数据目录 upgrade/state.json），跨越重启。
type State struct {
	Target       string    `json:"target"`
	From         string    `json:"from"`
	Phase        Phase     `json:"phase"`
	Irreversible bool      `json:"irreversible"`
	SwitchedAt   time.Time `json:"switched_at"`
	Starts       int       `json:"starts"`            // 新版本已启动的次数
	Counted      bool      `json:"counted,omitempty"` // 本次启动已由 Guard 计数（Begin 不再计数）
	Error        string    `json:"error,omitempty"`
}

type Manager struct {
	Self       Manifest          // 当前程序
	Binary     string            // 安装的程序路径（systemd 的 ExecStart）
	Dir        string            // 工作目录（数据目录下的 upgrade/）
	ReleaseKey ed25519.PublicKey // 平台发布公钥；nil 时拒绝一切升级

	Fetch   func(ctx context.Context, url string, w io.Writer) error // nil = HTTP GET
	Inspect func(ctx context.Context, path string) (Manifest, error) // nil = 执行 `path version -json`
	Free    func(path string) (uint64, error)                        // 可用磁盘字节；nil = statfs
	Drain   func(ctx context.Context) error                          // 整节点排空（NOD-003）
	Restart func()                                                   // 请求进程退出，由 systemd 重启
	Report  func(Status)                                             // 上报进度
	Healthy func() bool                                              // 新版本是否健康

	MaxSize       int64         // 默认 256 MiB
	Reserve       uint64        // 替换后仍须保留的磁盘字节，默认 64 MiB
	DrainTimeout  time.Duration // 默认 300 秒
	HealthTimeout time.Duration // 默认 60 秒
	MaxStarts     int           // 默认 3
	Poll          time.Duration // 健康检查间隔，默认 1 秒

	mu      sync.Mutex
	busy    bool
	hold    bool     // Begin 与 Await 之间：进度先排队
	queue   []Status // 排队的进度
	waiting *State   // Begin 发现的、待 Await 做健康检查的新版本
}

func (m *Manager) defaults() {
	if m.MaxSize == 0 {
		m.MaxSize = 256 << 20
	}
	if m.Reserve == 0 {
		m.Reserve = 64 << 20
	}
	if m.DrainTimeout == 0 {
		m.DrainTimeout = 300 * time.Second
	}
	if m.HealthTimeout == 0 {
		m.HealthTimeout = 60 * time.Second
	}
	if m.MaxStarts == 0 {
		m.MaxStarts = 3
	}
	if m.Poll == 0 {
		m.Poll = time.Second
	}
	if m.Fetch == nil {
		m.Fetch = httpFetch
	}
	if m.Inspect == nil {
		m.Inspect = execInspect
	}
	if m.Free == nil {
		m.Free = statfsFree
	}
}

func (m *Manager) report(target string, p Phase, err error) {
	if m.Report == nil {
		return
	}
	s := Status{Target: target, Phase: p}
	if err != nil {
		s.Error = err.Error()
	}
	m.mu.Lock()
	if m.hold {
		m.queue = append(m.queue, s)
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()
	m.Report(s)
}

func (m *Manager) statePath() string { return filepath.Join(m.Dir, "state.json") }

// LoadState 读取升级状态；没有时返回 nil。
func (m *Manager) LoadState() (*State, error) {
	b, err := os.ReadFile(m.statePath())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var s State
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

func (m *Manager) saveState(s *State) error {
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return writeAtomic(m.statePath(), b, 0o600)
}

func (m *Manager) clearState() error {
	err := os.Remove(m.statePath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// Start 异步执行一条升级指令；已有升级在进行时忽略（返回 ErrBusy）。
func (m *Manager) Start(ctx context.Context, cmd Command) error {
	m.mu.Lock()
	if m.busy {
		m.mu.Unlock()
		return ErrBusy
	}
	m.busy = true
	m.mu.Unlock()
	go func() {
		defer func() { m.mu.Lock(); m.busy = false; m.mu.Unlock() }()
		_ = m.Run(ctx, cmd)
	}()
	return nil
}

// Run 同步执行一条升级指令，成功时以 Restart 结束（新版本由 systemd 启动）。
func (m *Manager) Run(ctx context.Context, cmd Command) error {
	m.defaults()
	if cmd.Version == m.Self.Version {
		m.report(cmd.Version, PhaseHealthy, nil) // 已是目标版本
		return nil
	}
	if err := os.MkdirAll(m.Dir, 0o700); err != nil {
		m.report(cmd.Version, PhaseFailed, err)
		return err
	}
	m.report(cmd.Version, PhaseDownloading, nil)
	pkg, err := m.prepare(ctx, cmd)
	if pkg != "" {
		defer os.Remove(pkg)
	}
	if err != nil {
		m.report(cmd.Version, PhaseFailed, err)
		return err
	}
	// 排空之后引擎不再接受新会话（排空状态保持到进程结束），所以从这里起无论成败都重启
	m.report(cmd.Version, PhaseDraining, nil)
	dctx, cancel := context.WithTimeout(ctx, m.DrainTimeout)
	if m.Drain != nil {
		_ = m.Drain(dctx) // 超时即关闭剩余会话，继续升级
	}
	cancel()
	if err := m.switchTo(pkg, cmd); err != nil {
		m.report(cmd.Version, PhaseFailed, err)
		m.restart()
		return err
	}
	m.report(cmd.Version, PhaseSwitched, nil)
	m.restart()
	return nil
}

func (m *Manager) restart() {
	if m.Restart != nil {
		m.Restart()
	}
}

// prepare 下载、校验并检查升级包，返回下载文件路径。
func (m *Manager) prepare(ctx context.Context, cmd Command) (string, error) {
	if m.ReleaseKey == nil {
		return "", fmt.Errorf("%w: no release key configured", ErrSignature)
	}
	if len(cmd.SHA256) != sha256.Size {
		return "", ErrDigest
	}
	// 先验签名（覆盖版本与摘要），再下载：签名不对的指令不必下载
	if !ed25519.Verify(m.ReleaseKey, SignedMessage(cmd.Version, cmd.SHA256, cmd.Irreversible), cmd.Signature) {
		return "", ErrSignature
	}
	f, err := os.CreateTemp(m.Dir, "pkg-*")
	if err != nil {
		return "", err
	}
	path := f.Name()
	h := sha256.New()
	lw := &limitWriter{w: io.MultiWriter(f, h), n: m.MaxSize}
	err = m.Fetch(ctx, cmd.URL, lw) // 超过上限时写入返回 ErrTooLarge
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return path, err
	}
	if !bytes.Equal(h.Sum(nil), cmd.SHA256) {
		return path, ErrDigest
	}
	if err := os.Chmod(path, 0o755); err != nil {
		return path, err
	}
	// 只有通过签名与摘要校验的程序才会被执行
	ictx, cancel := context.WithTimeout(ctx, 10*time.Second)
	man, err := m.Inspect(ictx, path)
	cancel()
	if err != nil {
		return path, fmt.Errorf("%w: new binary does not run: %v", ErrIncompatible, err)
	}
	if err := m.compatible(man, cmd); err != nil {
		return path, err
	}
	free, err := m.Free(filepath.Dir(m.Binary))
	if err != nil {
		return path, err
	}
	if need := uint64(2*lw.written) + m.Reserve; free < need { // .prev 与 .new 各一份
		return path, fmt.Errorf("%w: %d bytes free, need %d", ErrDisk, free, need)
	}
	return path, nil
}

// compatible 检查兼容矩阵：新版本能读取当前数据；非不可逆的版本写入的数据须能被当前版本读取（以便回退）。
func (m *Manager) compatible(n Manifest, cmd Command) error {
	if n.Version != cmd.Version {
		return fmt.Errorf("%w: package reports version %q, command says %q", ErrIncompatible, n.Version, cmd.Version)
	}
	if m.Self.StoreSchema < n.StoreMin || m.Self.StoreSchema > n.StoreMax {
		return fmt.Errorf("%w: new version cannot read store schema %d", ErrIncompatible, m.Self.StoreSchema)
	}
	if !cmd.Irreversible && (n.StoreSchema < m.Self.StoreMin || n.StoreSchema > m.Self.StoreMax) {
		return fmt.Errorf("%w: store schema %d cannot be read after rollback; the release must be marked irreversible", ErrIncompatible, n.StoreSchema)
	}
	return nil
}

// switchTo 保留当前程序为 .prev，写入升级状态，再原子替换程序。
func (m *Manager) switchTo(pkg string, cmd Command) error {
	if err := copyAtomic(m.Binary, m.Binary+".prev"); err != nil {
		return err
	}
	st := &State{Target: cmd.Version, From: m.Self.Version, Phase: PhaseSwitched, Irreversible: cmd.Irreversible, SwitchedAt: time.Now()}
	if err := m.saveState(st); err != nil {
		return err
	}
	if err := copyAtomic(pkg, m.Binary); err != nil {
		_ = m.clearState()
		return err
	}
	return nil
}

// Resume 在启动时调用：Begin 之后 Await。返回 true 表示已请求重启（回退）。
func (m *Manager) Resume(ctx context.Context) bool {
	if m.Begin() {
		return true
	}
	return m.Await(ctx)
}

// Begin 在进程启动之初、打开本地数据之前同步调用（02 方案 3.8 的后半段）：
//   - 新版本启动：启动次数加一并落盘（启动后崩溃也被计数），超过上限即回退；
//   - 回退后的旧版本、替换途中崩溃后的旧版本：记下要报告的结果。
//
// 返回 true 表示已回退并请求重启，调用方应立即退出。进度在 Await 中报告（此时上报通道尚未建立）。
func (m *Manager) Begin() bool {
	m.defaults()
	m.mu.Lock()
	m.hold = true
	m.mu.Unlock()
	st, err := m.LoadState()
	if err != nil || st == nil {
		return false
	}
	switch {
	case st.Phase == PhaseSwitched && m.Self.Version == st.Target:
		if st.Counted {
			st.Counted = false
		} else {
			st.Starts++
		}
		if err := m.saveState(st); err != nil {
			m.report(st.Target, PhaseFailed, err)
			return false
		}
		if st.Starts > m.MaxStarts {
			return m.fail(st, fmt.Errorf("new version did not become healthy in %d starts", m.MaxStarts))
		}
		m.waiting = st
	case st.Phase == PhaseRolledBack && m.Self.Version == st.From:
		_ = m.clearState()
		m.report(st.Target, PhaseRolledBack, errors.New(st.Error))
	case st.Phase == PhaseSwitched && m.Self.Version == st.From:
		// 状态已写入而程序未替换（替换途中崩溃），仍在旧版本上
		_ = m.clearState()
		m.report(st.Target, PhaseFailed, errors.New("binary was not replaced"))
	default:
		_ = m.clearState() // 其他版本留下的状态
	}
	return false
}

// Await 报告 Begin 记下的结果；新版本则等待健康：60 秒内健康即完成，否则回退（不可逆迁移时只报告失败）。
// 返回 true 表示已回退并请求重启。
func (m *Manager) Await(ctx context.Context) bool {
	m.defaults()
	m.mu.Lock()
	q := m.queue
	m.queue, m.hold = nil, false
	st := m.waiting
	m.waiting = nil
	m.mu.Unlock()
	for _, s := range q {
		m.Report(s)
	}
	if st == nil {
		return false
	}
	m.report(st.Target, PhaseSwitched, nil)
	if m.waitHealthy(ctx) {
		_ = m.clearState()
		m.report(st.Target, PhaseHealthy, nil)
		return false
	}
	if ctx.Err() != nil {
		return false // 进程正在退出：下次启动继续计数
	}
	return m.fail(st, fmt.Errorf("new version not healthy within %v", m.HealthTimeout))
}

// Guard 由上一版本的程序（.prev）在每次启动新版本之前执行（systemd ExecStartPre，见 deploy/node）：
// 新版本处于切换后的检查期时，由它计数启动次数，超过上限即恢复 .prev。这样即使新版本在走到 Begin 之前就崩溃
// （例如运行时初始化阶段），也能回退。不可逆迁移不回退。返回是否已回退。没有升级在进行时什么也不做。
func (m *Manager) Guard() (bool, error) {
	m.defaults()
	st, err := m.LoadState()
	if err != nil || st == nil || st.Phase != PhaseSwitched || st.From != m.Self.Version {
		return false, err
	}
	st.Starts++
	st.Counted = true
	if st.Starts <= m.MaxStarts || st.Irreversible {
		return false, m.saveState(st)
	}
	st.Phase, st.Counted = PhaseRolledBack, false
	st.Error = fmt.Sprintf("new version did not become healthy in %d starts", m.MaxStarts)
	if err := m.saveState(st); err != nil {
		return false, err
	}
	if err := copyAtomic(m.Binary+".prev", m.Binary); err != nil {
		return false, err
	}
	return true, nil
}

func (m *Manager) waitHealthy(ctx context.Context) bool {
	deadline := time.NewTimer(m.HealthTimeout)
	defer deadline.Stop()
	t := time.NewTicker(m.Poll)
	defer t.Stop()
	for {
		if m.Healthy != nil && m.Healthy() {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-deadline.C:
			return false
		case <-t.C:
		}
	}
}

// fail 处理新版本不健康：不可逆迁移时只报告失败；否则恢复 .prev 并重启。
func (m *Manager) fail(st *State, cause error) bool {
	if st.Irreversible {
		_ = m.clearState()
		m.report(st.Target, PhaseFailed, fmt.Errorf("%v; irreversible migration, not rolling back", cause))
		return false
	}
	st.Phase, st.Error = PhaseRolledBack, cause.Error()
	if err := m.saveState(st); err != nil {
		m.report(st.Target, PhaseFailed, err)
		return false
	}
	if err := copyAtomic(m.Binary+".prev", m.Binary); err != nil {
		m.report(st.Target, PhaseFailed, fmt.Errorf("%v; rollback failed: %v", cause, err))
		return false
	}
	m.restart()
	return true
}

// ---------------------------------------------------------------- 文件与外部操作

type limitWriter struct {
	w       io.Writer
	n       int64
	written int64
}

func (l *limitWriter) Write(p []byte) (int, error) {
	if l.written+int64(len(p)) > l.n {
		return 0, ErrTooLarge
	}
	n, err := l.w.Write(p)
	l.written += int64(n)
	return n, err
}

func httpFetch(ctx context.Context, url string, w io.Writer) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := (&http.Client{Timeout: 10 * time.Minute}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("upgrade: download: %s", resp.Status)
	}
	_, err = io.Copy(w, resp.Body)
	return err
}

func execInspect(ctx context.Context, path string) (Manifest, error) {
	out, err := exec.CommandContext(ctx, path, "version", "-json").Output()
	if err != nil {
		return Manifest{}, err
	}
	var man Manifest
	if err := json.Unmarshal(out, &man); err != nil {
		return Manifest{}, err
	}
	return man, nil
}

// copyAtomic 把 src 复制到 dst：同目录临时文件 + fsync + rename + 目录 fsync，保留可执行权限。
func copyAtomic(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	fi, err := in.Stat()
	if err != nil {
		return err
	}
	dir := filepath.Dir(dst)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(dst)+".tmp-*")
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		if !ok {
			tmp.Close()
			os.Remove(tmp.Name())
		}
	}()
	if _, err := io.Copy(tmp, in); err != nil {
		return err
	}
	if err := tmp.Chmod(fi.Mode().Perm()); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), dst); err != nil {
		return err
	}
	ok = true
	return syncDir(dir)
}

func writeAtomic(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(data); err == nil {
		err = tmp.Chmod(mode)
	}
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp.Name(), path)
	}
	if err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
