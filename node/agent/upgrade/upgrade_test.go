package upgrade

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fixture：已安装的程序文件内容为 "bin:<version>"；伪 Inspect 从程序内容读出版本，按 schemas 给兼容范围。
type fixture struct {
	t          *testing.T
	dir        string
	bin        string
	pub        ed25519.PublicKey
	priv       ed25519.PrivateKey
	pkgs       map[string][]byte   // URL → 内容
	schemas    map[string]Manifest // 版本 → 兼容信息
	free       uint64
	mu         sync.Mutex
	got        []Status
	drained    int
	binAtDrain string
	restarts   int
}

func newFixture(t *testing.T) *fixture {
	dir := t.TempDir()
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	f := &fixture{t: t, dir: dir, bin: filepath.Join(dir, "bin", "vortex-node"), pub: pub, priv: priv, pkgs: map[string][]byte{},
		schemas: map[string]Manifest{"1.0.0": {StoreSchema: 1, StoreMin: 1, StoreMax: 1}, "2.0.0": {StoreSchema: 1, StoreMin: 1, StoreMax: 2}},
		free:    1 << 40}
	_ = os.MkdirAll(filepath.Dir(f.bin), 0o755)
	if err := os.WriteFile(f.bin, []byte("bin:1.0.0"), 0o755); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *fixture) manager(version string) *Manager {
	self := f.schemas[version]
	self.Version = version
	return &Manager{Self: self, Binary: f.bin, Dir: filepath.Join(f.dir, "data", "upgrade"), ReleaseKey: f.pub,
		Fetch: func(_ context.Context, url string, w io.Writer) error {
			b, ok := f.pkgs[url]
			if !ok {
				return errors.New("404")
			}
			_, err := w.Write(b)
			return err
		},
		Inspect: func(_ context.Context, path string) (Manifest, error) {
			b, _ := os.ReadFile(path)
			v, ok := strings.CutPrefix(string(b), "bin:")
			if !ok {
				return Manifest{}, errors.New("exec format error")
			}
			m := f.schemas[v]
			m.Version = v
			return m, nil
		},
		Free: func(string) (uint64, error) { return f.free, nil },
		Drain: func(context.Context) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.drained++
			b, _ := os.ReadFile(f.bin)
			f.binAtDrain = string(b)
			return nil
		},
		Restart:       func() { f.mu.Lock(); f.restarts++; f.mu.Unlock() },
		Report:        func(s Status) { f.mu.Lock(); f.got = append(f.got, s); f.mu.Unlock() },
		HealthTimeout: 100 * time.Millisecond, Poll: 5 * time.Millisecond}
}

// command 发布一个版本：内容 "bin:<version>"，签名覆盖版本与摘要。
func (f *fixture) command(version string) Command { return f.commandOpt(version, false) }

func (f *fixture) commandOpt(version string, irreversible bool) Command {
	body := []byte("bin:" + version)
	url := "https://releases.example/" + version
	f.pkgs[url] = body
	sum := sha256.Sum256(body)
	return Command{Version: version, URL: url, SHA256: sum[:], Irreversible: irreversible,
		Signature: ed25519.Sign(f.priv, SignedMessage(version, sum[:], irreversible))}
}

func (f *fixture) installed() string {
	b, _ := os.ReadFile(f.bin)
	return string(b)
}

func (f *fixture) phases() []Phase {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []Phase
	for _, s := range f.got {
		out = append(out, s.Phase)
	}
	f.got = nil
	return out
}

func (f *fixture) last() Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.got[len(f.got)-1]
}

func eq(a, b []Phase) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestSignedMessageFormat(t *testing.T) {
	sum := make([]byte, 32)
	sum[0] = 0xab
	want := append([]byte("vortex-node-release-v1\x001.2.3\x00"), sum...)
	if got := SignedMessage("1.2.3", sum, false); string(got) != string(append(want, 0)) {
		t.Fatalf("%q", got)
	}
	if got := SignedMessage("1.2.3", sum, true); string(got) != string(append(want, 1)) {
		t.Fatalf("%q", got)
	}
}

// 正常升级：下载 → 校验 → 排空（此时仍是旧程序）→ 替换（保留 .prev）→ 重启 → 新版本健康 → 完成。
func TestUpgradeHealthy(t *testing.T) {
	f := newFixture(t)
	if err := f.manager("1.0.0").Run(context.Background(), f.command("2.0.0")); err != nil {
		t.Fatal(err)
	}
	if p := f.phases(); !eq(p, []Phase{PhaseDownloading, PhaseDraining, PhaseSwitched}) {
		t.Fatalf("phases %v", p)
	}
	if f.drained != 1 || f.binAtDrain != "bin:1.0.0" || f.restarts != 1 || f.installed() != "bin:2.0.0" {
		t.Fatalf("drained %d (binary then %q) restarts %d installed %q", f.drained, f.binAtDrain, f.restarts, f.installed())
	}
	if b, _ := os.ReadFile(f.bin + ".prev"); string(b) != "bin:1.0.0" {
		t.Fatal("previous version not kept")
	}
	if fi, _ := os.Stat(f.bin); fi.Mode().Perm()&0o111 == 0 {
		t.Fatal("new binary is not executable")
	}
	if ents, _ := os.ReadDir(filepath.Join(f.dir, "data", "upgrade")); len(ents) != 1 { // 只剩 state.json
		t.Fatalf("leftover files %v", ents)
	}
	// 新版本启动
	m2 := f.manager("2.0.0")
	m2.Healthy = func() bool { return true }
	if m2.Resume(context.Background()) {
		t.Fatal("restart requested")
	}
	if p := f.phases(); !eq(p, []Phase{PhaseSwitched, PhaseHealthy}) || f.restarts != 1 || f.installed() != "bin:2.0.0" {
		t.Fatalf("phases %v restarts %d installed %q", p, f.restarts, f.installed())
	}
	if st, _ := m2.LoadState(); st != nil {
		t.Fatalf("state not cleared: %+v", st)
	}
	if _, err := os.Stat(f.bin + ".prev"); err != nil {
		t.Fatal("previous version removed after success")
	}
	if m2.Resume(context.Background()) || len(f.phases()) != 0 {
		t.Fatal("completed upgrade resumed again")
	}
}

// 新版本 60 秒内不健康：回退到 .prev 并重启；旧版本启动后报告已回退。
func TestUnhealthyRollsBack(t *testing.T) {
	f := newFixture(t)
	_ = f.manager("1.0.0").Run(context.Background(), f.command("2.0.0"))
	f.phases()
	m2 := f.manager("2.0.0")
	m2.Healthy = func() bool { return false }
	start := time.Now()
	if !m2.Resume(context.Background()) {
		t.Fatal("no rollback")
	}
	if d := time.Since(start); d < m2.HealthTimeout {
		t.Fatalf("rolled back after %v, before the health timeout", d)
	}
	if f.installed() != "bin:1.0.0" || f.restarts != 2 {
		t.Fatalf("installed %q restarts %d", f.installed(), f.restarts)
	}
	f.phases()
	m1 := f.manager("1.0.0")
	m1.Resume(context.Background())
	s := f.last()
	if s.Phase != PhaseRolledBack || s.Target != "2.0.0" || !strings.Contains(s.Error, "not healthy") {
		t.Fatalf("status %+v", s)
	}
	if st, _ := m1.LoadState(); st != nil {
		t.Fatal("state not cleared after rollback report")
	}
}

// 健康检查期间变为健康也算成功。
func TestBecomesHealthyWithinTimeout(t *testing.T) {
	f := newFixture(t)
	_ = f.manager("1.0.0").Run(context.Background(), f.command("2.0.0"))
	m2 := f.manager("2.0.0")
	at := time.Now().Add(40 * time.Millisecond)
	m2.Healthy = func() bool { return time.Now().After(at) }
	if m2.Resume(context.Background()) || f.installed() != "bin:2.0.0" || f.last().Phase != PhaseHealthy {
		t.Fatalf("installed %q last %+v", f.installed(), f.last())
	}
}

// 不可逆迁移：不健康时不回退，只报告失败。
func TestIrreversibleDoesNotRollBack(t *testing.T) {
	f := newFixture(t)
	if err := f.manager("1.0.0").Run(context.Background(), f.commandOpt("2.0.0", true)); err != nil {
		t.Fatal(err)
	}
	m2 := f.manager("2.0.0")
	m2.Healthy = func() bool { return false }
	if m2.Resume(context.Background()) {
		t.Fatal("rolled back an irreversible migration")
	}
	if s := f.last(); s.Phase != PhaseFailed || !strings.Contains(s.Error, "irreversible") || f.installed() != "bin:2.0.0" {
		t.Fatalf("status %+v installed %q", s, f.installed())
	}
}

// 新版本启动后崩溃（来不及完成健康检查）：超过启动次数上限即回退，不再等待。
func TestCrashLoopRollsBack(t *testing.T) {
	f := newFixture(t)
	_ = f.manager("1.0.0").Run(context.Background(), f.command("2.0.0"))
	for i := 0; i < 3; i++ {
		m2 := f.manager("2.0.0")
		m2.Healthy = func() bool { return false }
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // 进程在健康检查期间退出
		if m2.Resume(ctx) {
			t.Fatalf("start %d: rolled back early", i+1)
		}
	}
	m2 := f.manager("2.0.0")
	m2.Healthy = func() bool { return false }
	m2.HealthTimeout = time.Hour
	if !m2.Resume(context.Background()) || f.installed() != "bin:1.0.0" {
		t.Fatalf("installed %q", f.installed())
	}
}

// 校验与检查失败：报告失败，不排空、不替换、不重启。
func TestRejectedBeforeDrain(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*fixture, *Manager, *Command)
		want error
	}{
		{"bad signature", func(f *fixture, m *Manager, c *Command) { c.Signature[0] ^= 1 }, ErrSignature},
		{"signature for another version", func(f *fixture, m *Manager, c *Command) {
			o := f.command("2.0.1")
			c.Signature = o.Signature
		}, ErrSignature},
		{"irreversible flag removed", func(f *fixture, m *Manager, c *Command) {
			*c = f.commandOpt("2.0.0", true)
			c.Irreversible = false
		}, ErrSignature},
		{"no release key", func(f *fixture, m *Manager, c *Command) { m.ReleaseKey = nil }, ErrSignature},
		{"digest mismatch", func(f *fixture, m *Manager, c *Command) { f.pkgs[c.URL] = []byte("bin:2.0.0-tampered") }, ErrDigest},
		{"short digest", func(f *fixture, m *Manager, c *Command) { c.SHA256 = c.SHA256[:8] }, ErrDigest},
		{"too large", func(f *fixture, m *Manager, c *Command) { m.MaxSize = 4 }, ErrTooLarge},
		{"manifest version differs", func(f *fixture, m *Manager, c *Command) {
			f.schemas["1.9.0"] = f.schemas["1.0.0"]
			*c = f.signed("2.0.0", []byte("bin:1.9.0"))
		}, ErrIncompatible},
		{"not executable", func(f *fixture, m *Manager, c *Command) { *c = f.signed("2.0.0", []byte("garbage")) }, ErrIncompatible},
		{"cannot read current store", func(f *fixture, m *Manager, c *Command) {
			f.schemas["2.0.0"] = Manifest{StoreSchema: 2, StoreMin: 2, StoreMax: 2}
			*c = f.commandOpt("2.0.0", true) // 即使是不可逆迁移
		}, ErrIncompatible},
		{"rollback could not read new store", func(f *fixture, m *Manager, c *Command) {
			f.schemas["2.0.0"] = Manifest{StoreSchema: 2, StoreMin: 1, StoreMax: 2}
		}, ErrIncompatible},
		{"disk", func(f *fixture, m *Manager, c *Command) { f.free = 64<<20 + 2*9 - 1 }, ErrDisk},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFixture(t)
			m := f.manager("1.0.0")
			cmd := f.command("2.0.0")
			c.mut(f, m, &cmd)
			err := m.Run(context.Background(), cmd)
			if !errors.Is(err, c.want) {
				t.Fatalf("err %v, want %v", err, c.want)
			}
			if s := f.last(); s.Phase != PhaseFailed || s.Error == "" {
				t.Fatalf("status %+v", s)
			}
			if f.drained != 0 || f.restarts != 0 || f.installed() != "bin:1.0.0" {
				t.Fatalf("drained %d restarts %d installed %q", f.drained, f.restarts, f.installed())
			}
			if st, _ := m.LoadState(); st != nil {
				t.Fatal("state written")
			}
			if ents, _ := os.ReadDir(m.Dir); len(ents) != 0 {
				t.Fatalf("leftover files %v", ents)
			}
		})
	}
}

func (f *fixture) signed(version string, body []byte) Command {
	url := "https://releases.example/custom/" + version
	f.pkgs[url] = body
	sum := sha256.Sum256(body)
	return Command{Version: version, URL: url, SHA256: sum[:], Signature: ed25519.Sign(f.priv, SignedMessage(version, sum[:], false))}
}

// 写入格式旧版本读不了：只有标记为不可逆迁移时才允许；恰好够用的磁盘可以升级。
func TestIrreversibleAllowsNewStoreSchema(t *testing.T) {
	f := newFixture(t)
	f.schemas["2.0.0"] = Manifest{StoreSchema: 2, StoreMin: 1, StoreMax: 2}
	f.free = 64<<20 + 2*9
	if err := f.manager("1.0.0").Run(context.Background(), f.commandOpt("2.0.0", true)); err != nil || f.installed() != "bin:2.0.0" {
		t.Fatalf("%v %q", err, f.installed())
	}
}

func TestSameVersionAndBusy(t *testing.T) {
	f := newFixture(t)
	m := f.manager("1.0.0")
	if err := m.Run(context.Background(), Command{Version: "1.0.0"}); err != nil || f.last().Phase != PhaseHealthy || f.drained != 0 {
		t.Fatalf("same version: %v %+v", err, f.last())
	}
	block := make(chan struct{})
	m.Fetch = func(context.Context, string, io.Writer) error { <-block; return errors.New("stop") }
	if err := m.Start(context.Background(), f.command("2.0.0")); err != nil {
		t.Fatal(err)
	}
	if err := m.Start(context.Background(), f.command("2.0.0")); !errors.Is(err, ErrBusy) {
		t.Fatalf("second upgrade: %v", err)
	}
	close(block)
	deadline := time.Now().Add(2 * time.Second)
	for m.Start(context.Background(), Command{Version: "1.0.0"}) != nil {
		if time.Now().After(deadline) {
			t.Fatal("busy flag never cleared")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// 状态已写入而程序未替换（替换途中崩溃）：旧版本启动时报告失败并清除状态。
func TestSwitchInterrupted(t *testing.T) {
	f := newFixture(t)
	m := f.manager("1.0.0")
	_ = os.MkdirAll(m.Dir, 0o700)
	_ = m.saveState(&State{Target: "2.0.0", From: "1.0.0", Phase: PhaseSwitched})
	if m.Resume(context.Background()) {
		t.Fatal("restart requested")
	}
	if s := f.last(); s.Phase != PhaseFailed || s.Target != "2.0.0" {
		t.Fatalf("status %+v", s)
	}
	if st, _ := m.LoadState(); st != nil {
		t.Fatal("state kept")
	}
}

// 替换失败（排空之后）：报告失败并重启以解除排空，程序与状态不变。
func TestSwitchFailureRestarts(t *testing.T) {
	f := newFixture(t)
	m := f.manager("1.0.0")
	m.Drain = func(context.Context) error {
		f.drained++
		return os.Mkdir(f.bin+".prev", 0o755) // .prev 位置被目录占用：保留旧程序失败
	}
	if err := m.Run(context.Background(), f.command("2.0.0")); err == nil {
		t.Fatal("switch into a read-only directory succeeded")
	}
	if f.last().Phase != PhaseFailed || f.restarts != 1 || f.installed() != "bin:1.0.0" {
		t.Fatalf("last %+v restarts %d installed %q", f.last(), f.restarts, f.installed())
	}
	if st, _ := m.LoadState(); st != nil {
		t.Fatal("state kept after a failed switch")
	}
}

// 上一版本的 Guard（ExecStartPre）计数每次启动：新版本在 Begin 之前就崩溃，也会在超过上限时被回退；
// Guard 已计数的启动 Begin 不再重复计数。
func TestGuardRollsBackEarlyCrashes(t *testing.T) {
	f := newFixture(t)
	_ = f.manager("1.0.0").Run(context.Background(), f.command("2.0.0"))
	guard := f.manager("1.0.0") // .prev
	for i := 1; i <= 3; i++ {
		if rolled, err := guard.Guard(); rolled || err != nil {
			t.Fatalf("start %d: rolled %v err %v", i, rolled, err)
		}
		// 新版本崩溃，从未执行 Begin
	}
	rolled, err := guard.Guard()
	if !rolled || err != nil || f.installed() != "bin:1.0.0" {
		t.Fatalf("rolled %v err %v installed %q", rolled, err, f.installed())
	}
	f.phases()
	f.manager("1.0.0").Resume(context.Background()) // ExecStart 运行恢复后的旧版本
	if s := f.last(); s.Phase != PhaseRolledBack || !strings.Contains(s.Error, "3 starts") {
		t.Fatalf("status %+v", s)
	}
	if again, _ := guard.Guard(); again {
		t.Fatal("guard acted without an upgrade in progress")
	}
}

func TestGuardAndBeginCountOncePerStart(t *testing.T) {
	f := newFixture(t)
	_ = f.manager("1.0.0").Run(context.Background(), f.command("2.0.0"))
	guard := f.manager("1.0.0")
	for i := 1; i <= 3; i++ {
		_, _ = guard.Guard()
		m2 := f.manager("2.0.0")
		if m2.Begin() {
			t.Fatalf("start %d: rolled back by Begin", i)
		}
		st, _ := m2.LoadState()
		if st.Starts != i || st.Counted {
			t.Fatalf("start %d: state %+v", i, st)
		}
	}
	// 没有 Guard 的启动（例如上一版本没有 Guard 子命令）由 Begin 计数
	if !f.manager("2.0.0").Begin() || f.installed() != "bin:1.0.0" {
		t.Fatal("fourth start not rolled back by Begin")
	}
}

func TestGuardIrreversibleAndForeignState(t *testing.T) {
	f := newFixture(t)
	_ = f.manager("1.0.0").Run(context.Background(), f.commandOpt("2.0.0", true))
	guard := f.manager("1.0.0")
	for i := 0; i < 5; i++ {
		if rolled, _ := guard.Guard(); rolled {
			t.Fatal("irreversible migration rolled back by the guard")
		}
	}
	f2 := newFixture(t)
	_ = f2.manager("1.0.0").Run(context.Background(), f2.command("2.0.0"))
	stranger := f2.manager("2.0.0")
	for i := 0; i < 5; i++ {
		if rolled, _ := stranger.Guard(); rolled {
			t.Fatal("guard of another version acted")
		}
	}
	if st, _ := stranger.LoadState(); st.Starts != 0 {
		t.Fatalf("foreign guard counted: %+v", st)
	}
}
