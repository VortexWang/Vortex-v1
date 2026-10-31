// Package store 是 Agent 的本地持久化（AGT-003）。
//
// 开发方案 02 3.3 规定 SQLite（WAL）；Go 模块代理放行前先用只追加的 JSON 行文件：每次追加 fsync，
// 打开时截掉末尾写了一半的行（断电），中间行损坏拒绝打开；Rewrite 原子地以给定记录替换整个文件
// （临时文件 + fsync + rename + 目录 fsync），用于保留期清理。放行后换为 SQLite，Log 接口不变。
package store

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var ErrCorrupt = errors.New("store: corrupt log")

// Record 是一条日志记录。
type Record struct {
	Kind string          `json:"k"`
	Data json.RawMessage `json:"d"`
}

// Log 是只追加日志。
type Log interface {
	Append(rs ...Record) error
	Load(fn func(Record) error) error
	Rewrite(rs []Record) error
	Close() error
}

// Make 把值编码为记录。
func Make(kind string, v any) (Record, error) {
	b, err := json.Marshal(v)
	return Record{kind, b}, err
}

// ---------------------------------------------------------------- 内存

type Mem struct {
	mu   sync.Mutex
	rs   []Record
	Fail error // 非空时 Append 返回该错误（测试磁盘故障）
}

func (m *Mem) Append(rs ...Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Fail != nil {
		return m.Fail
	}
	m.rs = append(m.rs, rs...)
	return nil
}

func (m *Mem) Load(fn func(Record) error) error {
	m.mu.Lock()
	rs := append([]Record(nil), m.rs...)
	m.mu.Unlock()
	for _, r := range rs {
		if err := fn(r); err != nil {
			return err
		}
	}
	return nil
}

func (m *Mem) Rewrite(rs []Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rs = append([]Record(nil), rs...)
	return nil
}

func (m *Mem) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.rs)
}

func (m *Mem) Close() error { return nil }

// ---------------------------------------------------------------- 文件

type File struct {
	mu   sync.Mutex
	path string
	f    *os.File
}

func OpenFile(path string) (*File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	fs := &File{path: path, f: f}
	if err := fs.repairTail(); err != nil {
		f.Close()
		return nil, err
	}
	return fs, nil
}

func (fs *File) repairTail() error {
	b, err := io.ReadAll(io.NewSectionReader(fs.f, 0, 1<<62))
	if err != nil {
		return err
	}
	good := 0
	for good < len(b) {
		i := bytes.IndexByte(b[good:], '\n')
		if i < 0 {
			break
		}
		if !json.Valid(b[good : good+i]) {
			if bytes.IndexByte(b[good+i+1:], '\n') >= 0 {
				return fmt.Errorf("%w at byte %d", ErrCorrupt, good)
			}
			break
		}
		good += i + 1
	}
	if good < len(b) {
		if err := fs.f.Truncate(int64(good)); err != nil {
			return err
		}
		if err := fs.f.Sync(); err != nil {
			return err
		}
	}
	_, err = fs.f.Seek(0, io.SeekEnd)
	return err
}

func encode(rs []Record) ([]byte, error) {
	var buf bytes.Buffer
	for _, r := range rs {
		b, err := json.Marshal(r)
		if err != nil {
			return nil, err
		}
		buf.Write(b)
		buf.WriteByte('\n')
	}
	return buf.Bytes(), nil
}

func (fs *File) Append(rs ...Record) error {
	b, err := encode(rs)
	if err != nil {
		return err
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if _, err := fs.f.Write(b); err != nil {
		return err
	}
	return fs.f.Sync()
}

func (fs *File) Load(fn func(Record) error) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	sc := bufio.NewScanner(io.NewSectionReader(fs.f, 0, 1<<62))
	sc.Buffer(make([]byte, 64<<10), 64<<20)
	for sc.Scan() {
		var r Record
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			return fmt.Errorf("%w: %v", ErrCorrupt, err)
		}
		if err := fn(r); err != nil {
			return err
		}
	}
	return sc.Err()
}

// Rewrite 原子替换：中途崩溃时文件要么是旧内容，要么是新内容。
func (fs *File) Rewrite(rs []Record) error {
	b, err := encode(rs)
	if err != nil {
		return err
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	tmp := fs.path + ".tmp"
	t, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := t.Write(b); err != nil {
		t.Close()
		return err
	}
	if err := t.Sync(); err != nil {
		t.Close()
		return err
	}
	if err := os.Rename(tmp, fs.path); err != nil {
		t.Close()
		return err
	}
	if d, err := os.Open(filepath.Dir(fs.path)); err == nil {
		_ = d.Sync()
		d.Close()
	}
	fs.f.Close()
	fs.f = t
	_, err = t.Seek(0, io.SeekEnd)
	return err
}

func (fs *File) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.f.Close()
}
