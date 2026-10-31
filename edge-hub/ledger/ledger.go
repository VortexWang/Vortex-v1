// Package ledger 是 Edge Hub 的本地账本（HUB-012）：只追加，条目带递增序号 (hub_id, seq)；先写账本再生效
// （签发、结算、额度更新、模式变化），重启后按序重放恢复全部状态，恢复对账时按序上报（HUB-013）。
//
// 存储抽象为 Store。当前实现为 JSON 行文件（每次追加 fsync）；契约与开发方案规定的 SQLite（WAL）实现在
// 依赖可用后替换，接口不变。
package ledger

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"vortex/edgehub/clock"
)

// Entry 是一条账本记录。
type Entry struct {
	Seq  uint64          `json:"seq"`
	At   time.Time       `json:"at"`
	Kind string          `json:"kind"`
	Data json.RawMessage `json:"data"`
}

// Store 持久化账本条目。Append 返回前条目必须已持久化。
type Store interface {
	Append(es []Entry) error
	Load(fn func(Entry) error) error
	Close() error
}

var ErrCorrupt = errors.New("ledger: corrupt entry")

// Ledger 分配序号并写入 Store。
type Ledger struct {
	mu    sync.Mutex
	st    Store
	clk   clock.Clock
	last  uint64
	acked uint64 // 后端已确认的最大序号（LedgerAck）
}

// Open 打开账本并按序重放已有条目（apply 依次收到每一条）。重放中序号必须严格递增。
func Open(st Store, clk clock.Clock, apply func(Entry) error) (*Ledger, error) {
	l := &Ledger{st: st, clk: clk}
	err := st.Load(func(e Entry) error {
		if e.Seq <= l.last {
			return fmt.Errorf("%w: seq %d after %d", ErrCorrupt, e.Seq, l.last)
		}
		l.last = e.Seq
		if e.Kind == KindAck {
			var a ackData
			if err := json.Unmarshal(e.Data, &a); err != nil {
				return err
			}
			l.acked = max(l.acked, a.Through)
			return nil
		}
		if apply != nil {
			return apply(e)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return l, nil
}

// Append 写入一条记录并返回其序号。v 序列化为 JSON。
func (l *Ledger) Append(kind string, v any) (uint64, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return 0, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	e := Entry{Seq: l.last + 1, At: l.clk.Now().UTC(), Kind: kind, Data: data}
	if err := l.st.Append([]Entry{e}); err != nil {
		return 0, err
	}
	l.last = e.Seq
	return e.Seq, nil
}

// LastSeq 返回最大序号（HubRegister.ledger_last_seq）。
func (l *Ledger) LastSeq() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.last
}

// KindAck 记录后端确认（LedgerAck.acked_through_seq），本身不上报。
const KindAck = "ack"

type ackData struct {
	Through uint64 `json:"through"`
}

// Ack 记录后端已确认到 through（单调，较小或重复的确认被忽略）。
func (l *Ledger) Ack(through uint64) error {
	l.mu.Lock()
	if through <= l.acked {
		l.mu.Unlock()
		return nil
	}
	l.mu.Unlock()
	if _, err := l.Append(KindAck, ackData{Through: through}); err != nil {
		return err
	}
	l.mu.Lock()
	l.acked = max(l.acked, through)
	l.mu.Unlock()
	return nil
}

// Acked 返回后端已确认的最大序号。
func (l *Ledger) Acked() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.acked
}

// Unacked 按序返回尚未被后端确认、需要上报的条目（不含确认记录本身）。
func (l *Ledger) Unacked() ([]Entry, error) {
	acked := l.Acked()
	var out []Entry
	err := l.st.Load(func(e Entry) error {
		if e.Seq > acked && e.Kind != KindAck {
			out = append(out, e)
		}
		return nil
	})
	return out, err
}

func (l *Ledger) Close() error { return l.st.Close() }

// ---------------------------------------------------------------- 内存存储（测试）

type MemStore struct {
	mu sync.Mutex
	es []Entry
}

func (m *MemStore) Append(es []Entry) error {
	m.mu.Lock()
	m.es = append(m.es, es...)
	m.mu.Unlock()
	return nil
}

func (m *MemStore) Load(fn func(Entry) error) error {
	m.mu.Lock()
	es := append([]Entry(nil), m.es...)
	m.mu.Unlock()
	for _, e := range es {
		if err := fn(e); err != nil {
			return err
		}
	}
	return nil
}

func (m *MemStore) Close() error { return nil }

// ---------------------------------------------------------------- 文件存储

// FileStore：每行一条 JSON，每次追加后 fsync。打开时若最后一行不完整（写入中途断电），截掉该行；
// 中间行损坏视为账本损坏，拒绝打开。
type FileStore struct {
	mu sync.Mutex
	f  *os.File
}

func OpenFile(path string) (*FileStore, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	fs := &FileStore{f: f}
	if err := fs.repairTail(); err != nil {
		f.Close()
		return nil, err
	}
	return fs, nil
}

// repairTail 截掉末尾不完整的一行。
func (fs *FileStore) repairTail() error {
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
		line := b[good : good+i]
		if !json.Valid(line) {
			if bytes.IndexByte(b[good+i+1:], '\n') >= 0 {
				return fmt.Errorf("%w at byte %d", ErrCorrupt, good)
			}
			break // 最后一行（带换行但内容不完整）
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

func (fs *FileStore) Append(es []Entry) error {
	var buf bytes.Buffer
	for _, e := range es {
		b, err := json.Marshal(e)
		if err != nil {
			return err
		}
		buf.Write(b)
		buf.WriteByte('\n')
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if _, err := fs.f.Write(buf.Bytes()); err != nil {
		return err
	}
	return fs.f.Sync()
}

func (fs *FileStore) Load(fn func(Entry) error) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	sc := bufio.NewScanner(io.NewSectionReader(fs.f, 0, 1<<62))
	sc.Buffer(make([]byte, 64<<10), 16<<20)
	for sc.Scan() {
		var e Entry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			return fmt.Errorf("%w: %v", ErrCorrupt, err)
		}
		if err := fn(e); err != nil {
			return err
		}
	}
	return sc.Err()
}

func (fs *FileStore) Close() error { return fs.f.Close() }
