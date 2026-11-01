package ledger

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite" // 纯 Go 的 SQLite（不需要 cgo）
)

// SQLiteStore 是账本的 SQLite（WAL）存储（HUB-012，03 方案 2）。synchronous=FULL：Append 返回前已落盘。
// 单连接：账本只有一个写者，读也经同一连接，避免 WAL 下的读写竞争。
type SQLiteStore struct {
	mu sync.Mutex
	db *sql.DB
}

// OpenSQLite 打开（或创建）path 处的账本数据库。
func OpenSQLite(path string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS ledger (
		seq  INTEGER PRIMARY KEY,
		at   TEXT NOT NULL,
		kind TEXT NOT NULL,
		data BLOB NOT NULL)`); err != nil {
		db.Close()
		return nil, err
	}
	var mode string
	if err := db.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil || mode != "wal" {
		db.Close()
		return nil, fmt.Errorf("ledger: sqlite journal_mode %q (%v), want wal", mode, err)
	}
	return &SQLiteStore{db: db}, nil
}

func insert(tx *sql.Tx, es []Entry) error {
	st, err := tx.Prepare(`INSERT INTO ledger (seq, at, kind, data) VALUES (?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer st.Close()
	for _, e := range es {
		if _, err := st.Exec(int64(e.Seq), e.At.UTC().Format(time.RFC3339Nano), e.Kind, []byte(e.Data)); err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLiteStore) Append(es []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	if err := insert(tx, es); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *SQLiteStore) Load(fn func(Entry) error) error {
	s.mu.Lock()
	rows, err := s.db.Query(`SELECT seq, at, kind, data FROM ledger ORDER BY seq`)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	var es []Entry
	for rows.Next() {
		var seq int64
		var at, kind string
		var data []byte
		if err := rows.Scan(&seq, &at, &kind, &data); err != nil {
			rows.Close()
			s.mu.Unlock()
			return err
		}
		t, err := time.Parse(time.RFC3339Nano, at)
		if err != nil || !json.Valid(data) {
			rows.Close()
			s.mu.Unlock()
			return fmt.Errorf("%w: seq %d", ErrCorrupt, seq)
		}
		es = append(es, Entry{Seq: uint64(seq), At: t, Kind: kind, Data: data})
	}
	err = rows.Err()
	rows.Close()
	s.mu.Unlock() // 回调可能写账本（重放期间不会，但不在锁内调用）
	if err != nil {
		return err
	}
	for _, e := range es {
		if err := fn(e); err != nil {
			return err
		}
	}
	return nil
}

// Rewrite 在一个事务内替换全部条目（保留期清理）。
func (s *SQLiteStore) Rewrite(es []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM ledger`); err != nil {
		tx.Rollback()
		return err
	}
	if err := insert(tx, es); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *SQLiteStore) Close() error { return s.db.Close() }
