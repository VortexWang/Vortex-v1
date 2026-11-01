package store

import (
	"database/sql"
	"fmt"
	"sync"

	_ "modernc.org/sqlite" // 纯 Go 的 SQLite（不需要 cgo）
)

// SQLite 是 Agent 本地存储的 SQLite（WAL）实现（AGT-003，02 方案 3.3）。synchronous=FULL：Append 返回前
// 已落盘。每个日志一张表（启动标识、计量、最后生效配置分别一个数据库文件或一张表），记录按写入顺序编号。
type SQLite struct {
	mu    sync.Mutex
	db    *sql.DB
	table string
}

// OpenSQLite 打开 path 处数据库中的 table（不存在则创建）。
func OpenSQLite(path, table string) (*SQLite, error) {
	for _, c := range table {
		if !(c == '_' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9') {
			return nil, fmt.Errorf("store: bad table name %q", table)
		}
	}
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS ` + table + ` (id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, data BLOB NOT NULL)`); err != nil {
		db.Close()
		return nil, err
	}
	return &SQLite{db: db, table: table}, nil
}

func (s *SQLite) insert(tx *sql.Tx, rs []Record) error {
	st, err := tx.Prepare(`INSERT INTO ` + s.table + ` (kind, data) VALUES (?, ?)`)
	if err != nil {
		return err
	}
	defer st.Close()
	for _, r := range rs {
		if _, err := st.Exec(r.Kind, []byte(r.Data)); err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLite) Append(rs ...Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	if err := s.insert(tx, rs); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *SQLite) Load(fn func(Record) error) error {
	s.mu.Lock()
	rows, err := s.db.Query(`SELECT kind, data FROM ` + s.table + ` ORDER BY id`)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	var rs []Record
	for rows.Next() {
		var r Record
		var data []byte
		if err := rows.Scan(&r.Kind, &data); err != nil {
			rows.Close()
			s.mu.Unlock()
			return err
		}
		r.Data = data
		rs = append(rs, r)
	}
	err = rows.Err()
	rows.Close()
	s.mu.Unlock()
	if err != nil {
		return err
	}
	for _, r := range rs {
		if err := fn(r); err != nil {
			return err
		}
	}
	return nil
}

// Rewrite 在一个事务内替换全部记录。
func (s *SQLite) Rewrite(rs []Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM ` + s.table); err != nil {
		tx.Rollback()
		return err
	}
	if err := s.insert(tx, rs); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *SQLite) Close() error { return s.db.Close() }
