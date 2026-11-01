package store

import (
	"errors"
	"path/filepath"
	"testing"
)

// 三种实现行为一致：追加、按序读出、原子替换、重开后保留。
func TestLogsConform(t *testing.T) {
	dir := t.TempDir()
	mk := map[string]func(t *testing.T) Log{
		"mem": func(*testing.T) Log { return &Mem{} },
		"file": func(t *testing.T) Log {
			f, err := OpenFile(filepath.Join(dir, "f.log"))
			if err != nil {
				t.Fatal(err)
			}
			return f
		},
		"sqlite": func(t *testing.T) Log {
			s, err := OpenSQLite(filepath.Join(dir, "a.db"), "usage")
			if err != nil {
				t.Fatal(err)
			}
			return s
		},
	}
	for name, open := range mk {
		t.Run(name, func(t *testing.T) {
			l := open(t)
			for i := 0; i < 10; i++ {
				r, _ := Make("k", i)
				if err := l.Append(r); err != nil {
					t.Fatal(err)
				}
			}
			r1, _ := Make("a", 1)
			r2, _ := Make("b", 2)
			if err := l.Rewrite([]Record{r1, r2}); err != nil {
				t.Fatal(err)
			}
			r3, _ := Make("c", 3)
			_ = l.Append(r3)
			if name != "mem" {
				l.Close()
				l = open(t)
			}
			var got []string
			_ = l.Load(func(r Record) error { got = append(got, r.Kind+string(r.Data)); return nil })
			if len(got) != 3 || got[0] != "a1" || got[1] != "b2" || got[2] != "c3" {
				t.Fatalf("%v", got)
			}
			l.Close()
		})
	}
	// 同一数据库的不同表互不影响
	a, _ := OpenSQLite(filepath.Join(dir, "b.db"), "boot")
	b, _ := OpenSQLite(filepath.Join(dir, "b.db"), "config")
	r, _ := Make("x", 1)
	_ = a.Append(r)
	n := 0
	_ = b.Load(func(Record) error { n++; return nil })
	a.Close()
	b.Close()
	if n != 0 {
		t.Fatal("tables share records")
	}
	if _, err := OpenSQLite(filepath.Join(dir, "c.db"), "bad;name"); err == nil {
		t.Fatal("bad table name accepted")
	}
}

// 数据格式：新库写入 Schema；格式超出可读范围时拒绝打开（AGT-008 兼容矩阵）。
func TestSQLiteSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.db")
	s, err := OpenSQLite(path, "usage")
	if err != nil {
		t.Fatal(err)
	}
	var v int
	if err := s.db.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil || v != Schema {
		t.Fatalf("user_version %d %v", v, err)
	}
	if _, err := s.db.Exec(`PRAGMA user_version = 99`); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if _, err := OpenSQLite(path, "usage"); !errors.Is(err, ErrSchema) {
		t.Fatalf("newer schema opened: %v", err)
	}
}
