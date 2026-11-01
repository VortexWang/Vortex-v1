package ledger

import (
	"path/filepath"
	"testing"
	"time"

	"vortex/edgehub/clock"
)

// 三种存储行为一致：追加、按序重放、确认位置、清理（原子替换）、重开后序号继续。
func TestStoresConform(t *testing.T) {
	dir := t.TempDir()
	stores := map[string]func(t *testing.T) Store{
		"mem": func(*testing.T) Store { return &MemStore{} },
		"file": func(t *testing.T) Store {
			s, err := OpenFile(filepath.Join(dir, "l.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			return s
		},
		"sqlite": func(t *testing.T) Store {
			s, err := OpenSQLite(filepath.Join(dir, "l.db"))
			if err != nil {
				t.Fatal(err)
			}
			return s
		},
	}
	for name, mk := range stores {
		t.Run(name, func(t *testing.T) {
			clk := clock.NewFake(time.Date(2026, 11, 1, 0, 0, 0, 123456789, time.UTC))
			st := mk(t)
			l, err := Open(st, clk, nil)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 50; i++ {
				if _, err := l.Append("x", map[string]int{"i": i}); err != nil {
					t.Fatal(err)
				}
			}
			_ = l.Ack(30)
			n, err := l.Prune(func(e Entry) bool { return e.Seq <= 20 })
			if err != nil || n != 20 {
				t.Fatalf("prune %d %v", n, err)
			}
			if name != "mem" {
				st.Close()
				st = mk(t)
			}
			var got []Entry
			l2, err := Open(st, clk, func(e Entry) error { got = append(got, e); return nil })
			if err != nil {
				t.Fatal(err)
			}
			if l2.LastSeq() != 51 || l2.Acked() != 30 || len(got) != 30 || got[0].Seq != 21 || !got[0].At.Equal(clk.Now()) || string(got[0].Data) != `{"i":20}` {
				t.Fatalf("reopen: last %d acked %d entries %d first %+v", l2.LastSeq(), l2.Acked(), len(got), got[0])
			}
			if seq, _ := l2.Append("y", 1); seq != 52 {
				t.Fatalf("seq after reopen %d", seq)
			}
			st.Close()
		})
	}
}
