package ledger

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"vortex/edgehub/clock"
)

type rec struct {
	N int `json:"n"`
}

func TestFileLedgerReplayAckAndTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	clk := clock.NewFake(time.Unix(1000, 0))
	st, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	l, _ := Open(st, clk, nil)
	for i := 1; i <= 5; i++ {
		if seq, err := l.Append("test", rec{i}); err != nil || seq != uint64(i) {
			t.Fatalf("append %d: seq %d %v", i, seq, err)
		}
	}
	if err := l.Ack(3); err != nil {
		t.Fatal(err)
	}
	_ = l.Ack(2) // 较小的确认被忽略
	un, _ := l.Unacked()
	if len(un) != 2 || un[0].Seq != 4 || un[1].Seq != 5 {
		t.Fatalf("unacked %+v", un)
	}
	l.Close()

	// 模拟写入中途断电：末尾追加半行
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	_, _ = f.WriteString(`{"seq":7,"at":"2026-`)
	f.Close()

	st2, err := OpenFile(path)
	if err != nil {
		t.Fatalf("torn tail must be repaired: %v", err)
	}
	var got []int
	l2, err := Open(st2, clk, func(e Entry) error { got = append(got, int(e.Seq)); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 || l2.LastSeq() != 6 || l2.Acked() != 3 { // seq 6 是确认记录
		t.Fatalf("replayed %v last %d acked %d", got, l2.LastSeq(), l2.Acked())
	}
	if seq, _ := l2.Append("test", rec{9}); seq != 7 {
		t.Fatalf("next seq %d after repair", seq)
	}
	l2.Close()
}

func TestCorruptMiddleRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	if err := os.WriteFile(path, []byte("{\"seq\":1}\nnot json\n{\"seq\":3}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenFile(path); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt middle line: %v", err)
	}
}

func TestNonIncreasingSeqRejected(t *testing.T) {
	st := &MemStore{}
	_ = st.Append([]Entry{{Seq: 2, Kind: "x", Data: []byte("{}")}, {Seq: 2, Kind: "x", Data: []byte("{}")}})
	if _, err := Open(st, clock.System{}, nil); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("duplicate seq: %v", err)
	}
}
