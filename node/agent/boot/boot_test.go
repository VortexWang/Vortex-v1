package boot

import (
	"path/filepath"
	"testing"

	"vortex/engine/types"
	"vortex/node/agent/store"
)

func TestStart(t *testing.T) {
	f, err := store.OpenFile(filepath.Join(t.TempDir(), "boot.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	first, err := Start(f)
	if err != nil || first.Previous != (types.UUID{}) || first.Current == (types.UUID{}) {
		t.Fatalf("first %+v %v", first, err)
	}
	prev := first.Current
	seen := map[types.UUID]bool{prev: true}
	for i := 0; i < 3*Keep; i++ {
		b, err := Start(f)
		if err != nil || b.Previous != prev || seen[b.Current] {
			t.Fatalf("boot %d: %+v %v (previous must be the last boot, ids never reused)", i, b, err)
		}
		seen[b.Current] = true
		prev = b.Current
		if len(b.History) > 2*Keep+1 {
			t.Fatalf("history not compacted: %d", len(b.History))
		}
	}
}
