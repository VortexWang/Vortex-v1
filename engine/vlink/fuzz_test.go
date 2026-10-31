package vlink

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"
)

func seedVectors(f *testing.F) {
	b, err := os.ReadFile("ref/testdata/vectors.json")
	if err != nil {
		f.Fatal(err)
	}
	var vs []vector
	_ = json.Unmarshal(b, &vs)
	for _, v := range vs {
		raw, _ := hex.DecodeString(v.Hex)
		f.Add(raw)
	}
}

// 任意输入都不得 panic；解析成功的 HELLO / OPEN 重新编码后必须能再次解析为同一内容。
func FuzzFrames(f *testing.F) {
	seedVectors(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewReader(bytes.NewReader(data))
		for i := 0; i < 64; i++ {
			h, p, err := r.Next()
			if err != nil {
				return
			}
			if int(h.Length) != len(p) || h.Check(false) != nil {
				t.Fatalf("reader returned invalid frame %+v", h)
			}
			switch h.Type {
			case TypeHello:
				if hl, err := ParseHello(p); err == nil {
					again, err := ParseHello(hl.AppendPayload(nil))
					if err != nil || again.NodeID != hl.NodeID || again.Generation != hl.Generation || !bytes.Equal(again.ExporterSig, hl.ExporterSig) {
						t.Fatalf("hello re-encode: %v", err)
					}
				}
			case TypeOpen:
				if o, err := ParseOpen(p); err == nil {
					again, err := ParseOpen(o.AppendPayload(nil))
					if err != nil || again.RuleID != o.RuleID || again.Client != o.Client || len(again.Visited) != len(o.Visited) {
						t.Fatalf("open re-encode: %v", err)
					}
				}
			case TypeOpenErr, TypeRst:
				_, _, _ = ParseErrPayload(p)
			case TypeWindow:
				_, _ = ParseWindowPayload(p)
			case TypeGoaway:
				_, _, _ = ParseGoawayPayload(p)
			}
		}
	})
}
