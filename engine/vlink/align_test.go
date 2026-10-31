package vlink

import (
	"testing"
)

// ENG-044：连续写出的 DATA 帧每帧恰好 16,384 字节（帧头 + 16,376），与 TLS 记录一一对齐。
func TestDataFramesAlignToTLSRecords(t *testing.T) {
	l, raw := rawWith(t, LinkOptions{Initiator: false})
	_, _ = raw.TLS.Write(openFrame(1)) // 对端开流
	s, err := l.Accept(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	s.Accept()
	const n = 10*MaxDataPayload + 100
	go func() { _, _ = s.Write(make([]byte, n)) }()
	got, frames := 0, 0
	for got < n {
		h, p, err := raw.Reader.Next()
		if err != nil {
			t.Fatal(err)
		}
		if h.Type != TypeData {
			continue
		}
		frames++
		got += len(p)
		if got < n && len(p) != MaxDataPayload {
			t.Fatalf("DATA frame %d carries %d bytes, want %d", frames, len(p), MaxDataPayload)
		}
	}
	if frames != 11 || HeaderLen+MaxDataPayload != 16384 {
		t.Fatalf("%d frames", frames)
	}
	if !DefaultTLSRecordSizing() {
		t.Fatal("dynamic record sizing must be disabled on link TLS configs")
	}
}

func TestCPUHasAES(t *testing.T) {
	x86 := "processor\t: 0\nflags\t\t: fpu sse2 aes avx pclmulqdq\n"
	if ok, known := cpuHasAES("amd64", x86); !ok || !known {
		t.Fatal("x86 with aes+pclmulqdq")
	}
	if ok, known := cpuHasAES("amd64", "flags\t: fpu sse2 avx\n"); ok || !known {
		t.Fatal("x86 without aes")
	}
	if ok, _ := cpuHasAES("arm64", "Features\t: fp asimd aes pmull sha1\n"); !ok {
		t.Fatal("arm64 with aes+pmull")
	}
	if _, known := cpuHasAES("riscv64", x86); known {
		t.Fatal("unknown arch must be unknown")
	}
	ok, known := AESHardware()
	t.Logf("this machine: AES-GCM hardware %v (known %v)", ok, known)
}
