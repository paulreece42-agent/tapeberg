package main

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func fileSHA(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func TestECEncodeDecodeRoundTrip(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "obj")
	// 12 MiB + change: not shard-aligned, exercises padding.
	b := make([]byte, 12*1024*1024+31337)
	for i := range b {
		b[i] = byte(i * 7)
	}
	if err := os.WriteFile(src, b, 0o644); err != nil {
		t.Fatal(err)
	}
	want := fileSHA(t, src)

	const k, m = 2, 1
	shards, err := ecEncodeFile(src, k, m, dir, "t")
	if err != nil {
		t.Fatal(err)
	}
	if len(shards) != k+m {
		t.Fatalf("want %d shards, got %d", k+m, len(shards))
	}
	paths := make([]string, len(shards))
	for i, s := range shards {
		paths[i] = s.Path
	}

	// Full recovery: all shards present.
	dst := filepath.Join(dir, "out")
	if _, err := ecDecodeFile(paths, k, m, int64(len(b)), dst); err != nil {
		t.Fatal(err)
	}
	if fileSHA(t, dst) != want {
		t.Fatal("full decode differs")
	}

	// Shard loss: drop one DATA shard — parity reconstructs it.
	lost := append([]string(nil), paths...)
	lost[0] = ""
	dst2 := filepath.Join(dir, "out2")
	if _, err := ecDecodeFile(lost, k, m, int64(len(b)), dst2); err != nil {
		t.Fatal(err)
	}
	if fileSHA(t, dst2) != want {
		t.Fatal("degraded decode differs")
	}

	// Two losses (k=2): unrecoverable.
	lost2 := append([]string(nil), paths...)
	lost2[0] = ""
	lost2[2] = ""
	if _, err := ecDecodeFile(lost2, k, m, int64(len(b)), filepath.Join(dir, "out3")); err == nil {
		t.Fatal("expected unrecoverable error with 2 losses at k=2,m=1")
	}
}
