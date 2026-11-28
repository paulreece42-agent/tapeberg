package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// Issue #41: a backward seek left every later part's underlying file
// offset stale; the next Read skipped bytes and served them as 200/206.
// ReadAt (no file cursor at all) must behave like reading one virtual
// file: forward, backward, across boundaries, in pieces.
func TestConcatReadSeekerSeekBackward(t *testing.T) {
	dir := t.TempDir()
	want := []byte{}
	sizes := []int{64, 3, 128} // part 2 is deliberately tiny
	names := []string{"a", "b", "c"}
	for i, n := range sizes {
		b := bytes.Repeat([]byte(names[i]), n)
		want = append(want, b...)
		if err := os.WriteFile(filepath.Join(dir, "p"+string(rune('0'+i))), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	c, err := newConcatReadSeeker([]string{
		filepath.Join(dir, "p0"), filepath.Join(dir, "p1"), filepath.Join(dir, "p2"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if got := c.total(); int(got) != len(want) {
		t.Fatalf("total = %d, want %d", got, len(want))
	}

	readN := func(c *concatReadSeeker, n int) []byte {
		buf := make([]byte, 0, n)
		for len(buf) < n {
			tmp := make([]byte, n-len(buf))
			got, err := c.Read(tmp)
			buf = append(buf, tmp[:got]...)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("Read: %v", err)
			}
		}
		return buf
	}

	// Forward across a boundary, in awkward chunks.
	if got := readN(c, 70); !bytes.Equal(got, want[:70]) {
		t.Fatalf("forward read across boundary: got %d bytes that differ", len(got))
	}
	// Backward seek: the exact trigger for #41. Resume in the middle of
	// the last part must repeat, not carry a stale cursor.
	if _, err := c.Seek(70-10, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if got := readN(c, 10); !bytes.Equal(got, want[60:70]) {
		t.Fatalf("after backward seek: got %q, want %q", got, want[60:70])
	}
	// Walk the whole rest: must equal want[70:] byte for byte.
	for off := 70; off < len(want); {
		chunk := 7
		if off+chunk > len(want) {
			chunk = len(want) - off
		}
		got := readN(c, chunk)
		if !bytes.Equal(got, want[off:off+len(got)]) {
			t.Fatalf("at offset %d: got %q, want %q", off, got, want[off:off+len(got)])
		}
		off += len(got)
	}
	// At EOF: further reads are EOF.
	if _, err := c.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("read past end: %v", err)
	}
	// SeekEnd/SeekCurrent round trip.
	if n, _ := c.Seek(-5, io.SeekEnd); n != int64(len(want)-5) {
		t.Fatalf("SeekEnd(-5) = %d", n)
	}
	if got := readN(c, 5); !bytes.Equal(got, want[len(want)-5:]) {
		t.Fatalf("tail after SeekEnd: got %q", got)
	}
	if _, err := c.Seek(-1, io.SeekStart); err == nil {
		t.Fatal("negative seek must error")
	}
}
