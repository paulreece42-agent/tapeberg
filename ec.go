// Erasure coding: RAIT — Reed-Solomon shards striped across tapes.
//
// The pool's EC policy (ec_k + ec_m) splits each archived object or
// multipart part into k data shards plus m parity shards; every shard
// lands on a different tape, so up to m tape losses are survivable.
// Sizes are exact (the catalog knows them), so the classic
// unknown-length EC problem does not exist here: split, verify, join.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/klauspost/reedsolomon"
)

// shardFile is one encoded shard staged in the buffer directory.
type shardFile struct {
	Path   string
	Size   int64
	SHA256 string
}

// ecShardSize is the padded per-shard byte count for a file of size n.
func ecShardSize(n int64, k int) int64 {
	s := (n + int64(k) - 1) / int64(k)
	if s == 0 {
		s = 1
	}
	return s
}

// ecEncodeFile splits src into k data shards and encodes m parity
// shards, all staged under outDir. Sizes are exact; the last data
// shard is zero-padded to the common shard size (Join trims by the
// known total size, so padding never leaks into restores).
func ecEncodeFile(src string, k, m int, outDir, tag string) ([]shardFile, error) {
	enc, err := reedsolomon.NewStream(k, m)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(src)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}

	paths := make([]string, k+m)
	for i := range paths {
		paths[i] = filepath.Join(outDir, fmt.Sprintf("%s.s%02d", tag, i))
	}

	// 1. Split into k data shard files (readers closed below).
	dataW := make([]io.Writer, k)
	for i := 0; i < k; i++ {
		w, err := os.Create(paths[i])
		if err != nil {
			return nil, err
		}
		dataW[i] = w
		defer w.Close()
	}
	if err := enc.Split(f, dataW, st.Size()); err != nil {
		return nil, fmt.Errorf("ec split: %w", err)
	}
	for _, w := range dataW {
		if err := w.(*os.File).Close(); err != nil {
			return nil, err
		}
	}

	// 2. Parity from the data shards.
	dataR := make([]io.Reader, k)
	for i := 0; i < k; i++ {
		r, err := os.Open(paths[i])
		if err != nil {
			return nil, err
		}
		dataR[i] = r
		defer r.Close()
	}
	parityW := make([]io.Writer, m)
	for i := 0; i < m; i++ {
		w, err := os.Create(paths[k+i])
		if err != nil {
			return nil, err
		}
		parityW[i] = w
		defer w.Close()
	}
	if err := enc.Encode(dataR, parityW); err != nil {
		return nil, fmt.Errorf("ec encode: %w", err)
	}
	for _, r := range dataR {
		_ = r.(*os.File).Close()
	}
	for _, w := range parityW {
		if err := w.(*os.File).Close(); err != nil {
			return nil, err
		}
	}

	out := make([]shardFile, k+m)
	for i, p := range paths {
		fi, err := os.Stat(p)
		if err != nil {
			return nil, err
		}
		sha, err := sha256File(p)
		if err != nil {
			return nil, err
		}
		out[i] = shardFile{Path: p, Size: fi.Size(), SHA256: sha}
	}
	return out, nil
}

// ecDecodeFile rebuilds the original bytes of exact size n from shard
// files. shardPaths is the full k+m slice; an empty path marks a shard
// as lost. Missing DATA shards are reconstructed from the survivors
// (any k of k+m); more than m losses is an error. Returns the temp
// paths it created (caller removes them with the shard files).
func ecDecodeFile(shardPaths []string, k, m int, n int64, dst string) (temps []string, err error) {
	enc, err := reedsolomon.NewStream(k, m)
	if err != nil {
		return nil, err
	}
	if len(shardPaths) != k+m {
		return nil, fmt.Errorf("ec: expected %d shards, got %d", k+m, len(shardPaths))
	}

	valid := make([]io.Reader, k+m)
	losses := 0
	for i, p := range shardPaths {
		if p == "" {
			losses++
			continue
		}
		r, err := os.Open(p)
		if err != nil {
			return nil, err
		}
		valid[i] = r
	}
	if losses > m {
		return nil, fmt.Errorf("ec: %d shards lost, only %d survivable", losses, m)
	}

	// Reconstruct the missing data shards into temp files. Reconstruct
	// consumes the valid readers, so everything is reopened afterwards.
	needReconstruct := false
	for i := 0; i < k; i++ {
		if shardPaths[i] == "" {
			needReconstruct = true
		}
	}
	if needReconstruct {
		for i := 0; i < k; i++ {
			if shardPaths[i] != "" {
				continue
			}
			tmp := fmt.Sprintf("%s.rec%02d", dst, i)
			w, err := os.Create(tmp)
			if err != nil {
				return temps, err
			}
			fill := make([]io.Writer, k+m)
			fill[i] = w
			for j := range fill {
				if valid[j] == nil && j != i {
					fill[j] = io.Discard
				}
			}
			if err := enc.Reconstruct(valid, fill); err != nil {
				w.Close()
				return temps, fmt.Errorf("ec reconstruct shard %d: %w", i, err)
			}
			w.Close()
			temps = append(temps, tmp)
			shardPaths[i] = tmp
		}
		// Reopen every reader at position 0 (Reconstruct drained them).
		for i := range valid {
			if rc, ok := valid[i].(io.Closer); ok && valid[i] != nil {
				_ = rc.Close()
				valid[i] = nil
			}
			if shardPaths[i] == "" {
				continue
			}
			r, err := os.Open(shardPaths[i])
			if err != nil {
				return temps, err
			}
			valid[i] = r
		}
	}
	defer func() {
		for _, r := range valid {
			if rc, ok := r.(io.Closer); ok {
				_ = rc.Close()
			}
		}
	}()

	out, err := os.Create(dst)
	if err != nil {
		return temps, err
	}
	defer out.Close()
	if err := enc.Join(out, valid[:k], n); err != nil {
		return temps, fmt.Errorf("ec join: %w", err)
	}
	return temps, nil
}

func sha256File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
