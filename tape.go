// Tape device I/O for Tapeberg.
//
// Rig-verified constraints (see docs/dev-rig.md):
//   - single write() calls larger than ~512KiB fail with EBUSY against the
//     emulated drive, so all data moves in <=256KiB chunks;
//   - the st driver enforces single-open, so control operations (mt) run
//     only while no data fd is open;
//   - a just-loaded drive can briefly refuse work, so positioning retries.
package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"crypto/sha256"
	"encoding/hex"
)

const tapeChunk = 256 * 1024

// ErrTapeFull marks a write that hit end-of-media.
var ErrTapeFull = errors.New("tape full (EOM)")

func mt(dev string, args ...string) error {
	full := append([]string{"-f", dev}, args...)
	out, err := exec.Command("mt", full...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("mt %s %s: %v: %s", dev, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

// mtRetry retries a positioning op that can race a just-finished mount.
func mtRetry(dev string, tries int, args ...string) error {
	var err error
	for i := 0; i < tries; i++ {
		if err = mt(dev, args...); err == nil {
			return nil
		}
		time.Sleep(time.Second)
	}
	return err
}

// writeTapeFile writes src to the tape at EOD and appends one filemark.
// On EOM it returns ErrTapeFull and leaves the catalog untouched; any
// partially written data has no filemark and is overwritten by the next
// write (the tape is marked full by the caller).
func writeTapeFile(dev, src string) error {
	if err := mtRetry(dev, 5, "eod"); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dev, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	buf := make([]byte, tapeChunk)
	var werr error
write:
	for {
		n, rerr := io.ReadFull(in, buf)
		if n > 0 {
			if _, werr = out.Write(buf[:n]); werr != nil {
				break write
			}
		}
		if rerr != nil {
			if rerr != io.EOF && rerr != io.ErrUnexpectedEOF {
				werr = rerr
			}
			break write
		}
	}
	if cerr := out.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		if errors.Is(werr, syscall.ENOSPC) {
			return ErrTapeFull
		}
		return werr
	}
	// No explicit filemark: the st driver appends one on the final
	// write-mode close (verified against the rig — an explicit `mt weof`
	// on top of it produces double filemarks, and seeks into the empty
	// inter-filemark gap read zero bytes).
	return nil
}

// readTapeFile positions the drive at file archiveID (1-based, counting
// files from the beginning of the tape), streams it to dst, and returns
// the file's sha256 hex digest. Reading stops at the filemark.
func readTapeFile(dev string, archiveID int64, dst string) (string, error) {
	if err := mtRetry(dev, 5, "rewind"); err != nil {
		return "", err
	}
	if archiveID > 1 {
		if err := mt(dev, "fsf", strconv.FormatInt(archiveID-1, 10)); err != nil {
			return "", err
		}
	}
	f, err := os.Open(dev)
	if err != nil {
		return "", err
	}
	defer f.Close()
	out, err := os.Create(dst)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	buf := make([]byte, tapeChunk)
	var rerr error
read:
	for {
		n, e := f.Read(buf)
		if n > 0 {
			if _, werr := out.Write(buf[:n]); werr != nil {
				rerr = werr
				break read
			}
			h.Write(buf[:n])
		}
		if e != nil {
			if e != io.EOF {
				rerr = e
			}
			break read
		}
	}
	if cerr := out.Close(); rerr == nil {
		rerr = cerr
	}
	if rerr != nil {
		return "", rerr
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
