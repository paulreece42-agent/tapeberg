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

// tapeFileNumber parses "File number=N" from `mt status`. The st driver
// numbers the file the head is positioned IN (0-based); -1 means before
// the first file (blank tape / BOT).
func tapeFileNumber(dev string) (int64, error) {
	out, err := exec.Command("mt", "-f", dev, "status").CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("mt status: %v: %s", err, strings.TrimSpace(string(out)))
	}
	s := string(out)
	i := strings.Index(s, "File number=")
	if i < 0 {
		return 0, fmt.Errorf("mt status: no File number in %q", strings.TrimSpace(s))
	}
	rest := s[i+len("File number="):]
	if j := strings.IndexAny(rest, ",\n"); j >= 0 {
		rest = rest[:j]
	}
	n, err := strconv.ParseInt(strings.TrimSpace(rest), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("mt status: bad File number %q", rest)
	}
	return n, nil
}

// writeTapeFile writes src to the tape at EOD and appends one filemark.
// It returns the 1-based PHYSICAL file position the data landed in (as
// reported by the drive) — the catalog must record this, not assume it
// equals our archive_id, because real media can carry foreign files
// (e.g. a leading empty file at BOT) that shift every position.
// On EOM it returns ErrTapeFull and leaves the catalog untouched; any
// partially written data has no filemark and is overwritten by the next
// write (the tape is marked full by the caller).
func writeTapeFile(dev, src string) (int64, error) {
	if err := mtRetry(dev, 5, "eod"); err != nil {
		return 0, err
	}
	fn, err := tapeFileNumber(dev)
	if err != nil {
		return 0, err
	}
	if fn < 0 {
		// Position unknown (st reports -1 when no operation this open
		// established it — e.g. the drive was already at EOD after a
		// previous write). Force movement to re-establish, then
		// re-probe; a wrong guess here poisons the catalog.
		if err := mtRetry(dev, 5, "rewind"); err != nil {
			return 0, err
		}
		if err := mtRetry(dev, 5, "eod"); err != nil {
			return 0, err
		}
		if fn, err = tapeFileNumber(dev); err != nil {
			return 0, err
		}
		if fn < 0 {
			return 0, fmt.Errorf("cannot determine tape position after rewind+eod (status reports file -1)")
		}
	}
	physical := fn + 1
	in, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer in.Close()

	out, err := os.OpenFile(dev, os.O_WRONLY, 0)
	if err != nil {
		return 0, err
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
			return 0, ErrTapeFull
		}
		return 0, werr
	}
	// No explicit filemark: the st driver appends one on the final
	// write-mode close (verified against the rig — an explicit `mt weof`
	// on top of it produces double filemarks, and seeks into the empty
	// inter-filemark gap read zero bytes).
	return physical, nil
}

// readCurrentFile streams the file at the current drive position to
// dst until its filemark, returning the sha256 hex digest.
func readCurrentFile(dev, dst string) (string, error) {
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

// readTapeFileAt positions the drive at physical file `target` (1-based,
// counting every file on the tape) and streams it to dst. `cur` is the
// file the drive is known to sit at the START of (0 = unknown). After a
// read the drive sits exactly at the start of the next file, so when
// cur+1 == target NO positioning happens — sequential parts stream
// back-to-back with a single seek for the whole run instead of a
// rewind+space per file (which re-shoes the drive for minutes on big
// objects). Returns the new cur (== target) on success.
func readTapeFileAt(dev string, target, cur int64, dst string) (string, int64, error) {
	if !(cur > 0 && cur+1 == target) {
		if err := mtRetry(dev, 5, "rewind"); err != nil {
			return "", 0, err
		}
		if target > 1 {
			if err := mt(dev, "fsf", strconv.FormatInt(target-1, 10)); err != nil {
				return "", 0, err
			}
		}
	}
	sha, err := readCurrentFile(dev, dst)
	if err != nil {
		return "", 0, err
	}
	return sha, target, nil
}

// readTapeFile positions the drive at file archiveID (1-based, counting
// files from the beginning of the tape), streams it to dst, and returns
// the file's sha256 hex digest. Reading stops at the filemark.
func readTapeFile(dev string, archiveID int64, dst string) (string, error) {
	sha, _, err := readTapeFileAt(dev, archiveID, 0, dst)
	return sha, err
}
