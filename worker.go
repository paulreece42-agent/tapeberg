// The worker drains the catalog queues against real tape.
//
// Designed to run as a one-shot from cron (see deploy/tapeberg-worker.cron):
// a single flock-guarded `tapeberg worker` pass processes archive requests,
// then retrieve requests, then the pin-expiry sweep, then exits.
package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Config is the validated environment configuration (see main.go).
type Config struct {
	DBURL       string
	Drive       string
	BufferDir   string
	ChangerMode string
	ChangerDev  string
	AckDir      string
	Pool        string // tape pool this rig draws from (isolation)
	Auth        string // auth backend: builtin (default), ldap/ad later
	S3Listen    string
	S3Key       string
	S3Secret    string
}

func newChanger(cfg Config) (Changer, error) {
	switch cfg.ChangerMode {
	case "manual":
		return &ManualChanger{AckDir: cfg.AckDir, Drive: cfg.Drive, Notify: NewNotifierFromEnv()}, nil
	case "robot":
		return NewRobotChanger(cfg.ChangerDev)
	}
	return nil, fmt.Errorf("unknown changer mode %q", cfg.ChangerMode)
}

func RunWorker(cfg Config) error {
	db, err := openDB(cfg.DBURL)
	if err != nil {
		return err
	}
	defer db.Close()
	ch, err := newChanger(cfg)
	if err != nil {
		return err
	}
	if err := drainArchives(db, cfg, ch); err != nil {
		return err
	}
	if err := drainRestores(db, cfg, ch); err != nil {
		return err
	}
	return expirySweep(db)
}

// drainArchives writes each pending object to tape, oldest first, filling
// tapes in sequence. On EOM the tape is marked full and the request is
// re-queued; partial data (no filemark) is overwritten by the next write.
func drainArchives(db *sql.DB, cfg Config, ch Changer) error {
	for {
		reqs, err := pendingArchives(db, cfg.Pool)
		if err != nil {
			return err
		}
		if len(reqs) == 0 {
			return nil
		}
		if err := archiveOne(db, cfg, ch, &reqs[0]); err != nil {
			return err
		}
	}
}

func archiveOne(db *sql.DB, cfg Config, ch Changer, req *ArchiveReq) error {
	for try := 0; try < 8; try++ {
		tape, err := pickTape(db, cfg.Pool, req.Size)
		if err != nil {
			return failArchive(db, req.ID, err.Error())
		}
		if err := ch.Load(tape.Barcode); err != nil {
			return failArchive(db, req.ID, fmt.Sprintf("load %s: %v", tape.Barcode, err))
		}
		if err := setArchiveAssigned(db, req.ID, tape.ID); err != nil {
			return err
		}
		archiveID, err := nextArchiveID(db, tape.ID)
		if err != nil {
			return err
		}
		physical, werr := writeTapeFile(cfg.Drive, req.DiskPath)
		if werr == ErrTapeFull {
			if err := markTapeFull(db, tape.ID); err != nil {
				return err
			}
			_ = ch.Unload() // best effort
			if err := setArchivePending(db, req.ID); err != nil {
				return err
			}
			fmt.Printf("worker: %s hit EOM mid-%s — tape full, retrying elsewhere\n", tape.Barcode, req.Key)
			continue
		}
		if werr != nil {
			_ = failArchive(db, req.ID, werr.Error())
			_ = ch.Unload()
			return nil // request terminal; drain continues with the next
		}
		if err := insertTapeFile(db, tape.ID, req.ObjectID, int64(req.PartNumber), archiveID, physical, req.Size); err != nil {
			return err
		}
		if err := addTapeWritten(db, tape.ID, req.Size); err != nil {
			return err
		}
		if err := markArchiveWritten(db, req.ID); err != nil {
			return err
		}
		// The part left the disk buffer — its catalog path must go
		// with it, or the read layer would try the deleted file.
		if err := setPartDiskPath(db, req.ObjectID, req.PartNumber, ""); err != nil {
			return err
		}
		// GLACIER only once every part of the object is on tape —
		// a multipart upload may still be streaming parts.
		if pend, err := objectPendingParts(db, req.ObjectID); err != nil {
			return err
		} else if pend == 0 {
			if err := objectToGlacier(db, req.ObjectID); err != nil {
				return err
			}
		}
		_ = os.Remove(req.DiskPath)
		if err := ch.Unload(); err != nil {
			return err
		}
		mp, _ := isMultipart(db, req.ObjectID)
		if mp {
			fmt.Printf("worker: %s part %d → %s (archive_id %d)\n", req.Key, req.PartNumber, tape.Barcode, archiveID)
		} else {
			fmt.Printf("worker: %s → %s (archive_id %d)\n", req.Key, tape.Barcode, archiveID)
		}
		return nil
	}
	return failArchive(db, req.ID, "ran out of tapes that could fit the object"+partSuffix(req.PartNumber))
}

func partSuffix(part int) string {
	if part > 1 {
		return fmt.Sprintf(" (part %d)", part)
	}
	return ""
}

// drainRestores restores each pending request, oldest first, verifying
// the tape's bytes against the catalog's sha256 (pin-as-restore).
func drainRestores(db *sql.DB, cfg Config, ch Changer) error {
	for {
		reqs, err := pendingRetrieves(db, cfg.Pool)
		if err != nil {
			return err
		}
		if len(reqs) == 0 {
			return nil
		}
		if err := restoreOne(db, cfg, ch, &reqs[0]); err != nil {
			return err
		}
	}
}

func restoreOne(db *sql.DB, cfg Config, ch Changer, req *RetrieveReq) error {
	if mp, err := isMultipart(db, req.ObjectID); err != nil {
		return err
	} else if mp {
		return restoreMultipart(db, cfg, ch, req)
	}
	tf, err := objectTapeFile(db, req.ObjectID)
	if err != nil {
		_ = failRetrieve(db, req.ID, err.Error())
		return nil
	}
	if err := ch.Load(tf.Barcode); err != nil {
		_ = failRetrieve(db, req.ID, err.Error())
		return nil
	}
	if err := setRetrieveState(db, req.ID, "restoring"); err != nil {
		return err
	}
	dst := filepath.Join(cfg.BufferDir, req.Key)
	// Seek by the recorded PHYSICAL position — archive_id counts only
	// our files, the tape may hold more.
	sha, rerr := readTapeFile(cfg.Drive, tf.PhysicalFile, dst)
	if rerr != nil {
		_ = os.Remove(dst)
		_ = failRetrieve(db, req.ID, rerr.Error())
		_ = ch.Unload()
		return nil
	}
	if req.SHA256 != "" && sha != req.SHA256 {
		_ = os.Remove(dst)
		_ = failRetrieve(db, req.ID, fmt.Sprintf("checksum mismatch: tape %s gave %s, expected %s", tf.Barcode, sha, req.SHA256))
		_ = ch.Unload()
		return nil
	}
	if err := objectToStandard(db, req.ObjectID, dst); err != nil {
		return err
	}
	if err := restoreComplete(db, req.ID, time.Now().AddDate(0, 0, req.PinDays)); err != nil {
		return err
	}
	if err := ch.Unload(); err != nil {
		return err
	}
	fmt.Printf("worker: restored %s from %s (pinned %d days)\n", req.Key, tf.Barcode, req.PinDays)
	return nil
}

// restoreMultipart materializes every part of a multipart object,
// verifying each part's sha256 against the catalog. Parts may live on
// different tapes — the changer swaps as needed (and skips swaps when
// consecutive parts share a tape). The S3/CLI read layer concatenates
// the part files in order; they are the merge.
func restoreMultipart(db *sql.DB, cfg Config, ch Changer, req *RetrieveReq) error {
	parts, err := multipartTapeFiles(db, req.ObjectID)
	if err != nil {
		return err
	}
	if len(parts) == 0 {
		_ = failRetrieve(db, req.ID, "multipart object has no tape placement (upload incomplete?)")
		return nil
	}
	if err := setRetrieveState(db, req.ID, "restoring"); err != nil {
		return err
	}
	for i := range parts {
		p := &parts[i]
		// A part may already be on disk (still buffered or pinned).
		if p.DiskPath != "" {
			if _, err := os.Stat(p.DiskPath); err == nil {
				continue
			}
		}
		if err := ch.Load(p.Barcode); err != nil {
			_ = failRetrieve(db, req.ID, err.Error())
			return nil
		}
		dst := filepath.Join(cfg.BufferDir, fmt.Sprintf("part-%d-%05d", req.ObjectID, p.Number))
		sha, rerr := readTapeFile(cfg.Drive, p.PhysicalFile, dst)
		if rerr != nil {
			_ = os.Remove(dst)
			_ = failRetrieve(db, req.ID, fmt.Sprintf("part %d: %v", p.Number, rerr))
			_ = ch.Unload()
			return nil
		}
		if sha != p.SHA256 {
			_ = os.Remove(dst)
			_ = failRetrieve(db, req.ID, fmt.Sprintf("part %d checksum mismatch: tape %s gave %s, expected %s", p.Number, p.Barcode, sha, p.SHA256))
			_ = ch.Unload()
			return nil
		}
		if err := setPartDiskPath(db, req.ObjectID, p.Number, dst); err != nil {
			return err
		}
	}
	if err := objectToStandardMultipart(db, req.ObjectID); err != nil {
		return err
	}
	if err := restoreComplete(db, req.ID, time.Now().AddDate(0, 0, req.PinDays)); err != nil {
		return err
	}
	_ = ch.Unload()
	tapes := make([]string, 0, len(parts))
	for _, p := range parts {
		if len(tapes) == 0 || tapes[len(tapes)-1] != p.Barcode {
			tapes = append(tapes, p.Barcode)
		}
	}
	fmt.Printf("worker: restored %s (%d parts from %s, pinned %d days)\n", req.Key, len(parts), strings.Join(tapes, "+"), req.PinDays)
	return nil
}

// expirySweep unpins restores whose Glacier-style pin has lapsed.
func expirySweep(db *sql.DB) error {
	rows, err := expiredRestores(db)
	if err != nil {
		return err
	}
	for _, e := range rows {
		if e.DiskPath != "" {
			if err := os.Remove(e.DiskPath); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		// Multipart: the pin lives in the part files.
		paths, err := clearPartDiskPaths(db, e.ObjectID)
		if err != nil {
			return err
		}
		for _, p := range paths {
			if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		if err := objectToGlacier(db, e.ObjectID); err != nil {
			return err
		}
		if err := markRetrieveExpired(db, e.ReqID); err != nil {
			return err
		}
		if len(paths) > 0 {
			fmt.Printf("worker: pin expired — %s (%d parts) unpinned back to GLACIER\n", filepath.Base(paths[0]), len(paths))
		} else {
			fmt.Printf("worker: pin expired — %s unpinned back to GLACIER\n", e.DiskPath)
		}
	}
	return nil
}
