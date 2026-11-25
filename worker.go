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
}

func newChanger(cfg Config) (Changer, error) {
	switch cfg.ChangerMode {
	case "manual":
		return &ManualChanger{AckDir: cfg.AckDir, Drive: cfg.Drive}, nil
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
		reqs, err := pendingArchives(db)
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
		tape, err := pickTape(db, req.Size)
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
		werr := writeTapeFile(cfg.Drive, req.DiskPath)
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
		if err := insertTapeFile(db, tape.ID, req.ObjectID, archiveID, req.Size); err != nil {
			return err
		}
		if err := addTapeWritten(db, tape.ID, req.Size); err != nil {
			return err
		}
		if err := markArchiveWritten(db, req.ID); err != nil {
			return err
		}
		if err := objectToGlacier(db, req.ObjectID); err != nil {
			return err
		}
		_ = os.Remove(req.DiskPath)
		if err := ch.Unload(); err != nil {
			return err
		}
		fmt.Printf("worker: %s → %s (archive_id %d)\n", req.Key, tape.Barcode, archiveID)
		return nil
	}
	return failArchive(db, req.ID, "ran out of tapes that could fit the object")
}

// drainRestores restores each pending request, oldest first, verifying
// the tape's bytes against the catalog's sha256 (pin-as-restore).
func drainRestores(db *sql.DB, cfg Config, ch Changer) error {
	for {
		reqs, err := pendingRetrieves(db)
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
	sha, rerr := readTapeFile(cfg.Drive, tf.ArchiveID, dst)
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

// expirySweep unpins restores whose Glacier-style pin has lapsed.
func expirySweep(db *sql.DB) error {
	rows, err := expiredRestores(db)
	if err != nil {
		return err
	}
	for _, e := range rows {
		if err := os.Remove(e.DiskPath); err != nil && !os.IsNotExist(err) {
			return err
		}
		if err := objectToGlacier(db, e.ObjectID); err != nil {
			return err
		}
		if err := markRetrieveExpired(db, e.ReqID); err != nil {
			return err
		}
		fmt.Printf("worker: pin expired — %s unpinned back to GLACIER\n", e.DiskPath)
	}
	return nil
}
