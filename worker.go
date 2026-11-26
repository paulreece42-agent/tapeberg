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
	"sync"
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
	// RepackThreshold is the dead-bytes percentage that makes the
	// worker rewrite a tape (CTA-style repack). 0 disables.
	RepackThreshold int
	// Drives are all tape drives available to this rig, in DTE order
	// (TAPEBERG_DRIVES). RAIT shard stripes write/read in parallel
	// across them; the first entry is the single-drive path's drive.
	Drives     []string
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
	if err := repackPass(db, cfg, ch); err != nil {
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
	// RAIT: pools with an EC policy stripe every object/part across
	// k+m distinct tapes instead of writing one whole-file copy.
	if ec, err := poolEC(db, cfg.Pool); err != nil {
		return err
	} else if ec != nil {
		return ecArchive(db, cfg, ch, req, ec)
	}
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
	// Erasure-coded single object: the "tape file" is a k+m shard
	// stripe, any k of which rebuild the bytes.
	if ec, err := objectEC(db, req.ObjectID); err != nil {
		return err
	} else if ec != nil {
		return restoreOneEC(db, cfg, ch, req, ec)
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

// restoreOneEC rebuilds a single erasure-coded object from its shard
// stripe (with parity reconstruction when shards are missing).
func restoreOneEC(db *sql.DB, cfg Config, ch Changer, req *RetrieveReq, ec *ECConfig) error {
	var size int64
	if err := db.QueryRow(`SELECT size_bytes FROM objects WHERE id = $1`, req.ObjectID).Scan(&size); err != nil {
		return err
	}
	if err := setRetrieveState(db, req.ID, "restoring"); err != nil {
		return err
	}
	dst := filepath.Join(cfg.BufferDir, req.Key)
	// Single EC object: one stripe across k+m tapes. The archive request
	// carried part_number 1 (no parts table rows — the multipart
	// default), and the placements live under that part.
	if err := ecRestorePart(db, cfg, ch, req.ObjectID, 1, ec, size, req.SHA256, dst); err != nil {
		_ = os.Remove(dst)
		_ = failRetrieve(db, req.ID, "RAIT restore: "+err.Error())
		return nil
	}
	if err := objectToStandard(db, req.ObjectID, dst); err != nil {
		return err
	}
	if err := restoreComplete(db, req.ID, time.Now().AddDate(0, 0, req.PinDays)); err != nil {
		return err
	}
	fmt.Printf("worker: restored %s from RAIT %d+%d stripe (pinned %d days)\n", req.Key, ec.K, ec.M, req.PinDays)
	return nil
}

// restoreMultipart materializes every part of a multipart object,
// verifying each part's sha256 against the catalog. Parts may live on
// different tapes — the changer swaps as needed (and skips swaps when
// consecutive parts share a tape). The S3/CLI read layer concatenates
// the part files in order; they are the merge.
func restoreMultipart(db *sql.DB, cfg Config, ch Changer, req *RetrieveReq) error {
	// Erasure-coded multipart: each part is its own shard stripe
	// (tape_files rows are shards, not the part itself).
	if ec, err := objectEC(db, req.ObjectID); err != nil {
		return err
	} else if ec != nil {
		return restoreMultipartEC(db, cfg, ch, req, ec)
	}
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

// repackPass rewrites tapes whose dead (deleted-object) share meets
// TAPEBERG_REPACK_THRESHOLD (default 50%): every live file is read
// from the source tape (sha-verified against the catalog), written to
// another tape in the pool, and its placement row moved; the emptied
// source is marked 'retired' (pickTape ignores it — erase and re-add
// to reuse the media). Any read/verify failure aborts that tape's
// repack and leaves it untouched — a suspect tape is never freed.
func repackPass(db *sql.DB, cfg Config, ch Changer) error {
	if cfg.RepackThreshold <= 0 {
		return nil
	}
	cands, err := repackCandidates(db, cfg.Pool, cfg.RepackThreshold)
	if err != nil {
		return err
	}
	for _, src := range cands {
		files, err := liveTapeFiles(db, src.ID)
		if err != nil {
			return err
		}
		if len(files) == 0 {
			if err := markTapeRetired(db, src.ID); err != nil {
				return err
			}
			fmt.Printf("worker: repack — %s is 100%% dead (%d bytes); retired\n", src.Barcode, src.Written)
			continue
		}
		fmt.Printf("worker: repack — %s is %d%% dead (%d of %d bytes); moving %d live file(s)\n",
			src.Barcode, src.Dead*100/src.Written, src.Dead, src.Written, len(files))
		if err := ch.Load(src.Barcode); err != nil {
			fmt.Printf("worker: repack — cannot load %s: %v (left as-is)\n", src.Barcode, err)
			continue
		}
		aborted := false
		for _, f := range files {
			tmp := filepath.Join(cfg.BufferDir, fmt.Sprintf("repack-%d", f.TapeFileID))
			sha, rerr := readTapeFile(cfg.Drive, f.PhysicalFile, tmp)
			if rerr != nil {
				fmt.Printf("worker: repack — %s file %d read failed: %v (tape left as-is)\n", src.Barcode, f.PhysicalFile, rerr)
				aborted = true
				break
			}
			if f.Sha256 != "" && sha != f.Sha256 {
				fmt.Printf("worker: repack — %s file %d checksum mismatch: got %s, expected %s (tape SUSPECT, left as-is)\n",
					src.Barcode, f.PhysicalFile, sha, f.Sha256)
				aborted = true
				break
			}
			if err := repackOneFile(db, cfg, ch, src.ID, &f, tmp); err != nil {
				fmt.Printf("worker: repack — %s file %d: %v (tape left as-is)\n", src.Barcode, f.PhysicalFile, err)
				aborted = true
				break
			}
			_ = os.Remove(tmp)
		}
		if aborted {
			_ = ch.Unload()
			continue
		}
		if err := markTapeRetired(db, src.ID); err != nil {
			return err
		}
		if err := ch.Unload(); err != nil {
			return err
		}
		fmt.Printf("worker: repack — %s retired (%d bytes reclaimed; erase + re-add to reuse)\n", src.Barcode, src.Dead)
	}
	return nil
}

// repackOneFile re-archives one live file onto another tape and moves
// its placement row. RAIT shards pick targets through pickShardTarget
// so a repacked shard never lands on a sibling shard's tape.
func repackOneFile(db *sql.DB, cfg Config, ch Changer, srcTape int64, f *LiveTapeFile, tmp string) error {
	for try := 0; try < 8; try++ {
		var tape *Tape
		var err error
		if f.ShardNumber >= 0 {
			tape, err = pickShardTarget(db, cfg.Pool, f.Size, []int64{srcTape}, f.ObjectID, f.PartNumber)
		} else {
			tape, err = pickTapeExcluding(db, cfg.Pool, f.Size, srcTape)
		}
		if err != nil {
			return err
		}
		if err := ch.Load(tape.Barcode); err != nil {
			return err
		}
		archiveID, err := nextArchiveID(db, tape.ID)
		if err != nil {
			return err
		}
		physical, werr := writeTapeFile(cfg.Drive, tmp)
		if werr == ErrTapeFull {
			if err := markTapeFull(db, tape.ID); err != nil {
				return err
			}
			continue // target filled mid-write; overwrite the partial next round
		}
		if werr != nil {
			return werr
		}
		if err := moveTapeFile(db, f.TapeFileID, tape.ID, f.ObjectID, f.PartNumber, f.ShardNumber, archiveID, physical, f.Size); err != nil {
			return err
		}
		if err := addTapeWritten(db, tape.ID, f.Size); err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("ran out of target tapes")
}


// ---------- erasure coding (RAIT) ----------

// ecDrives returns the drives available for parallel shard I/O.
func ecDrives(cfg Config) []string {
	if len(cfg.Drives) > 0 {
		return cfg.Drives
	}
	return []string{cfg.Drive}
}

// ecArchive stripes one buffered object/part across k+m distinct
// tapes (one shard each). On a multi-drive robot the shard tapes are
// loaded into different DTEs and written in parallel — one stream per
// drive, RAIT-style; a single drive (or manual changer) falls back to
// sequential loads. EOM on a target marks that tape full and retries
// the shard on a fresh one. Catalog rows land per shard as it
// completes, so pickShardTarget's sibling exclusion stays honest.
func ecArchive(db *sql.DB, cfg Config, ch Changer, req *ArchiveReq, ec *ECConfig) error {
	shards, err := ecEncodeFile(req.DiskPath, ec.K, ec.M, cfg.BufferDir,
		fmt.Sprintf("ec-%d-%d", req.ObjectID, req.PartNumber))
	if err != nil {
		return failArchive(db, req.ID, "ec encode: "+err.Error())
	}
	defer func() {
		for _, s := range shards {
			_ = os.Remove(s.Path)
		}
	}()

	drives := ecDrives(cfg)
	multi, haveMulti := ch.(MultiDriveChanger)
	parallel := haveMulti && len(drives) > 1
	// Never strand tapes in drives on an error path.
	usedDTEs := 0
	defer func() {
		for d := 0; d < usedDTEs; d++ {
			_ = multi.UnloadDTE(d)
		}
	}()

	// One target tape per shard, all distinct, sibling-aware.
	targets := make([]*Tape, len(shards))
	used := []int64{}
	for i := range shards {
		t, err := pickShardTarget(db, cfg.Pool, shards[i].Size, used, req.ObjectID, int64(req.PartNumber))
		if err != nil {
			return failArchive(db, req.ID, fmt.Sprintf("ec stripe: %v", err))
		}
		used = append(used, t.ID)
		targets[i] = t
	}

	done := make([]bool, len(shards))
	for round := 0; round < 16; round++ {
		var batch []int
		for i := range shards {
			if !done[i] {
				batch = append(batch, i)
			}
		}
		if len(batch) == 0 {
			break
		}
		if parallel && len(batch) > len(drives) {
			batch = batch[:len(drives)]
		}

		// Load this batch: one shard tape per drive.
		for j, i := range batch {
			if parallel {
				if err := multi.LoadInto(targets[i].Barcode, j); err != nil {
					return failArchive(db, req.ID, "ec load "+targets[i].Barcode+": "+err.Error())
				}
				if j >= usedDTEs {
					usedDTEs = j + 1
				}
			} else if err := ch.Load(targets[i].Barcode); err != nil {
				return failArchive(db, req.ID, "ec load "+targets[i].Barcode+": "+err.Error())
			}
		}

		// Write the shards, in parallel when each has its own drive.
		type result struct {
			physical int64
			err      error
		}
		results := make([]result, len(batch))
		if parallel {
			var wg sync.WaitGroup
			for j, i := range batch {
				wg.Add(1)
				go func(j, i int) {
					defer wg.Done()
					p, werr := writeTapeFile(drives[j], shards[i].Path)
					results[j] = result{physical: p, err: werr}
				}(j, i)
			}
			wg.Wait()
		} else {
			for j, i := range batch {
				p, werr := writeTapeFile(cfg.Drive, shards[i].Path)
				results[j] = result{physical: p, err: werr}
			}
		}

		// Unload before cataloging/retrying.
		if parallel {
			for j := range batch {
				_ = multi.UnloadDTE(j)
			}
		} else {
			_ = ch.Unload()
		}

		for j, i := range batch {
			switch results[j].err {
			case nil:
				archiveID, err := nextArchiveID(db, targets[i].ID)
				if err != nil {
					return err
				}
				if err := insertShardTapeFile(db, targets[i].ID, req.ObjectID,
					int64(req.PartNumber), int64(i), archiveID, results[j].physical, shards[i].Size); err != nil {
					return err
				}
				if err := insertShardVerifier(db, req.ObjectID, int64(req.PartNumber),
					int64(i), shards[i].Size, shards[i].SHA256); err != nil {
					return err
				}
				if err := addTapeWritten(db, targets[i].ID, shards[i].Size); err != nil {
					return err
				}
				done[i] = true
			case ErrTapeFull:
				_ = markTapeFull(db, targets[i].ID)
				fmt.Printf("worker: ec — %s hit EOM mid-shard; retrying elsewhere\n", targets[i].Barcode)
				nt, err := pickShardTarget(db, cfg.Pool, shards[i].Size, used, req.ObjectID, int64(req.PartNumber))
				if err != nil {
					return failArchive(db, req.ID, fmt.Sprintf("ec stripe: %v", err))
				}
				used = append(used, nt.ID)
				targets[i] = nt
			default:
				return failArchive(db, req.ID, fmt.Sprintf("ec write %s: %v", targets[i].Barcode, results[j].err))
			}
		}
	}
	for i := range done {
		if !done[i] {
			return failArchive(db, req.ID, "ec stripe: ran out of tapes for shards")
		}
	}

	// Freeze the policy on the object, then the usual archive tail.
	if err := setObjectEC(db, req.ObjectID, ec.K, ec.M); err != nil {
		return err
	}
	if err := markArchiveWritten(db, req.ID); err != nil {
		return err
	}
	if err := setPartDiskPath(db, req.ObjectID, req.PartNumber, ""); err != nil {
		return err
	}
	if pend, err := objectPendingParts(db, req.ObjectID); err != nil {
		return err
	} else if pend == 0 {
		if err := objectToGlacier(db, req.ObjectID); err != nil {
			return err
		}
	}
	_ = os.Remove(req.DiskPath)
	tapes := make([]string, 0, len(shards))
	for i, t := range targets {
		if i == 0 || t.Barcode != targets[i-1].Barcode {
			tapes = append(tapes, t.Barcode)
		}
	}
	if mp, _ := isMultipart(db, req.ObjectID); mp {
		fmt.Printf("worker: %s part %d → RAIT %d+%d across %s\n",
			req.Key, req.PartNumber, ec.K, ec.M, strings.Join(tapes, "+"))
	} else {
		fmt.Printf("worker: %s → RAIT %d+%d across %s\n", req.Key, ec.K, ec.M, strings.Join(tapes, "+"))
	}
	return nil
}

// ecRestorePart rebuilds one part of an EC object: it reads k of the
// surviving shard placements (parallel across drives when possible,
// each shard sha-verified against the catalog), reconstructs missing
// data shards from parity, and writes the exact original bytes to dst
// (wantSHA is the part's — or single object's — verifier).
func ecRestorePart(db *sql.DB, cfg Config, ch Changer, objectID, partNumber int64, ec *ECConfig, size int64, wantSHA, dst string) error {
	placements, err := shardPlacements(db, objectID, partNumber)
	if err != nil {
		return err
	}
	if len(placements) < ec.K {
		return fmt.Errorf("ec: only %d of %d shards placed (need %d)", len(placements), ec.K+ec.M, ec.K)
	}

	// Read plan: every surviving DATA shard, topped up with parity
	// shards until k readable shards are in hand.
	plan := make([]*ShardPlacement, 0, ec.K)
	for i := range placements { // data shards first (shard order)
		if placements[i].ShardNumber < int64(ec.K) {
			plan = append(plan, &placements[i])
		}
	}
	for i := range placements { // then parity as needed
		if len(plan) >= ec.K {
			break
		}
		if placements[i].ShardNumber >= int64(ec.K) {
			plan = append(plan, &placements[i])
		}
	}
	if len(plan) < ec.K {
		return fmt.Errorf("ec: only %d readable shards (need %d)", len(plan), ec.K)
	}

	// Read the plan sequentially — load, read, verify, unload. (The
	// WRITE side stripes in parallel across drives; reads are rarer
	// and this keeps the degraded path simple and obviously correct.)
	paths := make([]string, ec.K+ec.M)
	for i := range paths {
		paths[i] = ""
	}
	for _, p := range plan {
		tmp := fmt.Sprintf("%s/ecshard-%d-%d-%d", cfg.BufferDir, objectID, partNumber, p.ShardNumber)
		if err := ch.Load(p.Barcode); err != nil {
			return err
		}
		sha, rerr := readTapeFile(cfg.Drive, p.PhysicalFile, tmp)
		if rerr != nil {
			_ = os.Remove(tmp)
			_ = ch.Unload()
			return fmt.Errorf("ec shard %d read from %s: %w", p.ShardNumber, p.Barcode, rerr)
		}
		if sha != p.SHA256 {
			_ = os.Remove(tmp)
			_ = ch.Unload()
			return fmt.Errorf("ec shard %d checksum mismatch: %s gave %s, expected %s",
				p.ShardNumber, p.Barcode, sha, p.SHA256)
		}
		paths[p.ShardNumber] = tmp
	}
	_ = ch.Unload()
	defer func() {
		for _, p := range paths {
			if p != "" {
				_ = os.Remove(p)
			}
		}
	}()

	// Decode: reconstruct missing data shards from parity, join to the
	// exact size, verify against the part's catalog sha.
	temps, derr := ecDecodeFile(paths, ec.K, ec.M, size, dst)
	defer func() {
		for _, t := range temps {
			_ = os.Remove(t)
		}
	}()
	if derr != nil {
		return derr
	}
	if wantSHA != "" {
		sha, err := sha256File(dst)
		if err != nil {
			return err
		}
		if sha != wantSHA {
			return fmt.Errorf("ec decoded bytes differ: got %s, expected %s", sha, wantSHA)
		}
	}
	return nil
}

// restoreMultipartEC rebuilds every part of an EC multipart object;
// each part is decoded from its own k+m shard stripe.
func restoreMultipartEC(db *sql.DB, cfg Config, ch Changer, req *RetrieveReq, ec *ECConfig) error {
	parts, err := listParts(db, req.ObjectID)
	if err != nil {
		return err
	}
	if len(parts) == 0 {
		_ = failRetrieve(db, req.ID, "multipart object has no parts (upload incomplete?)")
		return nil
	}
	if err := setRetrieveState(db, req.ID, "restoring"); err != nil {
		return err
	}
	for _, p := range parts {
		// A part may already be on disk (still buffered or pinned).
		if p.DiskPath != "" {
			if _, err := os.Stat(p.DiskPath); err == nil {
				continue
			}
		}
		dst := filepath.Join(cfg.BufferDir, fmt.Sprintf("part-%d-%05d", req.ObjectID, p.Number))
		if err := ecRestorePart(db, cfg, ch, req.ObjectID, int64(p.Number), ec, p.Size, p.SHA256, dst); err != nil {
			_ = os.Remove(dst)
			_ = failRetrieve(db, req.ID, fmt.Sprintf("part %d: %v", p.Number, err))
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
	fmt.Printf("worker: restored %s (%d RAIT %d+%d parts, pinned %d days)\n",
		req.Key, len(parts), ec.K, ec.M, req.PinDays)
	return nil
}
