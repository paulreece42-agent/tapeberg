package main

// Tape administration: list, register into a pool, move between pools,
// change state, and reuse a retired cartridge. A tape's pool decides
// which worker (= which library) may load it, so every change here is
// checked against what is on the tape and what is in flight.

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/lib/pq"
)

type TapeInfo struct {
	Barcode, Pool, State   string
	Cleaning               bool
	CapacityBytes, Written int64
	LiveFiles              int
	LiveBytes              int64
}

func listTapes(db *sql.DB, pool, state string) ([]TapeInfo, error) {
	rows, err := db.Query(`
		SELECT t.barcode, p.name, t.state, t.is_cleaning, t.capacity_bytes, t.written_bytes,
		       count(tf.id) FILTER (WHERE o.deleted_at IS NULL),
		       COALESCE(sum(tf.size_bytes) FILTER (WHERE o.deleted_at IS NULL), 0)
		FROM tapes t
		JOIN tape_pools p ON p.id = t.pool_id
		LEFT JOIN tape_files tf ON tf.tape_id = t.id
		LEFT JOIN objects o ON o.id = tf.object_id
		WHERE ($1 = '' OR p.name = $1) AND ($2 = '' OR t.state = $2)
		GROUP BY t.id, p.name ORDER BY p.name, t.barcode`, pool, state)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TapeInfo
	for rows.Next() {
		var t TapeInfo
		if err := rows.Scan(&t.Barcode, &t.Pool, &t.State, &t.Cleaning, &t.CapacityBytes, &t.Written,
			&t.LiveFiles, &t.LiveBytes); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// addTape registers a cartridge in an existing pool.
func addTape(db *sql.DB, barcode, pool string, capacity int64) error {
	if ok, err := poolExists(db, pool); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("pool %q does not exist ('tapeberg pool ls'; create it with 'tapeberg pool add')", pool)
	}
	res, err := db.Exec(`
		INSERT INTO tapes (barcode, pool_id, is_cleaning, capacity_bytes)
		VALUES ($1, (SELECT id FROM tape_pools WHERE name = $2), $3, $4)
		ON CONFLICT (barcode) DO NOTHING`,
		barcode, pool, strings.HasPrefix(strings.ToUpper(barcode), "CLN"), capacity)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		var st string
		_ = db.QueryRow(`SELECT state FROM tapes WHERE barcode = $1`, barcode).Scan(&st)
		if st == "retired" {
			return fmt.Errorf("tape %s is already registered (retired) — to put it back in service: 'tapeberg tape reuse %s'", barcode, barcode)
		}
		return fmt.Errorf("tape %s already registered", barcode)
	}
	return nil
}

// tapeInFlight reports work that pins a tape where it is: an archive
// assigned to it, or a restore past 'pending' reading an object on it.
// (Pending restores are fine — the worker re-routes them by placement.)
func tapeInFlight(tx *sql.Tx, tapeID int64) (string, error) {
	var archives, restores int
	err := tx.QueryRow(`
		SELECT (SELECT count(*) FROM archive_requests WHERE tape_id = $1 AND state = 'assigned'),
		       (SELECT count(*) FROM retrieve_requests r
		         WHERE r.state IN ('waiting_for_media', 'restoring')
		           AND EXISTS (SELECT 1 FROM tape_files tf WHERE tf.tape_id = $1 AND tf.object_id = r.object_id))`,
		tapeID).Scan(&archives, &restores)
	if err != nil {
		return "", err
	}
	var parts []string
	if archives > 0 {
		parts = append(parts, fmt.Sprintf("%d archive(s) being written to it", archives))
	}
	if restores > 0 {
		parts = append(parts, fmt.Sprintf("%d restore(s) reading from it", restores))
	}
	return strings.Join(parts, ", "), nil
}

// moveTapes puts a set of tapes into another pool, atomically. The set
// matters: an object's bytes must stay within one pool (restores are
// served by one library), so RAIT shard sets or multipart objects that
// span several tapes have to move together.
func moveTapes(db *sql.DB, barcodes []string, pool string) (liveFiles int, err error) {
	tx, err := db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var poolID int64
	err = tx.QueryRow(`SELECT id FROM tape_pools WHERE name = $1`, pool).Scan(&poolID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("pool %q does not exist ('tapeberg pool ls'; create it with 'tapeberg pool add')", pool)
	}
	if err != nil {
		return 0, err
	}
	var ids []int64
	for _, bc := range barcodes {
		var id, cur int64
		err := tx.QueryRow(`SELECT id, pool_id FROM tapes WHERE barcode = $1 FOR UPDATE`, bc).Scan(&id, &cur)
		if errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("tape %s: %w", bc, ErrNotFound)
		}
		if err != nil {
			return 0, err
		}
		if cur == poolID {
			return 0, fmt.Errorf("tape %s is already in pool %s", bc, pool)
		}
		if busy, err := tapeInFlight(tx, id); err != nil {
			return 0, err
		} else if busy != "" {
			return 0, fmt.Errorf("tape %s is in use (%s); retry when the worker is done", bc, busy)
		}
		ids = append(ids, id)
	}
	if _, err := tx.Exec(`UPDATE tapes SET pool_id = $1, updated_at = now() WHERE id = ANY($2)`, poolID, pq.Array(ids)); err != nil {
		return 0, err
	}
	// After the move, no live object on these tapes may have bytes in
	// another pool.
	rows, err := tx.Query(`
		SELECT o.key, string_agg(DISTINCT p.name, ',' ORDER BY p.name)
		FROM tape_files tf
		JOIN objects o ON o.id = tf.object_id AND o.deleted_at IS NULL
		JOIN tape_files tf2 ON tf2.object_id = o.id
		JOIN tapes t2 ON t2.id = tf2.tape_id
		JOIN tape_pools p ON p.id = t2.pool_id
		WHERE tf.tape_id = ANY($1)
		GROUP BY o.id, o.key
		HAVING count(DISTINCT t2.pool_id) > 1
		ORDER BY o.key LIMIT 5`, pq.Array(ids))
	if err != nil {
		return 0, err
	}
	var split []string
	for rows.Next() {
		var key, pools string
		if err := rows.Scan(&key, &pools); err != nil {
			rows.Close()
			return 0, err
		}
		split = append(split, fmt.Sprintf("%s (%s)", key, pools))
	}
	rows.Close()
	if len(split) > 0 {
		return 0, fmt.Errorf("moving only these tapes would split objects across pools: %s — move every tape holding them together ('tapeberg tape mv A B C --to %s')",
			strings.Join(split, ", "), pool)
	}
	if err := tx.QueryRow(`
		SELECT count(*) FROM tape_files tf JOIN objects o ON o.id = tf.object_id
		WHERE tf.tape_id = ANY($1) AND o.deleted_at IS NULL`, pq.Array(ids)).Scan(&liveFiles); err != nil {
		return 0, err
	}
	return liveFiles, tx.Commit()
}

// setTapeState moves a tape between active, full and retired. Retiring
// takes a tape out of service for good, so it must hold no live data
// (repack empties and retires tapes on its own; this is for media that
// should never be written or read again).
func setTapeState(db *sql.DB, barcode, state string) error {
	if state != "active" && state != "full" && state != "retired" {
		return fmt.Errorf("state must be active, full or retired")
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var id int64
	var cur string
	err = tx.QueryRow(`SELECT id, state FROM tapes WHERE barcode = $1 FOR UPDATE`, barcode).Scan(&id, &cur)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("tape %s: %w", barcode, ErrNotFound)
	}
	if err != nil {
		return err
	}
	if cur == state {
		return fmt.Errorf("tape %s is already %s", barcode, state)
	}
	if cur == "lost" {
		return fmt.Errorf("tape %s is marked lost; not changing it from the CLI", barcode)
	}
	if cur == "retired" {
		return fmt.Errorf("tape %s is retired; to put it back in service use 'tapeberg tape reuse %s'", barcode, barcode)
	}
	if busy, err := tapeInFlight(tx, id); err != nil {
		return err
	} else if busy != "" {
		return fmt.Errorf("tape %s is in use (%s); retry when the worker is done", barcode, busy)
	}
	if state == "retired" {
		var live int
		if err := tx.QueryRow(`SELECT count(*) FROM tape_files tf JOIN objects o ON o.id = tf.object_id
			WHERE tf.tape_id = $1 AND o.deleted_at IS NULL`, id).Scan(&live); err != nil {
			return err
		}
		if live > 0 {
			return fmt.Errorf("tape %s still holds %d live file(s); retiring it would strand them (repack retires tapes once their data is moved)", barcode, live)
		}
	}
	if _, err := tx.Exec(`UPDATE tapes SET state = $2, updated_at = now() WHERE id = $1`, id, state); err != nil {
		return err
	}
	return tx.Commit()
}

// reuseTape returns a retired, fully dead cartridge to service: its
// stale placement rows (all of deleted objects) are dropped and it
// becomes an empty active tape. Writes position by the drive's own EOD
// report, so an un-erased tape is still written and read correctly — it
// just keeps the dead bytes (unreadable through tapeberg, but physically
// there) and loses that capacity until erased.
func reuseTape(db *sql.DB, barcode string) (dropped int64, err error) {
	tx, err := db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var id int64
	var state string
	err = tx.QueryRow(`SELECT id, state FROM tapes WHERE barcode = $1 FOR UPDATE`, barcode).Scan(&id, &state)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("tape %s: %w", barcode, ErrNotFound)
	}
	if err != nil {
		return 0, err
	}
	if state != "retired" {
		return 0, fmt.Errorf("tape %s is %s, not retired; only retired tapes are reused", barcode, state)
	}
	var live int
	if err := tx.QueryRow(`SELECT count(*) FROM tape_files tf JOIN objects o ON o.id = tf.object_id
		WHERE tf.tape_id = $1 AND o.deleted_at IS NULL`, id).Scan(&live); err != nil {
		return 0, err
	}
	if live > 0 {
		return 0, fmt.Errorf("tape %s still holds %d live file(s) — refusing to drop their placement", barcode, live)
	}
	res, err := tx.Exec(`DELETE FROM tape_files WHERE tape_id = $1`, id)
	if err != nil {
		return 0, err
	}
	dropped, _ = res.RowsAffected()
	if _, err := tx.Exec(`UPDATE tapes SET state = 'active', written_bytes = 0, updated_at = now() WHERE id = $1`, id); err != nil {
		return 0, err
	}
	return dropped, tx.Commit()
}
