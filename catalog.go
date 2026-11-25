// Catalog: the PostgreSQL layer. The catalog owns STATE; the tape owns BYTES.
// The worker owns all state transitions; the CLI only INSERTs and SELECTs.
package main

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
)

// ErrNotFound is returned when a catalog row does not exist.
var ErrNotFound = errors.New("not found")

func openDB(url string) (*sql.DB, error) {
	db, err := sql.Open("postgres", url)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(2)
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("catalog unreachable: %w", err)
	}
	return db, nil
}

type Object struct {
	ID           int64
	Key          string
	Size         int64
	SHA256       string
	StorageClass string
	DiskPath     string // "" when tape-only
	CreatedAt    time.Time
}

func bucketID(db *sql.DB) (int64, error) { return bucketIDByName(db, "tapeberg") }

func bucketIDByName(db *sql.DB, name string) (int64, error) {
	var id int64
	err := db.QueryRow(`SELECT id FROM buckets WHERE name = $1`, name).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("bucket %q: %w", name, ErrNotFound)
	}
	return id, err
}

func createBucketIfAbsent(db *sql.DB, name string) error {
	_, err := db.Exec(`INSERT INTO buckets (name) VALUES ($1) ON CONFLICT (name) DO NOTHING`, name)
	return err
}

type BucketRow struct {
	Name    string
	Created time.Time
}

func listBuckets(db *sql.DB) ([]BucketRow, error) {
	rows, err := db.Query(`SELECT name, created_at FROM buckets ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BucketRow
	for rows.Next() {
		var b BucketRow
		if err := rows.Scan(&b.Name, &b.Created); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func listObjects(db *sql.DB, bucket, prefix string) ([]Object, error) {
	rows, err := db.Query(`
		SELECT o.id, o.key, o.size_bytes, COALESCE(o.sha256,''), o.storage_class, COALESCE(o.disk_path,''), o.created_at
		FROM objects o JOIN buckets b ON b.id = o.bucket_id
		WHERE b.name = $1 AND ($2 = '' OR o.key LIKE $2 || '%')
		ORDER BY o.key`, bucket, prefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Object
	for rows.Next() {
		var o Object
		if err := rows.Scan(&o.ID, &o.Key, &o.Size, &o.SHA256, &o.StorageClass, &o.DiskPath, &o.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

func getObjectIn(db *sql.DB, bucket, key string) (*Object, error) {
	bid, err := bucketIDByName(db, bucket)
	if err != nil {
		return nil, err
	}
	o := &Object{Key: key}
	var sha, path sql.NullString
	err = db.QueryRow(`
		SELECT id, size_bytes, sha256, storage_class, disk_path, created_at
		FROM objects WHERE bucket_id = $1 AND key = $2`, bid, key).
		Scan(&o.ID, &o.Size, &sha, &o.StorageClass, &path, &o.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("object %q: %w", key, ErrNotFound)
	}
	if err != nil {
		return nil, err
	}
	o.SHA256, o.DiskPath = sha.String, path.String
	return o, nil
}

// activeRetrieve reports the newest non-terminal restore request for an
// object, so HEAD/GET can surface x-amz-restore state.
func activeRetrieve(db *sql.DB, objectID int64) (state string, expires time.Time, found bool, err error) {
	var exp sql.NullTime
	err = db.QueryRow(`
		SELECT state, expires_at FROM retrieve_requests
		WHERE object_id = $1 AND state IN ('pending','waiting_for_media','restoring','restored')
		ORDER BY id DESC LIMIT 1`, objectID).Scan(&state, &exp)
	if errors.Is(err, sql.ErrNoRows) {
		return "", time.Time{}, false, nil
	}
	if err != nil {
		return "", time.Time{}, false, err
	}
	if exp.Valid {
		expires = exp.Time
	}
	return state, expires, true, nil
}

// insertObject records an already-staged disk copy as a STANDARD object.
func insertObject(db *sql.DB, bucket, key string, size int64, sha256hex, diskPath string) (*Object, error) {
	bid, err := bucketIDByName(db, bucket)
	if err != nil {
		return nil, err
	}
	o := &Object{Key: key, Size: size, SHA256: sha256hex, StorageClass: "STANDARD", DiskPath: diskPath}
	err = db.QueryRow(`
		INSERT INTO objects (bucket_id, key, size_bytes, sha256, storage_class, disk_path)
		VALUES ($1, $2, $3, $4, 'STANDARD', $5) RETURNING id`,
		bid, key, size, sha256hex, diskPath).Scan(&o.ID)
	if isUniqueViolation(err) {
		return nil, fmt.Errorf("object %q already exists (v0 has no overwrite)", key)
	}
	if err != nil {
		return nil, err
	}
	return o, nil
}

func isUniqueViolation(err error) bool {
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && pqErr.Code == "23505"
}

// ---- archive queue ----

func createArchiveRequest(db *sql.DB, objectID int64, pool string) error {
	_, err := db.Exec(`INSERT INTO archive_requests (object_id, pool) VALUES ($1, $2)`, objectID, pool)
	return err
}

type ArchiveReq struct {
	ID       int64
	ObjectID int64
	Key      string
	Size     int64
	DiskPath string
}

func pendingArchives(db *sql.DB, pool string) ([]ArchiveReq, error) {
	rows, err := db.Query(`
		SELECT r.id, r.object_id, o.key, o.size_bytes, o.disk_path
		FROM archive_requests r JOIN objects o ON o.id = r.object_id
		WHERE r.state = 'pending' AND r.pool = $1 ORDER BY r.requested_at`, pool)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ArchiveReq
	for rows.Next() {
		var r ArchiveReq
		var path sql.NullString
		if err := rows.Scan(&r.ID, &r.ObjectID, &r.Key, &r.Size, &path); err != nil {
			return nil, err
		}
		r.DiskPath = path.String
		out = append(out, r)
	}
	return out, rows.Err()
}

type Tape struct {
	ID            int64
	Barcode       string
	CapacityBytes int64
	WrittenBytes  int64
}

// pickTape chooses, from the configured pool, the most-written active
// tape that nominally still fits the object (fills tapes in sequence,
// CTA-style); ties break to the lowest tape id so the fill order is
// deterministic; falls back to the least-written one and lets the
// drive's EOM be the judge. Pool isolation keeps one rig's tapes (e.g.
// an mhvtl dev library) invisible to another's worker.
func pickTape(db *sql.DB, pool string, size int64) (*Tape, error) {
	q := `SELECT t.id, t.barcode, t.capacity_bytes, t.written_bytes
		FROM tapes t JOIN tape_pools p ON p.id = t.pool_id
		WHERE p.name = $2 AND t.state = 'active' AND NOT t.is_cleaning
		  AND t.written_bytes + $1 <= t.capacity_bytes
		ORDER BY t.written_bytes %s, t.id ASC LIMIT 1`
	t := &Tape{}
	err := db.QueryRow(fmt.Sprintf(q, "DESC"), size, pool).
		Scan(&t.ID, &t.Barcode, &t.CapacityBytes, &t.WrittenBytes)
	if errors.Is(err, sql.ErrNoRows) {
		err = db.QueryRow(fmt.Sprintf(q, "ASC"), size, pool).
			Scan(&t.ID, &t.Barcode, &t.CapacityBytes, &t.WrittenBytes)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("no active tape left: %w", ErrNotFound)
	}
	if err != nil {
		return nil, err
	}
	return t, nil
}

func setArchiveAssigned(db *sql.DB, reqID, tapeID int64) error {
	_, err := db.Exec(`UPDATE archive_requests SET state='assigned', tape_id=$2, updated_at=now() WHERE id=$1`, reqID, tapeID)
	return err
}

func setArchivePending(db *sql.DB, reqID int64) error {
	_, err := db.Exec(`UPDATE archive_requests SET state='pending', tape_id=NULL, updated_at=now() WHERE id=$1`, reqID)
	return err
}

func markArchiveWritten(db *sql.DB, reqID int64) error {
	_, err := db.Exec(`UPDATE archive_requests SET state='written', written_at=now(), updated_at=now() WHERE id=$1`, reqID)
	return err
}

func failArchive(db *sql.DB, reqID int64, msg string) error {
	_, err := db.Exec(`UPDATE archive_requests SET state='failed', error=$2, updated_at=now() WHERE id=$1`, reqID, msg)
	return err
}

func markTapeFull(db *sql.DB, tapeID int64) error {
	_, err := db.Exec(`UPDATE tapes SET state='full', updated_at=now() WHERE id=$1`, tapeID)
	return err
}

func addTapeWritten(db *sql.DB, tapeID, n int64) error {
	_, err := db.Exec(`UPDATE tapes SET written_bytes = written_bytes + $2, updated_at=now() WHERE id=$1`, tapeID, n)
	return err
}

func nextArchiveID(db *sql.DB, tapeID int64) (int64, error) {
	var id int64
	err := db.QueryRow(`SELECT COALESCE(MAX(archive_id), 0) + 1 FROM tape_files WHERE tape_id = $1`, tapeID).Scan(&id)
	return id, err
}

func insertTapeFile(db *sql.DB, tapeID, objectID, archiveID, physicalFile, size int64) error {
	_, err := db.Exec(`INSERT INTO tape_files (tape_id, object_id, archive_id, physical_file, size_bytes) VALUES ($1,$2,$3,$4,$5)`,
		tapeID, objectID, archiveID, physicalFile, size)
	return err
}

func objectToGlacier(db *sql.DB, objectID int64) error {
	_, err := db.Exec(`UPDATE objects SET storage_class='GLACIER', disk_path=NULL WHERE id=$1`, objectID)
	return err
}

func objectToStandard(db *sql.DB, objectID int64, path string) error {
	_, err := db.Exec(`UPDATE objects SET storage_class='STANDARD', disk_path=$2 WHERE id=$1`, objectID, path)
	return err
}

// ---- restore queue ----

func createRetrieveRequest(db *sql.DB, objectID int64, days int, pool string) error {
	_, err := db.Exec(`INSERT INTO retrieve_requests (object_id, pin_days, pool) VALUES ($1, $2, $3)`, objectID, days, pool)
	return err
}

type RetrieveReq struct {
	ID       int64
	ObjectID int64
	Key      string
	SHA256   string
	PinDays  int
}

func pendingRetrieves(db *sql.DB, pool string) ([]RetrieveReq, error) {
	rows, err := db.Query(`
		SELECT r.id, r.object_id, o.key, COALESCE(o.sha256, ''), r.pin_days
		FROM retrieve_requests r JOIN objects o ON o.id = r.object_id
		WHERE r.state = 'pending' AND r.pool = $1 ORDER BY r.requested_at`, pool)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RetrieveReq
	for rows.Next() {
		var r RetrieveReq
		if err := rows.Scan(&r.ID, &r.ObjectID, &r.Key, &r.SHA256, &r.PinDays); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

type TapeFileLoc struct {
	TapeID       int64
	Barcode      string
	ArchiveID    int64
	PhysicalFile int64
}

func objectTapeFile(db *sql.DB, objectID int64) (*TapeFileLoc, error) {
	t := &TapeFileLoc{}
	err := db.QueryRow(`
		SELECT tf.tape_id, t.barcode, tf.archive_id, tf.physical_file
		FROM tape_files tf JOIN tapes t ON t.id = tf.tape_id
		WHERE tf.object_id = $1 ORDER BY tf.written_at DESC LIMIT 1`, objectID).
		Scan(&t.TapeID, &t.Barcode, &t.ArchiveID, &t.PhysicalFile)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("object %d has no tape placement: %w", objectID, ErrNotFound)
	}
	if err != nil {
		return nil, err
	}
	return t, nil
}

func setRetrieveState(db *sql.DB, reqID int64, state string) error {
	_, err := db.Exec(`UPDATE retrieve_requests SET state=$2, updated_at=now() WHERE id=$1`, reqID, state)
	return err
}

func failRetrieve(db *sql.DB, reqID int64, msg string) error {
	_, err := db.Exec(`UPDATE retrieve_requests SET state='failed', error=$2, updated_at=now() WHERE id=$1`, reqID, msg)
	return err
}

func restoreComplete(db *sql.DB, reqID int64, expires time.Time) error {
	_, err := db.Exec(`
		UPDATE retrieve_requests
		SET state='restored', restored_at=now(), expires_at=$2, updated_at=now()
		WHERE id=$1`, reqID, expires)
	return err
}

// ---- pin expiry sweep ----

type ExpiredRow struct {
	ReqID    int64
	ObjectID int64
	DiskPath string
}

func expiredRestores(db *sql.DB) ([]ExpiredRow, error) {
	rows, err := db.Query(`
		SELECT r.id, r.object_id, o.disk_path
		FROM retrieve_requests r JOIN objects o ON o.id = r.object_id
		WHERE r.state = 'restored' AND r.expires_at < now() AND o.disk_path IS NOT NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ExpiredRow
	for rows.Next() {
		var e ExpiredRow
		if err := rows.Scan(&e.ReqID, &e.ObjectID, &e.DiskPath); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func markRetrieveExpired(db *sql.DB, reqID int64) error {
	_, err := db.Exec(`UPDATE retrieve_requests SET state='expired', updated_at=now() WHERE id=$1`, reqID)
	return err
}

// ---- status ----

func printStatus(db *sql.DB) error {
	report := func(title, q string) error {
		rows, err := db.Query(q)
		if err != nil {
			return err
		}
		defer rows.Close()
		fmt.Println(title)
		for rows.Next() {
			var a string
			var b int64
			if err := rows.Scan(&a, &b); err != nil {
				return err
			}
			fmt.Printf("  %-16s %d\n", a+":", b)
		}
		return rows.Err()
	}
	if err := report("objects by storage class:", `SELECT storage_class, count(*) FROM objects GROUP BY 1 ORDER BY 1`); err != nil {
		return err
	}
	if err := report("tapes by state:", `SELECT state, count(*) FROM tapes GROUP BY 1 ORDER BY 1`); err != nil {
		return err
	}
	if err := report("archive requests:", `SELECT state, count(*) FROM archive_requests GROUP BY 1 ORDER BY 1`); err != nil {
		return err
	}
	if err := report("retrieve requests:", `SELECT state, count(*) FROM retrieve_requests GROUP BY 1 ORDER BY 1`); err != nil {
		return err
	}
	fmt.Println("tape fill:")
	rows, err := db.Query(`
		SELECT t.barcode, t.state, t.written_bytes, t.capacity_bytes, count(tf.id)
		FROM tapes t LEFT JOIN tape_files tf ON tf.tape_id = t.id
		GROUP BY t.id ORDER BY t.barcode`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var bc, st string
		var wb, cb, files int64
		if err := rows.Scan(&bc, &st, &wb, &cb, &files); err != nil {
			return err
		}
		fmt.Printf("  %-10s %-7s %8d/%d bytes, %d files\n", bc, st, wb, cb, files)
	}
	return rows.Err()
}
