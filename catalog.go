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

func createBucketIfAbsent(db *sql.DB, name, pool string) error {
	return insertBucket(db, name, pool)
}

func insertBucket(db *sql.DB, name, pool string) error {
	_, err := db.Exec(`INSERT INTO buckets (name, pool) VALUES ($1, $2) ON CONFLICT (name) DO NOTHING`, name, pool)
	return err
}

// bucketPool reports the tape pool a bucket archives into. The binding
// is what makes pools visible in the S3 API: bucket = pool = tape set.
func bucketPool(db *sql.DB, name string) (string, error) {
	var pool string
	err := db.QueryRow(`SELECT pool FROM buckets WHERE name = $1`, name).Scan(&pool)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("bucket %q: %w", name, ErrNotFound)
	}
	return pool, err
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
		WHERE b.name = $1 AND ($2 = '' OR o.key LIKE $2 || '%') AND o.upload_id IS NULL
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
		FROM objects WHERE bucket_id = $1 AND key = $2 AND upload_id IS NULL`, bid, key).
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

func createArchiveRequest(db *sql.DB, objectID int64, pool string, part int) error {
	_, err := db.Exec(`INSERT INTO archive_requests (object_id, pool, part_number) VALUES ($1, $2, $3)`, objectID, pool, part)
	return err
}

type ArchiveReq struct {
	ID         int64
	ObjectID   int64
	Key        string
	Size       int64
	DiskPath   string
	PartNumber int
}

func pendingArchives(db *sql.DB, pool string) ([]ArchiveReq, error) {
	rows, err := db.Query(`
		SELECT r.id, r.object_id, o.key,
		       COALESCE(op.size_bytes, o.size_bytes),
		       COALESCE(op.disk_path, o.disk_path),
		       r.part_number
		FROM archive_requests r
		JOIN objects o ON o.id = r.object_id
		LEFT JOIN object_parts op
		  ON op.object_id = r.object_id AND op.part_number = r.part_number
		WHERE r.state = 'pending' AND r.pool = $1
		ORDER BY r.requested_at`, pool)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ArchiveReq
	for rows.Next() {
		var r ArchiveReq
		var path sql.NullString
		if err := rows.Scan(&r.ID, &r.ObjectID, &r.Key, &r.Size, &path, &r.PartNumber); err != nil {
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

func insertTapeFile(db *sql.DB, tapeID, objectID, partNumber, archiveID, physicalFile, size int64) error {
	_, err := db.Exec(`INSERT INTO tape_files (tape_id, object_id, part_number, archive_id, physical_file, size_bytes)
		VALUES ($1, $2, $3, $4, $5, $6)`, tapeID, objectID, partNumber, archiveID, physicalFile, size)
	return err
}

func objectToStandardMultipart(db *sql.DB, objectID int64) error {
	_, err := db.Exec(`UPDATE objects SET storage_class = 'STANDARD', disk_path = NULL WHERE id = $1`, objectID)
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

// ---- multipart uploads ----
// Parts stay parts on disk and tape; the S3/CLI layer merges them.
// Each part is one atomic tape file (parts are <= 5GB), so objects
// larger than any single tape archive as many parts.

type PartRow struct {
	Number   int
	Size     int64
	MD5      string
	SHA256   string
	DiskPath string // "" when tape-only
}

func createUpload(db *sql.DB, bucket, key, uploadID string) error {
	bid, err := bucketIDByName(db, bucket)
	if err != nil {
		return err
	}
	_, err = db.Exec(`INSERT INTO objects (bucket_id, key, size_bytes, storage_class, upload_id)
		VALUES ($1, $2, 0, 'STANDARD', $3)`, bid, key, uploadID)
	return err
}

func objectByUpload(db *sql.DB, bucket, key, uploadID string) (int64, error) {
	bid, err := bucketIDByName(db, bucket)
	if err != nil {
		return 0, err
	}
	var id int64
	err = db.QueryRow(`SELECT id FROM objects
		WHERE bucket_id = $1 AND key = $2 AND upload_id = $3`, bid, key, uploadID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("upload %q %q: %w", uploadID, key, ErrNotFound)
	}
	return id, err
}

// insertPart records an uploaded part. Re-uploading a part number
// replaces it: any pending archive request is dropped and an already
// written tape file becomes dead space on that tape (v0 has no
// reclamation; the catalog row is removed so it is never read).
func insertPart(db *sql.DB, objectID int64, number int, size int64, md5, sha, diskPath string) error {
	if _, err := db.Exec(`DELETE FROM archive_requests
		WHERE object_id = $1 AND part_number = $2 AND state IN ('pending','assigned')`, objectID, number); err != nil {
		return err
	}
	if _, err := db.Exec(`DELETE FROM tape_files WHERE object_id = $1 AND part_number = $2`, objectID, number); err != nil {
		return err
	}
	if _, err := db.Exec(`DELETE FROM object_parts WHERE object_id = $1 AND part_number = $2`, objectID, number); err != nil {
		return err
	}
	_, err := db.Exec(`INSERT INTO object_parts (object_id, part_number, size_bytes, md5, sha256, disk_path)
		VALUES ($1, $2, $3, $4, $5, $6)`, objectID, number, size, md5, sha, diskPath)
	return err
}

func listParts(db *sql.DB, objectID int64) ([]PartRow, error) {
	rows, err := db.Query(`SELECT part_number, size_bytes, md5, sha256, COALESCE(disk_path,'')
		FROM object_parts WHERE object_id = $1 ORDER BY part_number`, objectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PartRow
	for rows.Next() {
		var p PartRow
		if err := rows.Scan(&p.Number, &p.Size, &p.MD5, &p.SHA256, &p.DiskPath); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func isMultipart(db *sql.DB, objectID int64) (bool, error) {
	var n int
	err := db.QueryRow(`SELECT count(*) FROM object_parts WHERE object_id = $1`, objectID).Scan(&n)
	return n > 0, err
}

// completeUpload seals the object: upload closed, size = sum of parts,
// sha256 carries the S3-style multipart ETag ("md5-of-md5s-N") that
// HEAD/GET report. Per-part sha256s remain the restore verifier.
func completeUpload(db *sql.DB, objectID int64, etag string) error {
	_, err := db.Exec(`UPDATE objects SET upload_id = NULL, sha256 = $2,
		size_bytes = (SELECT COALESCE(sum(size_bytes),0) FROM object_parts WHERE object_id = $1)
		WHERE id = $1`, objectID, etag)
	return err
}

func setPartDiskPath(db *sql.DB, objectID int64, number int, path string) error {
	// "" maps to NULL: the part is tape-only.
	_, err := db.Exec(`UPDATE object_parts SET disk_path = NULLIF($3, '')
		WHERE object_id = $1 AND part_number = $2`, objectID, number, path)
	return err
}

// clearPartDiskPaths drops all restored/buffered part files (pin
// expiry). Returns the removed paths.
func clearPartDiskPaths(db *sql.DB, objectID int64) ([]string, error) {
	rows, err := db.Query(`SELECT disk_path FROM object_parts
		WHERE object_id = $1 AND disk_path IS NOT NULL`, objectID)
	if err != nil {
		return nil, err
	}
	var paths []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			rows.Close()
			return nil, err
		}
		paths = append(paths, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	_, err = db.Exec(`UPDATE object_parts SET disk_path = NULL WHERE object_id = $1`, objectID)
	return paths, err
}

// objectPendingParts counts archive requests still queued for the
// object — the worker flips it to GLACIER only when the last part is
// on tape.
func objectPendingParts(db *sql.DB, objectID int64) (int, error) {
	var n int
	err := db.QueryRow(`SELECT count(*) FROM archive_requests
		WHERE object_id = $1 AND state IN ('pending','assigned')`, objectID).Scan(&n)
	return n, err
}

// PartTapeFile locates one part's bytes for restoreOne.
type PartTapeFile struct {
	PartRow
	Barcode      string
	PhysicalFile int64
}

func multipartTapeFiles(db *sql.DB, objectID int64) ([]PartTapeFile, error) {
	rows, err := db.Query(`
		SELECT op.part_number, op.size_bytes, op.md5, op.sha256, COALESCE(op.disk_path,''),
		       t.barcode, tf.physical_file
		FROM object_parts op
		JOIN tape_files tf ON tf.object_id = op.object_id AND tf.part_number = op.part_number
		JOIN tapes t ON t.id = tf.tape_id
		WHERE op.object_id = $1 ORDER BY op.part_number`, objectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PartTapeFile
	for rows.Next() {
		var p PartTapeFile
		if err := rows.Scan(&p.Number, &p.Size, &p.MD5, &p.SHA256, &p.DiskPath, &p.Barcode, &p.PhysicalFile); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
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
		SELECT r.id, r.object_id, COALESCE(o.disk_path,'')
		FROM retrieve_requests r JOIN objects o ON o.id = r.object_id
		WHERE r.state = 'restored' AND r.expires_at < now()
		  AND (o.disk_path IS NOT NULL OR EXISTS (
		    SELECT 1 FROM object_parts op
		    WHERE op.object_id = o.id AND op.disk_path IS NOT NULL))`)
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
