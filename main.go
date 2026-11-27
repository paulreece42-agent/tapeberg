// Tapeberg: a personal Glacier on your own tape.
//
// Configuration comes from the environment:
//
//	TAPEBERG_DB_URL       required  postgres://... (see docs/database.md)
//	TAPEBERG_DRIVE        required  non-rewinding tape device, e.g. /dev/nst1
//	TAPEBERG_BUFFER       required  disk buffer directory (staging area)
//	TAPEBERG_CHANGER      robot | manual (default robot)
//	TAPEBERG_CHANGER_DEV  /dev/sgN for robot mode (default: auto-discover)
//	TAPEBERG_ACK_DIR      prompt/ack dir for manual mode (default /run/tapeberg)
package main

import (
	"crypto/md5"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const usageText = `tapeberg — a personal Glacier on your own tape

usage:
  tapeberg ingest <src-path> [key]   stage a file for archival (queues it)
  tapeberg restore <key> [days]      queue a Glacier-style restore (default 1 day, max 30)
  tapeberg get <key>                 cat a restored object to stdout
  tapeberg rm <key>                  soft-delete an object (tape bytes reclaimed at repack)
  tapeberg tape add <barcode> [capacity]  register a tape in the catalog (capacity like 1500GB; default 1500GB, LTO-5)
  tapeberg pool ls                   list pools: tapes by state, bound buckets, usage, EC policy
  tapeberg pool add <name> [--description X]   create a pool (then run a worker with TAPEBERG_POOL=<name>)
  tapeberg pool rm <name>            remove an unused pool (no tapes, buckets or queued work)
  tapeberg pool set <name> --ec-k 2 --ec-m 1   RAIT stripe per archive (or --no-ec)
  tapeberg bucket ls                 list buckets: bound pool, owner, object counts
  tapeberg bucket create <name> --pool P [--owner UID]   create a bucket bound to pool P
  tapeberg bucket set-pool <name> <pool>   rebind: new archives go to <pool>; existing objects stay put
  tapeberg bucket rm <name>          remove a bucket that never held objects
  tapeberg user <subcommand>         manage S3 users (add/rm/ls/info/suspend/unsuspend; --full-name, --email, --admin)
  tapeberg key <subcommand>         manage S3 keys (add --uid, rm --access-key; users may hold several)
  tapeberg status                    catalog + queue summary
  tapeberg worker                    drain archive/restore queues once (run from cron)
  tapeberg s3 [listen-addr]          serve the S3/Glacier wire protocol (default :7333)
  tapeberg doctor [--notify-test]    readiness check (catalog, drive, changer, notifier)
  tapeberg ack <what>                acknowledge a manual-changer prompt
`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "tapeberg:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if err := loadConfEnv(); err != nil {
		return fmt.Errorf("conf: %w", err)
	}
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usageText)
		os.Exit(2)
	}
	cmd, rest := args[0], args[1:]

	// ack does not need the catalog or a drive.
	if cmd == "ack" {
		if len(rest) != 1 {
			return fmt.Errorf("usage: tapeberg ack <what>")
		}
		return runAck(rest[0])
	}

	cfg, err := LoadConfig()
	if err != nil {
		return err
	}

	switch cmd {
	case "ingest":
		if len(rest) < 1 || len(rest) > 2 {
			return fmt.Errorf("usage: tapeberg ingest <src-path> [key]")
		}
		key := filepath.Base(rest[0])
		if len(rest) == 2 {
			key = rest[1]
		}
		if key != filepath.Base(key) {
			return fmt.Errorf("key must not contain path separators (v0)")
		}
		return cmdIngest(cfg, rest[0], key)

	case "restore":
		if len(rest) < 1 || len(rest) > 2 {
			return fmt.Errorf("usage: tapeberg restore <key> [days]")
		}
		days := 1
		if len(rest) == 2 {
			days, err = strconv.Atoi(rest[1])
			if err != nil || days < 1 || days > 30 {
				return fmt.Errorf("days must be an integer 1-30")
			}
		}
		return cmdRestore(cfg, rest[0], days)

	case "get":
		if len(rest) != 1 {
			return fmt.Errorf("usage: tapeberg get <key>")
		}
		return cmdGet(cfg, rest[0])

	case "tape":
		if len(rest) != 2 && len(rest) != 3 || rest[0] != "add" {
			return fmt.Errorf("usage: tapeberg tape add <barcode> [capacity]")
		}
		return cmdTapeAdd(cfg, rest[1:])

	case "user":
		return cmdUser(cfg, rest)

	case "key":
		return cmdKey(cfg, rest)

	case "rm":
		if len(rest) != 1 {
			return fmt.Errorf("usage: tapeberg rm <key>")
		}
		return cmdRm(cfg, rest[0])

	case "pool":
		return cmdPool(cfg, rest)

	case "bucket":
		return cmdBucket(cfg, rest)

	case "status":
		return cmdStatus(cfg)

	case "worker":
		return RunWorker(cfg)

	case "s3":
		listen := ""
		if len(rest) > 1 {
			return fmt.Errorf("usage: tapeberg s3 [listen-addr]")
		}
		if len(rest) == 1 {
			listen = rest[0]
		}
		return cmdS3Serve(cfg, listen)

	case "doctor":
		return cmdDoctor(cfg, rest)

	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n%s", cmd, usageText)
		os.Exit(2)
	}
	return nil
}

// cmdIngest copies src into the disk buffer and queues it for tape.
func cmdIngest(cfg Config, src, key string) error {
	db, err := openDB(cfg.DBURL)
	if err != nil {
		return err
	}
	defer db.Close()
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	obj, err := ingestStream(db, cfg, "tapeberg", key, in, cfg.Pool, 0)
	if err != nil {
		return err
	}
	fmt.Printf("ingested %s (%d bytes, sha256 %s) — queued for tape\n", key, obj.Size, obj.SHA256)
	return nil
}

// ingestStream stages r into the disk buffer, records the object, and
// queues it for tape. Shared by the CLI and the S3 PutObject path.
// owner 0 = system (CLI); S3 passes the uploading user.
func ingestStream(db *sql.DB, cfg Config, bucket, key string, r io.Reader, pool string, owner int64) (*Object, error) {
	if err := os.MkdirAll(cfg.BufferDir, 0o755); err != nil {
		return nil, err
	}
	dst := filepath.Join(cfg.BufferDir, key)
	out, err := os.Create(dst)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	mh := md5.New()
	size, err := io.Copy(io.MultiWriter(out, h, mh), r)
	if err != nil {
		out.Close()
		os.Remove(dst)
		return nil, err
	}
	if err := out.Close(); err != nil {
		os.Remove(dst)
		return nil, err
	}
	obj, err := insertObject(db, bucket, key, size, hex.EncodeToString(h.Sum(nil)),
		hex.EncodeToString(mh.Sum(nil)), dst, owner)
	if err != nil {
		os.Remove(dst)
		return nil, err
	}
	if err := createArchiveRequest(db, obj.ID, pool, 1); err != nil {
		return nil, err
	}
	return obj, nil
}

// cmdRestore queues a Glacier-style restore job for the object.
func cmdRestore(cfg Config, key string, days int) error {
	db, err := openDB(cfg.DBURL)
	if err != nil {
		return err
	}
	defer db.Close()

	obj, err := getObjectIn(db, "tapeberg", key)
	if err != nil {
		return err
	}
	if obj.StorageClass == "STANDARD" && obj.DiskPath != "" {
		fmt.Printf("%s is already on disk (restored); no new request needed\n", key)
		return nil
	}
	pool, err := createRetrieveRequest(db, obj.ID, days, cfg.Pool)
	if err != nil {
		return err
	}
	fmt.Printf("restore queued for %s in pool %s (pin %d days) — the %s worker drains it\n", key, pool, days, pool)
	return nil
}

// cmdGet streams a restored object to stdout; a tape-only object refuses,
// mirroring S3 Glacier's InvalidObjectState.
func cmdGet(cfg Config, key string) error {
	db, err := openDB(cfg.DBURL)
	if err != nil {
		return err
	}
	defer db.Close()

	obj, err := getObjectIn(db, "tapeberg", key)
	if err != nil {
		return err
	}
	// Multipart: merge at the CLI layer too — parts concatenate in order.
	if mp, err := isMultipart(db, obj.ID); err != nil {
		return err
	} else if mp {
		parts, err := listParts(db, obj.ID)
		if err != nil {
			return err
		}
		for _, p := range parts {
			if p.DiskPath == "" {
				return fmt.Errorf("InvalidObjectState: object is in storage class %s — run 'tapeberg restore %s' first", obj.StorageClass, key)
			}
		}
		for _, p := range parts {
			f, err := os.Open(p.DiskPath)
			if err != nil {
				return err
			}
			if _, err := io.Copy(os.Stdout, f); err != nil {
				f.Close()
				return err
			}
			f.Close()
		}
		return nil
	}
	if obj.StorageClass != "STANDARD" || obj.DiskPath == "" {
		return fmt.Errorf("InvalidObjectState: object is in storage class %s — run 'tapeberg restore %s' first", obj.StorageClass, key)
	}
	f, err := os.Open(obj.DiskPath)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := io.Copy(os.Stdout, f); err != nil {
		return err
	}
	return nil
}

func cmdStatus(cfg Config) error {
	db, err := openDB(cfg.DBURL)
	if err != nil {
		return err
	}
	defer db.Close()
	return printStatus(db)
}

func envDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envIntDefault(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// splitCSV splits a comma-separated env value (TAPEBERG_DRIVES); empty
// entries dropped. Empty env → nil (single-drive rig).
func splitCSV(v string) []string {
	var out []string
	for _, f := range strings.Split(v, ",") {
		if f = strings.TrimSpace(f); f != "" {
			out = append(out, f)
		}
	}
	return out
}

// parseCapacity accepts a byte count or a size with unit, e.g. 100MB,
// 1500GB, 1.5TB, 128GiB. Bare numbers are bytes.
func parseCapacity(s string) (int64, error) {
	units := []struct {
		suffix string
		mult   int64
	}{
		{"kib", 1 << 10}, {"mib", 1 << 20}, {"gib", 1 << 30}, {"tib", 1 << 40},
		{"kb", 1e3}, {"mb", 1e6}, {"gb", 1e9}, {"tb", 1e12},
	}
	s = strings.TrimSpace(s)
	for _, u := range units {
		if suffix := strings.ToLower(strings.TrimSpace(u.suffix)); strings.HasSuffix(strings.ToLower(s), u.suffix) {
			num := strings.TrimSpace(s[:len(s)-len(suffix)])
			f, err := strconv.ParseFloat(num, 64)
			if err != nil {
				return 0, fmt.Errorf("capacity %q: bad number %q", s, num)
			}
			return int64(f * float64(u.mult)), nil
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("capacity %q: expected a byte count or size like 1500GB", s)
	}
	return n, nil
}

// cmdTapeAdd registers a tape in the catalog (default pool). Capacity is
// only a scheduling hint — the drive's EOM sense is authoritative.
func cmdTapeAdd(cfg Config, args []string) error {
	barcode := args[0]
	capacity := int64(1500000000000) // LTO-5 native nominal
	if len(args) == 2 {
		var err error
		capacity, err = parseCapacity(args[1])
		if err != nil {
			return err
		}
	}
	db, err := openDB(cfg.DBURL)
	if err != nil {
		return err
	}
	defer db.Close()
	inserted, err := db.Exec(`
		INSERT INTO tapes (barcode, pool_id, is_cleaning, capacity_bytes)
		VALUES ($1, (SELECT id FROM tape_pools WHERE name = $2), $3, $4)
		ON CONFLICT (barcode) DO NOTHING`,
		barcode, cfg.Pool, strings.HasPrefix(strings.ToUpper(barcode), "CLN"), capacity)
	if err != nil {
		return err
	}
	if n, _ := inserted.RowsAffected(); n == 0 {
		return fmt.Errorf("tape %s already registered", barcode)
	}
	fmt.Printf("registered tape %s (pool default, capacity hint %d bytes — EOM is authoritative)\n", barcode, capacity)
	fmt.Println("next: 'tapeberg doctor' to verify, then ingest + worker will pick it up")
	return nil
}

// LoadConfig reads and validates the environment configuration.
func LoadConfig() (Config, error) {
	c := Config{
		DBURL:       os.Getenv("TAPEBERG_DB_URL"),
		Drive:       os.Getenv("TAPEBERG_DRIVE"),
		BufferDir:   os.Getenv("TAPEBERG_BUFFER"),
		ChangerMode: envDefault("TAPEBERG_CHANGER", "robot"),
		ChangerDev:  os.Getenv("TAPEBERG_CHANGER_DEV"),
		AckDir:      envDefault("TAPEBERG_ACK_DIR", "/run/tapeberg"),
		Pool:        envDefault("TAPEBERG_POOL", "default"),
		Auth:        envDefault("TAPEBERG_AUTH", "builtin"),
		RepackThreshold: envIntDefault("TAPEBERG_REPACK_THRESHOLD", 50),
		UploadTTLDays:   envIntDefault("TAPEBERG_UPLOAD_TTL_DAYS", 7),
		PartMinBytes: int64(envIntDefault("TAPEBERG_PART_MIN_BYTES", 5<<20)),
		PartMaxBytes: int64(envIntDefault("TAPEBERG_PART_MAX_BYTES", 5<<30)),
		Drives:      splitCSV(os.Getenv("TAPEBERG_DRIVES")),
		S3Listen:    envDefault("TAPEBERG_S3_LISTEN", ":7333"),
		S3Key:       os.Getenv("TAPEBERG_S3_KEY"),
		S3Secret:    os.Getenv("TAPEBERG_S3_SECRET"),
	}
	var missing []string
	if c.DBURL == "" {
		missing = append(missing, "TAPEBERG_DB_URL")
	}
	if c.Drive == "" {
		missing = append(missing, "TAPEBERG_DRIVE")
	}
	if c.BufferDir == "" {
		missing = append(missing, "TAPEBERG_BUFFER")
	}
	if c.ChangerMode != "robot" && c.ChangerMode != "manual" {
		return c, fmt.Errorf("TAPEBERG_CHANGER must be 'robot' or 'manual'")
	}
	if len(missing) > 0 {
		return c, fmt.Errorf("missing environment: %s", missing)
	}
	return c, nil
}

// runAck writes the manual-changer acknowledgement file.
func runAck(what string) error {
	dir := envDefault("TAPEBERG_ACK_DIR", "/run/tapeberg")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, ackFile), []byte(what), 0o644); err != nil {
		return err
	}
	fmt.Printf("acknowledged %q — the waiting worker will continue\n", what)
	return nil
}

// ---- user / key management (RGW-style; the builtin auth backend) ----

func flagValue(args []string, name string) (string, bool) {
	for i, a := range args {
		if a == name && i+1 < len(args) {
			return args[i+1], true
		}
		if strings.HasPrefix(a, name+"=") {
			return strings.TrimPrefix(a, name+"="), true
		}
	}
	return "", false
}

func hasFlag(args []string, name string) bool {
	for _, a := range args {
		if a == name || strings.HasPrefix(a, name+"=") {
			return true
		}
	}
	return false
}

func cmdUser(cfg Config, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: tapeberg user add <uid> [--full-name X] [--email Y] [--admin] | rm <uid> | ls | info <uid> | suspend <uid> | unsuspend <uid>")
	}
	db, err := openDB(cfg.DBURL)
	if err != nil {
		return err
	}
	defer db.Close()
	sub, rest := args[0], args[1:]
	switch sub {
	case "add":
		if len(rest) < 1 {
			return fmt.Errorf("usage: tapeberg user add <uid> [--full-name X] [--email Y] [--admin]")
		}
		uid := rest[0]
		fullName, _ := flagValue(rest, "--full-name")
		if fullName == "" {
			fullName = uid
		}
		email, _ := flagValue(rest, "--email")
		if exists, _ := dbUserExists(db, uid); exists {
			return fmt.Errorf("user %q already exists", uid)
		}
		if err := dbUserAdd(db, uid, fullName, email, hasFlag(rest, "--admin")); err != nil {
			return err
		}
		access, secret, err := dbKeyAdd(db, uid)
		if err != nil {
			return err
		}
		fmt.Printf("user %s created (full name: %s, email: %s, admin: %v)\n", uid, fullName, email, hasFlag(rest, "--admin"))
		fmt.Printf("access_key: %s\nsecret_key: %s\n", access, secret)
		fmt.Println("note: the endpoint switches from the static/env key to DB users as soon as one user exists")
		return nil
	case "rm":
		if len(rest) != 1 {
			return fmt.Errorf("usage: tapeberg user rm <uid>")
		}
		if err := dbUserRm(db, rest[0]); err != nil {
			return err
		}
		fmt.Printf("user %s removed (their keys are gone; their objects remain, owner-orphaned)\n", rest[0])
		return nil
	case "ls":
		users, err := dbUserList(db)
		if err != nil {
			return err
		}
		if len(users) == 0 {
			fmt.Println("no users (endpoint uses the static/env key, or unsigned dev mode)")
			return nil
		}
		for _, u := range users {
			admin := ""
			if u.IsAdmin {
				admin = " [admin]"
			}
			susp := ""
			if u.Suspended {
				susp = " [suspended]"
			}
			fmt.Printf("%s\t%s\t%s\t%d key(s)%s%s\n", u.UID, u.FullName, u.Email, u.Keys, admin, susp)
		}
		return nil
	case "info":
		if len(rest) != 1 {
			return fmt.Errorf("usage: tapeberg user info <uid>")
		}
		u, err := dbUserGet(db, rest[0])
		if err != nil {
			return err
		}
		fmt.Printf("uid: %s\nfull name: %s\nemail: %s\nadmin: %v\nsuspended: %v\nkeys: %d\n",
			u.UID, u.FullName, u.Email, u.IsAdmin, u.Suspended, u.Keys)
		keys, err := dbUserKeys(db, u.UID)
		if err != nil {
			return err
		}
		for _, k := range keys {
			fmt.Printf("access_key: %s\nsecret_key: %s\n", k.Access, k.Secret)
		}
		return nil
	case "suspend", "unsuspend":
		if len(rest) != 1 {
			return fmt.Errorf("usage: tapeberg user %s <uid>", sub)
		}
		if err := dbUserSetSuspended(db, rest[0], sub == "suspend"); err != nil {
			return err
		}
		verb := "suspended"
		if sub == "unsuspend" {
			verb = "unsuspended"
		}
		fmt.Printf("user %s %s\n", rest[0], verb)
		return nil
	default:
		return fmt.Errorf("unknown user subcommand %q (have: add rm ls info suspend unsuspend)", sub)
	}
}

func cmdKey(cfg Config, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: tapeberg key add --uid <uid> | rm --access-key <key>")
	}
	db, err := openDB(cfg.DBURL)
	if err != nil {
		return err
	}
	defer db.Close()
	switch args[0] {
	case "add":
		uid, ok := flagValue(args, "--uid")
		if !ok {
			return fmt.Errorf("usage: tapeberg key add --uid <uid>")
		}
		access, secret, err := dbKeyAdd(db, uid)
		if err != nil {
			return err
		}
		fmt.Printf("access_key: %s\nsecret_key: %s\n", access, secret)
		return nil
	case "rm":
		access, ok := flagValue(args, "--access-key")
		if !ok {
			return fmt.Errorf("usage: tapeberg key rm --access-key <key>")
		}
		if err := dbKeyRm(db, access); err != nil {
			return err
		}
		fmt.Printf("key %s removed\n", access)
		return nil
	default:
		return fmt.Errorf("unknown key subcommand %q (have: add rm)", args[0])
	}
}

// cmdRm soft-deletes an object from the CLI's 'tapeberg' bucket —
// same semantics as S3 DeleteObject: tombstone now, tape bytes
// reclaimed when the worker's repack pass rewrites the tape.
func cmdRm(cfg Config, key string) error {
	db, err := openDB(cfg.DBURL)
	if err != nil {
		return err
	}
	defer db.Close()
	obj, err := getObjectIn(db, "tapeberg", key)
	if err != nil {
		fmt.Printf("%s: already gone (no such object)\n", key)
		return nil
	}
	files, err := softDeleteObject(db, obj.ID)
	if err != nil {
		return err
	}
	for _, f := range files {
		if f != "" {
			_ = os.Remove(f)
		}
	}
	fmt.Printf("%s: deleted (tombstoned; tape bytes reclaimed at repack)\n", key)
	return nil
}

// cmdPool manages pools: create/remove, and the EC/RAIT stripe shape.
func cmdPool(cfg Config, args []string) error {
	const usage = "usage: tapeberg pool ls | add <name> [--description X] | rm <name> | set <name> [--ec-k N --ec-m M | --no-ec]"
	if len(args) < 1 {
		return errors.New(usage)
	}
	db, err := openDB(cfg.DBURL)
	if err != nil {
		return err
	}
	defer db.Close()
	switch args[0] {
	case "ls":
		pools, err := listPools(db)
		if err != nil {
			return err
		}
		for _, p := range pools {
			ec := "single copy"
			if p.ECK > 0 {
				ec = fmt.Sprintf("RAIT %d+%d (survives %d tape loss(es))", p.ECK, p.ECM, p.ECM)
			}
			fmt.Printf("%s\t%d tape(s) (%d active, %d full, %d retired/lost)\t%d bucket(s)\t%s written of %s\t%s\n",
				p.Name, p.Tapes, p.Active, p.Full, p.Gone, p.Buckets,
				humanBytes(p.WrittenBytes), humanBytes(p.CapacityBytes), ec)
		}
		return nil
	case "add":
		if len(args) < 2 || strings.HasPrefix(args[1], "--") {
			return fmt.Errorf("usage: tapeberg pool add <name> [--description X]")
		}
		desc, _ := flagValue(args, "--description")
		if err := addPool(db, args[1], desc); err != nil {
			return err
		}
		fmt.Printf("pool %s created (single copy; 'tapeberg pool set %s --ec-k N --ec-m M' for RAIT)\n", args[1], args[1])
		fmt.Printf("next: register its tapes, bind buckets ('tapeberg bucket create <name> --pool %s'),\n", args[1])
		fmt.Printf("      and run a worker with TAPEBERG_POOL=%s on the node whose library holds those tapes\n", args[1])
		return nil
	case "rm":
		if len(args) != 2 {
			return fmt.Errorf("usage: tapeberg pool rm <name>")
		}
		if err := removePool(db, args[1]); err != nil {
			return err
		}
		fmt.Printf("pool %s removed\n", args[1])
		return nil
	case "set":
		if len(args) < 2 {
			return fmt.Errorf("usage: tapeberg pool set <name> [--ec-k N --ec-m M | --no-ec]")
		}
		pool := args[1]
		if hasFlag(args, "--no-ec") {
			if err := setPoolEC(db, pool, 0, 0); err != nil {
				return err
			}
			fmt.Printf("pool %s: erasure coding off (new archives are single-copy)\n", pool)
			return nil
		}
		k, _ := flagValue(args, "--ec-k")
		m, _ := flagValue(args, "--ec-m")
		if k == "" || m == "" {
			return fmt.Errorf("pool set needs --ec-k and --ec-m (or --no-ec)")
		}
		ki, err1 := strconv.Atoi(k)
		mi, err2 := strconv.Atoi(m)
		if err1 != nil || err2 != nil || ki < 1 || mi < 1 {
			return fmt.Errorf("--ec-k and --ec-m must be positive integers")
		}
		if err := setPoolEC(db, pool, ki, mi); err != nil {
			return err
		}
		fmt.Printf("pool %s: RAIT %d+%d — new archives stripe across %d distinct tapes; any %d tape losses survive\n",
			pool, ki, mi, ki+mi, mi)
		fmt.Println("note: the policy freezes onto each object at archive time; existing objects keep their shape")
		return nil
	default:
		return fmt.Errorf("unknown pool subcommand %q (have: ls add rm set)", args[0])
	}
}

// cmdBucket manages buckets and their pool binding from the CLI.
func cmdBucket(cfg Config, args []string) error {
	const usage = "usage: tapeberg bucket ls | create <name> --pool P [--owner UID] | set-pool <name> <pool> | rm <name>"
	if len(args) < 1 {
		return errors.New(usage)
	}
	db, err := openDB(cfg.DBURL)
	if err != nil {
		return err
	}
	defer db.Close()
	switch args[0] {
	case "ls":
		buckets, err := listAllBuckets(db)
		if err != nil {
			return err
		}
		for _, b := range buckets {
			pool := b.Pool
			if b.PoolMissing {
				pool += " (NO SUCH POOL — 'tapeberg bucket set-pool')"
			}
			owner := b.Owner
			if owner == "" {
				owner = "(system)"
			}
			fmt.Printf("%s\tpool %s\towner %s\t%d object(s), %s\t%d deleted\n",
				b.Name, pool, owner, b.Objects, humanBytes(b.Bytes), b.Deleted)
		}
		return nil
	case "create":
		if len(args) < 2 || strings.HasPrefix(args[1], "--") {
			return fmt.Errorf("usage: tapeberg bucket create <name> --pool P [--owner UID]")
		}
		pool, ok := flagValue(args, "--pool")
		if !ok || pool == "" {
			return fmt.Errorf("bucket create needs --pool (S3 CreateBucket binds to the endpoint's TAPEBERG_POOL; the CLI makes you choose)")
		}
		owner, _ := flagValue(args, "--owner")
		if err := createBucketCLI(db, args[1], pool, owner); err != nil {
			return err
		}
		fmt.Printf("bucket %s created, bound to pool %s\n", args[1], pool)
		return nil
	case "set-pool":
		if len(args) != 3 {
			return fmt.Errorf("usage: tapeberg bucket set-pool <name> <pool>")
		}
		queued, err := setBucketPool(db, args[1], args[2])
		if err != nil {
			return err
		}
		fmt.Printf("bucket %s now bound to pool %s: new archives go to its tapes\n", args[1], args[2])
		fmt.Println("objects already on tape stay where they are (restores follow their tapes)")
		if queued > 0 {
			fmt.Printf("note: %d archive(s) already queued for this bucket keep their original pool\n", queued)
		}
		return nil
	case "rm":
		if len(args) != 2 {
			return fmt.Errorf("usage: tapeberg bucket rm <name>")
		}
		if err := removeBucket(db, args[1]); err != nil {
			return err
		}
		fmt.Printf("bucket %s removed\n", args[1])
		return nil
	default:
		return fmt.Errorf("unknown bucket subcommand %q (have: ls create set-pool rm)", args[0])
	}
}
