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
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

const usageText = `tapeberg — a personal Glacier on your own tape

usage:
  tapeberg ingest <src-path> [key]   stage a file for archival (queues it)
  tapeberg restore <key> [days]      queue a Glacier-style restore (default 1 day, max 30)
  tapeberg get <key>                 cat a restored object to stdout
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
	obj, err := ingestStream(db, cfg, "tapeberg", key, in)
	if err != nil {
		return err
	}
	fmt.Printf("ingested %s (%d bytes, sha256 %s) — queued for tape\n", key, obj.Size, obj.SHA256)
	return nil
}

// ingestStream stages r into the disk buffer, records the object, and
// queues it for tape. Shared by the CLI and the S3 PutObject path.
func ingestStream(db *sql.DB, cfg Config, bucket, key string, r io.Reader) (*Object, error) {
	if err := os.MkdirAll(cfg.BufferDir, 0o755); err != nil {
		return nil, err
	}
	dst := filepath.Join(cfg.BufferDir, key)
	out, err := os.Create(dst)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	size, err := io.Copy(io.MultiWriter(out, h), r)
	if err != nil {
		out.Close()
		os.Remove(dst)
		return nil, err
	}
	if err := out.Close(); err != nil {
		os.Remove(dst)
		return nil, err
	}
	obj, err := insertObject(db, bucket, key, size, hex.EncodeToString(h.Sum(nil)), dst)
	if err != nil {
		os.Remove(dst)
		return nil, err
	}
	if err := createArchiveRequest(db, obj.ID); err != nil {
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
	if err := createRetrieveRequest(db, obj.ID, days); err != nil {
		return err
	}
	fmt.Printf("restore queued for %s (pin %d days) — run 'tapeberg worker' to drain it\n", key, days)
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

// LoadConfig reads and validates the environment configuration.
func LoadConfig() (Config, error) {
	c := Config{
		DBURL:       os.Getenv("TAPEBERG_DB_URL"),
		Drive:       os.Getenv("TAPEBERG_DRIVE"),
		BufferDir:   os.Getenv("TAPEBERG_BUFFER"),
		ChangerMode: envDefault("TAPEBERG_CHANGER", "robot"),
		ChangerDev:  os.Getenv("TAPEBERG_CHANGER_DEV"),
		AckDir:      envDefault("TAPEBERG_ACK_DIR", "/run/tapeberg"),
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
