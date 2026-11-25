# Tapeberg catalog (PostgreSQL)

The catalog is the source of truth for *state*; the tape is the source of
truth for *bytes*. Go code talks to it with the URL in `.env`
(`TAPEBERG_DB_URL`, 600 perms, git-ignored).

## Apply

```sh
set -a; . ./.env; set +a
psql "$TAPEBERG_DB_URL" -f db/migrations/0001_init.sql
psql "$TAPEBERG_DB_URL" -f db/seed.sql
```

Migrations are plain SQL files under `db/migrations/NNNN_name.sql`; each
run in a transaction and are recorded in `schema_migrations`. There is no
migration *runner* yet — apply by hand until the Go side lands.

## Schema map

- `tape_pools` / `tapes` — media inventory (CTA's tape-pool abstraction;
  `state` drives the scheduler: `active` → writable, `full` → keep for
  reads, `retired`/`lost` → repack candidates).
- `buckets` / `objects` — the S3-shaped namespace. `storage_class`
  flips to `GLACIER`/`DEEP_ARCHIVE` semantics once the only copy is on
  tape; `disk_path` is the disk-buffer location (NULL when tape-only).
- `tape_files` — object placement on tape (which tape, which filemark
  index). This is what a restore walks to stream bytes back.
- `archive_requests` — write queue: `pending` → `assigned` (worker has a
  tape loaded) → `written`, or `failed`/`cancelled`.
- `retrieve_requests` — Glacier RestoreObject jobs: `pending` →
  `waiting_for_media` (manual changer mode: a human was prompted) →
  `restoring` → `restored` (disk copy pinned until `expires_at` =
  `requested_at + pin_days`), or `failed`/`cancelled`. The worker's
  expiry sweep unpins (deletes disk copies of) restored objects whose pin
  has lapsed.

## Conventions

- State machines are TEXT + CHECK so new states don't need ALTER TYPE.
- The worker owns all transitions; the API only INSERTs and SELECTs.
- Nominal `capacity_bytes` is scheduler hinting only — the drive's EOM
  sense is authoritative (rig quirks in docs/dev-rig.md).
