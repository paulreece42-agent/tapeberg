# Tapeberg catalog (PostgreSQL)

The catalog is the source of truth for *state*; the tape is the source of
truth for *bytes*. Go code talks to it with the URL in `TAPEBERG_DB_URL`
(conf file or `.env`, 600 perms, git-ignored).

The catalog holds only **logical** state — pools, tapes, buckets,
objects, queues. Hardware (drive and changer devices) stays in each
node's config; see [model.md](model.md) for why and how the two meet.

## Apply

On a fresh database, apply **every** migration in order, **then** the
seed (the seed puts the mhvtl fixture tapes in the `vtl` pool, which
migration 0004 creates — seeding straight after 0001 fails with a
`pool_id` not-null violation):

```sh
set -a; . ./.env; set +a
for f in db/migrations/*.sql; do
  psql "$TAPEBERG_DB_URL" -v ON_ERROR_STOP=1 -f "$f" || break
done
psql "$TAPEBERG_DB_URL" -v ON_ERROR_STOP=1 -f db/seed.sql
```

The seed is dev-rig specific: the `default` pool, the CLI's `tapeberg`
bucket, and the 16 mhvtl data tapes + 1 cleaning tape in pool `vtl`.
A production catalog wants the `default` pool and `tapeberg` bucket but
not the VTL tapes.

**schema.sql** is the whole schema as one file, generated from the
migrations by `scripts/gen-schema.sh` — handy for reading the model in
one place or bootstrapping tools that can't apply migrations one by
one. The migrations remain canonical; `test/e2e.sh` fails if the
snapshot drifts. Apply it to an empty database/schema with
`psql -f schema.sql` (search_path decides where the tables land), then
optionally the seed.

Migrations are plain SQL files, `db/migrations/NNNN_name.sql`. There is
no migration **runner** — apply new ones by hand, in order, after
pulling.

### Migration bookkeeping is incomplete

Only 0001 and 0002 run inside a transaction and record themselves in
`schema_migrations`; 0003 onward do neither. So `schema_migrations`
reads `1,2` on a fully migrated catalog and can't tell you what has
been applied. Check for the newest objects instead, e.g.
`\d objects` shows `etag` once 0011 is in. Several migrations are not
idempotent (`ADD COLUMN` without `IF NOT EXISTS`, constraint swaps), so
re-running one errors rather than silently no-op'ing — run the loop
above only on a fresh database.

## Schema map

| Table | Since | What it holds |
|---|---|---|
| `tape_pools` | 0001, EC 0010 | Named tape sets (CTA's tape pool). `ec_k`/`ec_m` = the pool's RAIT policy (NULL = single copy). |
| `tapes` | 0001 | Media inventory: barcode, `pool_id`, `state`, `capacity_bytes` (hint), `written_bytes`. `state`: `active` → writable; `full` → reads + repack source only; `retired` → emptied by repack, ignored by the scheduler; `lost` → reserved. |
| `buckets` | 0001, `pool` 0006, `owner_id` 0008 | S3 namespace. `pool` = the tape pool new archives in this bucket go to (free text, not a foreign key). |
| `objects` | 0001, +0007/0008/0009/0010/0011 | Keys, sizes, `sha256`, `etag` (S3 wire ETag), `storage_class`, `disk_path` (node-local staged/restored copy; NULL = tape-only), `upload_id` (open multipart), `owner_id`, `deleted_at` (tombstone), frozen `ec_k`/`ec_m`. Live keys are unique per bucket; tombstoned rows are not. |
| `object_parts` | 0007 | Multipart parts: size, `md5` (part ETag), `sha256` (restore verify), node-local `disk_path`. |
| `object_shards` | 0010 | Per-shard `sha256` for RAIT objects, keyed by (object, part, shard). |
| `tape_files` | 0001, +0003/0007/0010 | Placement: which tape, `archive_id` (Tapeberg's per-tape sequence), `physical_file` (true filemark position — real media can carry foreign files), `part_number`, `shard_number`. A restore walks these. |
| `archive_requests` | 0001, `pool` 0005, `part_number` 0007 | Write queue: `pending` → `assigned` → `written`, or `failed`/`cancelled`. `pool` decides which worker drains it. |
| `retrieve_requests` | 0001, `expired` 0002, `pool` 0005 | Glacier RestoreObject jobs: `pending` → `waiting_for_media` (manual changer) → `restoring` → `restored` (pinned until `expires_at`) → `expired`, or `failed`/`cancelled`. |
| `s3_users`, `s3_keys` | 0008 | RGW-style users (uid, full name, email, admin, suspended) and their access/secret key pairs. |
| `schema_migrations` | 0001 | Applied versions — see the bookkeeping caveat above. |

## Conventions

- State machines are TEXT + CHECK so new states don't need ALTER TYPE.
- The worker owns the queue and tape transitions; the S3 API and CLI
  INSERT requests and SELECT state.
- Nominal `capacity_bytes` is scheduler hinting only — the drive's EOM
  sense is authoritative (rig quirks in [dev-rig.md](dev-rig.md)).
- Pool names in `buckets.pool`, `archive_requests.pool` and
  `retrieve_requests.pool` are plain text matched against
  `tape_pools.name`; nothing enforces that the pool exists.
