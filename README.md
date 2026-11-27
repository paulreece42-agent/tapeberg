# Tapeberg

**Work in progress.** Personal AWS-Glacier-style cold archive on your own
LTO tapes. Go + PostgreSQL + cron. Drives an autochanger robot via `mtx`,
or runs in **manual mode**: it pings you on Telegram/a webhook — "load
tape TB0003L5" — and waits for `tapeberg ack`.

## Set up

Config: any `TAPEBERG_*` env var can live in a conf file instead —
`/etc/tapeberg.conf` or `~/.config/tapeberg.conf` (or `$TAPEBERG_CONF`),
`key = value` lines, keys minus the prefix. Env vars override the file;
chmod 600 (it carries credentials). Every key is listed in
[docs/config.md](docs/config.md):

```ini
db_url = postgres://tapeberg:...@127.0.0.1:5432/tapeberg
drive = /dev/nst0
buffer = /var/lib/tapeberg
changer = manual
telegram_token = ...        # or webhook_url / webhook_token
```

```sh
go build -o tapeberg .
export TAPEBERG_DB_URL=postgres://...        # catalog (db/migrations/)
export TAPEBERG_DRIVE=/dev/nst0             # non-rewinding tape device
export TAPEBERG_BUFFER=/var/lib/tapeberg   # disk staging area
export TAPEBERG_CHANGER=manual              # or robot (autochanger)
# notifications (optional, any mix):
export TAPEBERG_TELEGRAM_TOKEN=... TAPEBERG_TELEGRAM_CHAT=...
export TAPEBERG_WEBHOOK_URL=...             # POSTs {"text": ...} (Slack incoming-webhook shape)
tapeberg doctor                             # readiness check
tapeberg tape add 000259L5                  # register a new tape (capacity hint default 1500GB)
```

Run the worker from cron (see `deploy/tapeberg-worker.cron`). In manual
mode, label your physical tapes with the catalog barcodes (TB0003L5…).

## CLI

```sh
tapeberg ingest file.tar          # stage + queue for tape
tapeberg worker                   # drain archive/restore queues, once
tapeberg restore file.tar 3       # Glacier restore, pinned 3 days
tapeberg get file.tar > out       # errors InvalidObjectState until restored
tapeberg rm file.tar              # soft delete (tape bytes reclaimed by repack)
tapeberg status
tapeberg ack TB0003L5             # manual changer: "tape is in the drive"
tapeberg tape add TB0004L5 1500GB # register media into $TAPEBERG_POOL (or --pool P)
tapeberg tape ls                  # tapes: pool, state, live/dead bytes
tapeberg pool ls                  # pools: tapes by state, buckets, usage, EC policy
tapeberg pool add offsite         # new pool (give it tapes and a worker)
tapeberg bucket ls                # buckets and the pool each is bound to
tapeberg bucket create photos --pool offsite
tapeberg bucket set-pool photos default   # new archives go to default's tapes
```

The CLI's ingest/get/rm work on its own bucket, `tapeberg`; other
buckets' objects are S3-only.

## Pools, buckets and drives

The catalog holds the **logical** layout; each node's config holds the
**physical** one — so a catalog can one day be shared by several
machines without carrying device paths that only mean something on
one of them.

- A **pool** is a set of tapes (plus its erasure-coding policy).
- A **bucket** is bound to one pool: its new archives go to that
  pool's tapes. CreateBucket binds the endpoint's `TAPEBERG_POOL`;
  `bucket create --pool` / `bucket set-pool` choose explicitly.
- A **tape** belongs to one pool (`tape add` uses `TAPEBERG_POOL` or
  `--pool`; `tape mv … --to` moves tapes between pools).
- A **worker** serves exactly one pool with exactly one library — the
  `TAPEBERG_POOL`, `TAPEBERG_DRIVE(S)` and changer in its config. A
  node with two libraries runs two workers, each with its own conf
  file and buffer.

Recipes (adding a pool, two libraries on one node, rebinding a bucket)
and the current gaps are in [docs/model.md](docs/model.md).

## Auth (users, keys, tenancy)

Modeled on Ceph RGW: users carry a uid, full name, email, an admin
flag, and one or more rotatable access/secret key pairs — all in the
catalog DB (the `builtin` backend, default; `TAPEBERG_AUTH` selects a
backend — LDAP/AD drop in later behind the same interface).

```sh
tapeberg user add paul --full-name "Paul Reece" --email paul@example.com --admin
tapeberg user ls
tapeberg key add --uid paul           # rotation: second key, then remove the old
tapeberg key rm --access-key <old>
tapeberg user suspend paul
```

Tenancy: non-admin users see only their own buckets and objects
(CLI-ingested objects are system-owned, admin-visible). Suspended
users are refused. While no user exists the endpoint keeps the legacy
behavior (static `TAPEBERG_S3_KEY/SECRET`, or unsigned dev mode) —
the first created user switches it to real DB auth.

## Erasure coding (RAIT — Reed-Solomon across tapes)

Pools carry an EC policy: `k` data shards + `m` parity shards per
archive, each shard on a **different tape**. Any `m` tapes can be lost
and everything still restores — RAID for tape.

```sh
tapeberg pool set vtl --ec-k 2 --ec-m 1   # RAIT 2+1
tapeberg pool ls
tapeberg pool set vtl --no-ec             # back to single copy
```

The unit of coding is the object (or multipart **part** — sizes are
exact, the piece size-agnostic EC designs struggle with). The policy
freezes onto each object at archive time, so changing a pool never
invalidates what is already on tape. On a multi-drive robot
(`TAPEBERG_DRIVES=/dev/nst1,/dev/nst2,/dev/nst3`, DTE order) the
shard tapes are loaded into separate drives and written in parallel,
one stream per drive; a single-drive rig writes the shards
sequentially. Every shard is sha-verified against the catalog on
read, and a restore with missing shards reconstructs them from
parity. Repack is RAIT-aware: a repacked shard never lands on a
sibling shard's tape.

## Deletes & repack (the year-3 problem)

DeleteObject (and `tapeberg rm`) is a **soft delete**: the object gets
a tombstone in the catalog, GET/HEAD/list stop seeing it, queued
archives are cancelled — but the tape bytes stay. Tapes are
write-once, so deletes accumulate as dead space exactly like CTA.

The cron worker reclaims it: when a tape's dead share crosses
`TAPEBERG_REPACK_THRESHOLD` (default 50%), it **repacks** — every live
file is read from the source tape (checksum-verified against the
catalog), re-archived to another tape in the pool, and the emptied
tape is marked `retired`. A read or checksum failure aborts that
tape's repack and leaves it untouched — a suspect tape is never
freed. Erase a retired tape and `tapeberg tape reuse` it to put the media back
in service.

## S3 (awscli)

Buckets bind to tape pools: CreateBucket binds the serving endpoint's
pool (`TAPEBERG_POOL`), and PutObject/RestoreObject queue in the
bucket's pool — so a bucket maps to one tape set (see
[docs/model.md](docs/model.md)).

### Multipart (files larger than a single tape)

Multipart uploads work the CTA way: **parts stay parts**. Each part is
one atomic tape file (parts are capped at 5GB by S3 clients, so every
part fits a tape), parts archive to tape as they arrive — a
still-streaming upload never fills the buffer disk — and GET/restore
merge at the API layer by concatenating parts in order. Objects larger
than any single tape just span several.

The tar-pipe workflow works end to end:

```sh
tar c ~/photos | s3cmd put - s3://tapeberg/photos.tar      # multipart streaming upload
aws s3api restore-object --bucket tapeberg --key photos.tar --restore-request Days=3
aws s3api head-object --bucket tapeberg --key photos.tar   # x-amz-restore: ongoing-request="false"
s3cmd get s3://tapeberg/photos.tar - | tar xv              # streamed merged read
```

Part sizes are policy, not just protocol: `TAPEBERG_PART_MIN_BYTES`
(default 5MiB, S3's floor) and `TAPEBERG_PART_MAX_BYTES` (default 5GiB,
S3's cap). On real tape, raise the minimum — small parts shoe-shine the
drive and litter the catalog; 64-128MiB parts keep LTO streaming. The
minimum applies to every part except the final one and is enforced
S3-style at CompleteMultipartUpload (`EntityTooSmall`); the maximum is
rejected per UploadPart (`EntityTooLarge`).

Complete computes the S3-style multipart ETag (md5-of-md5s). Abort is
a delete: the in-progress object gets a tombstone, queued part
archives are cancelled, buffered disk parts are removed — parts
already on tape stay as dead bytes that repack reclaims. Ranged GETs
span part boundaries.

Uploads that are neither completed nor aborted (crashed clients,
forgotten pipelines) don't linger either: the worker tombstones any
open upload older than `TAPEBERG_UPLOAD_TTL_DAYS` (default 7 — S3's
lifecycle "AbortIncompleteMultipartUpload"; 0 disables the sweep).

```sh
export TAPEBERG_S3_KEY=... TAPEBERG_S3_SECRET=...
tapeberg s3                                        # serves :7333, SigV4
export AWS_ACCESS_KEY_ID=$TAPEBERG_S3_KEY AWS_SECRET_ACCESS_KEY=$TAPEBERG_S3_SECRET
AWS="aws --endpoint-url http://localhost:7333 --region us-east-1"
$AWS s3api list-buckets
$AWS s3api put-object --bucket tapeberg --key foo --body foo
$AWS s3api head-object --bucket tapeberg --key foo    # StorageClass: GLACIER once archived
$AWS s3api get-object --bucket tapeberg --key foo f1  # 403 InvalidObjectState
$AWS s3api restore-object --bucket tapeberg --key foo --restore-request Days=2
# after a worker pass: get-object succeeds, head-object shows ongoing-request="false"
```

## Status

Works (e2e-tested on an mhvtl LTO-5 rig, and run on a real LTO-5):
ingest → tape → GLACIER → restore → pin expiry; EOM retry and
tape swaps; the S3 Glacier contract via awscli and s3cmd (SigV4,
InvalidObjectState, RestoreObject, x-amz-restore); multipart uploads;
soft delete + repack; RAIT erasure coding with parallel multi-drive
writes; users, keys and tenancy; manual changer with Telegram/webhook
alerts; `doctor`.

Not yet: versioning, presigned URLs, DeleteBucket, CLI management of
pools/buckets/tapes, sharing one catalog between nodes — see
[docs/model.md](docs/model.md#caveats-known-gaps).
