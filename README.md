# Tapeberg

**Work in progress.** Personal AWS-Glacier-style cold archive on your own
LTO tapes. Go + PostgreSQL + cron. Drives an autochanger robot via `mtx`,
or runs in **manual mode**: it pings you on Telegram/a webhook — "load
tape TB0003L5" — and waits for `tapeberg ack`.

## Set up

Config: any `TAPEBERG_*` env var can live in a conf file instead —
`/etc/tapeberg.conf` or `~/.config/tapeberg.conf` (or `$TAPEBERG_CONF`),
`key = value` lines, keys minus the prefix. Env vars override the file;
chmod 600 (it carries credentials):

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
tapeberg status
tapeberg ack TB0003L5             # manual changer: "tape is in the drive"
```

## S3 (awscli)

Buckets bind to tape pools: CreateBucket binds the serving rig's pool
(`TAPEBERG_POOL`), and PutObject/RestoreObject queue in the bucket's
pool — so bucket = pool = tape set. `tapeberg doctor` reports the
catalog's buckets.

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

Multipart upload is not supported yet — use `s3api put-object` (or raise
awscli's multipart threshold).

## Status

Works (e2e-tested on an mhvtl LTO-5 rig): ingest → tape → GLACIER →
restore → pin expiry; EOM retry and tape swaps; the S3 Glacier contract
via awscli (SigV4, InvalidObjectState, RestoreObject, x-amz-restore);
manual changer with Telegram/webhook alerts; `doctor`.

Not yet: multipart, DeleteObject, versioning, presigned URLs, repack.
