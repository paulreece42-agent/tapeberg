# Configuration reference

Every setting is a `TAPEBERG_*` environment variable. Any of them can
instead live in a conf file as `key = value`, where the key is the
variable name minus the `TAPEBERG_` prefix (case-insensitive; `-` and
`_` are interchangeable):

```ini
# /etc/tapeberg.conf   (chmod 600 — it carries credentials)
db_url  = postgres://tapeberg:***@127.0.0.1:5432/tapeberg
drive   = /dev/nst0
buffer  = /var/lib/tapeberg
changer = manual
```

**Lookup:** `/etc/tapeberg.conf`, then `~/.config/tapeberg.conf`; all
files that exist are read, in that order. Setting `TAPEBERG_CONF=<path>`
**replaces** that search list with the one file — that is how one
machine runs several workers with different hardware (see
[model.md](model.md#two-libraries-on-one-node)). A missing file is
currently skipped *silently*, the explicit one included: a typo in
`TAPEBERG_CONF` surfaces only as "missing environment: …" for the
required keys.

**Precedence:** environment > conf file > built-in default. A conf file
never overrides a variable already set in the environment, and when
two files set the same key the first one read wins.

Everything here is **per node**: device paths, the buffer, and which
pool a process serves. Logical settings (pools, buckets, EC policy)
live in the catalog — see [model.md](model.md).

## Catalog, hardware, buffer

| Key | Default | Meaning |
|---|---|---|
| `DB_URL` | — **required** | PostgreSQL URL of the catalog ([database.md](database.md)) |
| `DRIVE` | — **required** | Non-rewinding tape device (`/dev/nstN`) used for every single-stream read/write. Robot mode: must be the library's **DTE 0** drive. Required by *every* command (even `s3` and `user`), so set it on endpoint-only nodes too. |
| `DRIVES` | unset (= `DRIVE` only) | All of the library's drives, comma-separated, **in DTE order**. Used for parallel RAIT shard writes; the first entry should equal `DRIVE`. |
| `BUFFER` | — **required** | Disk staging directory. Also holds the worker lock (`.worker.lock`): workers sharing a buffer run one at a time. |
| `CHANGER` | `robot` | `robot` (autochanger via `mtx`) or `manual` (a human loads tapes when pinged, then runs `tapeberg ack`). |
| `CHANGER_DEV` | auto-discovered | Robot mode: changer device for `mtx -f`. Prefer `/dev/sch0` over `/dev/sgN` (sg numbers shuffle across restarts). |
| `ACK_DIR` | `/run/tapeberg` | Manual mode: where the worker writes its prompt and `tapeberg ack` writes the acknowledgement. |
| `POOL` | `default` | The tape pool this process serves. A **worker** drains only this pool's queues and writes only to this pool's tapes. The **S3 endpoint** binds *new* buckets to it. The **CLI** ingests, restores and `tape add`s into it. |

## Policy

| Key | Default | Meaning |
|---|---|---|
| `REPACK_THRESHOLD` | `50` | Percent of a tape's written bytes that must be dead (deleted objects) before the worker repacks it. |
| `UPLOAD_TTL_DAYS` | `7` | Multipart uploads neither completed nor aborted after this many days are tombstoned. `0` disables the sweep. |
| `PART_MIN_BYTES` | `5242880` (5 MiB) | Smallest accepted multipart part (all but the last), enforced at CompleteMultipartUpload. Raise it on real tape (64–128 MiB keeps LTO streaming). |
| `PART_MAX_BYTES` | `5368709120` (5 GiB) | Largest accepted part, rejected per UploadPart. |

## S3 endpoint

| Key | Default | Meaning |
|---|---|---|
| `S3_LISTEN` | `:7333` | Listen address for `tapeberg s3` (a positional argument overrides it). |
| `AUTH` | `builtin` | Auth backend. `builtin` = users and keys in the catalog. |
| `S3_KEY`, `S3_SECRET` | unset | Legacy static key pair, used only while the catalog has **no** users. With neither set and no users, the endpoint runs unsigned (dev mode). |
| `S3_DEBUG` | unset | Any value: log SigV4 canonical requests when signature checks fail. |

## Notifications (manual changer)

Any mix; all optional. `tapeberg doctor --notify-test` sends a test message.

| Key | Meaning |
|---|---|
| `TELEGRAM_TOKEN`, `TELEGRAM_CHAT` | Telegram bot token and chat id |
| `WEBHOOK_URL` | POSTs `{"text": ...}` (Slack incoming-webhook shape) |
| `WEBHOOK_TOKEN` | Optional bearer token for the webhook |

## Meta

| Key | Meaning |
|---|---|
| `CONF` | Path of the one conf file to read, replacing the default search list. Environment only — it can't be set from inside a conf file. |
