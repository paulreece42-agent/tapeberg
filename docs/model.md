# Pools, buckets, tapes and hardware

How Tapeberg decides which tapes an object lands on, and which drive
reads them back. Read this before adding a pool, a library, or a
second machine.

## Two layers

Tapeberg splits its world in two, and the split is deliberate:

| Layer | Lives in | Holds | Scope |
|---|---|---|---|
| **Logical** | the catalog (PostgreSQL) | pools, tapes and their pool membership, buckets and their pool binding, objects, EC policy, the archive/restore queues | shareable — one catalog can eventually serve several nodes |
| **Physical** | the node config (`/etc/tapeberg.conf` + `TAPEBERG_*` env) | tape devices, the changer, which pool this process serves, the staging buffer | per machine (per process, really) |

Hardware never goes in the catalog: device paths like `/dev/nst0` and
`/dev/sch0` only mean something on the machine that owns them, so a
catalog shared between nodes cannot hold them. The catalog says *what
belongs together*; each node's config says *what this box can reach*.

```
            catalog (logical)                    node config (physical)
  ┌────────────────────────────────────┐      ┌──────────────────────────┐
  │ bucket ──binds──▶ pool ◀── tape    │      │ TAPEBERG_POOL   = vtl    │
  │                    │  (member of)  │◀─────│ TAPEBERG_DRIVE  = nst1   │
  │                    └─ EC policy    │serves│ TAPEBERG_DRIVES = nst1-3 │
  │ archive/restore requests carry a   │      │ TAPEBERG_CHANGER = robot │
  │ pool; workers drain only their own │      │ TAPEBERG_CHANGER_DEV     │
  └────────────────────────────────────┘      └──────────────────────────┘
```

## Who decides each link

| Link | Stored in | Set by | How to change it today |
|---|---|---|---|
| bucket → pool | `buckets.pool` | S3 **CreateBucket**: the bucket binds to the creating endpoint's `TAPEBERG_POOL`. The CLI's own bucket, `tapeberg`, is seeded into `default`. | No CLI yet — SQL only (see caveats before doing it) |
| tape → pool | `tapes.pool_id` | `tapeberg tape add` puts the tape in the invoking environment's `TAPEBERG_POOL` | No CLI yet — SQL only |
| pool → EC policy | `tape_pools.ec_k/ec_m` | `tapeberg pool set <pool> --ec-k K --ec-m M` / `--no-ec` | CLI |
| pool → drives/library | node config | a worker process serves exactly the pool in its `TAPEBERG_POOL`, using its `TAPEBERG_DRIVE(S)` and changer | edit that process's config |
| pool itself | `tape_pools` | seed (`default`) and migration 0004 (`vtl`) | No CLI yet — SQL only |

## The rules the code follows

- **One worker process = one pool = one library.** `tapeberg worker`
  drains only requests whose `pool` equals its `TAPEBERG_POOL`, and
  picks tapes only from that pool. Nothing else connects a pool to
  hardware.
- **Archive requests are stamped with a pool when queued.** S3
  PutObject and multipart parts use the **bucket's** pool. CLI
  `tapeberg ingest` uses the shell's **`TAPEBERG_POOL`** — not the
  `tapeberg` bucket's pool (see caveats).
- **Restore requests are stamped the same way.** S3 RestoreObject uses
  the bucket's current pool; CLI `tapeberg restore` uses
  `TAPEBERG_POOL`. Neither yet looks at where the object physically
  is (see caveats).
- **Every tape in a pool must be reachable by the library serving
  it.** Robot mode: the tape sits in that changer's slots (Tapeberg
  finds it by barcode with `mtx status`). Manual mode: a human can
  fetch it when the worker pings.
- **Drive roles:**
  - `TAPEBERG_DRIVE` is the drive for single-stream I/O — every
    non-EC archive, every restore, every repack read. In robot mode it
    must be the **DTE 0** device: Tapeberg loads with `mtx load <slot>`,
    which targets data transfer element 0.
  - `TAPEBERG_DRIVES` lists *all* the library's drives in DTE order
    (`/dev/nst1,/dev/nst2,/dev/nst3` = DTE 0,1,2). It is used only for
    RAIT shard writes, one shard stream per drive. Its first entry
    should be the same device as `TAPEBERG_DRIVE`. Unset = single
    drive, shards written one after another.
- **The S3 endpoint is not tied to one pool for writes.** It accepts
  PutObject for buckets in *any* pool; its `TAPEBERG_POOL` only
  decides where **new** buckets bind.
- **The worker lock is per buffer directory**
  (`$TAPEBERG_BUFFER/.worker.lock`). Two workers sharing a buffer
  serialise; workers with separate buffers run in parallel.
- **Staged files are referenced by absolute path** (`objects.disk_path`,
  `object_parts.disk_path`). Any process *on the same machine* can
  read what another staged, whatever its own `TAPEBERG_BUFFER`.

## Recipes

### Add a pool

There is no `pool add` yet:

```sh
psql "$TAPEBERG_DB_URL" -c "INSERT INTO tape_pools (name, description) VALUES ('offsite', 'second copy set')"
TAPEBERG_POOL=offsite tapeberg tape add TB0101L5 1500GB    # repeat per tape
tapeberg pool ls
```

Then give the pool a worker (next recipe), and either bind a bucket to
it (create the bucket through an endpoint running with
`TAPEBERG_POOL=offsite`) or ingest into it from the CLI with
`TAPEBERG_POOL=offsite`.

### Two libraries on one node

The reference rig does this: the real LTO-5 (manual changer, pool
`default`) and the mhvtl VTL (robot, three drives, pool `vtl`). Give
each library its own conf file **and its own buffer directory**, and one
cron line each:

```ini
# /etc/tapeberg.conf — the real drive
db_url  = postgres://tapeberg:***@127.0.0.1:5432/tapeberg
pool    = default
drive   = /dev/tape/by-id/scsi-HU1325W88B-nst
changer = manual
buffer  = /var/lib/tapeberg
```

```ini
# /etc/tapeberg-vtl.conf — the VTL (DTE 0/1/2 = TB-DRIVE01/02/03)
db_url      = postgres://tapeberg:***@127.0.0.1:5432/tapeberg
pool        = vtl
drive       = /dev/tape/by-id/scsi-TB-DRIVE01-nst
drives      = /dev/tape/by-id/scsi-TB-DRIVE01-nst,/dev/tape/by-id/scsi-TB-DRIVE02-nst,/dev/tape/by-id/scsi-TB-DRIVE03-nst
changer     = robot
changer_dev = /dev/tape/by-id/scsi-SSPECTRA_PYTHON_TAPEBERG1-changer
buffer      = /var/lib/tapeberg-vtl
```

Name devices by their `/dev/tape/by-id/` serial links, never
`/dev/nstN` / `/dev/sgN`: kernel numbering reshuffles across reboots,
and a stale number silently means a different drive
([config.md](config.md#stable-device-names)).

```cron
* * * * * root /usr/local/bin/tapeberg worker >>/var/log/tapeberg.log 2>&1
* * * * * root TAPEBERG_CONF=/etc/tapeberg-vtl.conf /usr/local/bin/tapeberg worker >>/var/log/tapeberg-vtl.log 2>&1
```

`TAPEBERG_CONF` *replaces* the default conf search path, so the VTL
worker never reads `/etc/tapeberg.conf`. Separate buffers keep the two
workers from blocking each other on the buffer lock. One S3 endpoint is
enough for both pools (see the rules above).

### Bind a bucket to a different pool

New bucket: create it through an S3 endpoint whose `TAPEBERG_POOL` is
the target pool. Existing bucket: SQL only
(`UPDATE buckets SET pool = 'offsite' WHERE name = 'photos'`). That
changes where **new** archives go; objects already on tape stay on the
old pool's tapes. **Don't rebind a bucket that already holds archived
objects yet** — see the restore caveat below.

## Caveats (known gaps)

- **Restores trust the request's pool, not the object's placement.**
  A restore is queued in the bucket's current pool (S3) or the shell's
  `TAPEBERG_POOL` (CLI). If the object's tapes are in a *different*
  pool, the wrong worker picks the request up and tries to load a tape
  its library doesn't hold. This happens to objects CLI-ingested with
  a non-default `TAPEBERG_POOL` (they live in the `tapeberg` bucket,
  bound to `default`) and to buckets rebound after archiving.
- **CLI ingest ignores the bucket's pool**; it uses `TAPEBERG_POOL`, so
  the `tapeberg` bucket can end up spanning several pools.
- **Pool, bucket and tape management is SQL-only** beyond `pool ls`,
  `pool set` and `tape add`: no pool create/remove, no bucket
  create/list/rebind/delete from the CLI, no tape list/move/state
  change. `tapeberg pool set <unknown> --no-ec` reports success
  without changing anything.
- **Retired tapes can't be re-added.** `tape add` refuses an existing
  barcode, so "erase and `tape add` again" fails with *already
  registered*. Until there is a CLI for it, re-activate by SQL — only
  for a tape repack has `retired` (so no *live* object is left on it),
  and only after erasing the media (`mt erase`):

  ```sql
  BEGIN;
  -- placements of deleted objects still point at the old bytes
  DELETE FROM tape_files tf USING objects o, tapes t
   WHERE tf.object_id = o.id AND tf.tape_id = t.id
     AND t.barcode = 'TB0007L5' AND o.deleted_at IS NOT NULL;
  UPDATE tapes SET state = 'active', written_bytes = 0, updated_at = now()
   WHERE barcode = 'TB0007L5' AND state = 'retired';
  COMMIT;
  ```
- **One library per process.** A node with several libraries needs a
  conf file and a cron line per library (recipe above).

## Sharing one catalog between nodes (not yet safe)

The logical/physical split is what makes a shared catalog possible,
but three things still assume a single node:

- **Queue claims are not atomic.** A worker reads pending requests,
  then marks them `assigned` with a plain `UPDATE`; the only mutual
  exclusion is the node-local buffer lock. Two nodes serving the same
  pool would process the same request twice.
- **Staged and restored copies are node-local.** `disk_path` is an
  absolute path on whichever machine staged or restored the object; a
  second node's endpoint would try to open a path that doesn't exist
  there.
- **Migration state is unreliable** (see `docs/database.md`), so nodes
  can't check they agree on the schema.

Until those are fixed, give each pool exactly one node.
