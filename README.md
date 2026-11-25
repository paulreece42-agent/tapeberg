# Tapeberg

A personal Glacier on your own tape. Tapeberg puts AWS-Glacier-shaped
cold-archive semantics in front of your own LTO drives — with an
autochanger robot when you have one, and **a human as the tape changer
when you don't**.

```
ingest → [disk buffer] → worker → tape (LTO) → ...
                                      ↓
get ← [disk buffer, pinned N days] ← worker ← restore
```

## How it behaves

- **ingest** stages a file in the disk buffer and queues it for tape.
- The **worker** (one-shot, meant for flock'd cron) writes queued files
  to tape — filling tapes in sequence, handling EOM by marking the tape
  full and moving on — then drops the disk copy: the object becomes
  tape-only (`GLACIER`).
- **restore** queues a Glacier-style job; the worker streams the file
  back from tape (verifying sha256 against the catalog) and pins the
  disk copy for N days (1–30, like RestoreObject).
- **get** refuses tape-only objects with `InvalidObjectState` until
  they are restored — Glacier's exact contract.
- A pin-expiry sweep returns lapsed restores to tape-only.
- **Changer modes:** `robot` drives an autochanger via `mtx` (barcode →
  slot, auto-discovered via `lsscsi`); `manual` writes a prompt file,
  pings stderr, and waits until you acknowledge with `tapeberg ack` —
  insert the tape yourself, single-drive homelab style.

## Quickstart (dev rig)

Requires the mhvtl rig (see `docs/dev-rig.md`), the PostgreSQL catalog
(`docs/database.md`), and `mtx`/`mt`/`lsscsi` on PATH.

```sh
go build -o tapeberg .

export TAPEBERG_DB_URL=...        # postgres://tapeberg:...@127.0.0.1/tapeberg
export TAPEBERG_DRIVE=/dev/nst1   # non-rewinding device
export TAPEBERG_BUFFER=/var/lib/tapeberg
export TAPEBERG_CHANGER=robot     # or: manual

./tapeberg ingest bigfile.tar
./tapeberg worker
./tapeberg status
./tapeberg get bigfile.tar        # → InvalidObjectState (GLACIER)
./tapeberg restore bigfile.tar 3
./tapeberg worker
./tapeberg get bigfile.tar > out.tar   # byte-identical (sha256 verified)
```

Run the whole loop against the virtual library with `./test/e2e.sh`
(includes a forced tape swap + EOM retry, filemark seeks, pin expiry,
and manual-changer mode).

## Manual-changer mode (the human is the robot arm)

```
$ tapeberg restore vacation-photos 7
$ tapeberg worker
MANUAL CHANGER: load TB0003L5 into /dev/nst0 — waiting for 'tapeberg ack TB0003L5'
```

Insert the tape, then:

```sh
tapeberg ack TB0003L5
# ... worker restores, then prompts:
MANUAL CHANGER: unload /dev/nst0 — waiting for 'tapeberg ack UNLOAD'
tapeberg ack UNLOAD
```

## Design

- **PostgreSQL catalog** owns state; **tape** owns bytes. State machines
  are TEXT + CHECK. Schema: `db/migrations/` (rationale in
  `docs/database.md`).
- **Go** for everything: single binary, `mt` for positioning (st's
  single-open rule), native chunked I/O on `/dev/nst*` — writes are
  chunked at 256 KiB because single writes above ~512 KiB fail against
  the emulated drive (rig-verified, see `docs/dev-rig.md`).
- Borrowed with credit: CTA's queue/tape-pool scheduling ideas and
  dCache's pin-as-restore model (`Days=N` maps to an expiry-swept pin).
- Cron + flock over a long-running daemon (see
  `deploy/tapeberg-worker.cron`): homelab-honest, restartable, logged.

## Status

v0 — proves the core loop end-to-end on a virtual LTO-5 library. Not
yet: S3 wire protocol, multipart, versioning, repack, drive-native
ioctls, concurrent workers.
