# Tapeberg dev rig — mhvtl on Ubuntu 24.04

The virtual tape library for Tapeberg development. No physical library
needed: mhvtl emulates LTO drives and a changer over the real SCSI stack,
so `mtx`, `mt`, `sg_*` and Tapeberg's own code exercise the genuine path.

## What's on this host

- Real drive: **TANDBERG LTO-5 HH** (fw Z629) — `/dev/st0`, `/dev/nst0`,
  generic node `/dev/sg1`, group `tape`.
- VTL: one emulated library ("SPECTRA PYTHON", serial TAPEBERG1) with
  **three emulated IBM ULT3580-TD5 (LTO-5) drives** — serials
  TB-DRIVE01/02/03, `/dev/nst1..nst3`, DTEs 0-2 — so RAIT shards
  stripe in parallel, plus 16 data tapes (barcodes `TB0001L5`..
  `TB0016L5`) and 1 cleaning tape.
- mhvtl 1.8.0 (git 2c40a93) built from source, kernel module `mhvtl.ko`
  installed for the running kernel and loaded at boot via systemd units
  (`mhvtl.target`, `vtllibrary@10`, `vtltape@11`/`12`/`13`).
  Drive count gotcha: the changer's DTE count comes from the
  `Drive N:` sections in **library_contents** (not just device.conf) —
  add `Drive 2:`/`Drive 3:` there or mtx reports one DTE. And
  `mtx unload` is `unload [<slot>] [<drive>]` — the slot arg defaults
  to drive 0's origin, so per-drive unloads must spell out the origin
  slot (parsed from the element status line).
- User `major` is in the `tape` group for unprivileged drive access
  (new sessions only; the changer node is group `cdrom`).

## Source / build (Ubuntu 24.04)

Repo: https://github.com/markh794/mhvtl  (the old `mhvtl/mhvtl` path is dead)

```sh
sudo apt-get install -y build-essential git pkg-config \
  linux-headers-$(uname -r) zlib1g-dev liblzo2-dev \
  libudev-dev liblz4-dev libzstd-dev
git clone --depth 1 https://github.com/markh794/mhvtl.git
cd mhvtl
make -j$(nproc)          # builds kernel/ and usr/
sudo make install        # installs .ko, daemons, systemd units
sudo systemctl daemon-reload
```

Gotchas found the hard way:
- `libudev-dev` is required (mhvtl-bay-monitor includes `<libudev.h>`).
- `liblz4-dev` is required for the codec path (`lz4.h` in `mhvtl_io.c`).
- If a `sudo make install` half-fails, the tree is left root-owned and
  the next user-space build dies with a linker "Permission denied" —
  `sudo chown -R $(id -u):$(id -g) .` and rebuild.
- Secure Boot off on this VM, so the unsigned module loads fine.

## Config

`deploy/mhvtl/` in this repo is the source of truth: `device.conf`
(library + drive), `library_contents.10` (media), `mhvtl.conf` (runtime).
Deploy:

```sh
sudo cp deploy/mhvtl/device.conf /etc/mhvtl/device.conf
sudo cp deploy/mhvtl/library_contents.10 /etc/mhvtl/library_contents.10
sudo cp deploy/mhvtl/mhvtl.conf /etc/mhvtl/mhvtl.conf
sudo rm -f /etc/mhvtl/library_contents.30   # stock sample library
sudo systemctl stop mhvtl.target            # media is created at gen time,
sudo rm -rf /var/lib/mhvtl/*                # so regenerate it after
sudo make_vtl_media                         # changing CAPACITY
sudo systemctl enable --now mhvtl.target
```

Media capacity is deliberately LOW: `CAPACITY=100` (MB per tape) forces
frequent EOM / tape-swap events, which is exactly what this rig is for.
Media data files only grow as bytes are actually written (17 tapes cost
at most ~1.7 GB). Bump to 1500000 to emulate real LTO-5 (1.5 TB).

## Rig quirks (verified empirically — respect these in Tapeberg code)

- **Tape capacity is accounted post-compression.** The emulated drive
  stores lzo-compressed data, and EOM fires on *stored* bytes: 131 MB of
  zeros fit on a "100 MB" tape with no EOM, while urandom hit EOM at
  exactly 102,367,232 bytes (1,562 x 64KiB blocks). **Use incompressible
  test data (`/dev/urandom`) whenever an EOM/swap test must trigger.**
- **Single write() calls larger than ~512 KiB return `EBUSY`** from the
  st driver against the emulated drive (verified with strace: open()
  succeeds, the write syscall fails before any SCSI WRITE is issued;
  64k and 512k writes stream at ~85-480 MB/s, 768k/896k/1M fail).
  Tapeberg's tape writer should chunk at 256k-512k, not rely on big
  single writes.
- **The st driver appends one filemark automatically on a write-mode
  close.** Adding an explicit `mt weof 1` on top produces DOUBLE
  filemarks — and a restore that seeks into the empty inter-filemark
  gap reads zero bytes (sha256 of empty input). Writers must rely on
  the auto-filemark, not add their own.
- **Virtual media must be erased to reclaim capacity** (`mtx load` +
  `mt erase`): data files persist across mounts and daemon restarts,
  so old test data keeps a "full" tape full. `mt erase` on mhvtl is
  instant and restores full capacity.
- **Group memberships apply per process tree, not per login.**
  Shells spawned by a long-lived daemon (e.g. the agent gateway) keep
  the credentials the daemon started with — adding a user to the
  `tape` group won't show up until the daemon restarts. Use
  `sudo -u <user>` (which builds fresh supplementary groups from the
  group DB) to run rig commands with new memberships.
- **Catalog and media must be reset together in tests.** Deleting
  `tape_files`/`objects` rows without erasing the physical media leaves
  the catalog's filemark placement out of sync with the tape — a later
  archive gets a new physical filemark index while the catalog reuses a
  freed one, and restores read the WRONG bytes (caught as a checksum
  mismatch). Test resets erase media (`mt erase`) and reset tape rows
  in the same step.
- **`/dev/sgN` numbering shuffles across mhvtl restarts** (changer and
  drive swapped sg2/sg3 after one restart). Never hardcode sgN —
  discover via `lsscsi -g` at runtime, or address the changer via the
  type-stable `/dev/sch0`.
- After `mtx load`, give the drive a moment (or poll `mt status`) before
  the first write; an immediate write can race the mount.

## Operating the VTL

```sh
lsscsi -g                     # find the mediumx /dev/sgN and drive
mtx -f /dev/sgN status        # slot map
mtx -f /dev/sgN load 1        # load tape 1 into drive 0
mtx -f /dev/sgN unload        # back to its slot
sudo vtlcmd <naa> help        # daemon control/debug
sudo journalctl -u vtltape@11 -u vtllibrary@10   # logs
```

The emulated drive appears as a new `/dev/stN` (+ `/dev/nstN`, `/dev/sgM`);
identify it with `lsscsi -g` (it reports as IBM ULT3580-TD5).
