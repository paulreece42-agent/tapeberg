# Tapeberg dev rig — mhvtl on Ubuntu 24.04

The virtual tape library for Tapeberg development. No physical library
needed: mhvtl emulates LTO drives and a changer over the real SCSI stack,
so `mtx`, `mt`, `sg_*` and Tapeberg's own code exercise the genuine path.

## What's on this host

- Real drive: **TANDBERG LTO-5 HH** (fw Z629) — `/dev/st0`, `/dev/nst0`,
  generic node `/dev/sg1`, group `tape`.
- VTL: one emulated library ("SPECTRA PYTHON", serial TAPEBERG1) with a
  single emulated **IBM ULT3580-TD5** (LTO-5) drive, serial TB-DRIVE01,
  plus 16 data tapes (barcodes `TB0001L5`..`TB0016L5`) and 1 cleaning tape.
- mhvtl 1.8.0 (git 2c40a93) built from source, kernel module `mhvtl.ko`
  installed for the running kernel and loaded at boot via systemd units.

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
- `liblz4-dev` is required for the new codec path (`lz4.h`, `mhvtl_io.c`).
- If a `sudo make install` half-fails, the tree is left root-owned and
  the next user-space build dies with a linker "Permission denied" —
  `sudo chown -R $(id -u):$(id -g) .` and rebuild.
- Secure Boot off on this VM, so the unsigned module loads fine.

## Config

`deploy/mhvtl/device.conf` and `deploy/mhvtl/library_contents.10` in this
repo are the source of truth. Deploy:

```sh
sudo cp deploy/mhvtl/device.conf /etc/mhvtl/device.conf
sudo cp deploy/mhvtl/library_contents.10 /etc/mhvtl/library_contents.10
sudo rm -f /etc/mhvtl/library_contents.30   # stock sample library
sudo make_vtl_media                        # creates /var/lib/mhvtl media
sudo systemctl enable --now mhvtl.target
```

Default media capacity (500 MB per tape) is set in `/etc/mhvtl/mhvtl.conf`
(`CAPACITY=500`) — small on purpose: EOM/early-warning handling is testable
in seconds. Raise it when we want realistic long streams.

## Operating the VTL

```sh
lsscsi -g                     # find the mediumx /dev/sgN and drive
mtx -f /dev/sgN status        # slot map
mtx -f /dev/sgN load 1       # load tape 1 into drive 1
mtx -f /dev/sgN unload        # back to its slot
sudo vtlcmd <naa> help        # daemon control/debug
sudo journalctl -u vtltape@11 -u vtllibrary@10   # logs
```

The emulated drive appears as a new `/dev/stN` (+ `/dev/nstN`, `/dev/sgM`);
identify it with `lsscsi -g` (it reports as IBM ULT3580-TD5).
