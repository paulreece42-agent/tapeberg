#!/usr/bin/env bash
# Tapeberg end-to-end test against the mhvtl dev rig.
#
# Exercises the full basic loop:
#   ingest -> worker archive (incl. a tape swap + EOM retry)
#   -> restore from tape 1 (filemark seek) and tape 2
#   -> byte-identical get
#   -> pin expiry sweep back to GLACIER
#   -> manual-changer mode with a scripted "human" ack
#
# Prereqs: mhvtl.target up, PostgreSQL catalog migrated+seeded, .env
# with TAPEBERG_DB_URL, lsscsi/mtx/mt in PATH, go toolchain.

set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d /tmp/tapeberg-e2e.XXXXXX)
BIN="$WORK/tapeberg"
trap 'rc=$?; rm -rf "$WORK"; scratch_catalog_exit $rc' EXIT

say() { printf '\n=== %s ===\n' "$*"; }
fail() { printf '\nE2E FAIL: %s\n' "$*" >&2; exit 1; }
. "$ROOT/test/scratch-catalog.sh"

say "building tapeberg"
cd "$ROOT"
go build -o "$BIN" . || fail "build failed"

say "discovering VTL devices"
CHANGER_DEV=$(lsscsi -g | awk '$2=="mediumx"{print $NF}')
ST=$(lsscsi -g | awk '$2=="tape" && $3=="IBM"{print $(NF-1); exit}')
DRIVE=${ST/\/st/\/nst}
[ -n "$CHANGER_DEV" ] || fail "no mediumx device — is mhvtl up?"
[ -n "$DRIVE" ] || fail "no VTL tape drive found"
echo "changer=$CHANGER_DEV drive=$DRIVE"

set -a; . "$ROOT/.env"; set +a
# Hermetic: this test's "human" is a script, not a phone. Never read the
# host's /etc/tapeberg.conf, and strip notification credentials so the
# manual-mode section cannot ping a real human.
export TAPEBERG_CONF=/dev/null
unset TAPEBERG_TELEGRAM_TOKEN TAPEBERG_TELEGRAM_CHAT TAPEBERG_WEBHOOK_URL TAPEBERG_WEBHOOK_TOKEN
export TAPEBERG_DRIVE="$DRIVE"
export TAPEBERG_CHANGER=robot
export TAPEBERG_CHANGER_DEV="$CHANGER_DEV"
export TAPEBERG_POOL=vtl
export TAPEBERG_BUFFER="$WORK/buffer"
mkdir -p "$TAPEBERG_BUFFER"

say "isolating the catalog (scratch schema — the real catalog's rows are never touched)"
scratch_catalog_up
PSQL="psql $TAPEBERG_DB_URL -tAq"

say "blanking VTL media (erase prior test data — capacity persists otherwise)"
mtx -f "$CHANGER_DEV" unload >/dev/null 2>&1 || true  # tolerate a leftover load
for bc in TB0001L5 TB0002L5 TB0003L5; do
  slot=$(mtx -f "$CHANGER_DEV" status | grep "VolumeTag=$bc" | sed 's/.*Storage Element \([0-9]*\):Full.*/\1/')
  mtx -f "$CHANGER_DEV" load "$slot" >/dev/null
  sleep 1
  mt -f "$DRIVE" rewind
  mt -f "$DRIVE" erase
  mt -f "$DRIVE" rewind
  mtx -f "$CHANGER_DEV" unload >/dev/null
done

say "resetting catalog to a clean slate (vtl pool + e2e keys only — never the real rig's rows)"
$PSQL -c "DELETE FROM retrieve_requests WHERE object_id IN (SELECT id FROM objects WHERE key LIKE 'e2e-%');
DELETE FROM archive_requests WHERE object_id IN (SELECT id FROM objects WHERE key LIKE 'e2e-%');
DELETE FROM tape_files WHERE tape_id IN (SELECT id FROM tapes WHERE pool_id=(SELECT id FROM tape_pools WHERE name='vtl'));
UPDATE tapes SET state='active', written_bytes=0 WHERE pool_id=(SELECT id FROM tape_pools WHERE name='vtl');
UPDATE tape_pools SET ec_k=NULL, ec_m=NULL;
DELETE FROM objects WHERE key LIKE 'e2e-%';" >/dev/null

say "generating 5 x 20MiB incompressible files (forces EOM + tape swap)"
for i in 1 2 3 4 5; do
  dd if=/dev/urandom of="$WORK/src$i" bs=1M count=20 status=none
done

say "ingesting 5 objects"
for i in 1 2 3 4 5; do "$BIN" ingest "$WORK/src$i" "e2e-file$i"; done
p=$($PSQL -c "SELECT count(*) FROM archive_requests r JOIN objects o ON o.id=r.object_id WHERE r.state='pending' AND o.key LIKE 'e2e-%'")
[ "$p" = 5 ] || fail "expected 5 pending archive requests, got $p"

say "worker pass 1: drain the archive queue"
"$BIN" worker

n1=$($PSQL -c "SELECT count(*) FROM tape_files tf JOIN tapes t ON t.id=tf.tape_id WHERE t.barcode='TB0001L5'")
n2=$($PSQL -c "SELECT count(*) FROM tape_files tf JOIN tapes t ON t.id=tf.tape_id WHERE t.barcode='TB0002L5'")
[ "$n1" = 4 ] || fail "expected 4 objects on TB0001L5, got $n1"
[ "$n2" = 1 ] || fail "expected 1 object on TB0002L5, got $n2"
s=$($PSQL -c "SELECT state FROM tapes WHERE barcode='TB0001L5'")
[ "$s" = full ] || fail "TB0001L5 should be state=full after EOM, got '$s'"
w=$($PSQL -c "SELECT count(*) FROM archive_requests r JOIN objects o ON o.id=r.object_id WHERE r.state='written' AND o.key LIKE 'e2e-%'")
[ "$w" = 5 ] || fail "expected 5 written e2e requests, got $w"
g=$($PSQL -c "SELECT count(*) FROM objects WHERE storage_class='GLACIER' AND key LIKE 'e2e-%'")
[ "$g" = 5 ] || fail "expected 5 GLACIER objects, got $g"
[ ! -e "$TAPEBERG_BUFFER/e2e-file1" ] || fail "disk copies should be dropped after archive"
echo "4 objects on TB0001L5 (EOM-retried), 1 on TB0002L5 — swap verified"

say "worker pass 2: restore from tape 1 (fsf seek) and tape 2"
"$BIN" restore e2e-file1 3
"$BIN" restore e2e-file4 3   # archive_id 4: exercises mt fsf positioning
"$BIN" restore e2e-file5 3   # lives on TB0002L5
"$BIN" worker

for i in 1 4 5; do
  s=$($PSQL -c "SELECT state FROM retrieve_requests r JOIN objects o ON o.id=r.object_id WHERE o.key='e2e-file$i'")
  [ "$s" = restored ] || fail "e2e-file$i restore state=$s"
  "$BIN" get e2e-file$i > "$WORK/out$i"
  cmp "$WORK/src$i" "$WORK/out$i" || fail "e2e-file$i differs from source bytes"
done
echo "3 restores byte-identical to sources"

say "worker pass 3: pin expiry sweep"
$PSQL -c "UPDATE retrieve_requests SET expires_at = now() - interval '1 hour' WHERE object_id=(SELECT id FROM objects WHERE key='e2e-file1')" >/dev/null
"$BIN" worker
s=$($PSQL -c "SELECT storage_class FROM objects WHERE key='e2e-file1'")
[ "$s" = GLACIER ] || fail "e2e-file1 should return to GLACIER after pin expiry, got $s"
[ ! -e "$TAPEBERG_BUFFER/e2e-file1" ] || fail "expired disk copy should be removed"
rs=$($PSQL -c "SELECT state FROM retrieve_requests r JOIN objects o ON o.id=r.object_id WHERE o.key='e2e-file1'")
[ "$rs" = expired ] || fail "request state expected 'expired', got '$rs'"
echo "pin expired: file unpinned, object back to GLACIER"

say "manual-changer mode: scripted human plays the robot arm"
export TAPEBERG_CHANGER=manual
export TAPEBERG_ACK_DIR="$WORK/ack"
mkdir -p "$TAPEBERG_ACK_DIR"
"$BIN" restore e2e-file2 2

"$BIN" worker &
WPID=$!
for _ in $(seq 1 60); do
  [ -e "$TAPEBERG_ACK_DIR/prompt" ] && break
  sleep 1
done
[ -e "$TAPEBERG_ACK_DIR/prompt" ] || { kill $WPID 2>/dev/null || true; fail "no manual prompt appeared"; }
ACTION=$(awk -F': ' '/^ACTION/{print $2}' "$TAPEBERG_ACK_DIR/prompt")
TAPE=$(awk -F': ' '/^TAPE/{print $2}' "$TAPEBERG_ACK_DIR/prompt")
echo "prompt received: $ACTION $TAPE — human (script) acting"
if [ "$ACTION" = "load" ]; then
  slot=$(mtx -f "$CHANGER_DEV" status | grep "VolumeTag=$TAPE" | sed 's/.*Storage Element \([0-9]*\):Full.*/\1/')
  mtx -f "$CHANGER_DEV" load "$slot"
  "$BIN" ack "$TAPE"
fi
# Manual mode no longer prompts to unload — tapes stay in the drive
# between jobs. The worker should finish the restore and exit cleanly;
# a stale unload prompt would mean a regression.
wait $WPID || fail "manual-mode worker failed"
[ ! -e "$TAPEBERG_ACK_DIR/prompt" ] || fail "manual mode unexpectedly prompted to unload (tapes should stay put)"
"$BIN" get e2e-file2 > "$WORK/out2"
cmp "$WORK/src2" "$WORK/out2" || fail "e2e-file2 differs from source bytes"
echo "manual-changer restore verified (prompt -> ack -> restore byte-identical)"
unset TAPEBERG_CHANGER TAPEBERG_ACK_DIR

say "tapeberg status"
TAPEBERG_CHANGER=robot "$BIN" status

say "rm: soft delete from the CLI"
head -c 1048576 /dev/urandom > "$WORK/rm-test"
"$BIN" ingest "$WORK/rm-test" e2e-rm-test >/dev/null || fail "ingest e2e-rm-test failed"
"$BIN" rm e2e-rm-test >/dev/null || fail "rm failed"
if "$BIN" get e2e-rm-test >/dev/null 2>"$WORK/rmerr" 2>&1; then
  fail "get on a deleted key must fail"
fi
[ "$($PSQL -c "SELECT count(*) FROM archive_requests WHERE state='pending' AND object_id IN (SELECT id FROM objects WHERE key='e2e-rm-test')")" = 0 ] || fail "rm must cancel pending archive requests"

printf '\nE2E PASS — all basic functions verified\n'
