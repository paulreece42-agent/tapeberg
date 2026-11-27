#!/usr/bin/env bash
# Tapeberg S3/Glacier wire-protocol test against the mhvtl dev rig,
# driven by the AWS CLI (SigV4-signed requests).
#
# Verifies: list-buckets, put-object, head-object (storage class flip to
# GLACIER), get-object → InvalidObjectState (Glacier contract),
# restore-object → 202, ongoing-request header, restored get byte-identical.

set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d /tmp/tapeberg-s3e2e.XXXXXX)
BIN="$WORK/tapeberg"
trap 'rc=$?; rm -rf "$WORK"; [ -n "${SRV_PID:-}" ] && kill "$SRV_PID" 2>/dev/null; scratch_catalog_exit $rc' EXIT

say() { printf '\n=== %s ===\n' "$*"; }
fail() { printf '\nS3 E2E FAIL: %s\n' "$*" >&2; exit 1; }
. "$ROOT/test/scratch-catalog.sh"

# The test's own endpoint port — never the production endpoint's 7333,
# so the suite runs beside a live tapeberg-s3.service.
S3E2E_PORT=${S3E2E_PORT:-7391}
S3L="127.0.0.1:$S3E2E_PORT"

command -v aws >/dev/null || fail "awscli not installed"

say "building tapeberg"
cd "$ROOT"
go build -o "$BIN" . || fail "build failed"

CHANGER_DEV=$(lsscsi -g | awk '$2=="mediumx"{print $NF}')
ST=$(lsscsi -g | awk '$2=="tape" && $3=="IBM"{print $(NF-1); exit}')
DRIVE=${ST/\/st/\/nst}
# All three VTL drives, DTE order — RAIT shards stripe across them in
# parallel. lsscsi lists them in SCSI target order, which IS DTE order
# (verified: DTE 0/1/2 -> targets 1/2/3). Don't sort by device name: st
# numbering shuffles across mhvtl restarts (targets 1/2/3 have come up
# as st3/st1/st2), and a sorted list then writes each shard through a
# different drive than the one its tape was loaded into.
DRIVES=$(lsscsi -g | awk '$2=="tape" && $3=="IBM"{print $(NF-1)}' | sed 's:/st:/nst:' | paste -sd,)

set -a; . "$ROOT/.env"; set +a
# Hermetic: never read the host's /etc/tapeberg.conf (robot mode, no
# notification channels) — the conf file belongs to the real rig.
export TAPEBERG_CONF=/dev/null
unset TAPEBERG_TELEGRAM_TOKEN TAPEBERG_TELEGRAM_CHAT TAPEBERG_WEBHOOK_URL TAPEBERG_WEBHOOK_TOKEN
export TAPEBERG_DRIVE="$DRIVE" TAPEBERG_CHANGER=robot TAPEBERG_CHANGER_DEV="$CHANGER_DEV" TAPEBERG_POOL=vtl
export TAPEBERG_DRIVES="$DRIVES"
export TAPEBERG_BUFFER="$WORK/buffer"
export TAPEBERG_S3_KEY="tapeberg-test" TAPEBERG_S3_SECRET="tapeberg-test-secret"
mkdir -p "$TAPEBERG_BUFFER"

say "isolating the catalog (scratch schema — the real catalog's rows are never touched)"
scratch_catalog_up
PSQL="psql $TAPEBERG_DB_URL -tAq"
AWS="aws --endpoint-url http://$S3L --region us-east-1"

say "blanking VTL media + resetting catalog together (they MUST stay in sync)"
mtx -f "$CHANGER_DEV" unload >/dev/null 2>&1 || true  # tolerate a leftover load
for bc in TB0001L5 TB0002L5 TB0003L5 TB0004L5 TB0005L5 TB0006L5 TB0007L5 TB0008L5 TB0009L5 TB0010L5 TB0011L5 TB0012L5 TB0013L5 TB0014L5 TB0015L5 TB0016L5; do
  slot=$(mtx -f "$CHANGER_DEV" status | grep "VolumeTag=$bc" | sed 's/.*Storage Element \([0-9]*\):Full.*/\1/')
  mtx -f "$CHANGER_DEV" load "$slot" >/dev/null
  sleep 1
  mt -f "$DRIVE" rewind
  mt -f "$DRIVE" erase
  mt -f "$DRIVE" rewind
  mtx -f "$CHANGER_DEV" unload >/dev/null
done
$PSQL -c "DELETE FROM retrieve_requests WHERE object_id IN (SELECT id FROM objects WHERE bucket_id IN (SELECT id FROM buckets WHERE name='alice-bkt'));
DELETE FROM archive_requests WHERE object_id IN (SELECT id FROM objects WHERE bucket_id IN (SELECT id FROM buckets WHERE name='alice-bkt'));
DELETE FROM tape_files WHERE object_id IN (SELECT id FROM objects WHERE bucket_id IN (SELECT id FROM buckets WHERE name='alice-bkt'));
DELETE FROM objects WHERE bucket_id IN (SELECT id FROM buckets WHERE name='alice-bkt');
DELETE FROM buckets WHERE name='alice-bkt';
DELETE FROM s3_users;
DELETE FROM retrieve_requests WHERE object_id IN (SELECT id FROM objects WHERE key LIKE 's3e2e%');
DELETE FROM archive_requests WHERE object_id IN (SELECT id FROM objects WHERE key LIKE 's3e2e%');
DELETE FROM tape_files WHERE tape_id IN (SELECT id FROM tapes WHERE pool_id=(SELECT id FROM tape_pools WHERE name='vtl'));
UPDATE tapes SET state='active', written_bytes=0 WHERE pool_id=(SELECT id FROM tape_pools WHERE name='vtl');
UPDATE tape_pools SET ec_k=NULL, ec_m=NULL;
DELETE FROM objects WHERE key LIKE 's3e2e%';" >/dev/null

say "starting the S3 endpoint"
if ss -ltn 2>/dev/null | grep -q ":$S3E2E_PORT "; then
  fail "port $S3E2E_PORT already in use — set S3E2E_PORT to a free port"
fi
"$BIN" s3 "$S3L" &
SRV_PID=$!
for _ in $(seq 1 30); do curl -s -o /dev/null "http://$S3L/" && break; sleep 0.5; done
kill -0 $SRV_PID 2>/dev/null || fail "s3 endpoint died at startup"

export AWS_ACCESS_KEY_ID="$TAPEBERG_S3_KEY" AWS_SECRET_ACCESS_KEY="$TAPEBERG_S3_SECRET"

say "create-bucket: binds to this rig's pool (vtl)"
$AWS s3api create-bucket --bucket vtl || fail "create-bucket failed"
bp=$($PSQL -c "SELECT pool FROM buckets WHERE name='vtl'")
[ "$bp" = vtl ] || fail "bucket vtl should be bound to pool vtl, got '$bp'"

say "object keys never become filesystem paths (path-traversal regression)"
# Signed raw requests: curl --path-as-is keeps '..' exactly as an attacker
# would send it (awscli/botocore may normalize). The endpoint has no
# ServeMux, so nothing upstream cleans the path for us.
$AWS s3api create-bucket --bucket s3e2e-keys >/dev/null || fail "create-bucket s3e2e-keys"
SIGCURL=(curl -s --aws-sigv4 "aws:amz:us-east-1:s3" --user "$TAPEBERG_S3_KEY:$TAPEBERG_S3_SECRET")
# Control: a legit nested key, signed the same way, must work (and used
# to fail: staging at BUFFER/<key> needed the key's directories to exist).
code=$("${SIGCURL[@]}" -X PUT --data-binary 'nested-ok' -o "$WORK/nested.out" -w '%{http_code}' "http://$S3L/s3e2e-keys/dir/sub/file.txt")
[ "$code" = 200 ] || fail "signed PUT of nested key dir/sub/file.txt: HTTP $code $(cat "$WORK/nested.out")"
dp=$($PSQL -c "SELECT o.disk_path FROM objects o JOIN buckets b ON b.id=o.bucket_id WHERE b.name='s3e2e-keys' AND o.key='dir/sub/file.txt' AND o.deleted_at IS NULL")
case "$dp" in "$TAPEBERG_BUFFER"/obj-*) ;; *) fail "nested key staged at '$dp', want $TAPEBERG_BUFFER/obj-*";; esac
[ "$(cat "$dp")" = nested-ok ] || fail "staged bytes of the nested key differ"
for variant in "../escape-1" "a/../../escape-3"; do
  code=$("${SIGCURL[@]}" --path-as-is -X PUT --data-binary 'PWNED' -o "$WORK/trav.out" -w '%{http_code}' "http://$S3L/s3e2e-keys/$variant")
  [ "$code" = 400 ] && grep -q InvalidArgument "$WORK/trav.out" \
    || fail "signed traversal key '$variant' not refused with 400 InvalidArgument: HTTP $code $(cat "$WORK/trav.out")"
done
# The same attack against an UNSIGNED dev-mode endpoint — no users and no
# static key, which is how a fresh install (and the reference rig) runs.
# There auth lets everything through, so the key check is all that stands
# between a request and the filesystem; '%2F' variants (which signing
# canonicalizes differently) reach it too. Must run before any user exists.
DEVPORT=$((S3E2E_PORT + 2))
[ "$($PSQL -c "SELECT count(*) FROM s3_users")" = 0 ] || fail "dev-mode probe needs a catalog without users"
env -u TAPEBERG_S3_KEY -u TAPEBERG_S3_SECRET "$BIN" s3 "127.0.0.1:$DEVPORT" > "$WORK/devsrv.log" 2>&1 &
DEV_PID=$!
for _ in $(seq 1 30); do curl -s -o /dev/null "http://127.0.0.1:$DEVPORT/" && break; sleep 0.3; done
grep -q 'UNSIGNED dev mode' "$WORK/devsrv.log" || { kill $DEV_PID; fail "second endpoint is not in dev mode: $(cat "$WORK/devsrv.log")"; }
for variant in "../escape-4" "..%2Fescape-5" "a/../../escape-6" "a%2F..%2F..%2Fescape-7"; do
  code=$(curl -s --path-as-is -X PUT --data-binary 'PWNED' -o "$WORK/trav.out" -w '%{http_code}' "http://127.0.0.1:$DEVPORT/s3e2e-keys/$variant")
  if [ "$code" != 400 ] || ! grep -q InvalidArgument "$WORK/trav.out"; then
    kill $DEV_PID; fail "unsigned traversal key '$variant' not refused with 400 InvalidArgument: HTTP $code $(cat "$WORK/trav.out")"
  fi
done
kill $DEV_PID; wait $DEV_PID 2>/dev/null || true
for n in 1 3 4 5 6 7; do
  [ ! -e "$WORK/escape-$n" ] || fail "path traversal wrote $WORK/escape-$n"
done
[ "$($PSQL -c "SELECT count(*) FROM objects WHERE key LIKE '%..%'")" = 0 ] || fail "a traversal key reached the catalog"
$AWS s3api delete-object --bucket s3e2e-keys --key dir/sub/file.txt >/dev/null || fail "delete nested key"
echo "nested keys stage as obj-*; '../' keys refused before touching disk or catalog"

say "list-buckets"
$AWS s3api list-buckets --query "Buckets[].Name" --output text | grep -qw vtl || fail "bucket not listed"

say "put-object (5 MiB)"
dd if=/dev/urandom of="$WORK/obj" bs=1M count=5 status=none
ETAG=$($AWS s3api put-object --bucket vtl --key s3e2e-obj --body "$WORK/obj" --query ETag --output text)
[ -n "$ETAG" ] || fail "put-object returned no ETag"
echo "put ETag: $ETAG"
sha=$(md5sum "$WORK/obj" | cut -d' ' -f1)
[ "$ETAG" = "\"$sha\"" ] || fail "ETag does not match client-side md5 (S3 contract: single-put ETag is the content md5)"

say "head-object while disk-buffered (STANDARD)"
$AWS s3api head-object --bucket vtl --key s3e2e-obj > "$WORK/head1.json"
grep -q '"StorageClass"' "$WORK/head1.json" && fail "fresh object should not report a storage class"

say "worker: archive to tape"
"$BIN" worker
# The bucket binding must have queued the archive in pool vtl.
ap=$($PSQL -c "SELECT r.pool FROM archive_requests r JOIN objects o ON o.id=r.object_id WHERE o.key='s3e2e-obj' ORDER BY r.id DESC LIMIT 1")
[ "$ap" = vtl ] || fail "archive request should be pool vtl (bucket binding), got '$ap'"

say "head-object after archive (GLACIER)"
$AWS s3api head-object --bucket vtl --key s3e2e-obj > "$WORK/head2.json"
grep -q '"StorageClass": "GLACIER"' "$WORK/head2.json" || fail "archived object should report GLACIER"

say "get-object on tape-only object → InvalidObjectState (Glacier contract)"
if $AWS s3api get-object --bucket vtl --key s3e2e-obj "$WORK/out-early" 2>"$WORK/geterr"; then
  fail "get-object should have failed on a GLACIER object"
fi
grep -q "InvalidObjectState" "$WORK/geterr" || fail "expected InvalidObjectState, got: $(cat "$WORK/geterr")"
echo "refused with InvalidObjectState as expected"

say "restore-object (Days=2) → 202"
$AWS s3api restore-object --bucket vtl --key s3e2e-obj --restore-request Days=2 || fail "restore-object failed"

say "head-object during restore (ongoing-request=true)"
$AWS s3api head-object --bucket vtl --key s3e2e-obj > "$WORK/head3.json"
python3 - "$WORK/head3.json" <<'PYEOF' || fail "expected x-amz-restore ongoing-request=true"
import json, sys
r = json.load(open(sys.argv[1])).get("Restore", "")
exit(0 if 'ongoing-request="true"' in r else 1)
PYEOF

say "second restore → RestoreAlreadyInProgress (409)"
if $AWS s3api restore-object --bucket vtl --key s3e2e-obj --restore-request Days=2 2>"$WORK/restoreerr"; then
  echo "note: second restore accepted (200/202) rather than 409"
else
  grep -q "RestoreAlreadyInProgress" "$WORK/restoreerr" && echo "409 RestoreAlreadyInProgress as expected"
fi

say "worker: drain the restore"
"$BIN" worker

say "head-object after restore (ongoing-request=false, expiry)"
$AWS s3api head-object --bucket vtl --key s3e2e-obj > "$WORK/head4.json"
python3 - "$WORK/head4.json" <<'PYEOF' || fail "expected ongoing-request=false with expiry-date after restore"
import json, sys
r = json.load(open(sys.argv[1])).get("Restore", "")
exit(0 if 'ongoing-request="false"' in r and "expiry-date" in r else 1)
PYEOF

say "get-object restored → byte-identical"
$AWS s3api get-object --bucket vtl --key s3e2e-obj "$WORK/out" >/dev/null || fail "restored get-object failed"
cmp "$WORK/obj" "$WORK/out" || fail "restored bytes differ from upload"

say "list-objects-v2"
$AWS s3api list-objects-v2 --bucket vtl --prefix s3e2e --query "Contents[].Key" --output text | grep -q s3e2e-obj || fail "object not listed"

say "multipart: 240MB via awscli (bigger than any single 100MB tape)"
dd if=/dev/urandom of="$WORK/big" bs=1M count=240 status=none
$AWS s3 cp "$WORK/big" s3://vtl/s3e2e-mp-big >/dev/null || fail "multipart upload failed"
sz=$($AWS s3api head-object --bucket vtl --key s3e2e-mp-big --query ContentLength --output text)
[ "$sz" = 251658240 ] || fail "completed multipart size should be 240MiB, got $sz"

say "worker: 30 part archives across multiple tapes"
"$BIN" worker || fail "worker failed archiving parts"
sc=$($PSQL -c "SELECT storage_class FROM objects WHERE key='s3e2e-mp-big'")
[ "$sc" = GLACIER ] || fail "object should be GLACIER after all parts archived, got $sc"
ntapes=$($PSQL -c "SELECT count(DISTINCT t.barcode) FROM tape_files tf JOIN tapes t ON t.id=tf.tape_id JOIN objects o ON o.id=tf.object_id WHERE o.key='s3e2e-mp-big'")
[ "$ntapes" -ge 2 ] || fail "240MB should span multiple tapes, found $ntapes"

say "get on tape-only multipart → InvalidObjectState (Glacier contract)"
if $AWS s3api get-object --bucket vtl --key s3e2e-mp-big "$WORK/mp-early" 2>"$WORK/mperr"; then
  fail "get-object should have failed on a GLACIER multipart object"
fi
grep -q "InvalidObjectState" "$WORK/mperr" || fail "expected InvalidObjectState, got: $(cat "$WORK/mperr")"

say "restore multipart → 202; worker materializes all parts"
$AWS s3api restore-object --bucket vtl --key s3e2e-mp-big --restore-request Days=2 || fail "restore-object failed"
"$BIN" worker || fail "worker failed restoring parts"
rstate=$($PSQL -c "SELECT r.state FROM retrieve_requests r JOIN objects o ON o.id=r.object_id WHERE o.key='s3e2e-mp-big' ORDER BY r.id DESC LIMIT 1")
[ "$rstate" = restored ] || fail "multipart restore ended '$rstate': $($PSQL -c "SELECT left(COALESCE(r.error,''),160) FROM retrieve_requests r JOIN objects o ON o.id=r.object_id WHERE o.key='s3e2e-mp-big' ORDER BY r.id DESC LIMIT 1")"

say "multipart GET → byte-identical (the API-layer merge)"
$AWS s3 cp s3://vtl/s3e2e-mp-big "$WORK/mp-out" >/dev/null || fail "multipart get failed"
cmp "$WORK/big" "$WORK/mp-out" || fail "merged bytes differ from upload"

say "ranged GET into the middle of a multipart object"
$AWS s3api get-object --bucket vtl --key s3e2e-mp-big --range bytes=100000000-100010000 "$WORK/mp-range" >/dev/null || fail "ranged get failed"
dd if="$WORK/big" bs=1 skip=100000000 count=10001 status=none of="$WORK/mp-range-expected"
cmp "$WORK/mp-range-expected" "$WORK/mp-range" || fail "ranged merge differs"

say "tar-pipe: s3cmd multipart streaming upload"
mkdir -p "$WORK/tarsrc"
echo "tapeberg-tar-pipe-$RANDOM" > "$WORK/tarsrc/hello.txt"
dd if=/dev/urandom of="$WORK/tarsrc/blob.bin" bs=1M count=12 status=none
cat > "$WORK/s3cfg" <<CFG
[default]
access_key = $TAPEBERG_S3_KEY
secret_key = $TAPEBERG_S3_SECRET
host_base = $S3L
host_bucket = $S3L
use_https = False
enable_multipart = True
multipart_chunk_size_mb = 5
CFG
tar -C "$WORK/tarsrc" -cf - . | s3cmd --config "$WORK/s3cfg" put - s3://vtl/s3e2e-tar-pipe >/dev/null || fail "s3cmd streaming put failed"
"$BIN" worker || fail "worker failed archiving tar-pipe parts"
$AWS s3api restore-object --bucket vtl --key s3e2e-tar-pipe --restore-request Days=1 || fail "tar-pipe restore failed"
"$BIN" worker || fail "worker failed restoring tar-pipe"

say "tar-pipe read-back: s3cmd streaming get | tar -t"
s3cmd --config "$WORK/s3cfg" get s3://vtl/s3e2e-tar-pipe - 2>/dev/null | tar -tf - | grep -q hello.txt || fail "tar-pipe round-trip failed"

say "abort multipart upload: eager archives → tombstone, not data loss"
UPID=$($AWS s3api create-multipart-upload --bucket vtl --key s3e2e-mp-abort --query UploadId --output text)
$AWS s3api upload-part --bucket vtl --key s3e2e-mp-abort --upload-id "$UPID" --part-number 1 --body "$WORK/obj" >/dev/null || fail "upload-part failed"
# Run the worker: the part is EAGERLY archived (buffer disk reclaimed).
"$BIN" worker >/dev/null || fail "worker failed archiving the aborted upload's part"
[ -n "$($PSQL -c "SELECT 1 FROM tape_files tf JOIN objects o ON o.id=tf.object_id WHERE o.key='s3e2e-mp-abort'")" ] || fail "part should be on tape already (eager archive)"
PARTPATH=$($PSQL -c "SELECT disk_path FROM object_parts WHERE object_id=(SELECT id FROM objects WHERE key='s3e2e-mp-abort')")
[ "$PARTPATH" = "" ] || fail "archived part should have no disk copy (worker already reclaimed it)"
$AWS s3api abort-multipart-upload --bucket vtl --key s3e2e-mp-abort --upload-id "$UPID" || fail "abort failed"
if $AWS s3api head-object --bucket vtl --key s3e2e-mp-abort >/dev/null 2>&1; then
  fail "aborted upload should not be visible"
fi
# The abort left a tombstone: tape bytes stay (dead, repack reclaims),
# queued archives cancelled, nothing orphaned.
[ "$($PSQL -c "SELECT count(*) FROM objects WHERE key='s3e2e-mp-abort' AND deleted_at IS NOT NULL")" = 1 ] || fail "abort must tombstone the object"
[ "$($PSQL -c "SELECT count(*) FROM archive_requests a JOIN objects o ON o.id=a.object_id WHERE o.key='s3e2e-mp-abort' AND a.state IN ('pending','assigned')")" = 0 ] || fail "abort must cancel queued archives"
[ "$($PSQL -c "SELECT count(*) FROM tape_files tf JOIN objects o ON o.id=tf.object_id WHERE o.key='s3e2e-mp-abort'")" -ge 1 ] || fail "tombstone must keep the dead tape placements for repack accounting"
ABOID=$($PSQL -c "SELECT id FROM objects WHERE key='s3e2e-mp-abort'")
ls "$TAPEBERG_BUFFER"/part-"$ABOID"-* 2>/dev/null | grep -q . && fail "abort left buffered part files on disk"

# Undo the aborted part's tape footprint surgically (only ITS rows and
# bytes — the tape may hold live data) so later placement sections see
# the fill order they were designed around. Stale physical bytes stay
# on the tape; a mid-write EOM is a handled retry, not a failure.
$PSQL -c "UPDATE tapes SET written_bytes = written_bytes -
        (SELECT COALESCE(sum(size_bytes),0) FROM tape_files
          WHERE object_id=(SELECT id FROM objects WHERE key='s3e2e-mp-abort'))
        WHERE id IN (SELECT DISTINCT tape_id FROM tape_files
          WHERE object_id=(SELECT id FROM objects WHERE key='s3e2e-mp-abort'));
        DELETE FROM tape_files WHERE object_id=(SELECT id FROM objects WHERE key='s3e2e-mp-abort');
        DELETE FROM object_shards WHERE object_id=(SELECT id FROM objects WHERE key='s3e2e-mp-abort');" >/dev/null

say "multipart part size policy (min on complete, max on upload)"
# A second endpoint with tape-sized limits, so stock-client sections
# above keep the protocol 5MiB floor.
S3L2="127.0.0.1:7392"
TAPEBERG_S3_LISTEN="$S3L2" TAPEBERG_PART_MIN_BYTES=$((32*1024*1024)) TAPEBERG_PART_MAX_BYTES=$((48*1024*1024)) \
  "$BIN" s3 >/dev/null 2>&1 &
SRV2_PID=$!
sleep 1
AWS2="$AWS --endpoint-url http://$S3L2"
UP2=$($AWS2 s3api create-multipart-upload --bucket vtl --key s3e2e-partsize --query UploadId --output text)
head -c 16MiB /dev/urandom > "$WORK/ps-p1"; head -c 16MiB /dev/urandom > "$WORK/ps-p2"
$AWS2 s3api upload-part --bucket vtl --key s3e2e-partsize --upload-id "$UP2" --part-number 1 --body "$WORK/ps-p1" >/dev/null || fail "small part upload itself must be accepted (S3 defers to complete)"
$AWS2 s3api upload-part --bucket vtl --key s3e2e-partsize --upload-id "$UP2" --part-number 2 --body "$WORK/ps-p2" >/dev/null || fail "small final part upload must be accepted"
E1=$($AWS2 s3api complete-multipart-upload --bucket vtl --key s3e2e-partsize --upload-id "$UP2" --multipart-upload "Parts=[{ETag=$($AWS2 s3api list-parts --bucket vtl --key s3e2e-partsize --upload-id "$UP2" --query 'Parts[0].ETag' --output text),PartNumber=1},{ETag=$($AWS2 s3api list-parts --bucket vtl --key s3e2e-partsize --upload-id "$UP2" --query 'Parts[1].ETag' --output text),PartNumber=2}]" 2>&1) && fail "complete must reject undersized non-final part"
echo "$E1" | grep -q EntityTooSmall || fail "expected EntityTooSmall, got: $E1"
head -c 49MiB /dev/urandom > "$WORK/ps-big"
E2=$($AWS2 s3api upload-part --bucket vtl --key s3e2e-partsize --upload-id "$UP2" --part-number 3 --body "$WORK/ps-big" 2>&1) && fail "oversized part must be rejected at upload"
echo "$E2" | grep -q EntityTooLarge || fail "expected EntityTooLarge, got: $E2"
# The accepted (but never completable) parts are queued for eager
# archive — abort the upload to cancel them, or they'd land on tape and
# skew every later placement assertion.
$AWS2 s3api abort-multipart-upload --bucket vtl --key s3e2e-partsize --upload-id "$UP2" || fail "abort of the partsize upload failed"
[ "$($PSQL -c "SELECT count(*) FROM archive_requests a JOIN objects o ON o.id=a.object_id WHERE o.key='s3e2e-partsize' AND a.state IN ('pending','assigned')")" = 0 ] || fail "partsize abort must cancel its queued part archives"
kill $SRV2_PID 2>/dev/null || true

say "lifecycle: abandoned multipart uploads expire (worker janitor)"
UPID3=$($AWS s3api create-multipart-upload --bucket vtl --key s3e2e-mp-stale --query UploadId --output text)
$AWS s3api upload-part --bucket vtl --key s3e2e-mp-stale --upload-id "$UPID3" --part-number 1 --body "$WORK/obj" >/dev/null || fail "upload-part (stale) failed"
UPID4=$($AWS s3api create-multipart-upload --bucket vtl --key s3e2e-mp-fresh --query UploadId --output text)
$AWS s3api upload-part --bucket vtl --key s3e2e-mp-fresh --upload-id "$UPID4" --part-number 1 --body "$WORK/obj" >/dev/null || fail "upload-part (fresh) failed"
# Age ONLY the stale one past the TTL (default 7 days).
$PSQL -c "UPDATE objects SET created_at = now() - interval '8 days' WHERE key='s3e2e-mp-stale'" >/dev/null
"$BIN" worker > "$WORK/janitor.log" 2>&1 || fail "worker failed running the upload janitor"
grep -q "s3e2e-mp-stale.*tombstoned" "$WORK/janitor.log" || fail "janitor did not expire the stale upload: $(tail -2 "$WORK/janitor.log")"
[ "$($PSQL -c "SELECT count(*) FROM objects WHERE key='s3e2e-mp-stale' AND deleted_at IS NOT NULL")" = 1 ] || fail "stale upload must be tombstoned"
[ "$($PSQL -c "SELECT count(*) FROM archive_requests a JOIN objects o ON o.id=a.object_id WHERE o.key='s3e2e-mp-stale' AND a.state IN ('pending','assigned')")" = 0 ] || fail "janitor must cancel the stale upload's queued archives"
[ "$($PSQL -c "SELECT count(*) FROM objects WHERE key='s3e2e-mp-fresh' AND deleted_at IS NULL AND upload_id IS NOT NULL")" = 1 ] || fail "fresh upload must NOT be expired"
ls "$TAPEBERG_BUFFER"/part-$(  $PSQL -c "SELECT id FROM objects WHERE key='s3e2e-mp-stale'")-* 2>/dev/null | grep -q . && fail "janitor left the stale upload's buffered part on disk"
$AWS s3api abort-multipart-upload --bucket vtl --key s3e2e-mp-fresh --upload-id "$UPID4" || fail "fresh upload cleanup abort failed"
# Same as the abort section: undo the stale part's tape footprint so
# later placement sections see the fill order they were designed for.
$PSQL -c "UPDATE tapes SET written_bytes = written_bytes -
        (SELECT COALESCE(sum(size_bytes),0) FROM tape_files
          WHERE object_id=(SELECT id FROM objects WHERE key='s3e2e-mp-stale'))
        WHERE id IN (SELECT DISTINCT tape_id FROM tape_files
          WHERE object_id=(SELECT id FROM objects WHERE key='s3e2e-mp-stale'));
        DELETE FROM tape_files WHERE object_id=(SELECT id FROM objects WHERE key='s3e2e-mp-stale');
        DELETE FROM object_shards WHERE object_id=(SELECT id FROM objects WHERE key='s3e2e-mp-stale');" >/dev/null

say "SigV4 is enforced (bad signature rejected)"
if AWS_SECRET_ACCESS_KEY="wrong-secret" $AWS s3api list-buckets >/dev/null 2>&1; then
  fail "bad credentials were accepted"
fi
echo "bad signature rejected"

say "S3 ETag: md5 on single put, 409 on live duplicate, delete unbricks the key"
ET=$($AWS s3api put-object --bucket vtl --key s3e2e-etag --body "$WORK/obj" --query ETag --output text | tr -d '"')
[ "$ET" = "$(md5sum "$WORK/obj" | awk '{print $1}')" ] || fail "single-put ETag must be the content md5 (got $ET, want $(md5sum "$WORK/obj" | awk '{print $1}'))"
H=$($AWS s3api head-object --bucket vtl --key s3e2e-etag --query ETag --output text | tr -d '"')
[ "$H" = "$ET" ] || fail "HEAD must present the same ETag (got $H)"
$AWS s3api put-object --bucket vtl --key s3e2e-etag --body "$WORK/obj" >/dev/null 2>&1 && fail "duplicate put must 409 (no overwrite)"
$AWS s3api delete-object --bucket vtl --key s3e2e-etag || fail "delete failed"
ET2=$($AWS s3api put-object --bucket vtl --key s3e2e-etag --body "$WORK/obj" --query ETag --output text | tr -d '"') || fail "delete-then-reput must work (was bricked by the v0 unique constraint)"
[ "$ET2" = "$ET" ] || fail "re-put ETag must match"
$AWS s3api delete-object --bucket vtl --key s3e2e-etag || fail "cleanup delete failed"

say "auth: builtin DB users, tenancy, key rotation (RGW-style)"
[ "$($PSQL -c "SELECT count(*) FROM s3_users")" = 0 ] || fail "expected no users at this point"
uout=$("$BIN" user add alice --full-name "Alice Example" --email alice@example.com 2>&1)
AK=$(echo "$uout" | awk '/^access_key:/{print $2}')
SK=$(echo "$uout" | awk '/^secret_key:/{print $2}')
[ -n "$AK" ] && [ -n "$SK" ] || fail "user add printed no keys"
if $AWS s3api list-buckets >/dev/null 2>&1; then
  fail "static env key must stop working once DB users exist"
fi
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK"
$AWS s3api create-bucket --bucket alice-bkt || fail "alice create-bucket failed"
$AWS s3api list-buckets --query "Buckets[].Name" --output text | grep -qw alice-bkt || fail "alice's bucket not listed"
if $AWS s3api list-buckets --query "Buckets[].Name" --output text | grep -qw vtl; then
  fail "alice must not see the system bucket"
fi
echo "tenancy" > "$WORK/hi"
$AWS s3api put-object --bucket alice-bkt --key alice-hi --body "$WORK/hi" >/dev/null || fail "alice put failed"
"$BIN" worker >/dev/null || fail "worker failed archiving alice's object"
$AWS s3api list-objects-v2 --bucket alice-bkt --query "Contents[].Key" --output text | grep -qw alice-hi || fail "alice cannot list her object"
if $AWS s3api list-objects-v2 --bucket vtl >/dev/null 2>&1; then
  fail "alice must not list another tenant's bucket"
fi
if $AWS s3api get-object --bucket vtl --key s3e2e-obj "$WORK/tx" >/dev/null 2>&1; then
  fail "alice must not read another tenant's object"
fi

say "suspend / unsuspend"
"$BIN" user suspend alice
if $AWS s3api list-buckets >/dev/null 2>&1; then
  fail "suspended user must be rejected"
fi
"$BIN" user unsuspend alice
$AWS s3api list-buckets >/dev/null || fail "unsuspend did not restore access"

say "key rotation (multiple keys per user)"
kout=$("$BIN" key add --uid alice 2>&1)
AK2=$(echo "$kout" | awk '/^access_key:/{print $2}')
SK2=$(echo "$kout" | awk '/^secret_key:/{print $2}')
[ -n "$AK2" ] && [ -n "$SK2" ] || fail "key add printed no keys"
export AWS_ACCESS_KEY_ID="$AK2" AWS_SECRET_ACCESS_KEY="$SK2"
$AWS s3api list-buckets >/dev/null || fail "second key must work"
"$BIN" key rm --access-key "$AK"
if AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" $AWS s3api list-buckets >/dev/null 2>&1; then
  fail "removed key must be rejected"
fi

say "admin sees everything"
aout=$("$BIN" user add siteadmin --full-name "Site Admin" --email admin@example.com --admin 2>&1)
ADM_AK=$(echo "$aout" | awk '/^access_key:/{print $2}')
ADM_SK=$(echo "$aout" | awk '/^secret_key:/{print $2}')
export AWS_ACCESS_KEY_ID="$ADM_AK" AWS_SECRET_ACCESS_KEY="$ADM_SK"
$AWS s3api list-buckets --query "Buckets[].Name" --output text | grep -qw vtl || fail "admin must see the system bucket"
$AWS s3api list-objects-v2 --bucket alice-bkt --query "Contents[].Key" --output text | grep -qw alice-hi || fail "admin must see alice's object"
$AWS s3api get-object --bucket vtl --key s3e2e-obj "$WORK/adminget" >/dev/null || fail "admin cross-tenant get failed"

say "user rm; static key returns"
"$BIN" user rm alice
"$BIN" user rm siteadmin
export AWS_ACCESS_KEY_ID="$TAPEBERG_S3_KEY" AWS_SECRET_ACCESS_KEY="$TAPEBERG_S3_SECRET"
$AWS s3api list-buckets >/dev/null || fail "static env key must work again once no users exist"

say "delete: tombstone semantics (S3 DeleteObject + multipart)"
# 50 + 36 MiB fits one tape's PHYSICAL capacity (~96 MiB for random
# data — nominal is 100) so the pair co-locates; deleting del-a leaves
# the tape 58% dead, over the repack threshold.
dd if=/dev/urandom of="$WORK/del-a" bs=1M count=50 status=none
dd if=/dev/urandom of="$WORK/del-b" bs=1M count=36 status=none
$AWS s3api put-object --bucket vtl --key s3e2e-del-a --body "$WORK/del-a" >/dev/null || fail "put del-a failed"
sleep 1 # distinct requested_at => strict FIFO (a tie once split the pair across tapes)
$AWS s3api put-object --bucket vtl --key s3e2e-del-b --body "$WORK/del-b" >/dev/null || fail "put del-b failed"
"$BIN" worker >/dev/null || fail "worker failed archiving del-a/del-b"
OLD=$($PSQL -c "SELECT t.barcode FROM tape_files tf JOIN tapes t ON t.id=tf.tape_id JOIN objects o ON o.id=tf.object_id WHERE o.key='s3e2e-del-b'")
OLD_A=$($PSQL -c "SELECT t.barcode FROM tape_files tf JOIN tapes t ON t.id=tf.tape_id JOIN objects o ON o.id=tf.object_id WHERE o.key='s3e2e-del-a'")
[ "$OLD" = "$OLD_A" ] || fail "test design: del-a ($OLD_A) and del-b ($OLD) must co-locate for the repack assertion"
$AWS s3api delete-object --bucket vtl --key s3e2e-del-a || fail "delete-object failed"
$AWS s3api delete-object --bucket vtl --key s3e2e-tar-pipe || fail "multipart delete-object failed"
if $AWS s3api head-object --bucket vtl --key s3e2e-del-a >/dev/null 2>&1; then
  fail "deleted object must 404"
fi
$AWS s3api delete-object --bucket vtl --key s3e2e-del-a || fail "delete must be idempotent (204)"
if $AWS s3api get-object --bucket vtl --key s3e2e-tar-pipe "$WORK/dead-mp" >/dev/null 2>&1; then
  fail "deleted multipart object must not be readable"
fi
if $AWS s3api list-objects-v2 --bucket vtl --prefix s3e2e-del --query "Contents[].Key" --output text | grep -qw s3e2e-del-a; then
  fail "deleted object must not be listed"
fi
$AWS s3api list-objects-v2 --bucket vtl --prefix s3e2e-del --query "Contents[].Key" --output text | grep -qw s3e2e-del-b || fail "surviving object must still be listed"

say "repack: the year-3 rewrite (>= 50% dead reclaims the tape)"
"$BIN" worker > "$WORK/repack.log" 2>&1 || fail "worker (repack pass) failed"
grep -q "repack" "$WORK/repack.log" || fail "repack did not trigger: $(grep repack "$WORK/repack.log")"
NEW=$($PSQL -c "SELECT t.barcode FROM tape_files tf JOIN tapes t ON t.id=tf.tape_id JOIN objects o ON o.id=tf.object_id WHERE o.key='s3e2e-del-b'")
[ -n "$NEW" ] && [ "$NEW" != "$OLD" ] || fail "del-b should have moved tapes ($OLD -> '$NEW')"
ST=$($PSQL -c "SELECT state FROM tapes WHERE barcode='$OLD'")
[ "$ST" = retired ] || fail "emptied tape should be retired, got $ST"
$AWS s3api restore-object --bucket vtl --key s3e2e-del-b --restore-request Days=1 || fail "restore of repacked object failed"
"$BIN" worker >/dev/null || fail "worker failed restoring repacked object"
$AWS s3api get-object --bucket vtl --key s3e2e-del-b "$WORK/del-b-out" >/dev/null || fail "get of repacked object failed"
cmp "$WORK/del-b" "$WORK/del-b-out" || fail "repacked bytes differ from upload"

say "erasure coding: RAIT 2+1 stripe across three tapes, three drives"
"$BIN" pool set vtl --ec-k 2 --ec-m 1 || fail "pool set --ec-k/--ec-m failed"
dd if=/dev/urandom of="$WORK/ec-src" bs=1M count=12 status=none
$AWS s3api put-object --bucket vtl --key s3e2e-ec --body "$WORK/ec-src" >/dev/null || fail "put ec object failed"
"$BIN" worker > "$WORK/ec.log" 2>&1 || fail "worker failed striping the EC object"
grep -q "RAIT 2+1" "$WORK/ec.log" || fail "worker did not RAIT-stripe: $(tail -2 "$WORK/ec.log")"
[ "$($PSQL -c "SELECT ec_k FROM objects WHERE key='s3e2e-ec'")" = 2 ] || fail "object did not freeze the EC policy"
NTAPES=$($PSQL -c "SELECT count(DISTINCT tf.tape_id) FROM tape_files tf JOIN objects o ON o.id=tf.object_id WHERE o.key='s3e2e-ec'")
[ "$NTAPES" = 3 ] || fail "shards must sit on 3 distinct tapes, got $NTAPES"
[ "$($PSQL -c "SELECT count(*) FROM object_shards os JOIN objects o ON o.id=os.object_id WHERE o.key='s3e2e-ec'")" = 3 ] || fail "expected 3 shard verifiers"
$AWS s3api restore-object --bucket vtl --key s3e2e-ec --restore-request Days=1 || fail "restore of EC object failed"
"$BIN" worker >/dev/null || fail "worker failed restoring the EC object"
$AWS s3api get-object --bucket vtl --key s3e2e-ec "$WORK/ec-out" >/dev/null || fail "get of EC object failed"
cmp "$WORK/ec-src" "$WORK/ec-out" || fail "EC round-trip bytes differ"

say "RAIT degrades: one tape lost, parity rebuilds the bytes"
LOST=$($PSQL -c "SELECT t.barcode FROM tape_files tf JOIN tapes t ON t.id=tf.tape_id JOIN objects o ON o.id=tf.object_id WHERE o.key='s3e2e-ec' AND tf.shard_number=0")
[ -n "$LOST" ] || fail "no shard-0 placement found"
# The tape is GONE: state lost, placement rows gone.
$PSQL -c "UPDATE tapes SET state='lost' WHERE barcode='$LOST'; DELETE FROM tape_files WHERE tape_id=(SELECT id FROM tapes WHERE barcode='$LOST');"
# Object back to tape-only, then restore from the surviving shards.
rm -f "$TAPEBERG_BUFFER/s3e2e-ec"
$PSQL -c "UPDATE objects SET storage_class='GLACIER', disk_path=NULL WHERE key='s3e2e-ec'; UPDATE retrieve_requests SET state='expired' WHERE object_id=(SELECT id FROM objects WHERE key='s3e2e-ec');" >/dev/null
$AWS s3api restore-object --bucket vtl --key s3e2e-ec --restore-request Days=1 || fail "restore request after tape loss failed"
"$BIN" worker > "$WORK/ec2.log" 2>&1 || fail "worker failed degraded restore"
grep -q "RAIT" "$WORK/ec2.log" || fail "degraded restore did not run the EC path: $(tail -2 "$WORK/ec2.log")"
$AWS s3api get-object --bucket vtl --key s3e2e-ec "$WORK/ec-out2" >/dev/null || fail "get after tape loss failed"
cmp "$WORK/ec-src" "$WORK/ec-out2" || fail "degraded EC bytes differ — parity reconstruction broken"

kill $SRV_PID 2>/dev/null || true
SRV_PID=""
printf '\nS3 E2E PASS — Glacier wire contract verified via awscli\n'
