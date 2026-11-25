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
trap 'rm -rf "$WORK"; [ -n "${SRV_PID:-}" ] && kill "$SRV_PID" 2>/dev/null || true' EXIT

say() { printf '\n=== %s ===\n' "$*"; }
fail() { printf '\nS3 E2E FAIL: %s\n' "$*" >&2; exit 1; }

command -v aws >/dev/null || fail "awscli not installed"

say "building tapeberg"
cd "$ROOT"
go build -o "$BIN" . || fail "build failed"

CHANGER_DEV=$(lsscsi -g | awk '$2=="mediumx"{print $NF}')
ST=$(lsscsi -g | awk '$2=="tape" && $3=="IBM"{print $(NF-1)}')
DRIVE=${ST/\/st/\/nst}

set -a; . "$ROOT/.env"; set +a
# Hermetic: never read the host's /etc/tapeberg.conf (robot mode, no
# notification channels) — the conf file belongs to the real rig.
export TAPEBERG_CONF=/dev/null
unset TAPEBERG_TELEGRAM_TOKEN TAPEBERG_TELEGRAM_CHAT TAPEBERG_WEBHOOK_URL TAPEBERG_WEBHOOK_TOKEN
export TAPEBERG_DRIVE="$DRIVE" TAPEBERG_CHANGER=robot TAPEBERG_CHANGER_DEV="$CHANGER_DEV" TAPEBERG_POOL=vtl
export TAPEBERG_BUFFER="$WORK/buffer"
export TAPEBERG_S3_KEY="tapeberg-test" TAPEBERG_S3_SECRET="tapeberg-test-secret"
mkdir -p "$TAPEBERG_BUFFER"
PSQL="psql $TAPEBERG_DB_URL -tAq"
AWS="aws --endpoint-url http://127.0.0.1:7333 --region us-east-1"

say "blanking VTL media + resetting catalog together (they MUST stay in sync)"
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
$PSQL -c "DELETE FROM retrieve_requests WHERE object_id IN (SELECT id FROM objects WHERE key LIKE 's3e2e%');
DELETE FROM archive_requests WHERE object_id IN (SELECT id FROM objects WHERE key LIKE 's3e2e%');
DELETE FROM tape_files WHERE tape_id IN (SELECT id FROM tapes WHERE pool_id=(SELECT id FROM tape_pools WHERE name='vtl'));
UPDATE tapes SET state='active', written_bytes=0 WHERE pool_id=(SELECT id FROM tape_pools WHERE name='vtl');
DELETE FROM objects WHERE key LIKE 's3e2e%';" >/dev/null

say "starting the S3 endpoint"
if ss -ltn 2>/dev/null | grep -q ':7333 '; then
  fail "port 7333 already in use — a production s3 server? Stop it first; running the test against it would write test data into the real catalog"
fi
"$BIN" s3 127.0.0.1:7333 &
SRV_PID=$!
for _ in $(seq 1 30); do curl -s -o /dev/null http://127.0.0.1:7333/ && break; sleep 0.5; done
kill -0 $SRV_PID 2>/dev/null || fail "s3 endpoint died at startup"

export AWS_ACCESS_KEY_ID="$TAPEBERG_S3_KEY" AWS_SECRET_ACCESS_KEY="$TAPEBERG_S3_SECRET"

say "create-bucket: binds to this rig's pool (vtl)"
$AWS s3api create-bucket --bucket vtl || fail "create-bucket failed"
bp=$($PSQL -c "SELECT pool FROM buckets WHERE name='vtl'")
[ "$bp" = vtl ] || fail "bucket vtl should be bound to pool vtl, got '$bp'"

say "list-buckets"
$AWS s3api list-buckets --query "Buckets[].Name" --output text | grep -qw vtl || fail "bucket not listed"

say "put-object (5 MiB)"
dd if=/dev/urandom of="$WORK/obj" bs=1M count=5 status=none
ETAG=$($AWS s3api put-object --bucket vtl --key s3e2e-obj --body "$WORK/obj" --query ETag --output text)
[ -n "$ETAG" ] || fail "put-object returned no ETag"
echo "put ETag: $ETAG"
sha=$(sha256sum "$WORK/obj" | cut -d' ' -f1)
[ "$ETAG" = "\"$sha\"" ] || fail "ETag does not match client-side sha256"

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
host_base = 127.0.0.1:7333
host_bucket = 127.0.0.1:7333
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

say "abort multipart upload"
UPID=$($AWS s3api create-multipart-upload --bucket vtl --key s3e2e-mp-abort --query UploadId --output text)
$AWS s3api upload-part --bucket vtl --key s3e2e-mp-abort --upload-id "$UPID" --part-number 1 --body "$WORK/obj" >/dev/null || fail "upload-part failed"
$AWS s3api abort-multipart-upload --bucket vtl --key s3e2e-mp-abort --upload-id "$UPID" || fail "abort failed"
if $AWS s3api head-object --bucket vtl --key s3e2e-mp-abort >/dev/null 2>&1; then
  fail "aborted upload should not be visible"
fi

say "SigV4 is enforced (bad signature rejected)"
if AWS_SECRET_ACCESS_KEY="wrong-secret" $AWS s3api list-buckets >/dev/null 2>&1; then
  fail "bad credentials were accepted"
fi
echo "bad signature rejected"

kill $SRV_PID 2>/dev/null || true
SRV_PID=""
printf '\nS3 E2E PASS — Glacier wire contract verified via awscli\n'
