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

kill $SRV_PID 2>/dev/null || true
SRV_PID=""
printf '\nS3 E2E PASS — Glacier wire contract verified via awscli\n'
