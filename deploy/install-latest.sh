#!/usr/bin/env bash
# install-latest.sh — build upstream/main and install to /usr/local/bin.
#
# Idempotent and merge-driven: it fetches the upstream repo and compares
# the recorded last-installed commit; if upstream/main moved, it builds
# and atomically replaces /usr/local/bin/tapeberg. Run from root cron:
#
#   */15 * * * * root /home/major/tapeberg/deploy/install-latest.sh >>/var/log/tapeberg-install.log 2>&1
#
# Requires: git, go, write access to /usr/local/bin (root), and a clone
# of the repo with an 'upstream' remote (the agent's clone at
# ~/tapeberg qualifies; adjust REPO below or set TAPEBERG_REPO).

set -euo pipefail

REPO=${TAPEBERG_REPO:-/home/major/tapeberg}
BIN=${TAPEBERG_BIN:-/usr/local/bin/tapeberg}
STAMP="$REPO/.last-installed-commit"   # not in git (ignored)

cd "$REPO"

if ! git fetch --quiet upstream; then
  echo "$(date -Is) install-latest: fetch failed (offline?), keeping $BIN as-is"
  exit 0
fi

head=$(git rev-parse upstream/main)
last=$(cat "$STAMP" 2>/dev/null || echo "")

if [ "$head" = "$last" ]; then
  exit 0                                   # nothing new; stay quiet for cron
fi

echo "$(date -Is) install-latest: upstream/main $last -> $head, building"

work=$(mktemp -d /tmp/tapeberg-build.XXXXXX)
trap 'rm -rf "$work"' EXIT
git archive --format=tar "$head" | tar -C "$work" -xf -
(cd "$work" && go build -trimpath -ldflags "-s -w" -o tapeberg .)

install -m 0755 -o root -g root "$work/tapeberg" "$BIN.new"
mv "$BIN.new" "$BIN"                       # atomic swap: no half-written bin
echo "$head" > "$STAMP"
echo "$(date -Is) install-latest: installed $(git log -1 --format='%h %s' "$head") -> $BIN"

# If the S3 endpoint runs under systemd, pick up the new binary now —
# a long-lived daemon would otherwise keep serving the old one.
if command -v systemctl >/dev/null 2>&1 && systemctl list-unit-files 2>/dev/null | grep -q '^tapeberg-s3\.service'; then
  if systemctl try-restart tapeberg-s3.service; then
    echo "$(date -Is) install-latest: restarted tapeberg-s3.service on the new binary"
  else
    echo "$(date -Is) install-latest: WARNING: tapeberg-s3.service try-restart failed; restart it manually"
  fi
fi