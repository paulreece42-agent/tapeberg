#!/bin/bash
# gen-schema.sh — regenerate schema.sql from db/migrations.
#
# schema.sql is a snapshot of the full catalog schema: convenient for
# reading, diffing across versions, or bootstrapping tools that can't
# run migrations one by one. db/migrations/ stays canonical; after
# touching it, re-run this script and include the refreshed schema.sql
# in the same commit (test/e2e.sh fails when they drift).
#
# Needs psql + pg_dump and TAPEBERG_DB_URL (read from ./.env when it
# exists). Writes schema.sql next to this script's parent.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
[ -n "${TAPEBERG_DB_URL:-}" ] || { [ -f .env ] && set -a && . ./.env && set +a; }
[ -n "${TAPEBERG_DB_URL:-}" ] || { echo "gen-schema: TAPEBERG_DB_URL is not set (or no .env)" >&2; exit 1; }
command -v pg_dump >/dev/null || { echo "gen-schema: pg_dump not found" >&2; exit 1; }

GEN=tapeberg_schema_gen
CHK=tapeberg_schema_chk
cleanup() { PGOPTIONS= psql "$TAPEBERG_DB_URL" -Xq -c "DROP SCHEMA IF EXISTS $GEN CASCADE; DROP SCHEMA IF EXISTS $CHK CASCADE" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup
PGOPTIONS= psql "$TAPEBERG_DB_URL" -Xq -c "CREATE SCHEMA $GEN" >/dev/null

# The migrations build the canonical schema…
for f in db/migrations/*.sql; do
  PGOPTIONS="-c search_path=$GEN" psql "$TAPEBERG_DB_URL" -Xq -v ON_ERROR_STOP=1 -f "$f" >/dev/null
done

# …the dump is un-schema-qualified so it applies anywhere…
{
  cat <<HDR
-- Tapeberg catalog schema, generated from db/migrations/ (0001 … $(ls db/migrations/ | tail -1) — update when they change).
--
-- db/migrations/ remains the canonical history; this file is a
-- convenience for reading the whole schema, diffing across versions, or
-- bootstrap of tools that can't run migrations. db/seed.sql is optional
-- fixture data. Regenerate with: scripts/gen-schema.sh
HDR
  pg_dump "$TAPEBERG_DB_URL" --schema-only --no-owner --no-privileges -n "$GEN"
} \
  | sed -e "s/$GEN\.//g" -e "s/; Schema: $GEN;/; Schema: -;/g" \
  | grep -v "^SELECT pg_catalog.set_config('search_path', '', false);" \
  | grep -v "^-- Dumped " \
  | grep -v "^-- Name: $GEN;" \
  | grep -v "^CREATE SCHEMA $GEN;" \
  | grep -v '^connect' \
  | grep -v 'restrict' \
  > schema.sql

# …and the snapshot must re-dump identical to the migrations, or it rotted.
PGOPTIONS= psql "$TAPEBERG_DB_URL" -Xq -c "CREATE SCHEMA $CHK" >/dev/null
PGOPTIONS="-c search_path=$CHK" psql "$TAPEBERG_DB_URL" -Xq -v ON_ERROR_STOP=1 -f schema.sql >/dev/null
norm() {
  pg_dump "$TAPEBERG_DB_URL" --schema-only --no-owner --no-privileges -n "$1" \
    | sed -e "s/$1\.//g" -e "s/; Schema: $1;/; Schema: -;/g" \
    | grep -v "^SELECT pg_catalog.set_config" | grep -v "^-- Dumped" \
    | grep -v "^-- Name: $1; Type: SCHEMA" | grep -v "^CREATE SCHEMA $1;" \
    | grep -v '^connect' | grep -v 'restrict'
}
if norm "$GEN" | diff - <(norm "$CHK") > /tmp/tapeberg-schema-diff.txt; then
  echo "schema.sql regenerated from db/migrations and verified (identical re-dump)"
else
  echo "gen-schema: VERIFICATION FAILED — schema.sql does not match the migrations:" >&2
  head -30 /tmp/tapeberg-schema-diff.txt >&2
  exit 1
fi
