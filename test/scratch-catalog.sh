# scratch-catalog.sh — sourced by the e2e suites (needs ROOT, BIN, fail).
#
# Runs a suite against a throwaway schema in the catalog database instead
# of the catalog's real rows. The suites used to share the production
# catalog and scope their damage by hand, which leaked anyway:
#   - `DELETE FROM s3_users` removed every real S3 user (keys cascade),
#     and while the test's own users existed the *production* endpoint
#     switched to DB auth and refused its clients;
#   - `UPDATE tape_pools SET ec_k=NULL, ec_m=NULL` cleared every pool's
#     EC policy, production pools included;
#   - CLI ingests landed in the real `tapeberg` bucket (bound to pool
#     `default`) while archiving to `vtl` tapes, leaving objects whose
#     restores would be queued for the real rig's worker.
#
# Isolation is one variable: PGOPTIONS='-c search_path=<schema>' is
# honored by psql (libpq) AND by the binary (lib/pq sends it in the
# startup packet), so every unqualified table name — all of them — lands
# in the scratch schema. The schema is rebuilt from db/migrations + the
# seed on every run, so the suites also exercise a fresh install.
#
# The suites can't run concurrently anyway (they drive the same VTL), so
# one fixed schema name is used and dropped before and after each run;
# a run killed mid-way leaves it behind only until the next run.

SCRATCH_SCHEMA=tapeberg_e2e

scratch_catalog_down() {
  [ -n "${TAPEBERG_DB_URL:-}" ] || return 0   # exit before .env loaded: nothing made yet
  PGOPTIONS= psql "$TAPEBERG_DB_URL" -Xq -c "DROP SCHEMA IF EXISTS $SCRATCH_SCHEMA CASCADE" >/dev/null 2>&1 || true
}

# For the EXIT trap: drop the schema after a clean run, keep it after a
# failure so the catalog state can be inspected (the next run drops it).
scratch_catalog_exit() {
  if [ "$1" = 0 ]; then
    scratch_catalog_down
  elif [ "${PGOPTIONS:-}" = "-c search_path=$SCRATCH_SCHEMA" ]; then
    echo "scratch catalog kept for inspection: PGOPTIONS='-c search_path=$SCRATCH_SCHEMA' psql \"\$TAPEBERG_DB_URL\"" >&2
  fi
}

scratch_catalog_up() {
  scratch_catalog_down
  PGOPTIONS= psql "$TAPEBERG_DB_URL" -Xq -v ON_ERROR_STOP=1 -c "CREATE SCHEMA $SCRATCH_SCHEMA" >/dev/null \
    || fail "cannot create scratch schema $SCRATCH_SCHEMA (the catalog role needs CREATE on its database)"
  export PGOPTIONS="-c search_path=$SCRATCH_SCHEMA"

  # Refuse to continue unless psql really resolves into the scratch schema.
  local cur
  cur=$(psql "$TAPEBERG_DB_URL" -XtAq -c "SELECT current_schema()")
  [ "$cur" = "$SCRATCH_SCHEMA" ] || fail "scratch catalog: psql resolves to schema '$cur', not $SCRATCH_SCHEMA — refusing to touch the real catalog"

  local f
  for f in "$ROOT"/db/migrations/*.sql; do
    psql "$TAPEBERG_DB_URL" -Xq -v ON_ERROR_STOP=1 -f "$f" >/dev/null || fail "scratch catalog: $(basename "$f") failed"
  done
  psql "$TAPEBERG_DB_URL" -Xq -v ON_ERROR_STOP=1 -f "$ROOT/db/seed.sql" >/dev/null || fail "scratch catalog: seed failed"

  # ...and the binary too: a pool that exists only in the scratch schema
  # must be visible to it.
  psql "$TAPEBERG_DB_URL" -Xq -c "INSERT INTO tape_pools (name) VALUES ('e2e-scratch-marker')" >/dev/null
  "$BIN" pool ls | grep '^e2e-scratch-marker' >/dev/null \
    || fail "scratch catalog: the binary does not see schema $SCRATCH_SCHEMA (PGOPTIONS ignored?) — refusing to run against the real catalog"
  psql "$TAPEBERG_DB_URL" -Xq -c "DELETE FROM tape_pools WHERE name = 'e2e-scratch-marker'" >/dev/null
  echo "catalog: scratch schema $SCRATCH_SCHEMA (all migrations + seed); the real catalog is not touched"
}
