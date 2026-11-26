-- 0009: soft delete + repack. DeleteObject (and CLI `rm`) only
-- tombstones the object (deleted_at); the tape bytes stay until the
-- cron worker's repack pass rewrites a tape whose dead share exceeds
-- TAPEBERG_REPACK_THRESHOLD (default 50%): live files are re-archived
-- to other tapes (sha-verified against the catalog), the source tape
-- is marked 'retired' (pickTape ignores it; erase + re-add to reuse
-- media). This is CTA's repack lifecycle, homelab-sized.
ALTER TABLE objects ADD COLUMN deleted_at timestamptz;