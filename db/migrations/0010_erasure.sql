-- 0010: erasure coding — RAIT (Reed-Solomon across tapes).
-- Pools gain an EC policy (ec_k data + ec_m parity shards, NULL = no
-- EC, plain single-copy). At archive time each buffered object or
-- multipart PART (exact size known from the catalog — the piece the
-- file-size-agnostic designs struggle with) is split into k data
-- shards, m parity shards are generated, and every shard is written
-- to a DIFFERENT tape. Any m tapes can be lost and the file still
-- restores. The policy is frozen onto each object at archive time
-- (objects.ec_k/ec_m), so later pool changes never invalidate what is
-- already on tape. object_shards holds the per-shard sha256 so each
-- shard is verified on read; tape_files.shard_number ties a placement
-- to its shard.
ALTER TABLE tape_pools ADD COLUMN ec_k int CHECK (ec_k IS NULL OR ec_k >= 1),
                        ADD COLUMN ec_m int CHECK (ec_m IS NULL OR ec_m >= 1);

ALTER TABLE objects ADD COLUMN ec_k int, ADD COLUMN ec_m int;

ALTER TABLE tape_files ADD COLUMN shard_number int;

CREATE TABLE object_shards (
    object_id    bigint NOT NULL REFERENCES objects(id) ON DELETE CASCADE,
    part_number  int    NOT NULL DEFAULT 0,
    shard_number int    NOT NULL,
    size_bytes   bigint NOT NULL,
    sha256       text   NOT NULL,
    PRIMARY KEY (object_id, part_number, shard_number)
);

CREATE INDEX tape_files_ec_idx ON tape_files(object_id, part_number)
    WHERE shard_number IS NOT NULL;

-- RAIT stripes: k+m shard placements share (object, part); the old
-- two-column uniqueness (0007) applies to whole files only. NULLs are
-- distinct in Postgres UNIQUE constraints, so non-EC rows are exempt.
ALTER TABLE tape_files DROP CONSTRAINT tape_files_object_part_uniq;
ALTER TABLE tape_files ADD CONSTRAINT tape_files_object_part_shard_uniq
  UNIQUE (object_id, part_number, shard_number);
