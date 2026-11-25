-- Tapeberg catalog, migration 0001: initial schema.
--
-- Design notes (CTA-informed, Glacier-shaped):
--   * The catalog owns STATE; the tape owns BYTES.
--   * State machines are TEXT + CHECK (cheaper to evolve than ENUMs).
--   * Retrieve semantics follow Glacier's RestoreObject: an async job
--     whose result is a disk copy pinned for N days (pin-as-restore,
--     borrowed from dCache's PinManager model).
--   * Queue-per-tape scheduling comes later in the worker; the schema
--     supports it via tapes.state + per-tape request assignment.

BEGIN;

-- ------------------------------------------------------------------
-- Migration bookkeeping
-- ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    integer PRIMARY KEY,
    filename    text NOT NULL,
    applied_at  timestamptz NOT NULL DEFAULT now()
);

-- ------------------------------------------------------------------
-- tape_pools: the bucket-level grouping of media (CTA pattern).
-- ------------------------------------------------------------------
CREATE TABLE tape_pools (
    id          bigserial PRIMARY KEY,
    name        text NOT NULL UNIQUE,
    description text,
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- ------------------------------------------------------------------
-- tapes: physical (or virtual) media inventory.
--   state: active  = writable, eligible for archive jobs
--          full    = no more writes; reads + repack source only
--          retired = pulled from rotation; data being migrated
--          lost    = gone; objects on it need re-archive or fail
--   capacity_bytes is a scheduling hint; the drive's EOM sense is
--   authoritative at write time.
-- ------------------------------------------------------------------
CREATE TABLE tapes (
    id               bigserial PRIMARY KEY,
    barcode          text NOT NULL UNIQUE,
    pool_id          bigint NOT NULL REFERENCES tape_pools(id),
    state            text NOT NULL DEFAULT 'active'
                     CHECK (state IN ('active','full','retired','lost')),
    is_cleaning      boolean NOT NULL DEFAULT false,
    capacity_bytes   bigint NOT NULL,
    written_bytes    bigint NOT NULL DEFAULT 0,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    last_verified_at timestamptz
);

CREATE INDEX tapes_pool_idx   ON tapes(pool_id);
CREATE INDEX tapes_state_idx  ON tapes(state);

-- ------------------------------------------------------------------
-- buckets / objects: the S3-shaped namespace.
--   objects.storage_class:
--     STANDARD      = disk-buffered (hot copy exists)
--     GLACIER       = tape-only; GET must 409 InvalidObjectState until
--                     a retrieve_request restores it
--     DEEP_ARCHIVE  = reserved; same semantics, slower SLA
--   objects.disk_path: where the disk-buffer copy lives, NULL once the
--   copy is dropped (the object is then tape-only).
-- ------------------------------------------------------------------
CREATE TABLE buckets (
    id          bigserial PRIMARY KEY,
    name        text NOT NULL UNIQUE,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE objects (
    id             bigserial PRIMARY KEY,
    bucket_id      bigint NOT NULL REFERENCES buckets(id),
    key            text NOT NULL,
    size_bytes     bigint NOT NULL,
    sha256         text,
    storage_class  text NOT NULL DEFAULT 'STANDARD'
                   CHECK (storage_class IN ('STANDARD','GLACIER','DEEP_ARCHIVE')),
    disk_path      text,
    created_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (bucket_id, key)
);

CREATE INDEX objects_bucket_idx ON objects(bucket_id);
CREATE INDEX objects_class_idx  ON objects(storage_class);

-- ------------------------------------------------------------------
-- tape_files: object placement on a tape. archive_id is the per-tape
-- filemark sequence used to position the drive for restore.
-- ------------------------------------------------------------------
CREATE TABLE tape_files (
    id           bigserial PRIMARY KEY,
    tape_id      bigint NOT NULL REFERENCES tapes(id),
    object_id    bigint NOT NULL REFERENCES objects(id),
    archive_id   bigint NOT NULL,
    size_bytes   bigint NOT NULL,
    written_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tape_id, archive_id)
);

CREATE INDEX tape_files_object_idx ON tape_files(object_id);
CREATE INDEX tape_files_tape_idx   ON tape_files(tape_id);

-- ------------------------------------------------------------------
-- archive_requests: the write queue.
--   pending   → queued, no tape assigned yet
--   assigned  → worker picked a tape (loaded / waiting for media)
--   written   → bytes + filemark on tape, tape_files row recorded
--   failed / cancelled = terminal
-- ------------------------------------------------------------------
CREATE TABLE archive_requests (
    id           bigserial PRIMARY KEY,
    object_id    bigint NOT NULL REFERENCES objects(id),
    state        text NOT NULL DEFAULT 'pending'
                 CHECK (state IN ('pending','assigned','written','failed','cancelled')),
    tape_id      bigint REFERENCES tapes(id),
    error        text,
    requested_at timestamptz NOT NULL DEFAULT now(),
    written_at   timestamptz,
    updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX archive_requests_state_idx  ON archive_requests(state);
CREATE INDEX archive_requests_object_idx ON archive_requests(object_id);

-- ------------------------------------------------------------------
-- retrieve_requests: Glacier RestoreObject jobs.
--   pending           → queued
--   waiting_for_media → manual changer mode: a human was prompted to
--                       load the tape (robot mode passes through fast)
--   restoring         → tape streaming to disk buffer
--   restored          → disk copy live, pinned until expires_at
--                       (expires_at = restored_at + pin_days); the
--                       worker's expiry sweep then unpins it
--   failed / cancelled = terminal
-- ------------------------------------------------------------------
CREATE TABLE retrieve_requests (
    id           bigserial PRIMARY KEY,
    object_id    bigint NOT NULL REFERENCES objects(id),
    state        text NOT NULL DEFAULT 'pending'
                 CHECK (state IN
                   ('pending','waiting_for_media','restoring','restored',
                    'failed','cancelled')),
    pin_days     integer NOT NULL DEFAULT 1
                 CHECK (pin_days >= 1 AND pin_days <= 30),
    expires_at   timestamptz,
    error        text,
    requested_at timestamptz NOT NULL DEFAULT now(),
    restored_at  timestamptz,
    updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX retrieve_requests_state_idx   ON retrieve_requests(state);
CREATE INDEX retrieve_requests_object_idx  ON retrieve_requests(object_id);
CREATE INDEX retrieve_requests_expiry_idx  ON retrieve_requests(expires_at)
    WHERE state = 'restored';

INSERT INTO schema_migrations (version, filename)
VALUES (1, '0001_init.sql');

COMMIT;
