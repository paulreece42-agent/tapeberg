-- 0006: buckets bind to tape pools. A bucket archives into its bound
-- pool (S3 PutObject queues the archive in the bucket's pool; so does
-- RestoreObject), which makes the S3 namespace mirror the tape sets:
-- one bucket per pool. CreateBucket binds the serving rig's pool
-- (TAPEBERG_POOL). The CLI keeps its own 'tapeberg' bucket; existing
-- buckets bind to the default pool.
ALTER TABLE buckets ADD COLUMN pool text NOT NULL DEFAULT 'default';
