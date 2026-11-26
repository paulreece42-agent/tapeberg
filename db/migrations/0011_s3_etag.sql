-- S3 ETag correctness + deletable keys.
--
-- 1. objects.etag: the S3 wire ETag. Multipart completes used to write
--    their md5-of-md5s ETag into sha256 (which single puts used for
--    the real sha256), so single-PUT objects presented a sha256 as
--    ETag — S3 clients (s3cmd) verify ETag == content md5 and failed.
--    Move existing ETag-shaped values over; those multipart rows have
--    no whole-file sha256 (part checksums are the integrity record).
ALTER TABLE objects ADD COLUMN etag text;
UPDATE objects SET etag = sha256 WHERE sha256 ~ '^[0-9a-f]{32}-[0-9]+$';
UPDATE objects SET sha256 = NULL WHERE etag IS NOT NULL AND etag = sha256;

-- 2. The v0 UNIQUE (bucket_id, key) covered tombstoned rows, so a
--    deleted key could never be created again — one failed upload
--    bricked the key forever. Live rows only.
ALTER TABLE objects DROP CONSTRAINT objects_bucket_id_key_key;
CREATE UNIQUE INDEX objects_bucket_key_live_uniq
  ON objects(bucket_id, key) WHERE deleted_at IS NULL;
