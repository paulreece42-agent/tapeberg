-- 0007: multipart uploads. Parts stay parts on disk and tape; the S3
-- API layer merges them (GET = concatenation in part order). A part is
-- atomic on one tape, and clients cap parts at 5GB, so objects larger
-- than any single tape archive as many parts (CTA-style: the catalog,
-- not the tape, provides contiguity).
ALTER TABLE objects ADD COLUMN upload_id text;
CREATE UNIQUE INDEX objects_upload_id_idx ON objects(upload_id)
  WHERE upload_id IS NOT NULL;

CREATE TABLE object_parts (
    object_id   bigint NOT NULL REFERENCES objects(id) ON DELETE CASCADE,
    part_number int    NOT NULL,
    size_bytes  bigint NOT NULL,
    md5         text   NOT NULL, -- S3 part ETag
    sha256      text   NOT NULL, -- restore verification
    disk_path   text,            -- set while buffered / restored; NULL = tape-only
    PRIMARY KEY (object_id, part_number)
);

ALTER TABLE tape_files      ADD COLUMN part_number int NOT NULL DEFAULT 1;
ALTER TABLE archive_requests ADD COLUMN part_number int NOT NULL DEFAULT 1;
ALTER TABLE tape_files ADD CONSTRAINT tape_files_object_part_uniq
  UNIQUE (object_id, part_number);
