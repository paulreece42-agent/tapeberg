-- 0003: physical_file — the 1-based position of each file ON THE TAPE.
--
-- archive_id counts only Tapeberg's files; the tape counts EVERYTHING.
-- Real media can carry foreign files (this rig's 000259L5 has a leading
-- empty file at BOT, shifting every archive by one — a restore seeked
-- by archive_id and failed its checksum). writeTapeFile now records the
-- physical position from `mt status` at EOD (File number=N; -1 on a
-- blank tape means the next write is file 1).
ALTER TABLE tape_files ADD COLUMN physical_file bigint;

-- Existing VTL rows were written to erased media from BOT: identical.
UPDATE tape_files SET physical_file = archive_id;

-- Rig backfill, verified by dumping the real tape (dd from /dev/nst0):
-- physical file 1 = 0 bytes (pre-existing), file 2 = NOVA-NSA.mp4
-- (sha bb7c…, size 1456150028), file 3 = real-test-1m (sha c6df…).
UPDATE tape_files SET physical_file = archive_id + 1 WHERE tape_id IN (SELECT id FROM tapes WHERE barcode = '000259L5');

ALTER TABLE tape_files ALTER COLUMN physical_file SET NOT NULL;
