-- 0004: pool isolation. The mhvtl dev rig's fixture tapes move to
-- their own 'vtl' pool so the real rig's worker (pool 'default') can
-- never pick them — a test reset had resurrected TB* tapes as 'active'
-- in the shared catalog and the real worker asked the human to load a
-- VIRTUAL tape into the PHYSICAL drive (and ejected the real one to
-- make room). Test resets now scope their damage to the vtl pool.
INSERT INTO tape_pools (name)
SELECT 'vtl' WHERE NOT EXISTS (SELECT 1 FROM tape_pools WHERE name = 'vtl');

UPDATE tapes SET pool_id = (SELECT id FROM tape_pools WHERE name = 'vtl')
WHERE barcode LIKE 'TB%' OR barcode LIKE 'CLN%';
