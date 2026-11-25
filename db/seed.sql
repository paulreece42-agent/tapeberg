-- Tapeberg dev seed: default pool, the mhvtl rig's media, default bucket.
-- Idempotent-ish: safe on a fresh database only.

BEGIN;

INSERT INTO tape_pools (name, description)
VALUES ('default', 'Tapeberg dev rig pool (mhvtl LTO-5 VTL)')
ON CONFLICT (name) DO NOTHING;

INSERT INTO buckets (name) VALUES ('tapeberg')
ON CONFLICT (name) DO NOTHING;

-- The 16 data tapes + 1 cleaning tape from deploy/mhvtl/library_contents.10.
-- capacity 104857600 = 100 MiB nominal (CAPACITY=100 in mhvtl.conf; the
-- drive's EOM sense is authoritative — see docs/dev-rig.md).
INSERT INTO tapes (barcode, pool_id, is_cleaning, capacity_bytes)
SELECT barcode,
       (SELECT id FROM tape_pools WHERE name = 'default'),
       barcode LIKE 'CLN%',
       104857600
FROM unnest(ARRAY[
    'TB0001L5','TB0002L5','TB0003L5','TB0004L5',
    'TB0005L5','TB0006L5','TB0007L5','TB0008L5',
    'TB0009L5','TB0010L5','TB0011L5','TB0012L5',
    'TB0013L5','TB0014L5','TB0015L5','TB0016L5',
    'CLN001L5'
]) AS barcode
ON CONFLICT (barcode) DO NOTHING;

COMMIT;
