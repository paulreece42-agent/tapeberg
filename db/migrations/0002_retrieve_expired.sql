-- Tapeberg catalog, migration 0002: 'expired' retrieve state.
--
-- The pin-expiry sweep needs a terminal state for restores whose
-- Glacier-style pin (Days=N on RestoreObject) has lapsed; without it the
-- sweep could not distinguish "pin lapsed, disk copy dropped" from
-- "still pinned".

BEGIN;

ALTER TABLE retrieve_requests DROP CONSTRAINT retrieve_requests_state_check;

ALTER TABLE retrieve_requests ADD CONSTRAINT retrieve_requests_state_check
    CHECK (state IN
      ('pending','waiting_for_media','restoring','restored','expired',
       'failed','cancelled'));

INSERT INTO schema_migrations (version, filename)
VALUES (2, '0002_retrieve_expired.sql');

COMMIT;
