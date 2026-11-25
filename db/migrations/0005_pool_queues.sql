-- 0005: pool-bind the request queues. Pool-scoped pickTape alone was not
-- enough: the queues were global, so a dev-rig (mhvtl) worker drained the
-- real rig's pending archive and wrote the user's data to a VIRTUAL
-- tape that the next test erase would have destroyed. Requests now
-- carry the pool of the rig that created them; workers drain only their
-- own pool's requests.
ALTER TABLE archive_requests ADD COLUMN pool text NOT NULL DEFAULT 'default';
ALTER TABLE retrieve_requests ADD COLUMN pool text NOT NULL DEFAULT 'default';

CREATE INDEX archive_requests_pool_idx  ON archive_requests(pool, state);
CREATE INDEX retrieve_requests_pool_idx ON retrieve_requests(pool, state);
