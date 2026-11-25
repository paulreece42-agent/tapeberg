-- 0008: S3 user/key auth (Ceph RGW-style: users carry a uid, full
-- name, email and one or more rotatable access/secret key pairs).
-- The backend is pluggable: TAPEBERG_AUTH selects it (builtin = this
-- DB; ldap/ad later). Tenancy: objects and buckets carry the creating
-- user as owner — non-admin users see only their own; admins see
-- everything; owner NULL = system (CLI ingests, legacy rows) and is
-- admin-visible. While no user exists the server keeps the legacy
-- behavior (static TAPEBERG_S3_KEY/SECRET pair, or unsigned dev
-- mode); the first created user switches the endpoint to real DB auth.
CREATE TABLE s3_users (
    id         bigserial PRIMARY KEY,
    user_id    text NOT NULL UNIQUE,     -- RGW calls this uid
    full_name  text NOT NULL,            -- RGW display_name
    email      text,
    is_admin   boolean NOT NULL DEFAULT false,
    suspended  boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE s3_keys (
    id          bigserial PRIMARY KEY,
    user_id     bigint NOT NULL REFERENCES s3_users(id) ON DELETE CASCADE,
    access_key  text NOT NULL UNIQUE,
    secret_key  text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX s3_keys_user_idx ON s3_keys(user_id);

ALTER TABLE objects ADD COLUMN owner_id bigint REFERENCES s3_users(id) ON DELETE SET NULL;
ALTER TABLE buckets  ADD COLUMN owner_id bigint REFERENCES s3_users(id) ON DELETE SET NULL;
