-- Partner API keys: hashed client credentials with lifecycle state.
-- The key_hash column carries a SHA-256 digest of the presented bearer
-- token; the plaintext token is shown exactly once at creation and is never
-- stored, logged, or recoverable. status is lifecycle state ('active' or
-- 'revoked'); revoked_at freezes the revocation moment; last_used_at is
-- advisory and updated off the request path.
CREATE TABLE IF NOT EXISTS partner_api_keys (
    key_id       text        NOT NULL PRIMARY KEY,
    partner_id   text        NOT NULL,
    key_hash     bytea       NOT NULL UNIQUE,
    status       text        NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'revoked')),
    created_at   timestamptz NOT NULL DEFAULT now(),
    revoked_at   timestamptz,
    last_used_at timestamptz
);

CREATE INDEX IF NOT EXISTS partner_api_keys_partner_status_idx
    ON partner_api_keys (partner_id, status);
