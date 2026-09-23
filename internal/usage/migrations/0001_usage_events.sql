-- One factual row per proxied request that reached the provider path.
-- Facts only: token counts as the upstream itself reported them (NULL when
-- it reported none — never a fabricated zero), byte counts, outcomes, and
-- the attempt counters. No pricing, no currency, no quota.
CREATE TABLE usage_events (
    event_id          uuid        NOT NULL PRIMARY KEY,
    occurred_at       timestamptz NOT NULL,
    partner_id        text        NOT NULL DEFAULT '',
    key_id            text        NOT NULL DEFAULT '',
    request_id        text        NOT NULL,
    config_generation bigint      NOT NULL,
    public_model      text        NOT NULL,
    provider          text        NOT NULL DEFAULT '',
    upstream_model    text        NOT NULL DEFAULT '',
    api               text        NOT NULL,
    stream            boolean     NOT NULL,
    http_status       integer     NOT NULL,
    outcome           text        NOT NULL,
    prompt_tokens     bigint,
    completion_tokens bigint,
    total_tokens      bigint,
    bytes_in          bigint      NOT NULL DEFAULT 0,
    bytes_out         bigint      NOT NULL DEFAULT 0,
    provider_attempts integer     NOT NULL DEFAULT 0,
    egress_attempts   integer     NOT NULL DEFAULT 0,
    egress_kind       text        NOT NULL DEFAULT '',
    latency_ms        bigint      NOT NULL DEFAULT 0
);

-- The real query dimensions: who used what, when. partner×time, model,
-- provider, and outcome each index (dimension, occurred_at) so time-window
-- reports per partner/model/provider/outcome are index scans.
CREATE INDEX usage_events_partner_time_idx ON usage_events (partner_id, occurred_at);
CREATE INDEX usage_events_model_time_idx   ON usage_events (public_model, occurred_at);
CREATE INDEX usage_events_provider_time_idx ON usage_events (provider, occurred_at);
CREATE INDEX usage_events_outcome_time_idx ON usage_events (outcome, occurred_at);
