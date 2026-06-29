-- jobs: per-shard idempotency anchor. UNIQUE(merchant_id, idempotency_key) is I3.
CREATE TABLE jobs (
    id              TEXT PRIMARY KEY,
    merchant_id     TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash    TEXT NOT NULL,
    type            TEXT NOT NULL CHECK (type IN ('transfer', 'bulk_payout')),
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'completed', 'failed')),
    failure_reason  TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    UNIQUE (merchant_id, idempotency_key)
);

CREATE INDEX jobs_merchant_idx ON jobs (merchant_id, created_at);
