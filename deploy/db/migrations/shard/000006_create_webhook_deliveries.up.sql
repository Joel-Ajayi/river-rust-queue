-- webhook_deliveries: per-shard outbound retry state. merchant_id is plain TEXT
-- (merchants is in the global DB); source_event_id is a same-shard FK to events.
CREATE TABLE webhook_deliveries (
    id              TEXT PRIMARY KEY,
    merchant_id     TEXT NOT NULL,
    source_event_id TEXT NOT NULL REFERENCES events(event_id),
    url             TEXT NOT NULL,
    payload         JSONB NOT NULL,
    signature       TEXT NOT NULL,
    attempt_count   INT NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMPTZ,
    next_retry_at   TIMESTAMPTZ,
    last_error      TEXT,
    last_status     INT,
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'delivered', 'dlq')),
    error_classification TEXT
                    CHECK (error_classification IN ('poison', 'transient', 'terminal', 'infrastructure')),
    delivered_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- The retry scheduler's hot query.
CREATE INDEX webhook_deliveries_pending_idx ON webhook_deliveries (status, next_retry_at)
    WHERE status = 'pending';
CREATE INDEX webhook_deliveries_merchant_idx ON webhook_deliveries (merchant_id, created_at);
-- DLQ'd webhook deliveries, classified.
CREATE INDEX webhook_deliveries_dlq_idx ON webhook_deliveries (status, error_classification)
    WHERE status = 'dlq';
