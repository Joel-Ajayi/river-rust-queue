-- dlq_entries: unroutable payloads (e.g. missing merchant) awaiting human attention.
CREATE TABLE dlq_entries (
    id                TEXT PRIMARY KEY,
    source            TEXT NOT NULL CHECK (source IN ('ledger', 'webhook', 'fraud', 'outbox-relay')),
    original_payload  JSONB NOT NULL,
    error_message     TEXT NOT NULL,
    error_classification TEXT NOT NULL CHECK (error_classification IN ('poison', 'transient', 'terminal', 'infrastructure')),
    attempt_count     INT NOT NULL,
    first_failed_at   TIMESTAMPTZ NOT NULL,
    last_failed_at    TIMESTAMPTZ NOT NULL,
    status            TEXT NOT NULL DEFAULT 'open'
                      CHECK (status IN ('open', 'replayed', 'resolved')),
    replayed_at       TIMESTAMPTZ,
    replayed_job_id   TEXT,
    resolved_at       TIMESTAMPTZ,
    resolved_by       TEXT,
    resolution_note   TEXT,
    trace_id          TEXT,
    span_id           TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- "open entries, newest first" — the dominant operator query.
CREATE INDEX dlq_entries_open_idx   ON dlq_entries (created_at DESC) WHERE status = 'open';
CREATE INDEX dlq_entries_source_idx ON dlq_entries (source, status);
CREATE INDEX dlq_entries_trace_idx  ON dlq_entries (trace_id) WHERE trace_id IS NOT NULL;
