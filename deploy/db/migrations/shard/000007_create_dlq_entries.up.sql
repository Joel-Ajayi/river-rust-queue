-- dlq_entries: terminal failures awaiting human attention.
CREATE TABLE dlq_entries (
    id                TEXT PRIMARY KEY,
    source            TEXT NOT NULL CHECK (source IN ('ledger', 'webhook')),
    original_payload  JSONB NOT NULL,
    error_message     TEXT NOT NULL,
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
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- "open entries, newest first" — the dominant operator query.
CREATE INDEX dlq_entries_open_idx   ON dlq_entries (created_at DESC) WHERE status = 'open';
CREATE INDEX dlq_entries_source_idx ON dlq_entries (source, status);
