-- dlq_entries: add error_classification, trace_id, span_id columns for enhanced DLQ observability
ALTER TABLE dlq_entries
    ADD COLUMN IF NOT EXISTS error_classification TEXT
        CHECK (error_classification IN ('poison', 'transient', 'terminal', 'infrastructure'));

ALTER TABLE dlq_entries
    ADD COLUMN IF NOT EXISTS trace_id TEXT;

ALTER TABLE dlq_entries
    ADD COLUMN IF NOT EXISTS span_id TEXT;

-- Index for trace correlation
CREATE INDEX IF NOT EXISTS dlq_entries_trace_idx ON dlq_entries (trace_id) WHERE trace_id IS NOT NULL;

-- Update existing rows with default classification based on source
UPDATE dlq_entries
SET error_classification = 'terminal'
WHERE error_classification IS NULL;