-- Revert: remove error_classification, trace_id, span_id columns from dlq_entries
ALTER TABLE dlq_entries
    DROP COLUMN IF EXISTS error_classification;

ALTER TABLE dlq_entries
    DROP COLUMN IF EXISTS trace_id;

ALTER TABLE dlq_entries
    DROP COLUMN IF EXISTS span_id;

DROP INDEX IF EXISTS dlq_entries_trace_idx;