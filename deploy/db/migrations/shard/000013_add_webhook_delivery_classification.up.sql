-- webhook_deliveries: add error_classification to align with unified dlq_entries schema
ALTER TABLE webhook_deliveries
    ADD COLUMN IF NOT EXISTS error_classification TEXT
        CHECK (error_classification IN ('poison', 'transient', 'terminal', 'infrastructure'));

-- Index for querying DLQ'd webhook deliveries
CREATE INDEX IF NOT EXISTS webhook_deliveries_dlq_idx ON webhook_deliveries (status, error_classification)
    WHERE status = 'dlq';

-- Update existing DLQ entries with default classification based on status
UPDATE webhook_deliveries
SET error_classification = 'terminal'
WHERE status = 'dlq' AND error_classification IS NULL;