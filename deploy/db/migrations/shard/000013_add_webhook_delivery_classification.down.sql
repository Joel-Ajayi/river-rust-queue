-- Revert: remove error_classification from webhook_deliveries
ALTER TABLE webhook_deliveries
    DROP COLUMN IF EXISTS error_classification;

DROP INDEX IF EXISTS webhook_deliveries_dlq_idx;