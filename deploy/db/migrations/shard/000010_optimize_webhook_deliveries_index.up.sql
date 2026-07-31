-- Ensure optimal compound index for webhook retry scheduler queries (FOR UPDATE SKIP LOCKED)
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_status_retry_asc 
    ON webhook_deliveries (status, next_retry_at ASC) 
    WHERE status = 'pending';
