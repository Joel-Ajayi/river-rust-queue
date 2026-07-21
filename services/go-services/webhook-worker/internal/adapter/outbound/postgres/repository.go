package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/domain"
	"go.opentelemetry.io/otel/trace"
)

type Repository struct {
	pools *platform.ShardPools
}

func NewRepository(pools *platform.ShardPools) *Repository {
	return &Repository{pools: pools}
}

// GetMerchantConfig retrieves merchant webhook config from the global `merchants` table.
func (r *Repository) GetMerchantConfig(ctx context.Context, merchantID string) (*domain.Merchant, error) {
	var m domain.Merchant
	err := r.pools.MerchantsPoolRO().QueryRow(ctx, `
		SELECT id, webhook_url, webhook_secret, status, shard_id
		FROM merchants
		WHERE id = $1
	`, merchantID).Scan(&m.ID, &m.WebhookURL, &m.WebhookSecret, &m.Status, &m.ShardID)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &m, nil
}

func (r *Repository) SaveDelivery(ctx context.Context, shardID string, d *domain.WebhookDelivery) error {
	pool, err := r.pools.ShardPool(shardID)
	if err != nil {
		return err
	}
	query := `
		INSERT INTO webhook_deliveries (
			id, merchant_id, source_event_id, url, payload, signature, 
			attempt_count, last_attempt_at, next_retry_at, last_error, 
			last_status, status, delivered_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, 
			$7, $8, $9, $10, 
			$11, $12, $13
		)
		ON CONFLICT (id) DO UPDATE SET
			attempt_count = EXCLUDED.attempt_count,
			last_attempt_at = EXCLUDED.last_attempt_at,
			next_retry_at = EXCLUDED.next_retry_at,
			last_error = EXCLUDED.last_error,
			last_status = EXCLUDED.last_status,
			status = EXCLUDED.status,
			delivered_at = EXCLUDED.delivered_at
	`
	_, err = pool.Exec(ctx, query,
		d.ID, d.MerchantID, d.SourceEventID, d.URL, d.Payload, d.Signature,
		d.AttemptCount, d.LastAttemptAt, d.NextRetryAt, d.LastError,
		d.LastStatus, d.Status, d.DeliveredAt,
	)
	return err
}

func (r *Repository) FetchPendingRetries(ctx context.Context, shardID string, limit int) ([]*domain.WebhookDelivery, error) {
	pool, err := r.pools.ShardPool(shardID)
	if err != nil {
		return nil, err
	}

	rows, err := pool.Query(ctx, `
		SELECT 
			id, merchant_id, source_event_id, url, payload, signature,
			attempt_count, last_attempt_at, next_retry_at, last_error,
			last_status, status, delivered_at, created_at
		FROM webhook_deliveries
		WHERE status = $2 AND next_retry_at <= NOW()
		ORDER BY next_retry_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, limit, domain.StatusPending)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var deliveries []*domain.WebhookDelivery
	for rows.Next() {
		d := &domain.WebhookDelivery{}
		err := rows.Scan(
			&d.ID, &d.MerchantID, &d.SourceEventID, &d.URL, &d.Payload, &d.Signature,
			&d.AttemptCount, &d.LastAttemptAt, &d.NextRetryAt, &d.LastError,
			&d.LastStatus, &d.Status, &d.DeliveredAt, &d.CreatedAt,
		)
		if err != nil {
			return nil, err
		}
		deliveries = append(deliveries, d)
	}
	return deliveries, rows.Err()
}

// RecordEvent records an event inside the outbox table.
func (r *Repository) RecordEvent(ctx context.Context, shardID string, eventID, eventType, aggregateType, aggregateID string, payload []byte) error {
	pool, err := r.pools.ShardPool(shardID)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, payload, occurred_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
	`, eventID, eventType, aggregateType, aggregateID, payload)
	return err
}

// RouteToDLQ writes a terminal delivery to the DLQ table.
func (r *Repository) RouteToDLQ(ctx context.Context, shardID string, source string, payload []byte, errorMsg string, attemptCount int, firstFailedAt, lastFailedAt time.Time) error {
	pool, err := r.pools.ShardPool(shardID)
	if err != nil {
		return err
	}
	spanCtx := trace.SpanContextFromContext(ctx)
	var traceID, spanID string
	if spanCtx.IsValid() {
		traceID = spanCtx.TraceID().String()
		spanID = spanCtx.SpanID().String()
	}

	_, err = pool.Exec(ctx, `
		INSERT INTO dlq_entries (id, source, original_payload, error_message, error_classification, attempt_count, first_failed_at, last_failed_at, status, trace_id, span_id)
		VALUES (gen_random_uuid()::text, $1, $2, $3, $4, $5, $6, $7, $8, NULLIF($9, ''), NULLIF($10, ''))
	`, source, payload, errorMsg, string(platform.ClassificationTerminal), attemptCount, firstFailedAt, lastFailedAt, domain.StatusOpen, traceID, spanID)
	if err != nil {
		return err
	}

	platform.RecordDLQIngestion(ctx, platform.ServiceNameWebhookWorker)
	return nil
}

func (r *Repository) GetAvailableShardIDs() []string {
	return r.pools.GetAvailableShardIDs()
}
