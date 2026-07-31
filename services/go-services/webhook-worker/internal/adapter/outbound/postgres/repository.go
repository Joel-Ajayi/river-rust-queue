package postgres

import (
	"context"
	"fmt"
	"strings"
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

func (r *Repository) CreatePendingDelivery(ctx context.Context, shardID string, d *domain.WebhookDelivery) error {
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
		ON CONFLICT (id) DO NOTHING
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

	query := fmt.Sprintf(`
		UPDATE webhook_deliveries
		SET next_retry_at = NOW() + INTERVAL '%d minutes'
		WHERE id IN (
			SELECT id
			FROM webhook_deliveries
			WHERE status = $2 AND next_retry_at <= NOW()
			ORDER BY next_retry_at ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, merchant_id, source_event_id, url, payload, signature,
			attempt_count, last_attempt_at, next_retry_at, last_error,
			last_status, status, delivered_at, created_at
	`, domain.SchedulerVisibilityTimeoutMinutes)

	rows, err := pool.Query(ctx, query, limit, domain.StatusPending)
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

func (r *Repository) CompleteDelivery(ctx context.Context, shardID string, d *domain.WebhookDelivery, successEventPayload []byte, eventID string) error {
	pool, err := r.pools.ShardPool(shardID)
	if err != nil {
		return err
	}
	
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

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
	_, err = tx.Exec(ctx, query,
		d.ID, d.MerchantID, d.SourceEventID, d.URL, d.Payload, d.Signature,
		d.AttemptCount, d.LastAttemptAt, d.NextRetryAt, d.LastError,
		d.LastStatus, d.Status, d.DeliveredAt,
	)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, payload, occurred_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
	`, eventID, domain.EventTypeWebhookDelivered, string(platform.AggregateTypeWebhook), d.ID, successEventPayload)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (r *Repository) FailDeliveryAndRouteToDLQ(ctx context.Context, shardID string, d *domain.WebhookDelivery, errorMsg string, failEventPayload []byte, eventID string, firstFailedAt time.Time, lastFailedAt time.Time) error {
	pool, err := r.pools.ShardPool(shardID)
	if err != nil {
		return err
	}
	
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

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
	_, err = tx.Exec(ctx, query,
		d.ID, d.MerchantID, d.SourceEventID, d.URL, d.Payload, d.Signature,
		d.AttemptCount, d.LastAttemptAt, d.NextRetryAt, d.LastError,
		d.LastStatus, d.Status, d.DeliveredAt,
	)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, payload, occurred_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
	`, eventID, domain.EventTypeWebhookFailed, string(platform.AggregateTypeWebhook), d.ID, failEventPayload)
	if err != nil {
		return err
	}

	traceID, spanID := extractTraceAndSpanID(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO dlq_entries (id, source, original_payload, error_message, error_classification, attempt_count, first_failed_at, last_failed_at, status, trace_id, span_id)
		VALUES (gen_random_uuid()::text, $1, $2, $3, $4, $5, $6, $7, $8, NULLIF($9, ''), NULLIF($10, ''))
	`, domain.EventSourceWebhook, d.Payload, errorMsg, string(platform.ClassificationTerminal), d.AttemptCount, firstFailedAt, lastFailedAt, domain.StatusOpen, traceID, spanID)
	if err != nil {
		return err
	}

	err = tx.Commit(ctx)
	if err == nil {
		platform.RecordDLQIngestion(ctx, platform.ServiceNameWebhookWorker)
	}
	return err
}

func (r *Repository) ScheduleRetry(ctx context.Context, shardID string, d *domain.WebhookDelivery, failEventPayload []byte, eventID string) error {
	pool, err := r.pools.ShardPool(shardID)
	if err != nil {
		return err
	}
	
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

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
	_, err = tx.Exec(ctx, query,
		d.ID, d.MerchantID, d.SourceEventID, d.URL, d.Payload, d.Signature,
		d.AttemptCount, d.LastAttemptAt, d.NextRetryAt, d.LastError,
		d.LastStatus, d.Status, d.DeliveredAt,
	)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, payload, occurred_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
	`, eventID, domain.EventTypeWebhookFailed, string(platform.AggregateTypeWebhook), d.ID, failEventPayload)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (r *Repository) GetAvailableShardIDs() []string {
	return r.pools.GetAvailableShardIDs()
}

func extractTraceAndSpanID(ctx context.Context) (string, string) {
	spanCtx := trace.SpanContextFromContext(ctx)
	if spanCtx.IsValid() {
		return spanCtx.TraceID().String(), spanCtx.SpanID().String()
	}
	tp := platform.ExtractTraceparent(ctx)
	if len(tp) >= 55 {
		parts := strings.Split(tp, "-")
		if len(parts) >= 3 {
			return parts[1], parts[2]
		}
	}
	return "", ""
}

func (r *Repository) RouteToGlobalDLQ(ctx context.Context, payload []byte, errorMsg string) error {
	pool := r.pools.MerchantsPool() // Use global DB pool
	
	traceID, spanID := extractTraceAndSpanID(ctx)

	_, err := pool.Exec(ctx, `
		INSERT INTO dlq_entries (
			id, source, original_payload, error_message, error_classification, 
			attempt_count, first_failed_at, last_failed_at, status, trace_id, span_id
		) VALUES (
			gen_random_uuid()::text, $1, $2, $3, $4, 
			$5, NOW(), NOW(), $6, NULLIF($7, ''), NULLIF($8, '')
		)
	`, domain.EventSourceWebhook, payload, errorMsg, string(platform.ClassificationTerminal), 1, domain.StatusOpen, traceID, spanID)
	
	if err == nil {
		platform.RecordDLQIngestion(ctx, platform.ServiceNameWebhookWorker)
	}
	return err
}
