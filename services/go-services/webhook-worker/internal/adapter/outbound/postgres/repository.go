package postgres

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/domain"
	"go.opentelemetry.io/otel/trace"
)

type Repository struct {
	pools       *platform.ShardPools
	logger      *zap.Logger
	dlqRetryCfg platform.RetryConfig
}

func NewRepository(pools *platform.ShardPools, logger *zap.Logger, dlqRetryCfg platform.RetryConfig) *Repository {
	return &Repository{pools: pools, logger: logger, dlqRetryCfg: dlqRetryCfg}
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

	// Use an atomic UPDATE ... RETURNING to enforce a visibility timeout.
	// This hides the fetched webhooks from other concurrent pods for 5 minutes,
	// preventing a thundering herd. The worker will overwrite next_retry_at when done.
	query := `
		UPDATE webhook_deliveries
		SET next_retry_at = NOW() + INTERVAL '5 minutes'
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
	`

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

	// origin: stable per-delivery identity (SourceEventID when present, else the
	// delivery row id) so a redelivery of the same failed delivery upserts
	// (ON CONFLICT) instead of duplicating a dlq_entries row.
	origin := d.SourceEventID
	if origin == "" {
		origin = d.ID
	}

	entry := platform.NewDLQEntry(
		platform.DLQSourceWebhook, "notify", "", d.Payload, origin, errorMsg,
		platform.ClassificationTerminal, firstFailedAt, lastFailedAt, traceID, spanID,
	)
	
	merchantsPool := r.pools.MerchantsPool()
	if merchantsPool == nil {
		return platform.ErrMerchantsPoolNotInitialized
	}

	if err := platform.WriteDLQEntryWithRetry(ctx, r.logger, merchantsPool, entry, r.dlqRetryCfg, platform.ServiceNameWebhookWorker); err != nil {
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

func (r *Repository) RouteToGlobalDLQ(ctx context.Context, payload []byte, topic string, key string, errorMsg string) error {
	pool := r.pools.MerchantsPool() // global DB pool (webhook deliveries are global)

	traceID, spanID := extractTraceAndSpanID(ctx)
	now := time.Now()

	entry := platform.NewDLQEntry(
		platform.DLQSourceWebhook, topic, key, payload,
		platform.DLQEntryOrigin(payload, topic, key), // stable: envelope event_id, else topic|key
		errorMsg,
		platform.ClassificationTerminal, now, now, traceID, spanID,
	)

	// Retry the DLQ write with the per-service budget. On exhaustion the error
	// is propagated so the caller does NOT ack the source message (no data loss);
	// ExecuteWithJitter only retries Transient/Infrastructure and honors ctx.
	return platform.WriteDLQEntryWithRetry(ctx, r.logger, pool, entry, r.dlqRetryCfg, platform.ServiceNameWebhookWorker)
}
