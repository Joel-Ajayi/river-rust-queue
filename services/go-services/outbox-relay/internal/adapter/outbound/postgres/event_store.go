package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/port"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

type EventStore struct {
	pools       *platform.ShardPools
	logger      *zap.Logger
	dlqRetryCfg platform.RetryConfig
	component   string
}

var _ port.EventStore = (*EventStore)(nil)

func NewEventStore(pools *platform.ShardPools, logger *zap.Logger, dlqRetryCfg platform.RetryConfig, component string) *EventStore {
	return &EventStore{
		pools:       pools,
		logger:      logger,
		dlqRetryCfg: dlqRetryCfg,
		component:   component,
	}
}

// ProcessUnpublishedEvents publishes unpublished events with At-Least-Once
// delivery: lock rows (FOR UPDATE SKIP LOCKED), publish, then mark published.
func (e *EventStore) ProcessUnpublishedEvents(ctx context.Context, shardID string, limit int, publisher func(ctx context.Context, events []domain.Event) error) error {
	pool, err := e.pools.ShardPool(shardID)
	if err != nil {
		return err
	}

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// 1. Get unpublished events and lock rows with "for update skip locked"
	rows, err := tx.Query(ctx,
		`SELECT event_id, event_type, aggregate_type, aggregate_id, COALESCE(correlation_id, ''), payload, occurred_at, publish_topic
		 FROM events
		 WHERE published_at IS NULL AND publish_topic IS NOT NULL
		 ORDER BY id
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`,
		limit)
	if err != nil {
		return err
	}
	defer rows.Close()

	var events []domain.Event
	var eventIDs []string
	for rows.Next() {
		var event domain.Event
		if err := rows.Scan(
			&event.ID,
			&event.EventType,
			&event.AggregateType,
			&event.AggregateID,
			&event.CorrelationID,
			&event.Payload,
			&event.OccurredAt,
			&event.PublishTopic,
		); err != nil {
			return err
		}
		events = append(events, event)
		eventIDs = append(eventIDs, event.ID)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows.Close()

	if len(events) == 0 {
		return nil
	}

	// 2 & 3. Publish to Kafka, then mark events as published.
	// The fetch batch size already bounds the number of events per transaction,
	// so a sub-batch loop is unnecessary (see issue 21).
	if err := publisher(ctx, events); err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `UPDATE events SET published_at = NOW() WHERE event_id = ANY($1)`, eventIDs)
	if err != nil {
		return err
	}

	// 4. Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	return nil
}

func (e *EventStore) GetOldestUnpublishedEventAge(ctx context.Context, shardID string) (time.Duration, error) {
	pool, err := e.pools.ShardPool(shardID)
	if err != nil {
		return 0, err
	}

	var occurredAt time.Time
	err = pool.QueryRow(ctx, `SELECT occurred_at FROM events WHERE published_at IS NULL AND publish_topic IS NOT NULL ORDER BY occurred_at ASC LIMIT 1`).Scan(&occurredAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}

	return time.Since(occurredAt), nil
}

// RouteToDLQ writes an un-publishable event to the global merchants DLQ.
func (e *EventStore) RouteToDLQ(ctx context.Context, event domain.Event, errorMsg string) error {
	pool := e.pools.MerchantsPool()
	if pool == nil {
		return platform.ErrMerchantsPoolNotInitialized
	}

	var traceID, spanID string
	if env, err := platform.UnmarshalEnvelope(event.Payload); err == nil && env.Traceparent != "" {
		traceID, spanID = platform.ExtractTraceFromParentString(env.Traceparent)
	}

	now := time.Now()
	entry := platform.NewDLQEntry(
		platform.ServiceNameOutboxRelay, event.PublishTopic, domain.DeriveKafkaKey(event), event.Payload,
		event.ID, // deterministic origin: stable envelope event_id -> idempotent upsert across retries
		errorMsg,
		platform.ClassificationPoison, now, now, traceID, spanID,
	)

	if err := platform.WriteDLQEntryWithRetry(ctx, e.logger, pool, entry, e.dlqRetryCfg, e.component); err != nil {
		return err
	}

	return nil
}
