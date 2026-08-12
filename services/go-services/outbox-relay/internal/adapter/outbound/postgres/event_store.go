package postgres

import (
	"context"
	"errors"
	"time"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/port"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type EventStore struct {
	pools *platform.ShardPools
}

var _ port.EventStore = (*EventStore)(nil)

func NewEventStore(pools *platform.ShardPools, _ *zap.Logger) *EventStore {
	return &EventStore{
		pools: pools,
	}
}

// ProcessUnpublishedEvents processes unpublished events with At-Least-Once Delivery pattern
// 1. Get unpublished events and lock rows with "for update skip locked"
// 2. Publish events
// 3. Mark as published
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
		`SELECT event_id, event_type, aggregate_type, aggregate_id, correlation_id, payload, occurred_at, publish_topic
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

func (e *EventStore) RouteToDLQ(ctx context.Context, shardID string, event domain.Event, errorMsg string) error {
	pool, err := e.pools.ShardPool(shardID)
	if err != nil {
		return err
	}

	var traceID, spanID string
	var envelope eventsv1.EventEnvelope
	if err := proto.Unmarshal(event.Payload, &envelope); err == nil && envelope.Traceparent != "" {
		traceID, spanID = platform.ExtractTraceFromParentString(envelope.Traceparent)
	}

	_, err = pool.Exec(ctx, `
		INSERT INTO dlq_entries (id, source, original_payload, error_message, error_classification, attempt_count, first_failed_at, last_failed_at, status, created_at, trace_id, span_id)
		VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW(), $7, NOW(), NULLIF($8, ''), NULLIF($9, ''))
		ON CONFLICT (id) DO UPDATE SET
			error_message = EXCLUDED.error_message,
			error_classification = EXCLUDED.error_classification,
			attempt_count = EXCLUDED.attempt_count + 1,
			last_failed_at = EXCLUDED.last_failed_at,
			status = EXCLUDED.status
	`, event.ID, platform.ServiceNameOutboxRelay, event.Payload, errorMsg, string(platform.ClassificationPoison), 0, platform.DLQStatusOpen, traceID, spanID)

	if err != nil {
		return err
	}

	platform.RecordDLQIngestion(ctx, platform.ServiceNameOutboxRelay)
	return nil
}
