package postgres

import (
	"context"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/port"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

type EventStore struct {
	pools  *platform.ShardPools
	logger *zap.Logger
}

var _ port.EventStore = (*EventStore)(nil)

func NewEventStore(pools *platform.ShardPools, logger *zap.Logger) *EventStore {
	return &EventStore{
		pools:  pools,
		logger: logger,
	}
}

func (e *EventStore) FetchUnpublishedEvents(ctx context.Context, shardID string, limit int) ([]domain.Event, error) {
	pool, err := e.pools.ShardPool(shardID)
	if err != nil {
		return nil, err
	}

	txCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Begin a transaction so FOR UPDATE SKIP LOCKED holds locks until we mark as published.
	tx, err := pool.BeginTx(txCtx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(txCtx)

	// ORDER BY id preserves causal ordering (I4, I5).
	// FOR UPDATE SKIP LOCKED prevents duplicate relay pods from grabbing the same rows.
	rows, err := tx.Query(txCtx,
		`SELECT event_id, event_type, aggregate_type, aggregate_id, correlation_id, payload, occurred_at, publish_topic
		 FROM events
		 WHERE published_at IS NULL AND publish_topic IS NOT NULL
		 ORDER BY id
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`,
		limit)
	if err != nil {
		return nil, err
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
			return nil, err
		}
		events = append(events, event)
		eventIDs = append(eventIDs, event.ID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// The transaction commits and releases the locks. If another relayer polls
	// before we MarkPublished, it will fetch the same events and publish duplicates.
	// This is safe because consumers are strictly idempotent.
	if err := tx.Commit(txCtx); err != nil {
		return nil, err
	}
	if len(eventIDs) > 0 {
		e.logger.Info("Fetched unpublished events", zap.Int("count", len(eventIDs)), zap.String("shard_id", shardID))
	}

	return events, nil
}

func (e *EventStore) MarkPublished(ctx context.Context, shardID string, eventIDs []string) error {
	if len(eventIDs) == 0 {
		return nil
	}

	pool, err := e.pools.ShardPool(shardID)
	if err != nil {
		return err
	}

	txCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err = pool.Exec(txCtx, `UPDATE events SET published_at = NOW() WHERE event_id = ANY($1)`, eventIDs)
	if err != nil {
		return err
	}

	e.logger.Info("Marked events as published", zap.Int("count", len(eventIDs)), zap.String("shard_id", shardID))
	return nil
}
