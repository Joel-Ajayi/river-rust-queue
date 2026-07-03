package postgres

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/port"
)

type EventStore struct {
	pools *platform.ShardPools
}

var _ port.EventStore = (*EventStore)(nil)

func NewEventStore(pools *platform.ShardPools) *EventStore {
	return &EventStore{
		pools: pools,
	}
}

func (e *EventStore) FetchUnpublishedEvents(ctx context.Context, shardID string, limit int) ([]domain.Event, error) {
	// get pool for the shard
	pool, err := e.pools.ShardPool(shardID)
	if err != nil {
		return nil, err
	}

	// get unpublished events
	// SELECT ... FOR UPDATE SKIP LOCKED ensures that if another relay pod is already looking at an event,
	//postgres will immediately skip that row and give us the next available one. No blocking!
	rows, err := pool.Query(ctx,
		`SELECT event_id, event_type, aggregate_type, aggregate_id, correlation_id, payload, occurred_at, publish_topic
		 FROM events 
		 WHERE published_at IS NULL 
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`,
		limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []domain.Event

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
	}

	return events, nil
}

func (e *EventStore) MarkAsPublished(ctx context.Context, shardID string, eventIDs []string) error {
	if len(eventIDs) == 0 {
		return nil
	}

	// Update the published_at timestamp so we never fetch these again
	pool, err := e.pools.ShardPool(shardID)
	if err != nil {
		return err
	}

	_, err = pool.Exec(ctx, `UPDATE events SET published_at = NOW() WHERE event_id = ANY($1)`, eventIDs)
	return err
}
