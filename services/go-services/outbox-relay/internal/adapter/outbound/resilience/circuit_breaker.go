package resilience

import (
	"context"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/port"
)

// -- Event Store Decorator (per-shard DB circuit breaker) --

type eventStoreCB struct {
	next port.EventStore
	cb   *platform.CircuitBreaker
}

// NewEventStoreCB wraps the EventStore with a per-shard DB circuit breaker.
func NewEventStoreCB(next port.EventStore, shardID string, cbs *platform.DBCircuitBreakers) port.EventStore {
	return &eventStoreCB{
		next: next,
		cb:   cbs.ShardRW(shardID),
	}
}

func (c *eventStoreCB) ProcessUnpublishedEvents(ctx context.Context, shardID string, batchSize int, processor func(ctx context.Context, events []domain.Event) error) error {
	_, err := c.cb.Execute(func() (interface{}, error) {
		return nil, c.next.ProcessUnpublishedEvents(ctx, shardID, batchSize, processor)
	})
	return err
}

func (c *eventStoreCB) GetOldestUnpublishedEventAge(ctx context.Context, shardID string) (time.Duration, error) {
	return c.next.GetOldestUnpublishedEventAge(ctx, shardID)
}

func (c *eventStoreCB) RouteToDLQ(ctx context.Context, shardID string, event domain.Event, reason string) error {
	return c.next.RouteToDLQ(ctx, shardID, event, reason)
}
