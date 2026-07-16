package resilience

import (
	"context"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/port"
	"github.com/sony/gobreaker"
)

type eventPublisherCB struct {
	next port.EventPublisher
	cb   *gobreaker.CircuitBreaker
}

func NewEventPublisherCB(next port.EventPublisher, kafkaCB *platform.KafkaCircuitBreaker) port.EventPublisher {
	return &eventPublisherCB{
		next: next,
		cb:   kafkaCB.Breaker(),
	}
}

func (c *eventPublisherCB) PublishEvents(ctx context.Context, events []domain.Event) ([]string, error) {
	res, err := c.cb.Execute(func() (interface{}, error) {
		return c.next.PublishEvents(ctx, events)
	})
	if err != nil {
		return nil, err
	}
	return res.([]string), nil
}

// -- Event Store Decorator (no CB, no retry — daemon owns backoff) --

type eventStoreCB struct {
	next port.EventStore
}

// NewEventStoreCB is a pass-through decorator. No CB or retry here;
// the daemon loop at Layer 1 owns exponential backoff.
func NewEventStoreCB(next port.EventStore, shardID string) port.EventStore {
	_ = shardID
	return &eventStoreCB{next: next}
}

func (c *eventStoreCB) ProcessUnpublishedEvents(ctx context.Context, shardID string, batchSize int, processor func(ctx context.Context, events []domain.Event) error) error {
	return c.next.ProcessUnpublishedEvents(ctx, shardID, batchSize, processor)
}

func (c *eventStoreCB) GetOldestUnpublishedEventAge(ctx context.Context, shardID string) (time.Duration, error) {
	return c.next.GetOldestUnpublishedEventAge(ctx, shardID)
}

func (c *eventStoreCB) PurgePublishedEvents(ctx context.Context, shardID string, olderThan time.Duration) error {
	return c.next.PurgePublishedEvents(ctx, shardID, olderThan)
}

func (c *eventStoreCB) RouteToDLQ(ctx context.Context, shardID string, event domain.Event, reason string) error {
	return c.next.RouteToDLQ(ctx, shardID, event, reason)
}
