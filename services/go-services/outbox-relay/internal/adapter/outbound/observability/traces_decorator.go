package observability

import (
	"context"
	"fmt"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/port"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Trace wrapper for EventStore - adds spans to DB operations
type eventStoreTraces struct {
	next port.EventStore
}

func NewEventStoreTraces(next port.EventStore) port.EventStore {
	return &eventStoreTraces{next: next}
}

func (t *eventStoreTraces) ProcessUnpublishedEvents(ctx context.Context, shardID string, batchSize int, processor func(ctx context.Context, events []domain.Event) error) error {
	return t.next.ProcessUnpublishedEvents(ctx, shardID, batchSize, processor)
}

func (t *eventStoreTraces) GetOldestUnpublishedEventAge(ctx context.Context, shardID string) (time.Duration, error) {
	return t.next.GetOldestUnpublishedEventAge(ctx, shardID)
}

func (t *eventStoreTraces) RouteToDLQ(ctx context.Context, event domain.Event, reason string) error {
	ctx, span := platform.GetTracer().Start(ctx, platform.SpanOutboxStoreRouteDLQ,
		trace.WithAttributes(
			attribute.String(platform.MetricLabelJobID, event.AggregateID)))
	defer span.End()

	if err := t.next.RouteToDLQ(ctx, event, reason); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.SetAttributes(
			attribute.String(platform.MetricLabelErrorType, fmt.Sprintf("%T", err)),
			attribute.String(platform.MetricLabelErrorMessage, err.Error()),
		)
		return err
	}
	return nil
}
