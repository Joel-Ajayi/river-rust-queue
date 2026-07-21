package observability

import (
	"context"
	"fmt"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/port"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
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
	ctx, span := platform.GetTracer().Start(ctx, "outbox.store.process_unpublished",
		trace.WithAttributes(
			attribute.String(platform.MetricLabelShard, shardID),
			attribute.Int("batch_size", batchSize)))
	defer span.End()

	err := t.next.ProcessUnpublishedEvents(ctx, shardID, batchSize, processor)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.SetAttributes(
			attribute.String(platform.MetricLabelErrorType, fmt.Sprintf("%T", err)),
			attribute.String(platform.MetricLabelErrorMessage, err.Error()),
		)
	}
	return err
}

func (t *eventStoreTraces) GetOldestUnpublishedEventAge(ctx context.Context, shardID string) (time.Duration, error) {
	ctx, span := platform.GetTracer().Start(ctx, "outbox.store.get_oldest_age",
		trace.WithAttributes(
			attribute.String(platform.MetricLabelShard, shardID)))
	defer span.End()

	return t.next.GetOldestUnpublishedEventAge(ctx, shardID)
}

func (t *eventStoreTraces) PurgePublishedEvents(ctx context.Context, shardID string, olderThan time.Duration) error {
	ctx, span := platform.GetTracer().Start(ctx, "outbox.store.purge",
		trace.WithAttributes(
			attribute.String(platform.MetricLabelShard, shardID),
			attribute.String("older_than", olderThan.String())))
	defer span.End()

	return t.next.PurgePublishedEvents(ctx, shardID, olderThan)
}

func (t *eventStoreTraces) RouteToDLQ(ctx context.Context, shardID string, event domain.Event, reason string) error {
	ctx, span := platform.GetTracer().Start(ctx, "outbox.store.route_dlq",
		trace.WithAttributes(
			attribute.String(platform.MetricLabelShard, shardID),
			attribute.String(platform.MetricLabelJobID, event.AggregateID)))
	defer span.End()

	if err := t.next.RouteToDLQ(ctx, shardID, event, reason); err != nil {
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

// Trace wrapper for EventPublisher - injects trace context into Kafka headers
type eventPublisherTraces struct {
	next port.EventPublisher
}

func NewEventPublisherTraces(next port.EventPublisher) port.EventPublisher {
	return &eventPublisherTraces{next: next}
}

func (t *eventPublisherTraces) PublishEvents(ctx context.Context, events []domain.Event) ([]string, error) {
	ctx, span := platform.GetTracer().Start(ctx, "outbox.publisher.publish",
		trace.WithAttributes(
			attribute.Int("event_count", len(events))))
	defer span.End()

	// CRITICAL: Inject trace context into Kafka headers for trace continuity
	for i := range events {
		if events[i].Headers == nil {
			events[i].Headers = make(map[string]string)
		}
		carrier := propagation.MapCarrier{}
		otel.GetTextMapPropagator().Inject(ctx, carrier)
		for k, v := range carrier {
			events[i].Headers[k] = v
		}
	}

	ids, err := t.next.PublishEvents(ctx, events)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.SetAttributes(
			attribute.String(platform.MetricLabelErrorType, fmt.Sprintf("%T", err)),
			attribute.String(platform.MetricLabelErrorMessage, err.Error()),
		)
		return ids, err
	}

	span.SetAttributes(attribute.StringSlice("published_event_ids", ids))
	return ids, nil
}