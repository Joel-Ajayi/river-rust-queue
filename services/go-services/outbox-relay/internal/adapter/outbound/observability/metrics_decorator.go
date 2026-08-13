package observability

import (
	"context"
	"errors"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/port"
	"github.com/failsafe-go/failsafe-go/circuitbreaker"
)

type metricsPublisherDecorator struct {
	next    port.EventPublisher
	shardID string
}

// NewMetricsPublisherDecorator wraps an EventPublisher to record duration and traffic metrics.
func NewMetricsPublisherDecorator(next port.EventPublisher, shardID string) port.EventPublisher {
	return &metricsPublisherDecorator{
		next:    next,
		shardID: shardID,
	}
}

func (m *metricsPublisherDecorator) PublishBatch(ctx context.Context, shardID string, events []domain.Event) ([]string, error) {
	if len(events) == 0 {
		return m.next.PublishBatch(ctx, shardID, events)
	}

	res, err := m.next.PublishBatch(ctx, shardID, events)
	if err != nil {
		if platform.ClassifyError(err, nil) == platform.ClassificationInfrastructure || errors.Is(err, circuitbreaker.ErrOpen) {
			platform.RecordInfrastructureError(ctx, platform.ComponentKafkaPublisher)
		}
		return res, err
	}

	topicCounts := make(map[string]int)
	for _, e := range events {
		topicCounts[e.PublishTopic]++
	}
	for topic, count := range topicCounts {
		cleanTopic := topic
		if len(topic) > len(platform.TopicXShardPrefix) && topic[:len(platform.TopicXShardPrefix)] == platform.TopicXShardPrefix {
			cleanTopic = platform.TopicLabelXShard
		}
		platform.RecordOutboxEventsPublished(ctx, m.shardID, cleanTopic, count)
	}
	return res, nil
}

type metricsStoreDecorator struct {
	next    port.EventStore
	shardID string
}

// NewMetricsStoreDecorator wraps an EventStore
func NewMetricsStoreDecorator(next port.EventStore, shardID string) port.EventStore {
	return &metricsStoreDecorator{
		next:    next,
		shardID: shardID,
	}
}

func (m *metricsStoreDecorator) ProcessUnpublishedEvents(ctx context.Context, shardID string, batchSize int, processor func(ctx context.Context, events []domain.Event) error) error {
	err := m.next.ProcessUnpublishedEvents(ctx, shardID, batchSize, processor)
	if err != nil && (platform.ClassifyError(err, nil) == platform.ClassificationInfrastructure || platform.ClassifyError(err, nil) == platform.ClassificationTransient || errors.Is(err, circuitbreaker.ErrOpen)) {
		platform.RecordInfrastructureError(ctx, platform.ComponentEventStore)
	}
	return err
}

func (m *metricsStoreDecorator) GetOldestUnpublishedEventAge(ctx context.Context, shardID string) (time.Duration, error) {
	dur, err := m.next.GetOldestUnpublishedEventAge(ctx, shardID)
	if err != nil && (platform.ClassifyError(err, nil) == platform.ClassificationInfrastructure || platform.ClassifyError(err, nil) == platform.ClassificationTransient || errors.Is(err, circuitbreaker.ErrOpen)) {
		platform.RecordInfrastructureError(ctx, platform.ComponentEventStore)
	}
	return dur, err
}

func (m *metricsStoreDecorator) RouteToDLQ(ctx context.Context, shardID string, event domain.Event, reason string) error {
	err := m.next.RouteToDLQ(ctx, shardID, event, reason)
	if err != nil && (platform.ClassifyError(err, nil) == platform.ClassificationInfrastructure || platform.ClassifyError(err, nil) == platform.ClassificationTransient || errors.Is(err, circuitbreaker.ErrOpen)) {
		platform.RecordInfrastructureError(ctx, platform.ComponentEventStore)
	}
	return err
}
