package app

import (
	"context"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/port"
	"go.uber.org/zap"
)

const (
	PoolInterval      = 500 * time.Millisecond // 500ms pool interval across all shards
	BatchSize    uint = 1000                   // 1000 events per batch across all shards
)

type RelayService struct {
	store     port.EventStore
	publisher port.EventPublisher
	log       *zap.Logger
}

func NewRelayService(store port.EventStore, publisher port.EventPublisher, log *zap.Logger) *RelayService {
	return &RelayService{
		store:     store,
		publisher: publisher,
		log:       log,
	}
}

// Start runs an infinite polling loop until the context is canceled.
func (s *RelayService) Start(ctx context.Context, shardID string) {
	ticker := time.NewTicker(PoolInterval)
	defer ticker.Stop()
	s.log.Info("Starting relay service", zap.String("shard", shardID))

	for {
		select {
		case <-ctx.Done():
			s.log.Info("Shutting down relay service", zap.String("shard", shardID))
			return
		// Poll outbox table for events
		case <-ticker.C:
			s.processBatch(ctx, shardID)
		}
	}
}

func (s *RelayService) processBatch(ctx context.Context, shardID string) {
	// 1. Fetch Batch of Unpublished events
	events, err := s.store.FetchUnpublishedEvents(ctx, shardID, int(BatchSize))
	if err != nil || events == nil {
		s.log.Error("Failed to fetch unpublished events", zap.Error(err), zap.String("shard", shardID))
		return
	}

	if len(events) == 0 {
		s.log.Info("No unpublished events", zap.String("shard", shardID))
		return
	}

	// 2. Publish Events
	eventIDs, err := s.publisher.PublishEvents(ctx, events)
	if err != nil {
		s.log.Error("Failed to publish events", zap.Error(err), zap.String("shard", shardID))
		return
	}

	// 3. Mark Published ONLY after successful Kafka ACK
	if err := s.store.MarkPublished(ctx, shardID, eventIDs); err != nil {
		s.log.Error("Failed to mark events as published", zap.Error(err), zap.String("shard", shardID))
		return
	}
}
