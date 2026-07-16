package app

import (
	"context"
	"encoding/json"
	"time"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/port"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
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

func (s *RelayService) Start(ctx context.Context, shardID string) {
	purgeTicker := time.NewTicker(domain.OutboxPurgeInterval)
	defer purgeTicker.Stop()

	s.log.Info("Starting relay service", zap.String(platform.LogFieldShardID, shardID))

	attempt := 0
	for {
		jitterDelay := domain.RelayPoolInterval
		if attempt > 0 {
			// Outer layer exponential backoff for infrastructure failures
			jitterDelay = platform.CalculateJitterBackoff(attempt, domain.RelayBackoffMinDelay, domain.RelayBackoffMaxDelay)
		}

		timer := time.NewTimer(jitterDelay)

		select {
		case <-ctx.Done():
			timer.Stop()
			s.log.Info("Shutting down relay service", zap.String(platform.LogFieldShardID, shardID))
			return
		case <-purgeTicker.C:
			timer.Stop()
			if err := s.store.PurgePublishedEvents(ctx, shardID, domain.OutboxPurgeAge); err != nil {
				// Purge failure is non-fatal; log at debug level if needed
				_ = err
			}
		case <-timer.C:
			err := s.processBatch(ctx, shardID)
			if err != nil {
				attempt++
			} else {
				attempt = 0
			}
		}
	}
}

func (s *RelayService) processBatch(ctx context.Context, shardID string) error {
	// Record Outbox Lag Metric
	lag, lagErr := s.store.GetOldestUnpublishedEventAge(ctx, shardID)
	if lagErr == nil {
		platform.RecordOutboxLag(ctx, shardID, lag)
	}

	// 1. Process Batch of Unpublished events
	batchCtx, batchCancel := context.WithTimeout(ctx, domain.RelayProcessTimeout)
	defer batchCancel()
	err := s.store.ProcessUnpublishedEvents(batchCtx, shardID, domain.RelayBatchSize, func(ctx context.Context, events []domain.Event) (err error) {
		// Panic Middleware for Poison Pills - per-event isolation
		defer func() {
			if r := recover(); r != nil {
				s.log.Error("Panic recovered during event processing. Routing failed events to DLQ.", zap.Any(platform.LogFieldPanic, r))
				// Note: We cannot identify which specific event caused the panic without per-event try-catch.
				// Route all events in batch to DLQ as safety measure, but log for investigation.
				for _, e := range events {
					_ = s.store.RouteToDLQ(ctx, shardID, e, domain.ReasonPanic)
				}
				// Force the database to COMMIT and mark the batch as published to permanently break the poison loop
				err = nil
			}
		}()

		var validEvents []domain.Event

		for _, e := range events {
			// Per-event panic isolation for precise DLQ routing
			func() {
				defer func() {
					if r := recover(); r != nil {
						s.log.Error("Panic processing individual event, routing to DLQ", zap.Any(platform.LogFieldPanic, r), zap.String(platform.LogFieldEventID, e.ID))
						_ = s.store.RouteToDLQ(ctx, shardID, e, domain.ReasonPanic)
					}
				}()

				//1. Corrupted payload check
				if !json.Valid(e.Payload) {
					if dlqErr := s.store.RouteToDLQ(ctx, shardID, e, domain.ReasonCorruptedPayload); dlqErr != nil {
						err = dlqErr // capture for outer return
					}
					return
				}

				//2. Message too large check
				if len(e.Payload) > platform.KafkaMaxMessageBytes {
					if dlqErr := s.store.RouteToDLQ(ctx, shardID, e, domain.ReasonMessageTooLarge); dlqErr != nil {
						err = dlqErr
					}
					return
				}

				//3. TopicNotify specific validation
				if e.PublishTopic == platform.TopicNotify {
					var envelope eventsv1.EventEnvelope
					if unmarshalErr := protojson.Unmarshal(e.Payload, &envelope); unmarshalErr != nil {
						if dlqErr := s.store.RouteToDLQ(ctx, shardID, e, domain.ReasonInvalidSchema); dlqErr != nil {
							err = dlqErr
						}
						return
					}
				}

				validEvents = append(validEvents, e)
			}()
			if err != nil {
				return err
			}
		}

		// 4. Publish Valid Events
		if len(validEvents) > 0 {
			publishCtx, publishCancel := context.WithTimeout(ctx, domain.RelayPublishTimeout)
			_, err := s.publisher.PublishEvents(publishCtx, validEvents)
			publishCancel()
			if err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return err
	}
	return nil
}
