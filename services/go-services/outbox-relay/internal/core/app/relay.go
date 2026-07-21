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
	"google.golang.org/protobuf/proto"
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

	platform.LoggerWithTrace(ctx, s.log).Info(platform.LogEventRelayServiceStarted, zap.String(platform.LogFieldShardID, shardID))

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
			platform.LoggerWithTrace(ctx, s.log).Info(platform.LogEventRelayServiceShutdown, zap.String(platform.LogFieldShardID, shardID))
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
				platform.LoggerWithTrace(ctx, s.log).Error(platform.LogEventRelayBatchProcessFailed, zap.Error(err), zap.String(platform.LogFieldShardID, shardID))
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
				platform.LoggerWithTrace(ctx, s.log).Error(platform.LogEventPanicRecoveredDLQ, zap.Any(platform.LogFieldPanic, r))
				var lastDlqErr error
				for _, e := range events {
					if dlqErr := s.store.RouteToDLQ(ctx, shardID, e, domain.ReasonPanic); dlqErr != nil {
						lastDlqErr = dlqErr
					}
				}
				// If DLQ write fails, propagate error to rollback the transaction; otherwise nil to commit
				err = lastDlqErr
			}
		}()

		var validEvents []domain.Event

		// 1. Validate each event in the batch and route invalid ones to DLQ
		// maintain order of events in the batch
		for _, e := range events {
			// 1. Corrupted payload check
			if !json.Valid(e.Payload) {
				if dlqErr := s.store.RouteToDLQ(ctx, shardID, e, domain.ReasonCorruptedPayload); dlqErr != nil {
					err = dlqErr
				}
				continue
			}

			// 2. Message too large check
			if len(e.Payload) > platform.KafkaMaxMessageBytes {
				if dlqErr := s.store.RouteToDLQ(ctx, shardID, e, domain.ReasonMessageTooLarge); dlqErr != nil {
					err = dlqErr
				}
				continue
			}

			// 3. Schema validation
			var envelope eventsv1.EventEnvelope
			if unmarshalErr := proto.Unmarshal(e.Payload, &envelope); unmarshalErr != nil {
				if dlqErr := s.store.RouteToDLQ(ctx, shardID, e, domain.ReasonInvalidSchema); dlqErr != nil {
					err = dlqErr
				}
				continue
			}

			validEvents = append(validEvents, e)
		}

		// 4. Publish Valid Events
		if len(validEvents) > 0 {
			publishCtx, publishCancel := context.WithTimeout(ctx, domain.RelayPublishTimeout)
			_, err := s.publisher.PublishEvents(publishCtx, validEvents)
			publishCancel()
			if err != nil {
				return err
			}

			// Canonical log: outbox events published (per event)
			for _, e := range validEvents {
				platform.LogCanonicalEvent(ctx, s.log, platform.ServiceNameOutboxRelay, platform.CanonicalLogLine{
					Event:      platform.EventOutboxPublished,
					Status:     platform.StatusSuccess,
					TransferID: e.AggregateID,
				})
			}
		}

		return nil
	})

	if err != nil {
		return err
	}
	return nil
}
