package app

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/domain"
	"go.uber.org/zap"
)

// processEvents runs the full batch pipeline: validate, group by key, fan-out sequentially per key, collect results.
func (s *RelayService) processEvents(ctx context.Context, events []domain.Event) (err error) {
	shardID := s.shardID

	// 1. Set up panic recovery defer to write poison events to DLQ and record outbox panic metrics
	defer func() {
		if r := recover(); r != nil {
			platform.RecordOutboxPanic(ctx, shardID)
			platform.LoggerWithTrace(ctx, s.log).Error(platform.LogEventPanicRecoveredDLQ,
				zap.String(platform.LogFieldShardID, shardID),
				zap.Any(platform.LogFieldPanic, r),
				zap.String(platform.LogFieldStack, string(debug.Stack())),
			)
			var dlqFailed bool
			for _, e := range events {
				if dlqErr := s.store.RouteToDLQ(ctx, e, fmt.Sprintf("%v: %v", domain.ReasonPanic, r)); dlqErr != nil {
					platform.LoggerWithTrace(ctx, s.log).Error(platform.LogEventPanicDLQWriteFailed,
						zap.String(platform.LogFieldShardID, shardID),
						zap.String(platform.LogFieldEventID, e.ID),
						zap.Error(dlqErr),
					)
					dlqFailed = true
				}
			}
			if dlqFailed {
				err = fmt.Errorf("panic recovered, but DLQ write failed: %v", r)
			} else {
				err = nil // Successfully written to DLQ. Return nil to commit transaction and mark poison pills as published
			}
		}
	}()

	var validEvents []domain.Event

	// 2. Validate payload size and schema for each event; route invalid ones to DLQ
	for _, e := range events {
		valid, vErr := s.validateEvent(ctx, shardID, e)
		if vErr != nil {
			return vErr
		}
		if valid {
			validEvents = append(validEvents, e)
		}
	}

	if len(validEvents) == 0 {
		return nil
	}

	// 3. Publish valid events to Kafka as a single batch
	if _, err := s.publisher.PublishBatch(ctx, shardID, validEvents); err != nil {
		return err
	}

	// 4. Emit canonical log upon successful batch publish
	platform.LogCanonicalEvent(ctx, s.log, platform.ServiceNameOutboxRelay, platform.CanonicalLogLine{
		Event:  platform.EventOutboxPublished,
		Status: platform.StatusSuccess,
	})

	return nil
}

// validateEvent checks payload integrity; returns false to route to DLQ.
func (s *RelayService) validateEvent(ctx context.Context, shardID string, e domain.Event) (bool, error) {
	// 1. Check if event payload exceeds maximum byte budget
	if len(e.Payload) > s.maxPayloadSize {
		platform.LoggerWithTrace(ctx, s.log).Warn(platform.LogEventPoisonPill,
			zap.String(platform.LogFieldShardID, shardID),
			zap.String(platform.LogFieldEventID, e.ID),
			zap.String(platform.LogFieldReason, domain.ReasonMessageTooLarge),
		)
		return false, s.store.RouteToDLQ(ctx, e, domain.ReasonMessageTooLarge)
	}

	// 2. Validate protobuf EventEnvelope structure
	if _, err := platform.UnmarshalEnvelope(e.Payload); err != nil {
		platform.LoggerWithTrace(ctx, s.log).Warn(platform.LogEventPoisonPill,
			zap.String(platform.LogFieldShardID, shardID),
			zap.String(platform.LogFieldEventID, e.ID),
			zap.String(platform.LogFieldReason, domain.ReasonInvalidSchema),
			zap.Error(err),
		)
		return false, s.store.RouteToDLQ(ctx, e, domain.ReasonInvalidSchema)
	}

	return true, nil
}
