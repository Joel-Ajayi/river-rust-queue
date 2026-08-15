package app

import (
	"context"
	"fmt"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/domain"
	"go.uber.org/zap"
)

// processEvents runs the full batch pipeline: validate, group by key, fan-out sequentially per key, collect results.
func (s *RelayService) processEvents(ctx context.Context, events []domain.Event) (err error) {
	shardID := s.shardID
	// Panic recovery: route all events to DLQ on panic.
	defer func() {
		if r := recover(); r != nil {
			platform.RecordOutboxPanic(ctx, shardID)
			platform.LoggerWithTrace(ctx, s.log).Error(platform.LogEventPanicRecoveredDLQ, zap.Any(platform.LogFieldPanic, r))
			var dlqFailed bool
			for _, e := range events {
				if dlqErr := s.store.RouteToDLQ(ctx, e, fmt.Sprintf("%v: %v", domain.ReasonPanic, r)); dlqErr != nil {
					s.log.Error("Failed to write to DLQ during panic recovery", zap.Error(dlqErr))
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

	// 1. Validate each event; route invalid ones to DLQ.
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

	// 2. Publish to Kafka (Single Batch)
	if _, err := s.publisher.PublishBatch(ctx, shardID, validEvents); err != nil {
		// Return error to rollback database transaction for retry
		return err
	}

	// 3. Canonical batch log
	platform.LogCanonicalEvent(ctx, s.log, platform.ServiceNameOutboxRelay, platform.CanonicalLogLine{
		Event:  platform.EventOutboxPublished,
		Status: platform.StatusSuccess,
	})

	return nil
}

// validateEvent checks payload integrity; returns false to route to DLQ.
func (s *RelayService) validateEvent(ctx context.Context, shardID string, e domain.Event) (bool, error) {
	if len(e.Payload) > s.maxPayloadSize {
		return false, s.store.RouteToDLQ(ctx, e, domain.ReasonMessageTooLarge)
	}
	// Accept both canonical JSON (protojson) and legacy binary protobuf
	// payloads. A payload that decodes under neither encoding is unroutable.
	if _, err := platform.UnmarshalEnvelope(e.Payload); err != nil {
		return false, s.store.RouteToDLQ(ctx, e, domain.ReasonInvalidSchema)
	}
	return true, nil
}
