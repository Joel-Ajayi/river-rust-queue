package resilience

import (
	"context"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/port"
	"github.com/failsafe-go/failsafe-go"
)

// eventPublisherRetry adds per-event retry (capacity-derived backoff, full jitter) with a
type eventPublisherRetry struct {
	next           port.EventPublisher
	retryCfg       platform.RetryConfig
	publishTimeout time.Duration
}

func NewEventPublisherRetry(next port.EventPublisher, retryCfg platform.RetryConfig, publishTimeout time.Duration) port.EventPublisher {
	return &eventPublisherRetry{next: next, retryCfg: retryCfg, publishTimeout: publishTimeout}
}

func (r *eventPublisherRetry) PublishBatch(ctx context.Context, shardID string, events []domain.Event) ([]string, error) {
	var published []string

	err := platform.ExecuteWithJitter(ctx, r.retryCfg, func(exec failsafe.Execution[any]) error {
		// Fresh timeout context per attempt
		attemptCtx, cancel := context.WithTimeout(ctx, r.publishTimeout)
		defer cancel()

		var pubErr error
		published, pubErr = r.next.PublishBatch(attemptCtx, shardID, events)
		return pubErr
	})

	return published, err
}
