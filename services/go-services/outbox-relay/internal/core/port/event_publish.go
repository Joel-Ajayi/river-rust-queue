package port

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/domain"
)

// -- outbound --
// EventPublisher is a driven port for publishing events to Kafka.
type EventPublisher interface {
	PublishEvents(ctx context.Context, events []domain.Event) ([]string, error)
}
