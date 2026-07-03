package port

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/domain"
)

// EventStore is a driven (outbound) port for retrieving and updating outbox events in the database.
type EventStore interface {
	// FetchUnpublished retrieves up to 'limit' events that haven't been published yet.
	FetchUnpublishedEvents(ctx context.Context, shardID string, limit int) ([]domain.Event, error)
	// MarkPublished updates the events to indicate they've been successfully sent to Kafka.
	MarkAsPublished(ctx context.Context, shardID string, eventIDs []string) error
}
