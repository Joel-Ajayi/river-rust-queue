package port

import (
	"context"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/domain"
)

// EventStore is a driven (outbound) port for retrieving and updating outbox events in the database.
type EventStore interface {
	// FetchUnpublished retrieves up to 'limit' events that haven't been published yet.
	ProcessUnpublishedEvents(ctx context.Context, shardID string, limit int, publisher func(ctx context.Context, events []domain.Event) error) error
	GetOldestUnpublishedEventAge(ctx context.Context, shardID string) (time.Duration, error)
	RouteToDLQ(ctx context.Context, shardID string, event domain.Event, errorMsg string) error
}
