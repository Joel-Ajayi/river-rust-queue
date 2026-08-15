package port

import (
	"context"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
)

// DLQStore is for failed messages that can't be retried. Entries are the global
// eventsv1.DLQEntry proto — never a per-service domain struct.
type DLQStore interface {
	WriteDLQEntry(ctx context.Context, entry *eventsv1.DLQEntry) error
}
