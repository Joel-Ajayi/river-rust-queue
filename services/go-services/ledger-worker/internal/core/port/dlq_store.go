package port

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/domain"
)

// DLQStore is for failed messages that can't be retried.
type DLQStore interface {
	WriteDLQEntry(ctx context.Context, shardID string, entry domain.DLQEntry) error
}
