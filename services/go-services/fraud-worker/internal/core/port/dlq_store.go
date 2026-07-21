package port

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/core/domain"
)

type DLQStore interface {
	WriteDLQEntry(ctx context.Context, shardID string, entry domain.DLQEntry) error
}
