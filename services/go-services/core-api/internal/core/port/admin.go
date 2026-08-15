package port

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

// AdminUseCase is the driving port for administrative operator actions.
type AdminUseCase interface {
	ReplayDLQ(ctx context.Context, source string, limit int) (ReplayResult, error)
	ListDLQEntries(ctx context.Context, source string, status string, limit int, offset int) ([]platform.DLQEntrySummary, error)
	ReplayDLQEntry(ctx context.Context, source string, id string) (platform.DLQEntrySummary, error)
}

// ReplayResult is the outcome of a batch DLQ replay operation (driven port).
// Kept here (not domain) so replay plumbing aligns with the eventsv1 proto.
type ReplayResult struct {
	ReplayedCount int
}

// DLQReplayer is the driven port for replaying / inspecting dead-letter entries.
type DLQReplayer interface {
	ReplayDLQ(ctx context.Context, source string, limit int) (int, error)
	ListDLQEntries(ctx context.Context, source string, status string, limit int, offset int) ([]platform.DLQEntrySummary, error)
	ReplayDLQEntry(ctx context.Context, source string, id string) (platform.DLQEntrySummary, error)
}
