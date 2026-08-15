package app

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/port"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

// AdminService handles administrative platform operations.
type AdminService struct {
	replayer port.DLQReplayer
}

// NewAdminService creates a new AdminService application instance.
func NewAdminService(replayer port.DLQReplayer) *AdminService {
	return &AdminService{
		replayer: replayer,
	}
}

// ReplayDLQ validates replay parameters and executes batch DLQ replay on the target shard.
func (s *AdminService) ReplayDLQ(ctx context.Context, source string, limit int) (port.ReplayResult, error) {
	count, err := s.replayer.ReplayDLQ(ctx, source, limit)
	if err != nil {
		return port.ReplayResult{}, err
	}

	return port.ReplayResult{
		ReplayedCount: count,
	}, nil
}

// ListDLQEntries retrieves paginated summaries of open DLQ entries for operator review.
func (s *AdminService) ListDLQEntries(ctx context.Context, source string, status string, limit int, offset int) ([]platform.DLQEntrySummary, error) {
	return s.replayer.ListDLQEntries(ctx, source, status, limit, offset)
}

// ReplayDLQEntry republishes a specific open DLQ entry by ID and marks it replayed.
func (s *AdminService) ReplayDLQEntry(ctx context.Context, source string, id string) (platform.DLQEntrySummary, error) {
	return s.replayer.ReplayDLQEntry(ctx, source, id)
}
