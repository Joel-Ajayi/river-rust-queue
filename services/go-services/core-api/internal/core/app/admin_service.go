package app

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
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
func (s *AdminService) ReplayDLQ(ctx context.Context, shardID string, source string, limit int) (domain.DLQReplayResult, error) {
	if shardID == "" {
		shardID = platform.DefaultShardID
	}

	count, err := s.replayer.ReplayDLQ(ctx, shardID, source, limit)
	if err != nil {
		return domain.DLQReplayResult{}, err
	}

	return domain.DLQReplayResult{
		ReplayedCount: count,
		ShardID:       shardID,
	}, nil
}
