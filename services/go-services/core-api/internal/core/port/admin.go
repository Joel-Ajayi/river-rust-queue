package port

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
)

// -- Driving ports (Primary) --
// AdminUseCase is the driving port for administrative operator actions.
type AdminUseCase interface {
	ReplayDLQ(ctx context.Context, shardID string, source string, limit int) (domain.DLQReplayResult, error)
}

// -- Driven ports (Secondary) --
// DLQReplayer is the driven port for replaying dead-letter entries from persistence.
type DLQReplayer interface {
	ReplayDLQ(ctx context.Context, shardID string, source string, limit int) (int, error)
}
