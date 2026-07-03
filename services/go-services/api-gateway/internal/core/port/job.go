package port

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
)

// -- Incoming ports --
// JobReader is the driving port for retrieving a job's status.
type JobReader interface {
	GetJobStatus(ctx context.Context, merchantID, jobID string) (domain.Job, error)
}

// JobStore is a driven port for persisting jobs and idempotency claims.
type JobStore interface {
	ClaimAndRecord(
		ctx context.Context,
		shardID string,
		job domain.Job,
		t domain.Transfer,
		idempKey string,
	) (domain.SubmitResult, error)

	GetJob(ctx context.Context, shardID, jobID string) (domain.Job, error)
}
