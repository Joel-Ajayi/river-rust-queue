package port

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
)

// TransferSubmitter is the driving port for accepting a transfer request.
type TransferSubmitter interface {
	Submit(ctx context.Context, t domain.Transfer, idempKey string) (domain.SubmitResult, error)
}

type Authenticator interface {
	Authenticate(ctx context.Context, apiKey string) (domain.Principal, error)
}

// JobReader is the driving port for retrieving a job's status.
type JobReader interface {
	GetJobStatus(ctx context.Context, merchantID, jobID string) (domain.Job, error)
}

// APIGatewayService is the unified driving port for the API gateway application.
type APIGatewayService interface {
	TransferSubmitter
	Authenticator
	JobReader
}
