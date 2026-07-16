package port

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
)

// -- Incoming Ports --
type Authenticator interface {
	Authenticate(ctx context.Context, apiKey string) (domain.Principal, error)
}
