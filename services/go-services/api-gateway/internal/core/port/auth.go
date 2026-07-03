package port

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
)

// -- Incoming Ports --
type Authenticator interface {
	Authenticate(ctx context.Context, apiKey string) (domain.Principal, error)
	GetPrincipal(ctx context.Context, merchantID string) (domain.Principal, error)
}
