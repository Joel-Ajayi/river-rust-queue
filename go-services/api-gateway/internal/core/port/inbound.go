// Package port declares the gateway's hexagonal boundaries as Go interfaces.
//
// Inbound (driving) ports are what the outside world calls to drive the core.
// Outbound (driven) ports are what the core calls to reach the outside world.
// Both are defined here, in terms of domain types only — never pgx, http, or kafka.
package port

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
)

// TransferSubmitter is the driving port for accepting a transfer request.
// The rest adapter calls this; the app package implements it.
type TransferSubmitter interface {
	Submit(ctx context.Context, t domain.Transfer, idempotencyKey string) (domain.SubmitResult, error)
}

// Authenticator is the driving port for exchanging an API key for an identity.
type Authenticator interface {
	Authenticate(ctx context.Context, apiKey string) (domain.Principal, error)
}
