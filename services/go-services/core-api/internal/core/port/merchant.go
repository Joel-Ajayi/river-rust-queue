package port

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
)

// -- Outgoing ports --

// MerchantDirectory is a driven port for looking up merchant information
type MerchantDirectory interface {
	ShardFor(ctx context.Context, merchantID string) (string, error)
	AuthenticateAPIKey(ctx context.Context, apiKey string) (domain.Principal, error)
}
