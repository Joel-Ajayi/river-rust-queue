package port

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
)

// -- Outgoing ports --
// MerchantDirectory is a driven port for looking up merchant information
type MerchantDirectory interface {
	ShardFor(ctx context.Context, merchantID string) (string, error)
}

// -- Incoming ports --
// MerchantStore is a driven port for merchant persistence.
type MerchantStore interface {
	Create(ctx context.Context, id, name, apiKeyHash, tier, status, shardID, webhookURL, webhookSecret string) error
	AuthenticateAPIKey(ctx context.Context, apiKey string) (domain.Principal, error)
}

// MerchantUseCase is a driving port for creating merchants.
type MerchantUseCase interface {
	CreateMerchant(ctx context.Context, name, webhookURL, webhookSecret, tier string) (string, string, string, error)
	Authenticate(ctx context.Context, apiKey string) (domain.Principal, error)
}
