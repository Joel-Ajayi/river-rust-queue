package port

import (
	"context"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/kong-sync-worker/internal/core/domain"
)

// MerchantDirectory provides access to merchant data.
type MerchantDirectory interface {
	// GetActiveMerchants returns all merchants with status = 'active'.
	GetActiveMerchants(ctx context.Context) ([]domain.Merchant, error)
}
