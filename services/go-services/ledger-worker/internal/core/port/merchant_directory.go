package port

import (
	"context"
)

// MerchantDirectory tells us about merchants, used to enforce per-merchant rules.
type MerchantDirectory interface {
	ShardFor(ctx context.Context, merchantID string) (string, error)
}
