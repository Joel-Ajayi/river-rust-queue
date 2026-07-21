package port

import (
	"context"
)

type MerchantDirectory interface {
	ShardFor(ctx context.Context, merchantID string) (string, error)
}
