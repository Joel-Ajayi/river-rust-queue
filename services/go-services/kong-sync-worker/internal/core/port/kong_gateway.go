package port

import (
	"context"
)

// KongGateway provides methods to sync configuration to Kong API Gateway.
type KongGateway interface {
	SyncConsumer(ctx context.Context, merchantID string) error
	SyncRateLimitPlugin(ctx context.Context, merchantID, tier string) error
	SyncJWTCredential(ctx context.Context, merchantID string) error
}
