package port

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MerchantDirectory is a driven port for looking up merchant information
type MerchantDirectory interface {
	ShardFor(ctx context.Context, merchantID string) (string, error)
	AuthenticateAPIKey(ctx context.Context, apiKey string) (domain.Principal, error)
}

// WalletDirectory is a driven port for verifying wallet information on a shard.
type WalletDirectory interface {
	CheckWalletOwnership(ctx context.Context, shardID, walletID, merchantID string) error
}

// ShardPools is driven port for looking up pool for the given shard ID or an error if unknown.
type ShardPools interface {
	ShardPool(shardId string) (*pgxpool.Pool, error)
}

// JobStore is a driven port for persisting jobs and idempotency claims.
type JobStore interface {
	ClaimAndRecord(
		ctx context.Context,
		shardID string,
		job domain.Job,
		t domain.Transfer,
		idempKey string,
	) (domain.SubmitResult, error)

	GetJob(ctx context.Context, shardID, jobID string) (domain.Job, error)
}
