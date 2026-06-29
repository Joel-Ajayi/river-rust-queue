package port

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
)

// MerchantDirectory is a driven port over the global merchants database.
// The postgres adapter implements it; the app package depends only on this.
type MerchantDirectory interface {
	// ShardFor returns the shard id that owns the merchant, or
	// domain.ErrMerchantInactive if the merchant is not active.
	ShardFor(ctx context.Context, merchantID string) (string, error)

	// AuthenticateAPIKey resolves a raw API key to its merchant identity, or
	// returns domain.ErrInvalidCredentials / domain.ErrMerchantInactive.
	AuthenticateAPIKey(ctx context.Context, apiKey string) (domain.Principal, error)
}

// JobStore is a driven port that persists an accepted job and its outbox event
// atomically, claiming the idempotency key in the same transaction.
type JobStore interface {
	// ClaimAndRecord inserts the job + outbox event on the given shard. If the
	// (merchant, key) pair already exists it returns the prior job with
	// AlreadyExisted=true, or domain.ErrIdempotencyConflict when the stored
	// request hash differs from requestHash.
	ClaimAndRecord(
		ctx context.Context,
		shardID string,
		job domain.Job,
		t domain.Transfer,
		idempotencyKey string,
		requestHash string,
	) (domain.SubmitResult, error)
}
