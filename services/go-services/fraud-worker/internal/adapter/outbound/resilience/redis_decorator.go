package resilience

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/core/port"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

type redisStoreResilience struct {
	next port.RedisStore
	cb   *platform.RedisCircuitBreaker
}

// NewRedisStoreResilience guards all velocity counter writes with a single
// Redis circuit breaker. While the breaker is open, calls fail fast without
// touching the Redis client, so the hot path never stalls on a dead Redis.
func NewRedisStoreResilience(next port.RedisStore, cb *platform.RedisCircuitBreaker) port.RedisStore {
	return &redisStoreResilience{next: next, cb: cb}
}

func (r *redisStoreResilience) UpdateVelocity(ctx context.Context, walletID string, eventID string, timestampMs int64, windowMs int) (int, error) {
	res, err := r.cb.Execute(ctx, func() (any, error) {
		return r.next.UpdateVelocity(ctx, walletID, eventID, timestampMs, windowMs)
	})
	if err != nil {
		return 0, err
	}
	return res.(int), nil
}
