package redis

import (
	"context"
	"fmt"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/core/port"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	client *redis.Client
}

var _ port.RedisStore = (*RedisStore)(nil)

func NewRedisStore(client *redis.Client) *RedisStore {
	return &RedisStore{client: client}
}

func (s *RedisStore) UpdateVelocity(ctx context.Context, walletID string, eventID string, timestampMs int64, windowSeconds int) (int, error) {
	key := fmt.Sprintf(platform.RedisKeyVelocity, walletID)
	windowMs := int64(windowSeconds) * platform.MillisecondsPerSecond

	res, err := s.client.Eval(ctx, `
		-- KEYS[1] = velocity:wallet:<id>
		-- ARGV[1] = now_ms
		-- ARGV[2] = event_id
		-- ARGV[3] = window_ms

		redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
		redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1] - ARGV[3])
		redis.call('PEXPIRE', KEYS[1], ARGV[3] * 2)  -- TTL prevents long-idle sets from growing
		return redis.call('ZCARD', KEYS[1])`,
		[]string{key}, timestampMs, eventID, windowMs).Result()
	if err != nil {
		return 0, err
	}

	count, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("unexpected script return type: %T", res)
	}

	return int(count), nil
}
