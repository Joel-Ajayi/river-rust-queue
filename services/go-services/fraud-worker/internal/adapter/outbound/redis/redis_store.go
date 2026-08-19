package redis

import (
	"context"
	"fmt"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/core/port"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	client redis.UniversalClient
}

var _ port.RedisStore = (*RedisStore)(nil)

func NewRedisStore(client redis.UniversalClient) *RedisStore {
	return &RedisStore{client: client}
}

func (s *RedisStore) UpdateVelocity(ctx context.Context, walletID string, eventID string, timestampMs int64, windowMs int) (int, error) {
	key := fmt.Sprintf(platform.RedisKeyVelocity, walletID)

	count, err := s.client.Eval(ctx, `
		-- KEYS[1] = velocity:wallet:<id>
		-- ARGV[1] = now_ms
		-- ARGV[2] = event_id
		-- ARGV[3] = window_ms

		redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
		redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1] - ARGV[3])
		redis.call('PEXPIRE', KEYS[1], ARGV[3] * 2)  -- TTL prevents long-idle sets from growing
		return redis.call('ZCARD', KEYS[1])`,
		[]string{key}, timestampMs, eventID, windowMs).Int64()
	if err != nil {
		return 0, err
	}

	return int(count), nil
}
