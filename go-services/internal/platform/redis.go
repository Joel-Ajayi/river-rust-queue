package platform

import (
	"context"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// NewRedisClient connects to the data Redis instance.
func NewRedisClient(ctx context.Context, addr string, log *zap.Logger) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{Addr: addr})
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}
	log.Info("connected to redis", zap.String("addr", addr))
	return client, nil
}
