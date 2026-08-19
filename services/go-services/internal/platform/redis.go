package platform

import (
	"context"
	"fmt"

	redisotel "github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// NewRedisClient connects to the data Redis instance with optional password authentication.
func NewRedisClient(ctx context.Context, masterName string, addr string, password string, log *zap.Logger) (redis.UniversalClient, error) {
	client := redis.NewUniversalClient(&redis.UniversalOptions{
		MasterName:       masterName,
		Addrs:            []string{addr},
		Password:         password,
		SentinelPassword: password,

		// Route read operations to the nearest/random replica pod
		RouteByLatency: true,
	})
	if err := redisotel.InstrumentTracing(client, redisotel.WithDBStatement(false)); err != nil {
		return nil, fmt.Errorf("instrument redis tracing: %w", err)
	}
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}
	redisLog := log.Named(LogComponentRedis)
	redisLog.Info("Connected to Redis database", zap.String(LogFieldEvent, LogEventRedisConnected), zap.String(LogFieldAddr, addr))
	return client, nil
}
