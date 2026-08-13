package platform

import (
	"context"
	"fmt"

	redisotel "github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// NewRedisClient connects to the data Redis instance with optional password authentication.
func NewRedisClient(ctx context.Context, addr string, password string, log *zap.Logger) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
	})
	// Manually trace every Redis command (eBPF auto-instrumentation does not
	// cover go-redis). db.statement is intentionally DISABLED — the velocity
	// Lua script embeds wallet/event identifiers that must not reach traces.
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
