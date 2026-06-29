// Command api-gateway is the composition root for the gateway service.
//
// This is the ONE place allowed to know every concrete type: it constructs the
// infrastructure (config, pools, redis), builds the driven adapters, injects
// them into the core use-cases, hands the core to the driving adapter, and runs.
// Nothing below this file imports anything "more concrete" than an interface.
package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/adapter/inbound/rest"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/adapter/outbound/postgres"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/app"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func main() {
	cfg, err := platform.LoadConfig()
	if err != nil {
		panic("config: " + err.Error())
	}

	log, err := platform.NewLogger(cfg.LogLevel)
	if err != nil {
		panic("logger: " + err.Error())
	}
	defer log.Sync()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Infrastructure ---
	pools, err := platform.NewShardPools(ctx, cfg, log)
	if err != nil {
		log.Fatal("postgres pools", zap.Error(err))
	}
	defer pools.Close()

	rdb, err := platform.NewRedisClient(ctx, cfg.RedisAddr(), log)
	if err != nil {
		log.Fatal("redis", zap.Error(err))
	}
	defer rdb.Close()

	// --- Driven adapters (outbound) ---
	directory := postgres.NewMerchantDirectory(pools)
	jobStore := postgres.NewJobStore(pools, platform.NewEventID)

	// --- Core use-cases ---
	svc := app.NewService(directory, jobStore, platform.NewJobID)

	// --- Driving adapter (inbound) ---
	ready := func(ctx context.Context) error { return readiness(ctx, pools, rdb) }
	srv := rest.NewServer(rest.Addr(cfg.HTTPPort), svc, svc, cfg.JWTSigningKey, ready, log)

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Info("received shutdown signal")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer shutdownCancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Error("shutdown error", zap.Error(err))
		}
		cancel()
	}()

	if err := srv.Start(); err != nil && err != http.ErrServerClosed {
		log.Fatal("server error", zap.Error(err))
	}
}

// readiness pings every backing store the gateway needs to serve traffic.
func readiness(ctx context.Context, pools *platform.ShardPools, rdb *redis.Client) error {
	if err := pools.Ping(ctx); err != nil {
		return err
	}
	return rdb.Ping(ctx).Err()
}
