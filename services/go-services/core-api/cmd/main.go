package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/adapter/inbound/rest"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/adapter/outbound/observability"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/adapter/outbound/postgres"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/adapter/outbound/resilience"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/app"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/port"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/sony/gobreaker"
	"go.uber.org/zap"
)

func main() {
	// Config & Logger
	cfg, err := platform.LoadConfig()
	if err != nil {
		panic("config: " + err.Error())
	}

	log, err := platform.NewLogger(cfg.LogLevel)
	if err != nil {
		panic("logger: " + err.Error())
	}
	defer log.Sync()

	if err := platform.InitTelemetry(platform.ServiceNameAPIGateway); err != nil {
		log.Panic("Failed to initialize telemetry", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Infrastructure
	pools, err := platform.NewShardPools(ctx, cfg, log)
	if err != nil {
		log.Panic("Failed to initialize PostgreSQL pools", zap.String(platform.LogFieldEvent, platform.LogEventStartupFailed), zap.Error(err))
	}
	defer pools.Close()

	cbs := platform.NewDBCircuitBreakers(
		platform.CBNameMerchantsGlobal,
		pools.GetAvailableShardIDs(),
		domain.IsTerminalError,
		platform.CircuitBreakerConfig{
			MaxRequests: domain.CBMaxRequests,
			Timeout:     domain.CBTimeout,
			Interval:    domain.CBInterval,
			MinRequests: domain.CBMinRequests,
			ErrorRate:   domain.CBErrorRate,
		},
	)

	// Driven adapters (outbound)
	var merchantDir port.MerchantDirectory = postgres.NewMerchantDirectory(pools)
	merchantDir = resilience.NewMerchantDirectoryCB(merchantDir, cbs)
	merchantDir = observability.NewMerchantDirectoryMetrics(merchantDir)

	var walletDir port.WalletDirectory = postgres.NewWalletDirectory(pools)
	walletDir = resilience.NewWalletDirectoryCB(walletDir, cbs)
	walletDir = observability.NewWalletDirectoryMetrics(walletDir)

	var jobStore port.JobStore = postgres.NewJobStore(pools)
	jobStore = resilience.NewJobStoreCB(jobStore, cbs)
	jobStore = observability.NewJobStoreMetrics(jobStore)

	// Core use-cases
	authSvc := app.NewAuthService(merchantDir)
	jobSvc := app.NewJobService(merchantDir, jobStore)
	transferSvc := app.NewTransferService(merchantDir, walletDir, jobStore, platform.NewJobID)
	var transferSubmitter port.TransferSubmitter = transferSvc
	transferSubmitter = observability.NewTransferServiceMetrics(transferSubmitter)

	// Driving adapter (inbound)
	ready := func(ctx context.Context) error { return readiness(ctx, pools, cbs) }
	srv := rest.NewServer(cfg.HTTPPort, cfg.JWTSigningKeys, cfg.JWTActiveKeyID, authSvc, transferSubmitter, jobSvc, ready, log)

	go func() {
		if err := srv.Start(); err != nil && err != http.ErrServerClosed {
			log.Panic("Server encountered fatal error", zap.String(platform.LogFieldEvent, platform.LogEventServerFailed), zap.Error(err))
		}
	}()

	// Graceful Shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh  // Blocker until signal is received
	cancel() // Trigger global context cancellation

	log.Info("Received shutdown signal", zap.String(platform.LogFieldEvent, platform.LogEventShutdownSignalReceived))
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), domain.ServerShutdownTimeout)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("Failed to shutdown gracefully", zap.String(platform.LogFieldEvent, platform.LogEventShutdownFailed), zap.Error(err))
	}
	_ = platform.ShutdownTelemetry(shutdownCtx)

}

// readiness pings every backing store the gateway needs to serve traffic.
func readiness(ctx context.Context, pools *platform.ShardPools, cbs *platform.DBCircuitBreakers) error {
	// Check PG connectivity
	if err := pools.Ping(ctx); err != nil {
		return err
	}
	// Check circuit breaker state - if any CB is open, we're not ready
	if cbs.Merchants().State() == gobreaker.StateOpen {
		return fmt.Errorf("merchants circuit breaker is open")
	}
	for _, shardID := range pools.GetAvailableShardIDs() {
		if cbs.ShardRW(shardID).State() == gobreaker.StateOpen {
			return fmt.Errorf("shard %s RW circuit breaker is open", shardID)
		}
		if cbs.ShardRO(shardID).State() == gobreaker.StateOpen {
			return fmt.Errorf("shard %s RO circuit breaker is open", shardID)
		}
	}
	return nil
}
