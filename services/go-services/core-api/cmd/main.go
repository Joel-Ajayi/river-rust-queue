package main

import (
	"context"
	"fmt"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/adapter/inbound/rest"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/adapter/outbound/observability"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/adapter/outbound/postgres"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/adapter/outbound/resilience"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/app"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/port"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/failsafe-go/failsafe-go/circuitbreaker"
	"go.uber.org/zap"
)

func main() {
	// Config & Logger
	cfg := platform.LoadConfig("CORE_API_")

	platform.SetArgon2Params(
		cfg.Capacity.Argon2Iterations,
		cfg.Capacity.Argon2MemoryKib,
		cfg.Capacity.Argon2Parallelism,
	)
	rest.SetMaxRequestBodyBytes(int64(cfg.Capacity.MaxRequestBytes))

	log, err := platform.NewLogger(cfg.LogLevel)
	if err != nil {
		panic("logger: " + err.Error())
	}
	defer log.Sync()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Infrastructure
	pools, err := platform.NewShardPools(ctx, cfg, log)
	if err != nil {
		log.Panic(platform.LogEventPostgresInitFailed, zap.Error(err))
	}

	cbs := platform.NewDBCircuitBreakers(
		platform.CBNameMerchantsGlobal,
		pools.GetAvailableShardIDs(),
		domain.IsTerminalError,
		platform.CircuitBreakerConfig{
			MaxRequests: uint32(cfg.Capacity.CBHalfOpenProbes),
			Timeout:     time.Duration(cfg.Capacity.CBTimeoutMs) * time.Millisecond,
			Interval:    time.Duration(cfg.Capacity.CBIntervalMs) * time.Millisecond,
			MinRequests: uint32(cfg.Capacity.CBMinRequests),
			ErrorRate:   cfg.Capacity.CBErrorThreshold,
			MaxFails:    uint32(cfg.Capacity.CBMaxFails),
		},
		log,
	)

	// Driven adapters (outbound)
	mDirImpl := postgres.NewMerchantDirectory(pools)
	var merchantDir port.MerchantDirectory = mDirImpl
	var merchantStore port.MerchantStore = mDirImpl
	merchantDir = resilience.NewMerchantDirectoryCB(merchantDir, cbs)
	merchantDir = observability.NewMerchantDirectoryMetrics(merchantDir)
	merchantDir = observability.NewMerchantDirectoryTraces(merchantDir) // trace decorator

	wDirImpl := postgres.NewWalletDirectory(pools)
	var walletDir port.WalletDirectory = wDirImpl
	var walletStore port.WalletStore = wDirImpl
	walletDir = resilience.NewWalletDirectoryCB(walletDir, cbs)
	walletDir = observability.NewWalletDirectoryMetrics(walletDir)
	walletDir = observability.NewWalletDirectoryTraces(walletDir) // trace decorator

	var jobStore port.JobStore = postgres.NewJobStore(pools)
	jobStore = resilience.NewJobStoreCB(jobStore, cbs)
	jobStore = observability.NewJobStoreMetrics(jobStore)
	jobStore = observability.NewJobStoreTraces(jobStore) // trace decorator

	// Core use-cases
	dlqReplayer := postgres.NewDLQReplayer(cfg, pools, log)
	adminSvc := app.NewAdminService(dlqReplayer)
	jobSvc := app.NewJobService(merchantDir, jobStore)
	merchantSvc := app.NewMerchantService(merchantStore, pools.HashRing())
	var walletUseCase port.WalletUseCase = app.NewWalletService(merchantDir, walletDir, walletStore, jobStore, platform.NewJobID)
	walletUseCase = observability.NewWalletUseCaseMetrics(walletUseCase)
	walletUseCase = observability.NewWalletUseCaseTraces(walletUseCase) // trace decorator
	transferSvc := app.NewTransferService(merchantDir, walletDir, jobStore, platform.NewJobID)
	var transferSubmitter port.TransferSubmitter = transferSvc
	transferSubmitter = observability.NewTransferServiceMetrics(transferSubmitter)
	transferSubmitter = observability.NewTransferSubmitterTraces(transferSubmitter) // trace decorator

	// Driving adapter (inbound)
	ready := func(ctx context.Context) error { return readiness(ctx, pools, cbs) }
	srv := rest.NewServer(cfg, transferSubmitter, jobSvc, merchantSvc, walletUseCase, adminSvc, ready, log)

	go func() {
		if err := srv.Start(); err != nil && err != http.ErrServerClosed {
			log.Panic(platform.LogEventServerFatalError, zap.Error(err))
		}
	}()

	<-ctx.Done() // Blocker until signal is received

	// Begin draining: readiness returns 503 so Kubernetes removes this pod from
	// Service endpoints before we shut the server down. New traffic stops
	srv.BeginDrain()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Capacity.ShutdownTimeoutMs)*time.Millisecond)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error(platform.LogEventShutdownFailed, zap.Error(err))
	}

	pools.Close()

	_ = platform.ShutdownTelemetry(shutdownCtx)

}

// readiness pings every backing store the gateway needs to serve traffic.
func readiness(ctx context.Context, pools *platform.ShardPools, cbs *platform.DBCircuitBreakers) error {
	// Check PG connectivity
	if err := pools.Ping(ctx); err != nil {
		return err
	}
	// Check circuit breaker state - if any CB is open, we're not ready
	if cbs.Merchants().State() == circuitbreaker.OpenState {
		return fmt.Errorf("%w", platform.ErrCBMerchantsOpen)
	}
	for _, shardID := range pools.GetAvailableShardIDs() {
		if cbs.ShardRW(shardID).State() == circuitbreaker.OpenState {
			return fmt.Errorf("%w: %s", platform.ErrCBRWOpen, shardID)
		}
		if cbs.ShardRO(shardID).State() == circuitbreaker.OpenState {
			return fmt.Errorf("%w: %s", platform.ErrCBROpen, shardID)
		}
	}
	return nil
}
