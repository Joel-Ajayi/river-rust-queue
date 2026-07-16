package main

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"os"
	"os/signal"
	"syscall"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/kong-sync-worker/internal/adapter/outbound/kubernetes"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/kong-sync-worker/internal/adapter/outbound/observability"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/kong-sync-worker/internal/adapter/outbound/postgres"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/kong-sync-worker/internal/adapter/outbound/resilience"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/kong-sync-worker/internal/core/app"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/kong-sync-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/kong-sync-worker/internal/core/port"
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

	// Initialize Telemetry
	if err := platform.InitTelemetry(platform.ServiceNameKongSyncWorker); err != nil {
		log.Panic("Failed to initialize telemetry", zap.String(platform.LogFieldEvent, platform.LogEventStartupFailed), zap.Error(err))
	}

	// Init context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize Database Pools
	pools, err := platform.NewShardPools(ctx, cfg, log)
	if err != nil {
		log.Panic("Failed to initialize PostgreSQL pools", zap.String(platform.LogFieldEvent, platform.LogEventStartupFailed), zap.Error(err))
	}
	defer pools.Close()

	// Extract RSA Public Key to PEM for Kong JWT Plugin
	pubKey := &cfg.JWTSigningKeys[cfg.JWTActiveKeyID].PublicKey
	pubASN1, err := x509.MarshalPKIXPublicKey(pubKey)
	if err != nil {
		log.Panic("Failed to marshal RSA public key", zap.Error(err))
	}
	pubPEM := string(pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubASN1,
	}))

	// Initialize Circuit Breakers
	cbs := platform.NewDBCircuitBreakers(
		platform.CBNameMerchantsGlobal,
		pools.GetAvailableShardIDs(),
		func(err error) bool { return false }, // No specific terminal DB errors for this worker
		platform.CircuitBreakerConfig{
			MaxRequests: uint32(domain.CBMaxRequests),
			Timeout:     domain.CBTimeout,
			MaxFails:    uint32(domain.CBMaxFails),
		},
	)

	// Initialize Adapters & Ports
	var merchantDir port.MerchantDirectory = postgres.NewMerchantDirectory(pools)
	merchantDir = resilience.NewMerchantDirectoryCB(merchantDir, cbs)
	merchantDir = observability.NewMerchantDirectoryMetrics(merchantDir)

	kongClient, err := kubernetes.NewKongClient(cfg, log, pubPEM)
	if err != nil {
		log.Panic("Failed to initialize Kong Client", zap.Error(err))
	}
	var kongGateway port.KongGateway = observability.NewKongGatewayMetrics(kongClient)

	// Start Sync Worker
	worker := app.NewKongSyncWorker(kongGateway, merchantDir, domain.WorkerSyncInterval, log)

	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()
	go worker.Start(workerCtx)

	log.Info(platform.LogEventServerStarted)

	// Shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh  // Blocker until signal is received
	cancel() // Triggers ctx.Done() for worker

	log.Info("Received shutdown signal", zap.String(platform.LogFieldEvent, platform.LogEventShutdownSignalReceived))

	waitChan := make(chan struct{})
	go func() {
		defer close(waitChan)
		worker.Wait() // Block until the worker finishes its current iteration gracefully
		log.Info("Worker shutdown gracefully")
	}()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), domain.WorkerShutdownTimeout)
	defer shutdownCancel()

	select {
	case <-waitChan:
		// Worker finished normally
	case <-shutdownCtx.Done():
		log.Warn("Forcefully shutting down worker", zap.String(platform.LogFieldEvent, platform.LogEventShutdownSignalReceived))
	}

	// Flush remaining telemetry (includes buffered logs, metrics, and traces)
	_ = platform.ShutdownTelemetry(context.Background())
}
