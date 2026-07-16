package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/adapter/inbound/kafka"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/adapter/outbound/observability"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/adapter/outbound/postgres"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/adapter/outbound/resilience"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/app"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/domain"
	segmentio "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

func main() {
	fmt.Printf("Starting %s service...\n", os.Args[0])
	// Load Config
	cfg, err := platform.LoadConfig()
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}
	// Load Logger
	logger, err := platform.NewLogger(cfg.LogLevel)
	if err != nil {
		fmt.Printf("Failed to init logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	// Initialize Telemetry
	if err := platform.InitTelemetry(platform.ServiceNameLedgerWorker); err != nil {
		logger.Panic("Failed to initialize telemetry", zap.String(platform.LogFieldEvent, platform.LogEventStartupFailed), zap.Error(err))
	}

	// Init context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize Database Pools
	pools, err := platform.NewShardPools(ctx, cfg, logger)
	if err != nil {
		logger.Panic("Failed to initialize PostgreSQL pools", zap.String(platform.LogFieldEvent, platform.LogEventStartupFailed), zap.Error(err))
	}
	defer pools.Close()

	// Initialize Circuit Breakers (per-pool, with terminal error policy)
	cbs := platform.NewDBCircuitBreakers(
		platform.CBNameMerchantsGlobal,
		pools.GetAvailableShardIDs(),
		domain.IsTerminalError,
		platform.CircuitBreakerConfig{
			MaxRequests: domain.CBMaxRequests,
			Timeout:     domain.CBTimeout,
			MaxFails:    domain.CBMaxFails,
		},
	)

	// Initialize Stores & Directory
	merchantDir := observability.NewMerchantDirectoryMetrics(resilience.NewMerchantDirectoryResilience(postgres.NewMerchantDirectory(pools, logger), cbs))
	ledgerStore := observability.NewLedgerStoreMetrics(resilience.NewLedgerStoreResilience(postgres.NewLedgerStore(pools, logger), cbs))
	xshardStore := observability.NewCrossShardStoreMetrics(resilience.NewCrossShardStoreResilience(postgres.NewCrossShardStore(pools, logger), cbs))
	dlqStore := observability.NewDLQStoreMetrics(resilience.NewDLQStoreResilience(postgres.NewDLQStore(pools, logger), cbs))

	// Initialize Services
	jobService := app.NewJobService(logger, ledgerStore, xshardStore, merchantDir)
	jobHandler := observability.NewJobHandlerMetrics(jobService)
	xshardService := app.NewXShardService(logger, xshardStore)
	sagaHandler := observability.NewSagaHandlerMetrics(xshardService)

	// Initialize Kafka Readers
	groupID := platform.ConsumerGroupLedgerWorker
	jobReader := platform.NewKafkaReader(cfg.KafkaBrokers, platform.TopicJobs, groupID, logger.Named(platform.LogComponentKafka))
	defer jobReader.Close()

	var xshardReaders []*segmentio.Reader

	for shardID := range cfg.ShardURIs {
		topic := platform.TopicXShardPrefix + shardID
		consumerGroup := groupID + shardID
		r := platform.NewKafkaReader(cfg.KafkaBrokers, topic, consumerGroup, logger.Named(platform.LogComponentKafka))
		xshardReaders = append(xshardReaders, r)
		defer r.Close()
	}

	// Start Consumer Manager
	consumerManager := kafka.NewConsumerManager(logger, jobReader, xshardReaders, jobHandler, sagaHandler, dlqStore, merchantDir, pools, pools.GetAvailableShardIDs()[0])
	consumerManager.Start(ctx)
	logger.Info(platform.LogEventServerStarted)

	// Shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	cancel() // Triggers ctx.Done() for all background components immediately (A6 consistency)
	logger.Info(platform.LogEventShutdownSignalReceived)

	// Graceful shutdown: signal consumers to stop and wait
	consumerManager.Shutdown()

	// Wait with timeout for consumers to finish
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), domain.ServerShutdownTimeout)
	defer shutdownCancel()

	done := make(chan struct{})
	go func() {
		consumerManager.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("All consumers shut down gracefully")
	case <-shutdownCtx.Done():
		logger.Warn("Shutdown timeout exceeded for consumers, forcing exit")
	}

	_ = platform.ShutdownTelemetry(shutdownCtx)
}
