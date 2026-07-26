package main

import (
	"context"
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
	// Load Config
	cfg := platform.LoadConfig()

	// Load Logger
	logger, err := platform.NewLogger(cfg.LogLevel)
	if err != nil {
		panic("logger:" + err.Error())
	}
	defer logger.Sync()

	// Initialize Telemetry
	if err := platform.InitTelemetry(platform.ServiceNameLedgerWorker, cfg.OtelExporterEndpoint); err != nil {
		logger.Panic(platform.LogEventTelemetryInitFailed, zap.Error(err))
	}

	// Init context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize Database Pools
	pools, err := platform.NewShardPools(ctx, cfg, logger)
	if err != nil {
		logger.Panic(platform.LogEventPostgresInitFailed, zap.Error(err))
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
	jobHandler := observability.NewJobHandlerTraces(observability.NewJobHandlerMetrics(jobService))
	xshardService := app.NewXShardService(logger, xshardStore)
	sagaHandler := observability.NewSagaHandlerTraces(observability.NewSagaHandlerMetrics(xshardService))

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
	consumerManager := kafka.NewConsumerManager(logger, jobReader, xshardReaders, jobHandler, sagaHandler, dlqStore, merchantDir, pools)
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
		logger.Info(platform.LogEventAllConsumersShutdown)
	case <-shutdownCtx.Done():
		logger.Warn(platform.LogEventConsumerShutdownTimeout)
	}

	_ = platform.ShutdownTelemetry(shutdownCtx)
}
