package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

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
	cfg := platform.LoadConfig("LEDGER_WORKER_")

	logger, err := platform.NewLogger(cfg.LogLevel)
	if err != nil {
		panic("logger:" + err.Error())
	}
	defer logger.Sync()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pools, err := platform.NewShardPools(ctx, cfg, logger)
	if err != nil {
		logger.Panic(platform.LogEventPostgresInitFailed, zap.Error(err))
	}
	defer pools.Close()

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
		logger,
	)

	merchantDir := observability.NewMerchantDirectoryMetrics(resilience.NewMerchantDirectoryResilience(postgres.NewMerchantDirectory(pools, logger), cbs))
	ledgerStore := observability.NewLedgerStoreMetrics(resilience.NewLedgerStoreResilience(postgres.NewLedgerStore(pools, logger), cbs))
	xshardStore := observability.NewCrossShardStoreMetrics(resilience.NewCrossShardStoreResilience(postgres.NewCrossShardStore(pools, logger), cbs))
	dlqRetryCfg := platform.DLQRetryConfig(*cfg.Capacity)
	dlqStore := observability.NewDLQStoreMetrics(resilience.NewDLQStoreResilience(
		postgres.NewDLQStore(pools, logger, dlqRetryCfg, platform.ServiceNameLedgerWorker), cbs))

	jobService := app.NewJobService(logger, ledgerStore, xshardStore, merchantDir)
	jobHandler := observability.NewJobHandlerTraces(observability.NewJobHandlerMetrics(jobService))
	xshardService := app.NewXShardService(logger, xshardStore)
	sagaHandler := observability.NewSagaHandlerTraces(observability.NewSagaHandlerMetrics(xshardService))

	groupID := platform.ConsumerGroupLedgerWorker
	jobReader := platform.NewKafkaConsumerReader(cfg, cfg.KafkaBrokers, platform.TopicJobs, groupID, time.Duration(cfg.Capacity.KafkaSessionMs)*time.Millisecond, time.Duration(cfg.Capacity.KafkaHeartbeatMs)*time.Millisecond, logger.Named(platform.LogComponentKafka))
	defer jobReader.Close()

	var xshardReaders []*segmentio.Reader
	for shardID := range cfg.ShardURIs {
		topic := platform.TopicXShardPrefix + shardID
		consumerGroup := groupID + shardID
		r := platform.NewKafkaConsumerReader(cfg, cfg.KafkaBrokers, topic, consumerGroup, time.Duration(cfg.Capacity.KafkaSessionMs)*time.Millisecond, time.Duration(cfg.Capacity.KafkaHeartbeatMs)*time.Millisecond, logger.Named(platform.LogComponentKafka))
		xshardReaders = append(xshardReaders, r)
		defer r.Close()
	}

	consumerManager := kafka.NewConsumerManager(logger, jobReader, xshardReaders, jobHandler, sagaHandler, dlqStore, merchantDir, pools, cfg)

	retryCfg := platform.RetryConfig{
		MaxRetries: cfg.Capacity.MaxRetries,
		BaseDelay:  time.Duration(cfg.Capacity.BackoffBaseMs) * time.Millisecond,
		MaxDelay:   time.Duration(cfg.Capacity.BackoffCapMs) * time.Millisecond,
		Budget: platform.NewRetryBudget(
			int64(cfg.Capacity.RetryBudgetMinTokens),
			int64(cfg.Capacity.RetryBudgetMaxTokens),
			cfg.Capacity.RetryBudgetFraction,
		),
	}
	resilientConsumer := resilience.NewConsumerResilience(consumerManager, retryCfg, logger)
	logger.Info(platform.LogEventServerStarted)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := resilientConsumer.Consume(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error(platform.LogEventKafkaConsumerStopped, zap.Error(err))
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info(platform.LogEventShutdownSignalReceived)
	cancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Capacity.ShutdownTimeoutMs)*time.Millisecond)
	defer shutdownCancel()

	select {
	case <-done:
		logger.Info(platform.LogEventAllConsumersShutdown)
	case <-shutdownCtx.Done():
		logger.Warn(platform.LogEventConsumerShutdownTimeout)
	}

	consumerManager.Shutdown()

	_ = platform.ShutdownTelemetry(shutdownCtx)
}
