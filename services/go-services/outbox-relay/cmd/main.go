package main

import (
	"context"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/adapter/outbound/kafka"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/adapter/outbound/observability"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/adapter/outbound/postgres"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/adapter/outbound/resilience"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/app"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/port"
	"go.uber.org/zap"
)

func main() {
	// --- Config & Logs ---
	cfg := platform.LoadConfig("OUTBOX_RELAY_")
	logger, err := platform.NewLogger(cfg.LogLevel)
	if err != nil {
		panic("logger: " + err.Error())
	}
	defer logger.Sync()

	// --- Context (webhook-worker pattern: signal.NotifyContext) ---
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := platform.InitTelemetry(ctx, "outbox-relay"); err != nil {
		logger.Panic("Failed to initialize telemetry", zap.Error(err))
	}

	// --- Infrastructure ---
	pools, err := platform.NewShardPools(ctx, cfg, logger)
	if err != nil {
		logger.Panic(platform.LogEventPostgresInitFailed, zap.Error(err))
	}
	defer pools.Close()

	kafkaWriter := platform.NewKafkaWriter(cfg, cfg.KafkaBrokers, "", cfg.Capacity.RelayBatchMsgCount, time.Duration(cfg.Capacity.RelayBatchTimeoutMs)*time.Millisecond, logger.Named(platform.LogComponentKafka))

	// Per-shard DB circuit breakers (RW pool, the one the EventStore actually uses)
	dbCBs := platform.NewDBCircuitBreakers(
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

	// --- Driven adapters (outbound base) ---
	dlqRetryCfg := platform.DLQRetryConfig(*cfg.Capacity)
	var baseEventStore port.EventStore = postgres.NewEventStore(pools, logger, dlqRetryCfg, platform.ServiceNameOutboxRelay)
	baseKafkaPublisher := kafka.NewEventPublisher(kafkaWriter)
	publishRetryCfg := platform.RetryConfig{
		MaxRetries: cfg.Capacity.MaxRetries,
		BaseDelay:  time.Duration(cfg.Capacity.BackoffBaseMs) * time.Millisecond,
		MaxDelay:   time.Duration(cfg.Capacity.BackoffCapMs) * time.Millisecond,
		Budget: platform.NewRetryBudget(
			int64(cfg.Capacity.RetryBudgetMinTokens),
			int64(cfg.Capacity.RetryBudgetMaxTokens),
			cfg.Capacity.RetryBudgetFraction,
		),
	}
	publishTimeout := time.Duration(cfg.Capacity.RequestTimeoutMs) * time.Millisecond
	baseEventPublisher := resilience.NewEventPublisherRetry(baseKafkaPublisher, publishRetryCfg, publishTimeout)

	// Add trace decorators (global, not per-shard)
	baseEventStore = observability.NewEventStoreTraces(baseEventStore)

	// --- Workers ---
	var wg sync.WaitGroup
	var relays []*app.RelayService
	for _, shardId := range pools.GetAvailableShardIDs() {
		// Decorate dependencies per shard to isolate circuit breakers and metrics
		shardEventStore := observability.NewMetricsStoreDecorator(
			resilience.NewEventStoreCB(baseEventStore, shardId, dbCBs),
			shardId,
		)

		shardEventPublisher := observability.NewMetricsPublisherDecorator(
			baseEventPublisher,
			shardId,
		)

		shardRelayApp := app.NewRelayService(
			shardEventStore,
			shardEventPublisher,
			logger,
			shardId,
			app.RelayServiceConfig{
				ProcessTimeout: time.Duration(cfg.Capacity.RequestTimeoutMs) * time.Millisecond,
				FetchBatchSize: cfg.Capacity.FetchBatchSize,
				PollInterval:   time.Duration(cfg.Capacity.RelayPoolIntervalMs) * time.Millisecond,
				MaxPayloadSize: cfg.Capacity.RelayMaxPayloadBytes,
			},
		)
		relays = append(relays, shardRelayApp)

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := shardRelayApp.Start(ctx, shardId); err != nil {
				logger.Error(platform.LogEventRelayWorkerError, zap.String(platform.LogFieldShardID, shardId), zap.Error(err))
			}
		}()
	}

	// --- Kafka buffer monitor: throttle/pause polling when in-flight bytes fill ---
	go monitorKafkaBuffer(ctx, baseKafkaPublisher, relays, cfg, logger)

	// --- Graceful Shutdown (webhook-worker pattern: wait on the signal context) ---
	<-ctx.Done() // Triggers ctx.Done() for all relayers

	// 1. Signal all shards to stop starting new poll cycles (they finish the current batch).
	for _, r := range relays {
		r.SetDraining()
	}

	// 2. Wait for all in-flight batches to complete (or timeout).
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Capacity.ShutdownTimeoutMs)*time.Millisecond)
	defer shutdownCancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info(platform.LogEventAllRelayersShutdown)
	case <-shutdownCtx.Done():
		logger.Warn(platform.LogEventOutboxShutdownTimeout)
	}

	// 3. Close the Kafka producer (flushes buffered messages automatically).
	if err := kafkaWriter.Close(); err != nil {
		logger.Error(platform.LogEventKafkaWriterCloseFailed, zap.Error(err))
	}

	// 5. Flush telemetry.
	_ = platform.ShutdownTelemetry(context.Background())
}
