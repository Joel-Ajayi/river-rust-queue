package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/adapter/outbound/kafka"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/adapter/outbound/observability"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/adapter/outbound/postgres"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/adapter/outbound/resilience"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/app"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/domain"
	"go.uber.org/zap"
)

func main() {
	// --- Config & Logs ---
	cfg, err := platform.LoadConfig()
	if err != nil {
		panic("config: " + err.Error())
	}

	logger, err := platform.NewLogger(cfg.LogLevel)
	if err != nil {
		panic("logger: " + err.Error())
	}
	defer logger.Sync()

	if err := platform.InitTelemetry(platform.ServiceNameOutboxRelay); err != nil {
		logger.Panic("Failed to initialize telemetry", zap.Error(err))
	}

	// --- Context ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Infrastructure ---
	pools, err := platform.NewShardPools(ctx, cfg, logger)
	if err != nil {
		logger.Panic("Failed to initialize PostgreSQL pools", zap.String(platform.LogFieldEvent, platform.LogEventStartupFailed), zap.Error(err))
	}
	defer pools.Close()

	kafkaWriter := platform.NewKafkaWriter(cfg.KafkaBrokers, "", logger.Named(platform.LogComponentKafka))
	defer kafkaWriter.Close()

	// Single shared Kafka Egress Circuit Breaker for all shards
	kafkaCB := platform.NewKafkaCircuitBreaker("", platform.CircuitBreakerConfig{
		MaxRequests: domain.CBMaxRequests,
		Timeout:     domain.CBTimeout,
		MaxFails:    domain.CBMaxFails,
	})

	// --- Driven adapters (outbound base) ---
	baseEventStore := postgres.NewEventStore(pools, logger)
	baseEventPublisher := resilience.NewEventPublisherCB(kafka.NewEventPublisher(kafkaWriter), kafkaCB)

	// --- Workers ---
	var wg sync.WaitGroup
	for _, shardId := range pools.GetAvailableShardIDs() {
		// Decorate dependencies per shard to isolate circuit breakers and metrics
		shardEventStore := observability.NewMetricsStoreDecorator(
			resilience.NewEventStoreCB(baseEventStore, shardId),
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
		)

		wg.Add(1)
		go func() {
			defer wg.Done()
			shardRelayApp.Start(ctx, shardId)
		}()
	}

	// --- Graceful Shutdown ---
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// Read your standard shutdown timeout limit from your configuration structs
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), domain.ServerShutdownTimeout)
	defer shutdownCancel()

	// Fix: Protect the WaitGroup against permanent thread lockups
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	cancel() // Triggers ctx.Done() for all relayers

	select {
	case <-done:
		logger.Info("All relayers shut down gracefully")
	case <-shutdownCtx.Done():
		// Force close the database pool and telemetry here to release handles before crashing
		logger.Warn("Outbox shutdown timeout exceeded, forcing process termination")
	}

	_ = platform.ShutdownTelemetry(context.Background())
}
