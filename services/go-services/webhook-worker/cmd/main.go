package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/adapter/inbound/kafka"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/adapter/outbound/http"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/adapter/outbound/observability"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/adapter/outbound/postgres"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/adapter/outbound/resilience"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/app"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/domain"
)

func main() {
	cfg := platform.LoadConfig()

	logger, err := platform.NewLogger(cfg.LogLevel)
	if err != nil {
		panic("Failed to initialize logger" + err.Error())
	}
	defer logger.Sync()

	// Initialize Telemetry
	if err := platform.InitTelemetry(platform.ServiceNameWebhookWorker, cfg.OtelExporterEndpoint); err != nil {
		logger.Panic(platform.LogEventTelemetryInitFailed, zap.Error(err))
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 1. Database
	pools, err := platform.NewShardPools(ctx, cfg, logger)
	if err != nil {
		logger.Panic(platform.LogEventPostgresInitFailed, zap.Error(err))
	}
	defer pools.Close()

	// Initialize Circuit Breakers (per-pool, with terminal error policy)
	shardIDs := pools.GetAvailableShardIDs()
	if len(shardIDs) == 0 {
		logger.Panic(platform.LogEventNoShardsAvailable)
	}
	cbs := platform.NewDBCircuitBreakers(
		platform.CBNameMerchantsGlobal,
		shardIDs,
		domain.IsTerminalError,
		platform.CircuitBreakerConfig{
			MaxRequests: domain.CBMaxRequests,
			Timeout:     domain.CBTimeout,
			MaxFails:    domain.CBMaxFails,
		},
		logger,
	)

	// 2. Adapters
	repo := postgres.NewRepository(pools)
	decoratedRepo := observability.NewRepositoryMetrics(resilience.NewRepositoryResilience(repo, cbs))

	httpClient := http.NewWebhookClient()
	breakers := resilience.NewBreakerRegistry(logger)
	resilientHttpClient := resilience.NewHTTPClientResilience(httpClient, breakers)

	// 3. Application core
	service := app.NewWebhookService(decoratedRepo, resilientHttpClient, logger)
	decoratedService := resilience.NewWebhookAppResilience(
		observability.NewWebhookAppTraces(observability.NewWebhookAppMetrics(service)),
		logger,
	)

	// 4. Kafka consumer
	reader := platform.NewKafkaReader(
		cfg.KafkaBrokers,
		platform.TopicNotify,
		platform.ConsumerGroupWebhookWorker,
		logger,
	)
	defer reader.Close()

	resilientReader := resilience.NewKafkaReaderResilience(reader)
	consumer := kafka.NewConsumer(resilientReader, decoratedService, logger)
	resilientConsumer := resilience.NewConsumerResilience(consumer, logger)

	// 5. Start background routines
	g, gCtx := errgroup.WithContext(ctx)

	g.Go(service.StartFastLaneWorkers(gCtx, domain.FastLaneWorkerPoolSize))

	g.Go(func() error {
		return resilientConsumer.Consume(gCtx)
	})

	g.Go(func() error {
		return decoratedService.RetryScheduler(gCtx)
	})

	g.Go(func() error {
		ticker := time.NewTicker(domain.BreakerEvictionInterval)
		defer ticker.Stop()
		for {
			select {
			case <-gCtx.Done():
				return nil
			case <-ticker.C:
				breakers.CleanupEvicted(domain.BreakerEvictionTTL)
			}
		}
	})

	// 6. Wait for shutdown or fatal error with timeout protection
	waitCh := make(chan struct{})
	var groupErr error
	go func() {
		groupErr = g.Wait()
		close(waitCh)
	}()

	select {
	case <-waitCh:
		// Background worker failed fatally
		if groupErr != nil && groupErr != context.Canceled {
			logger.Error(platform.LogEventServerFatalError, zap.Error(groupErr))
			os.Exit(1)
		}
	case <-ctx.Done():
		// Received OS Shutdown Signal
		logger.Info(platform.LogEventShutdownSignalReceived)

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), domain.ServerShutdownTimeout)
		defer shutdownCancel()

		select {
		case <-waitCh:
			logger.Info(platform.LogEventServerShutdown)
		case <-shutdownCtx.Done():
			logger.Warn(platform.LogEventWebhookShutdownTimeout)
		}
	}

	_ = platform.ShutdownTelemetry(context.Background())
}
