package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

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
	cfg, err := platform.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	logger, err := platform.NewLogger(cfg.LogLevel)
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}
	defer logger.Sync()

	// Initialize Telemetry
	if err := platform.InitTelemetry(platform.ServiceNameWebhookWorker); err != nil {
		logger.Panic(platform.LogEventTelemetryInitFailed, zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	)

	// 2. Adapters
	repo := postgres.NewRepository(pools)
	decoratedRepo := observability.NewRepositoryMetrics(resilience.NewRepositoryResilience(repo, cbs))

	httpClient := http.NewWebhookClient()
	breakers := resilience.NewBreakerRegistry()

	// 3. Application core
	service := app.NewWebhookService(decoratedRepo, httpClient, breakers, logger)
	decoratedService := observability.NewWebhookAppTraces(observability.NewWebhookAppMetrics(service))

	// 4. Kafka consumer
	reader := platform.NewKafkaReader(
		cfg.KafkaBrokers,
		platform.TopicNotify,
		platform.ConsumerGroupWebhookWorker,
		logger,
	)
	defer reader.Close()

	consumer := kafka.NewConsumer(reader, decoratedService, logger)

	// 5. Start background routines
	go func() {
		logger.Info(platform.LogEventKafkaMessageHandled, zap.String("component", "kafka_consumer"))
		if err := consumer.Consume(ctx); err != nil && err != context.Canceled {
			logger.Error(platform.LogEventKafkaConsumerStopped, zap.Error(err))
		}
	}()

	go func() {
		logger.Info(platform.LogEventRetrySchedulerStarted)
		if err := service.RetryScheduler(ctx); err != nil && err != context.Canceled {
			logger.Error(platform.LogEventRetrySchedulerStopped, zap.Error(err))
		}
	}()

	// 6. Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	logger.Info(platform.LogEventShutdownSignalReceived)
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), domain.ServerShutdownTimeout)
	defer shutdownCancel()

	_ = platform.ShutdownTelemetry(shutdownCtx)

	logger.Info(platform.LogEventServerShutdown)
}
