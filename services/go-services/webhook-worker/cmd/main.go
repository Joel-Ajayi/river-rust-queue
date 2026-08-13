package main

import (
	"context"
	"errors"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform/platform_consumer"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/adapter/inbound/kafka"
	adapterHttp "github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/adapter/outbound/http"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/adapter/outbound/observability"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/adapter/outbound/postgres"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/adapter/outbound/resilience"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/app"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/domain"
)

func main() {
	cfg := platform.LoadConfig("WEBHOOK_WORKER_")

	logger, err := platform.NewLogger(cfg.LogLevel)
	if err != nil {
		panic("Failed to initialize logger" + err.Error())
	}
	defer logger.Sync()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 1. Database
	pools, err := platform.NewShardPools(ctx, cfg, logger)
	if err != nil {
		logger.Panic(platform.LogEventPostgresInitFailed, zap.Error(err))
	}
	defer pools.Close()

	shardIDs := pools.GetAvailableShardIDs()
	if len(shardIDs) == 0 {
		logger.Panic(platform.LogEventNoShardsAvailable)
	}
	cbs := platform.NewDBCircuitBreakers(
		platform.CBNameMerchantsGlobal,
		shardIDs,
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

	// 2. Adapters (Outbound)
	repo := postgres.NewRepository(pools)
	decoratedRepo := observability.NewRepositoryMetrics(resilience.NewRepositoryResilience(repo, cbs))

	httpAdapter := adapterHttp.NewWebhookClient(cfg)
	breakers := resilience.NewBreakerRegistry(logger, cfg)
	resilientHttpClient := resilience.NewHTTPClientResilience(httpAdapter, breakers)

	// 3. Application core
	service := app.NewWebhookService(decoratedRepo, resilientHttpClient, logger, app.WebhookConfig{
		MaxDeliveryAttempts:   cfg.Capacity.DeliveryMaxAttempts,
		BaseRetryDelaySec:     float64(cfg.Capacity.DeliveryBackoffBaseMs) / 1000,
		CapRetryDelaySec:      float64(cfg.Capacity.DeliveryBackoffCapMs) / 1000,
		SchedulerPollInterval: time.Duration(cfg.Capacity.SchedulerPollIntervalMs) * time.Millisecond,
		SchedulerBatchSize:    cfg.Capacity.SchedulerBatchSize,
		FastLaneGracePeriod:   time.Duration(cfg.Capacity.FastLaneGracePeriodMs) * time.Millisecond,
		FastLaneBufferSize:    cfg.Capacity.FastLaneBufferSize,
		MaxConcurrency:        cfg.Capacity.WebhookMaxConcurrency,
	})
	sharedBudget := platform.NewRetryBudget(
		int64(cfg.Capacity.RetryBudgetMinTokens),
		int64(cfg.Capacity.RetryBudgetMaxTokens),
		cfg.Capacity.RetryBudgetFraction,
	)
	appRetryCfg := platform.RetryConfig{
		BaseDelay: time.Duration(cfg.Capacity.BackoffBaseMs) * time.Millisecond,
		MaxDelay:  time.Duration(cfg.Capacity.BackoffCapMs) * time.Millisecond,
		Budget:    sharedBudget,
	}
	decoratedService := resilience.NewWebhookAppResilience(
		observability.NewWebhookAppTraces(observability.NewWebhookAppMetrics(service)),
		appRetryCfg,
		logger,
	)

	// 4. Kafka consumer
	reader := platform.NewKafkaConsumerReader(cfg, cfg.KafkaBrokers, platform.TopicNotify, platform.ConsumerGroupWebhookWorker, time.Duration(cfg.Capacity.KafkaSessionMs)*time.Millisecond, time.Duration(cfg.Capacity.KafkaHeartbeatMs)*time.Millisecond, logger)
	defer reader.Close()

	consumerRetryCfg := platform.RetryConfig{
		MaxRetries: int(cfg.Capacity.MaxRetries),
		BaseDelay:  time.Duration(cfg.Capacity.BackoffBaseMs) * time.Millisecond,
		MaxDelay:   time.Duration(cfg.Capacity.BackoffCapMs) * time.Millisecond,
		Budget:     sharedBudget,
	}

	handler := kafka.WebhookHandler(decoratedService, consumerRetryCfg, logger)
	pipeline := platform_consumer.NewConsumerPipeline(reader, handler,
		platform_consumer.NewConsumerConfigFromCapacity(cfg), logger)

	resilientConsumer := resilience.NewConsumerResilience(pipeline, consumerRetryCfg, logger)

	// 5. Background routines
	g, gCtx := errgroup.WithContext(ctx)

	g.Go(service.StartFastLaneWorkers(gCtx, cfg.Capacity.FastLaneWorkerPoolSize))
	g.Go(func() error {
		return resilientConsumer.Consume(gCtx)
	})
	g.Go(func() error {
		return decoratedService.RetryScheduler(gCtx)
	})
	g.Go(func() error {
		ticker := time.NewTicker(time.Duration(cfg.Capacity.BreakerEvictionIntervalMs) * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-gCtx.Done():
				return nil
			case <-ticker.C:
				breakers.CleanupEvicted(time.Duration(cfg.Capacity.BreakerEvictionTTLMs) * time.Millisecond)
			}
		}
	})

	// 6. Wait for shutdown or fatal error
	waitCh := make(chan struct{})
	var groupErr error
	go func() {
		groupErr = g.Wait()
		close(waitCh)
	}()

	select {
	case <-waitCh:
		if groupErr != nil && !errors.Is(groupErr, context.Canceled) {
			logger.Error(platform.LogEventServerFatalError, zap.Error(groupErr))
		}
	case <-ctx.Done():
		logger.Info(platform.LogEventShutdownSignalReceived)
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Capacity.ShutdownTimeoutMs)*time.Millisecond)
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
