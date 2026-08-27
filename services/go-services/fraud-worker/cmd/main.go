package main

import (
	"context"
	"errors"
	"fmt"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/adapter/inbound/kafka"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/adapter/outbound/observability"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/adapter/outbound/postgres"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/adapter/outbound/redis"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/adapter/outbound/resilience"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/core/app"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"go.uber.org/zap"
)

func main() {
	cfg := platform.LoadConfig("FRAUD_WORKER_")

	logger, err := platform.NewLogger(cfg.LogLevel)
	if err != nil {
		panic("logger: " + err.Error())
	}
	defer logger.Sync()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := platform.InitTelemetry(ctx, "fraud-worker"); err != nil {
		logger.Panic("Failed to initialize telemetry", zap.Error(err))
	}

	pools, err := platform.NewShardPools(ctx, cfg, logger)
	if err != nil {
		logger.Panic(platform.LogEventPostgresInitFailed, zap.Error(err))
	}
	defer pools.Close()

	redisClient, err := platform.NewRedisClient(ctx, cfg.RedisDataMaster, cfg.RedisAddr(), cfg.RedisDataPassword, logger)
	if err != nil {
		logger.Panic(platform.LogEventRedisInitFailed, zap.Error(err))
	}
	defer redisClient.Close()

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

	walletRepo := observability.NewWalletRepositoryMetrics(resilience.NewWalletRepositoryResilience(postgres.NewWalletRepository(pools, logger), cbs))
	merchantDir := observability.NewMerchantDirectoryMetrics(resilience.NewMerchantDirectoryResilience(postgres.NewMerchantDirectory(pools, logger), cbs))
	dlqRetryCfg := platform.DLQRetryConfig(*cfg.Capacity)
	dlqStore := observability.NewDLQStoreMetrics(resilience.NewDLQStoreResilience(
		postgres.NewDLQStore(pools, logger, dlqRetryCfg, platform.ServiceNameFraudWorker), cbs))

	redisBreaker := platform.NewRedisCircuitBreaker(
		platform.CBNameRedis,
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
	redisStore := observability.NewRedisStoreMetrics(resilience.NewRedisStoreResilience(redis.NewRedisStore(redisClient), redisBreaker))
	velocityThreshold := int(cfg.Capacity.VelocityThreshold)
	velocityWindowMs := cfg.Capacity.VelocityWindowMs

	rules := []domain.VelocityRule{
		{
			Name:      domain.DefaultVelocityRuleName,
			WindowMs:  velocityWindowMs,
			Threshold: velocityThreshold,
			Reason:    fmt.Sprintf("more than %d transfers from a wallet within %dms", velocityThreshold, velocityWindowMs),
		},
	}

	fraudService := app.NewFraudService(logger, walletRepo, redisStore, merchantDir, rules)
	handler := observability.NewJobHandlerTraces(observability.NewJobHandlerMetrics(fraudService))

	reader := platform.NewKafkaConsumerReader(cfg, cfg.KafkaBrokers, platform.TopicJobs, platform.ConsumerGroupFraudWorker, time.Duration(cfg.Capacity.KafkaSessionMs)*time.Millisecond, time.Duration(cfg.Capacity.KafkaHeartbeatMs)*time.Millisecond, logger)
	consumerManager := kafka.NewConsumerManager(logger, reader, handler, dlqStore, merchantDir, pools, cfg)
	retryCfg := platform.RetryConfig{
		MaxRetries: int(cfg.Capacity.MaxRetries),
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

	<-ctx.Done()
	logger.Info(platform.LogEventShutdownSignalReceived)

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

	consumerManager.Stop()

	_ = platform.ShutdownTelemetry(context.Background())
}
