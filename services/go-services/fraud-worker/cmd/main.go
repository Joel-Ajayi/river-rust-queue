package main

import (
	"context"
	"os"
	"os/signal"
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
	// Load Config
	cfg := platform.LoadConfig()

	// Initialize Logger
	logger, err := platform.NewLogger(cfg.LogLevel)
	if err != nil {
		panic("logger: " + err.Error())
	}
	defer logger.Sync()

	// Initialize Telemetry
	if err := platform.InitTelemetry(platform.ServiceNameFraudWorker, cfg.OtelExporterEndpoint); err != nil {
		logger.Panic(platform.LogEventTelemetryInitFailed, zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize Database Pools
	pools, err := platform.NewShardPools(ctx, cfg, logger)
	if err != nil {
		logger.Panic(platform.LogEventPostgresInitFailed, zap.Error(err))
	}
	defer pools.Close()

	// Initialize Redis
	redisClient, err := platform.NewRedisClient(ctx, cfg.RedisAddr(), cfg.RedisDataPassword, logger)
	if err != nil {
		logger.Panic(platform.LogEventRedisInitFailed, zap.Error(err))
	}
	defer redisClient.Close()

	// Initialize Circuit Breakers (per-pool, with default CB configurations)
	cbs := platform.NewDBCircuitBreakers(
		platform.CBNameMerchantsGlobal,
		pools.GetAvailableShardIDs(),
		func(error) bool { return false },
		platform.CircuitBreakerConfig{
			MaxRequests: 10,
			Timeout:     5 * time.Second,
			MaxFails:    3,
		},
	)

	// Initialize adapters with metrics & resilience decorators
	walletRepo := observability.NewWalletRepositoryMetrics(resilience.NewWalletRepositoryResilience(postgres.NewWalletRepository(pools, logger), cbs))
	merchantDir := observability.NewMerchantDirectoryMetrics(resilience.NewMerchantDirectoryResilience(postgres.NewMerchantDirectory(pools, logger), cbs))
	dlqStore := observability.NewDLQStoreMetrics(resilience.NewDLQStoreResilience(postgres.NewDLQStore(pools, logger), cbs))

	redisStore := redis.NewRedisStore(redisClient)

	// Default velocity rule: 50 transfers in 60s
	rules := []domain.VelocityRule{
		{
			Name:          "velocity_high",
			WindowSeconds: 60,
			Threshold:     50,
			Reason:        "50+ transfers from wallet in 60 seconds",
		},
	}

	// Initialize Fraud Service (no in-memory goroutine dispatch — Redis is the single source of truth for velocity state)
	fraudService := app.NewFraudService(logger, walletRepo, redisStore, merchantDir, rules)

	// Wrap in traces + metrics handlers
	handler := observability.NewJobHandlerTraces(observability.NewJobHandlerMetrics(fraudService))

	// Initialize Kafka Reader & ConsumerManager
	reader := platform.NewKafkaReader(cfg.KafkaBrokers, platform.TopicJobs, platform.ConsumerGroupFraudWorker, logger)
	consumerManager := kafka.NewConsumerManager(logger, reader, handler, dlqStore, merchantDir, pools)
	consumerManager.Start(ctx)

	// Wait for shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info(platform.LogEventShutdownSignalReceived)
	consumerManager.Stop()
	logger.Info(platform.LogEventServerShutdown)
}
