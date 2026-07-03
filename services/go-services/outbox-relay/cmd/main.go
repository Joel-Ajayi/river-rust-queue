package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/adapter/outbound/kafka"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/adapter/outbound/postgres"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/app"
)

func main() {
	// -- 1a. Init Config --
	cfg, err := platform.LoadConfig()
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// -- 1b. Init Logger --
	logger, _ := platform.NewLogger(cfg.LogLevel)
	defer logger.Sync()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// -- 1c. Initialize Postgres Shard Pools --
	pools, err := platform.NewShardPools(ctx, cfg, logger)
	if err != nil {
		fmt.Printf("Failed to initialize shards: %v\n", err)
		os.Exit(1)
	}
	defer pools.Close()

	// -- 1d. Initialize Kafka Writer --
	// We pass an empty string because the EventPublisher dynamically routes based on the Event's PublishTopic
	kafkaWriter := platform.NewKafkaWriter(cfg.KafkaBrokers, "", logger)
	defer kafkaWriter.Close()

	// -- 2. Initialize Adapters --
	eventStore := postgres.NewEventStore(pools)
	eventPublisher := kafka.NewEventPublisher(kafkaWriter)

	// -- 3. Initialize App --
	relayApp := app.NewRelayService(
		eventStore,
		eventPublisher,
		logger,
	)

	// 4. Start the Relayer in background
	var wg sync.WaitGroup
	for _, shardId := range pools.GetAvailableShardIDs() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			relayApp.Start(ctx, shardId)
		}()
	}

	logger.Info("Outbox Relay running. Press Ctrl+C to stop")

	// -- 5. Graceful Shutdown --
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Info("Shutting down gracefully... wait for all relayers to finish")
	cancel() // triggers ctx.Done() for all relayers
	wg.Wait()
	logger.Info("Shutting down gracefully")
}
