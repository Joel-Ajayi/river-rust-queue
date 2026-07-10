package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/adapters/inbound/kafka"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/adapters/outbound/postgres"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/app"
	segmentio "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

func main() {
	fmt.Printf("Starting %s service...\n", os.Args[0])
	// -- Load Config --
	cfg, err := platform.LoadConfig()
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}
	// -- Load Logger --
	logger, err := platform.NewLogger(cfg.LogLevel)
	if err != nil {
		fmt.Printf("Failed to init logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	// --Init context --
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Initialize Database Pools
	pools, err := platform.NewShardPools(ctx, cfg, logger)
	if err != nil {
		logger.Fatal("failed to initialize db pools", zap.Error(err))
	}
	defer pools.Close()

	// 2. Initialize Stores & Directory
	merchantDir := postgres.NewMerchantDirectory(pools, logger)
	ledgerStore := postgres.NewLedgerStore(pools, logger)
	xshardStore := postgres.NewCrossShardStore(pools, logger)
	dlqStore := postgres.NewDLQStore(pools, logger)

	// 3. Initialize Services
	processor := app.NewProcessor(logger, ledgerStore, xshardStore, merchantDir)
	xshardService := app.NewXShardService(logger, xshardStore)

	// 4. Initialize Kafka Readers
	groupID := platform.ConsumerGroupLedgerWorker
	jobReader := platform.NewKafkaReader(cfg.KafkaBrokers, platform.TopicJobs, groupID, logger)
	defer jobReader.Close()

	var xshardReaders []*segmentio.Reader
	
	for shardID := range cfg.ShardURIs {
		topic := platform.TopicXShardPrefix + shardID
		consumerGroup := groupID + "-" + shardID
		r := platform.NewKafkaReader(cfg.KafkaBrokers, topic, consumerGroup, logger)
		xshardReaders = append(xshardReaders, r)
		defer r.Close()
	}

	// 5. Start Consumer Manager
	consumerManager := kafka.NewConsumerManager(logger, jobReader, xshardReaders, processor, xshardService, dlqStore, pools)
	consumerManager.Start(ctx)
	logger.Info("ledger-worker started successfully")

	// 6. Shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	logger.Info("Shutting down...")
	cancel()
}
