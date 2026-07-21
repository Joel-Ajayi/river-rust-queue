//go:build integration

package app_test

import (
	"context"
	"testing"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/testutil"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/adapter/outbound/kafka"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/adapter/outbound/postgres"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/adapter/outbound/resilience"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/app"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kafkalib "github.com/segmentio/kafka-go"
	tc_kafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"go.uber.org/zap"
)

// setupEnvironment spins up real Postgres and Kafka containers.
func setupEnvironment(t *testing.T) (*platform.ShardPools, *testutil.TestDB, *tc_kafka.KafkaContainer, []string, *kafkalib.Reader) {
	ctx := context.Background()
	log, _ := zap.NewDevelopment()

	// 1. Start Postgres (this gives us merchants, shard_a, shard_b)
	merchantsDB, shardA, shardB := testutil.SetupTestDB(t)

	cfg := &platform.Config{
		MerchantsDBURI: merchantsDB.URI,
		ShardURIs: map[string]string{
			"shard_a": shardA.URI,
			"shard_b": shardB.URI,
		},
	}
	pools, err := platform.NewShardPools(ctx, cfg, log)
	require.NoError(t, err)
	t.Cleanup(func() { pools.Close() })

	// 2. Start Kafka
	kafkaContainer, err := tc_kafka.Run(ctx, "confluentinc/cp-kafka:7.3.2")
	require.NoError(t, err)
	t.Cleanup(func() {
		kafkaContainer.Terminate(context.Background())
	})

	brokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err)
	t.Logf("TEST KAFKA BROKERS: %v", brokers)

	// 3. Setup a Kafka Consumer to assert published messages
	conn, err := kafkalib.Dial("tcp", brokers[0])
	require.NoError(t, err)
	defer conn.Close()

	// Pre-create topics to prevent "Unknown Topic" errors failing the first publish synchronously
	err = conn.CreateTopics(
		kafkalib.TopicConfig{Topic: platform.TopicJobs, NumPartitions: 1, ReplicationFactor: 1},
		kafkalib.TopicConfig{Topic: platform.TopicNotify, NumPartitions: 1, ReplicationFactor: 1},
	)
	require.NoError(t, err)

	reader := kafkalib.NewReader(kafkalib.ReaderConfig{
		Brokers:     brokers,
		Topic:       platform.TopicJobs,
		Partition:   0,
		StartOffset: kafkalib.FirstOffset,
		MaxWait:     100 * time.Millisecond, // fast for tests
	})
	t.Cleanup(func() { reader.Close() })

	return pools, &shardA, kafkaContainer, brokers, reader
}

func TestOutboxRelay_BasicPublishing(t *testing.T) {
	pools, shardA, _, brokers, consumer := setupEnvironment(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	log, _ := zap.NewDevelopment()

	eventID := platform.NewEventID()
	aggID := platform.NewTransferID()
	_, err := shardA.Pool.Exec(ctx, `
		INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, correlation_id, payload, occurred_at, publish_topic)
		VALUES ($1, 'test_event', 'test_agg', $2, $2, '{"foo":"bar"}', NOW(), $3)
	`, eventID, aggID, platform.TopicJobs)
	require.NoError(t, err)

	store := postgres.NewEventStore(pools, log)
	storeDecorated := resilience.NewEventStoreCB(store, "shard_a")

	kafkaWriter := platform.NewKafkaWriter(brokers, "", log)
	t.Cleanup(func() { kafkaWriter.Close() })

	kafkaCB := platform.NewKafkaCircuitBreaker("", platform.CircuitBreakerConfig{})
	pub := kafka.NewEventPublisher(kafkaWriter)
	pubDecorated := resilience.NewEventPublisherCB(pub, kafkaCB)

	var count int
	err = shardA.Pool.QueryRow(ctx, "SELECT count(*) FROM events WHERE published_at IS NULL").Scan(&count)
	require.NoError(t, err)
	t.Logf("UNPUBLISHED COUNT BEFORE START: %d", count)

	relayer := app.NewRelayService(storeDecorated, pubDecorated, log)
	go relayer.Start(ctx, "shard_a")

	msg, err := consumer.ReadMessage(ctx)
	require.NoError(t, err)

	assert.Equal(t, aggID, string(msg.Key))
	assert.JSONEq(t, `{"foo":"bar"}`, string(msg.Value))
	assert.Equal(t, platform.TopicJobs, msg.Topic)

	require.Eventually(t, func() bool {
		var publishedAt *time.Time
		if err := shardA.Pool.QueryRow(ctx, "SELECT published_at FROM events WHERE event_id = $1", eventID).Scan(&publishedAt); err != nil {
			return false
		}
		return publishedAt != nil
	}, 5*time.Second, 50*time.Millisecond)
}

func TestOutboxRelay_Batching(t *testing.T) {
	pools, shardA, _, brokers, consumer := setupEnvironment(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	log := zap.NewNop()

	for i := 0; i < 500; i++ {
		_, err := shardA.Pool.Exec(ctx, `
			INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, correlation_id, payload, occurred_at, publish_topic)
			VALUES ($1, 'test_event', 'test_agg', $2, $2, '{"foo":"batch"}', NOW(), $3)
		`, platform.NewEventID(), platform.NewTransferID(), platform.TopicJobs)
		require.NoError(t, err)
	}

	store := resilience.NewEventStoreCB(postgres.NewEventStore(pools, log), "shard_a")

	kafkaWriter := platform.NewKafkaWriter(brokers, "", log)
	t.Cleanup(func() { kafkaWriter.Close() })

	kafkaCB := platform.NewKafkaCircuitBreaker("", platform.CircuitBreakerConfig{})
	pub := resilience.NewEventPublisherCB(kafka.NewEventPublisher(kafkaWriter), kafkaCB)

	relayer := app.NewRelayService(store, pub, log)
	go relayer.Start(ctx, "shard_a")

	consumed := 0
	for consumed < 500 {
		msg, err := consumer.ReadMessage(ctx)
		require.NoError(t, err)
		assert.JSONEq(t, `{"foo":"batch"}`, string(msg.Value))
		consumed++
	}
	assert.Equal(t, 500, consumed)

	require.Eventually(t, func() bool {
		var count int
		if err := shardA.Pool.QueryRow(ctx, "SELECT count(*) FROM events WHERE published_at IS NULL").Scan(&count); err != nil {
			return false
		}
		return count == 0
	}, 5*time.Second, 50*time.Millisecond)
}

func TestOutboxRelay_ConcurrentReplicas(t *testing.T) {
	pools, shardA, _, brokers, consumer := setupEnvironment(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	log := zap.NewNop()

	// Insert 1000 events
	for i := 0; i < 1000; i++ {
		_, err := shardA.Pool.Exec(ctx, `
			INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, correlation_id, payload, occurred_at, publish_topic)
			VALUES ($1, 'test_event', 'test_agg', $2, $2, '{"foo":"bar"}', NOW(), $3)
		`, platform.NewEventID(), platform.NewTransferID(), platform.TopicJobs)
		require.NoError(t, err)
	}

	kafkaWriter := platform.NewKafkaWriter(brokers, "", log)
	t.Cleanup(func() { kafkaWriter.Close() })

	kafkaCB := platform.NewKafkaCircuitBreaker("", platform.CircuitBreakerConfig{})

	// Start two instances of Relay
	store1 := resilience.NewEventStoreCB(postgres.NewEventStore(pools, log), "shard_a")
	pub1 := resilience.NewEventPublisherCB(kafka.NewEventPublisher(kafkaWriter), kafkaCB)
	relayer1 := app.NewRelayService(store1, pub1, log)

	store2 := resilience.NewEventStoreCB(postgres.NewEventStore(pools, log), "shard_a")
	pub2 := resilience.NewEventPublisherCB(kafka.NewEventPublisher(kafkaWriter), kafkaCB)
	relayer2 := app.NewRelayService(store2, pub2, log)

	go relayer1.Start(ctx, "shard_a")
	go relayer2.Start(ctx, "shard_a")

	// Consume 1000 messages exactly
	consumed := 0
	keys := make(map[string]bool)
	for consumed < 1000 {
		msg, err := consumer.ReadMessage(ctx)
		require.NoError(t, err)
		k := string(msg.Key)
		assert.False(t, keys[k], "Duplicate event published!")
		keys[k] = true
		consumed++
	}
	assert.Equal(t, 1000, consumed)
}

func TestOutboxRelay_GracefulShutdown(t *testing.T) {
	pools, shardA, _, brokers, _ := setupEnvironment(t)

	ctx, cancel := context.WithCancel(context.Background())
	log := zap.NewNop()

	// Insert 10 events
	for i := 0; i < 10; i++ {
		_, err := shardA.Pool.Exec(ctx, `
			INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, correlation_id, payload, occurred_at, publish_topic)
			VALUES ($1, 'test_event', 'test_agg', $2, $2, '{"foo":"bar"}', NOW(), $3)
		`, platform.NewEventID(), platform.NewTransferID(), platform.TopicJobs)
		require.NoError(t, err)
	}

	store := resilience.NewEventStoreCB(postgres.NewEventStore(pools, log), "shard_a")
	kafkaWriter := platform.NewKafkaWriter(brokers, "", log)
	t.Cleanup(func() { kafkaWriter.Close() })
	kafkaCB := platform.NewKafkaCircuitBreaker("", platform.CircuitBreakerConfig{})
	pub := resilience.NewEventPublisherCB(kafka.NewEventPublisher(kafkaWriter), kafkaCB)

	relayer := app.NewRelayService(store, pub, log)

	// Start in background and wait a split second
	done := make(chan struct{})
	go func() {
		relayer.Start(ctx, "shard_a")
		close(done)
	}()

	// Wait briefly so it starts processing, then cancel (SIGTERM simulation)
	time.Sleep(100 * time.Millisecond)
	cancel()

	// Wait for Start to return
	select {
	case <-done:
		// Success
	case <-time.After(10 * time.Second):
		t.Fatal("Relayer did not shut down gracefully")
	}
}
