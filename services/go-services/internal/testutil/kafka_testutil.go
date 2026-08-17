package testutil

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	tc_kafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

var (
	kafkaOnce      sync.Once
	kafkaContainer *tc_kafka.KafkaContainer
	kafkaBrokers   []string
	kafkaInitError error
)

// StartKafka spins up a persistent Kafka container (reused across test runs).
// The container stays running until explicitly terminated by running `make test-clean` or `docker rm -f $(docker ps -q --filter label=org.testcontainers=true)`.
func StartKafka(t *testing.T) (*tc_kafka.KafkaContainer, []string) {
	t.Helper()
	ctx := context.Background()

	kafkaOnce.Do(func() {
		container, err := tc_kafka.Run(ctx,
			"confluentinc/cp-kafka:7.5.0",
		)
		if err != nil {
			kafkaInitError = fmt.Errorf("failed to start persistent kafka container: %w", err)
			return
		}
		kafkaContainer = container

		brokers, err := container.Brokers(ctx)
		if err != nil {
			kafkaInitError = fmt.Errorf("failed to get kafka brokers: %w", err)
			return
		}
		kafkaBrokers = brokers

		conn, err := kafka.Dial("tcp", brokers[0])
		if err != nil {
			kafkaInitError = fmt.Errorf("failed to dial kafka: %w", err)
			return
		}
		defer conn.Close()

		topics := []string{platform.TopicJobs, platform.TopicNotify, "xshard.shard-a", "xshard.shard-b"}
		for _, topic := range topics {
			_ = conn.CreateTopics(kafka.TopicConfig{
				Topic:             topic,
				NumPartitions:     1,
				ReplicationFactor: 1,
			})
		}
	})

	if kafkaInitError != nil {
		t.Fatalf("failed to setup persistent test kafka container: %v", kafkaInitError)
	}

	return kafkaContainer, kafkaBrokers
}

// NewTestKafkaReader creates a Kafka reader for test assertions.
func NewTestKafkaReader(t *testing.T, brokers []string, topic string) *kafka.Reader {
	t.Helper()
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		Partition:   0,
		StartOffset: kafka.FirstOffset,
		MaxWait:     100 * time.Millisecond,
	})
	t.Cleanup(func() { reader.Close() })
	return reader
}
