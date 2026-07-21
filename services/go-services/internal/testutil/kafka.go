package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	tc_kafka "github.com/testcontainers/testcontainers-go/modules/kafka"
)

// StartKafka spins up a Kafka container, creates the standard topics, and returns the container and broker addresses.
func StartKafka(t *testing.T) (*tc_kafka.KafkaContainer, []string) {
	t.Helper()
	ctx := context.Background()

	kafkaContainer, err := tc_kafka.Run(ctx, "confluentinc/cp-kafka:7.3.2")
	if err != nil {
		t.Fatalf("failed to start kafka container: %s", err)
	}
	t.Cleanup(func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate kafka container: %s", err)
		}
	})

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		t.Fatalf("failed to get kafka brokers: %s", err)
	}

	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		t.Fatalf("failed to dial kafka: %s", err)
	}
	defer conn.Close()

	for _, topic := range []string{"rrq.jobs", "rrq.notify"} {
		err = conn.CreateTopics(kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		})
		if err != nil {
			t.Fatalf("failed to create topic %s: %s", topic, err)
		}
	}

	return kafkaContainer, brokers
}

// NewTestKafkaReader creates a Kafka reader pointed at the test brokers for assertion purposes.
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
