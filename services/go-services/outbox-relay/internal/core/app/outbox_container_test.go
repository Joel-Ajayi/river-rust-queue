//go:build integration

package app

import (
	"context"
	"testing"
	"time"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/testutil"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/adapter/outbound/kafka"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/adapter/outbound/postgres"
	segment_kafka "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestOutboxRelay_Container_Publish_WithRealPostgresAndKafka(t *testing.T) {
	cluster := testutil.SetupTestDB(t)
	_, brokers := testutil.StartKafka(t)

	logger, _ := zap.NewDevelopment()

	retryCfg := platform.RetryConfig{
		MaxRetries: 3,
		BaseDelay:  10 * time.Millisecond,
		MaxDelay:   100 * time.Millisecond,
	}
	eventStore := postgres.NewEventStore(cluster.ShardPools, logger, retryCfg, "outbox-relay")

	writer := &segment_kafka.Writer{
		Addr:                   segment_kafka.TCP(brokers...),
		Balancer:               &segment_kafka.Hash{},
		RequiredAcks:           segment_kafka.RequireAll,
		BatchTimeout:           10 * time.Millisecond,
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()
	eventPublisher := kafka.NewEventPublisher(writer)

	cfg := RelayServiceConfig{
		ProcessTimeout: 5 * time.Second,
		FetchBatchSize: 10,
		PollInterval:   100 * time.Millisecond,
		MaxPayloadSize: 1024 * 1024,
	}
	svc := NewRelayService(eventStore, eventPublisher, logger, "shard-a", cfg)

	eventID := platform.NewEventID()
	envelope := &eventsv1.EventEnvelope{
		EventId:       eventID,
		EventType:     string(platform.EventTypeTransferCompleted),
		AggregateType: string(platform.AggregateTypeTransfer),
		AggregateId:   "tr_container_123",
		CorrelationId: "job_container_123",
		OccurredAt:    timestamppb.New(time.Now()),
		Payload: &eventsv1.EventEnvelope_TransferCompleted{
			TransferCompleted: &eventsv1.TransferCompletedPayload{
				JobId:      "job_container_123",
				TransferId: "tr_container_123",
				MerchantId: "merch_1",
			},
		},
	}
	payloadBytes, _ := platform.MarshalEnvelope(envelope)

	_, err := cluster.ShardA.Pool.Exec(context.Background(), `
		INSERT INTO outbox_events (id, event_type, aggregate_type, aggregate_id, correlation_id, payload, occurred_at, publish_topic, status)
		VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7, 'pending')
	`, eventID, platform.EventTypeTransferCompleted, platform.AggregateTypeTransfer, "tr_container_123", "job_container_123", payloadBytes, platform.TopicNotify)
	if err != nil {
		t.Fatalf("failed to insert pending event into container postgres: %v", err)
	}

	reader := testutil.NewTestKafkaReader(t, brokers, platform.TopicNotify)

	// Process batch which fetches from Postgres, publishes to Kafka, and updates Postgres status
	err = svc.processBatch(context.Background(), "shard-a")
	if err != nil {
		t.Fatalf("processBatch failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	msg, err := reader.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("failed to read from kafka container: %v", err)
	}

	if string(msg.Key) != "tr_container_123" {
		t.Fatalf("expected kafka message key to be tr_container_123, got %s", string(msg.Key))
	}

	var status string
	err = cluster.ShardA.Pool.QueryRow(context.Background(), `SELECT status FROM outbox_events WHERE id = $1`, eventID).Scan(&status)
	if err != nil {
		t.Fatalf("failed to query event status: %v", err)
	}
	if status != "published" {
		t.Fatalf("expected event status to be published in db, got %s", status)
	}
}
