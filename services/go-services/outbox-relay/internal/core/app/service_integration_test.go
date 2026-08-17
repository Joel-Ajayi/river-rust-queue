package app

import (
	"context"
	"testing"
	"time"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/domain"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// === Mock Port Implementations ===

type mockEventStore struct {
	oldestAgeFunc func(ctx context.Context, shardID string) (time.Duration, error)
	processFunc   func(ctx context.Context, shardID string, batchSize int, fn func(ctx context.Context, events []domain.Event) error) error
	dlqFunc       func(ctx context.Context, e domain.Event, reason string) error
}

func (m *mockEventStore) GetOldestUnpublishedEventAge(ctx context.Context, shardID string) (time.Duration, error) {
	if m.oldestAgeFunc != nil {
		return m.oldestAgeFunc(ctx, shardID)
	}
	return 0, nil
}

func (m *mockEventStore) ProcessUnpublishedEvents(ctx context.Context, shardID string, batchSize int, fn func(ctx context.Context, events []domain.Event) error) error {
	if m.processFunc != nil {
		return m.processFunc(ctx, shardID, batchSize, fn)
	}
	return nil
}

func (m *mockEventStore) RouteToDLQ(ctx context.Context, e domain.Event, reason string) error {
	if m.dlqFunc != nil {
		return m.dlqFunc(ctx, e, reason)
	}
	return nil
}

type mockEventPublisher struct {
	publishBatchFunc func(ctx context.Context, shardID string, events []domain.Event) ([]string, error)
}

func (m *mockEventPublisher) PublishBatch(ctx context.Context, shardID string, events []domain.Event) ([]string, error) {
	if m.publishBatchFunc != nil {
		return m.publishBatchFunc(ctx, shardID, events)
	}
	ids := make([]string, len(events))
	for i, e := range events {
		ids[i] = e.ID
	}
	return ids, nil
}

func setupRelayService(store *mockEventStore, pub *mockEventPublisher) *RelayService {
	logger, _ := zap.NewDevelopment()
	cfg := RelayServiceConfig{
		ProcessTimeout: 5 * time.Second,
		FetchBatchSize: 10,
		PollInterval:   100 * time.Millisecond,
		MaxPayloadSize: 1024 * 1024,
	}
	return NewRelayService(store, pub, logger, "shard-a", cfg)
}

// === Integration Tests ===

func TestOutboxRelay_ProcessEvents_Success(t *testing.T) {
	pubCalled := false
	store := &mockEventStore{}
	pub := &mockEventPublisher{
		publishBatchFunc: func(ctx context.Context, shardID string, events []domain.Event) ([]string, error) {
			pubCalled = true
			ids := make([]string, len(events))
			for i, e := range events {
				ids[i] = e.ID
			}
			return ids, nil
		},
	}

	svc := setupRelayService(store, pub)

	envelope := &eventsv1.EventEnvelope{
		EventId:       platform.NewEventID(),
		EventType:     string(platform.EventTypeTransferCompleted),
		AggregateType: string(platform.AggregateTypeTransfer),
		AggregateId:   "tr_123",
		CorrelationId: "job_123",
		OccurredAt:    timestamppb.New(time.Now()),
		Payload: &eventsv1.EventEnvelope_TransferCompleted{
			TransferCompleted: &eventsv1.TransferCompletedPayload{
				JobId:      "job_123",
				TransferId: "tr_123",
				MerchantId: "merch_1",
			},
		},
	}
	payloadBytes, err := platform.MarshalEnvelope(envelope)
	if err != nil {
		t.Fatalf("failed to marshal event envelope: %v", err)
	}

	events := []domain.Event{
		{
			ID:            envelope.EventId,
			EventType:     platform.EventTypeTransferCompleted,
			AggregateType: platform.AggregateTypeTransfer,
			AggregateID:   envelope.AggregateId,
			CorrelationID: envelope.CorrelationId,
			Payload:       payloadBytes,
			OccurredAt:    time.Now(),
			PublishTopic:  platform.TopicNotify,
		},
	}

	err = svc.processEvents(context.Background(), events)
	if err != nil {
		t.Fatalf("expected nil error for valid outbox event processing, got %v", err)
	}
	if !pubCalled {
		t.Fatalf("expected PublishBatch to be called")
	}
}

func TestOutboxRelay_ProcessEvents_InvalidSchema_RoutesToDLQ(t *testing.T) {
	dlqCalled := false
	store := &mockEventStore{
		dlqFunc: func(ctx context.Context, e domain.Event, reason string) error {
			dlqCalled = true
			return nil
		},
	}
	pub := &mockEventPublisher{}

	svc := setupRelayService(store, pub)

	invalidEvents := []domain.Event{
		{
			ID:           platform.NewEventID(),
			Payload:      []byte(`{invalid envelope json}`),
			PublishTopic: platform.TopicNotify,
		},
	}

	err := svc.processEvents(context.Background(), invalidEvents)
	if err != nil {
		t.Fatalf("expected nil error when poison pill event is routed to DLQ, got %v", err)
	}
	if !dlqCalled {
		t.Fatalf("expected RouteToDLQ to be called for invalid event payload")
	}
}
