package app

import (
	"context"
	"testing"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/domain"
	"go.uber.org/zap"
)

// === Mock Port Implementations ===

type mockRepo struct {
	getMerchantFunc func(ctx context.Context, merchantID string) (*domain.Merchant, error)
	createFunc      func(ctx context.Context, shardID string, d *domain.WebhookDelivery) error
	completeFunc    func(ctx context.Context, shardID string, d *domain.WebhookDelivery, payload []byte, eventID string) error
	dlqFunc         func(ctx context.Context, shardID string, d *domain.WebhookDelivery, errMsg string, payload []byte, eventID string, firstFailedAt time.Time, lastFailedAt time.Time) error
	globalDLQFunc   func(ctx context.Context, payload []byte, topic, key, errMsg string) error
}

func (m *mockRepo) GetMerchantConfig(ctx context.Context, merchantID string) (*domain.Merchant, error) {
	if m.getMerchantFunc != nil {
		return m.getMerchantFunc(ctx, merchantID)
	}
	return &domain.Merchant{ID: merchantID, ShardID: "shard-a", WebhookURL: "https://merchant.example.com/webhook", WebhookSecret: "secret", Status: domain.StatusActive}, nil
}

func (m *mockRepo) CreatePendingDelivery(ctx context.Context, shardID string, d *domain.WebhookDelivery) error {
	if m.createFunc != nil {
		return m.createFunc(ctx, shardID, d)
	}
	return nil
}

func (m *mockRepo) CompleteDelivery(ctx context.Context, shardID string, d *domain.WebhookDelivery, payload []byte, eventID string) error {
	if m.completeFunc != nil {
		return m.completeFunc(ctx, shardID, d, payload, eventID)
	}
	return nil
}

func (m *mockRepo) ScheduleRetry(ctx context.Context, shardID string, d *domain.WebhookDelivery, payload []byte, eventID string) error {
	return nil
}

func (m *mockRepo) FailDeliveryAndRouteToDLQ(ctx context.Context, shardID string, d *domain.WebhookDelivery, errMsg string, payload []byte, eventID string, firstFailedAt time.Time, lastFailedAt time.Time) error {
	if m.dlqFunc != nil {
		return m.dlqFunc(ctx, shardID, d, errMsg, payload, eventID, firstFailedAt, lastFailedAt)
	}
	return nil
}

func (m *mockRepo) RouteToGlobalDLQ(ctx context.Context, payload []byte, topic, key, errMsg string) error {
	if m.globalDLQFunc != nil {
		return m.globalDLQFunc(ctx, payload, topic, key, errMsg)
	}
	return nil
}

func (m *mockRepo) GetAvailableShardIDs() []string {
	return []string{"shard-a"}
}

func (m *mockRepo) FetchPendingRetries(ctx context.Context, shardID string, limit int) ([]*domain.WebhookDelivery, error) {
	return []*domain.WebhookDelivery{}, nil
}

type mockHTTPClient struct {
	postFunc func(ctx context.Context, merchantID, url string, payload []byte, sig, timestamp, eventID string, attempt int) (int, error)
}

func (m *mockHTTPClient) Post(ctx context.Context, merchantID, url string, payload []byte, sig, timestamp, eventID string, attempt int) (int, error) {
	if m.postFunc != nil {
		return m.postFunc(ctx, merchantID, url, payload, sig, timestamp, eventID, attempt)
	}
	return 200, nil
}

func setupWebhookService(repo *mockRepo, client *mockHTTPClient) *WebhookService {
	logger, _ := zap.NewDevelopment()
	cfg := WebhookConfig{
		MaxDeliveryAttempts:   3,
		BaseRetryDelaySec:     1,
		CapRetryDelaySec:      10,
		SchedulerPollInterval: 1 * time.Second,
		SchedulerBatchSize:    10,
		FastLaneGracePeriod:   100 * time.Millisecond,
		FastLaneBufferSize:    100,
		MaxConcurrency:        10,
	}
	return NewWebhookService(repo, client, logger, cfg)
}

// === Service Integration Tests ===

func TestWebhook_HandleMessage_Success(t *testing.T) {
	repo := &mockRepo{}
	client := &mockHTTPClient{}
	svc := setupWebhookService(repo, client)

	eventID := platform.NewEventID()
	payload := []byte(`{"event_id":"` + eventID + `","type":"transfer.completed"}`)

	err := svc.HandleMessage(context.Background(), "merch-123", "notify", "merch-123", payload)
	if err != nil {
		t.Fatalf("expected nil error for HandleMessage, got %v", err)
	}
}

func TestWebhook_HandleMessage_InvalidJSON_RoutesToGlobalDLQ(t *testing.T) {
	dlqCalled := false
	repo := &mockRepo{
		globalDLQFunc: func(ctx context.Context, payload []byte, topic, key, errMsg string) error {
			dlqCalled = true
			return nil
		},
	}
	client := &mockHTTPClient{}
	svc := setupWebhookService(repo, client)

	invalidPayload := []byte(`{invalid json payload}`)
	err := svc.HandleMessage(context.Background(), "merch-123", "notify", "merch-123", invalidPayload)
	if err != nil {
		t.Fatalf("expected nil error when poison pill routed to DLQ, got %v", err)
	}
	if !dlqCalled {
		t.Fatalf("expected RouteToGlobalDLQ to be called for invalid JSON payload")
	}
}

func TestWebhook_HandleMessage_InactiveMerchant_RoutesToGlobalDLQ(t *testing.T) {
	dlqCalled := false
	repo := &mockRepo{
		getMerchantFunc: func(ctx context.Context, merchantID string) (*domain.Merchant, error) {
			return &domain.Merchant{ID: merchantID, Status: "inactive"}, nil
		},
		globalDLQFunc: func(ctx context.Context, payload []byte, topic, key, errMsg string) error {
			dlqCalled = true
			return nil
		},
	}
	client := &mockHTTPClient{}
	svc := setupWebhookService(repo, client)

	eventID := platform.NewEventID()
	payload := []byte(`{"event_id":"` + eventID + `","type":"transfer.completed"}`)

	err := svc.HandleMessage(context.Background(), "merch-123", "notify", "merch-123", payload)
	if err != nil {
		t.Fatalf("expected nil error for inactive merchant, got %v", err)
	}
	if !dlqCalled {
		t.Fatalf("expected RouteToGlobalDLQ to be called for inactive merchant")
	}
}
