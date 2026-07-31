package app_test

import (
	"context"
	"time"

	"errors"
	"testing"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/adapter/outbound/resilience"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/app"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/domain"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func newMockBreakerRegistry() *resilience.BreakerRegistry {
	mockLogger := zap.NewNop()
	reg := resilience.NewBreakerRegistry(mockLogger)
	_ = reg.For("test-merchant")
	return reg
}

type MockBreakerRegistry struct{ mock.Mock }

func (m *MockBreakerRegistry) For(merchantID string) *platform.WebhookResilience {
	args := m.Called(merchantID)
	if cb := args.Get(0); cb != nil {
		return cb.(*platform.WebhookResilience)
	}
	return nil
}

type MockRepository struct{ mock.Mock }

func (m *MockRepository) GetMerchantConfig(ctx context.Context, merchantID string) (*domain.Merchant, error) {
	args := m.Called(ctx, merchantID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*domain.Merchant), args.Error(1)
}

func (m *MockRepository) CreatePendingDelivery(ctx context.Context, shardID string, d *domain.WebhookDelivery) error {
	args := m.Called(ctx, shardID, d)
	return args.Error(0)
}

func (m *MockRepository) CompleteDelivery(ctx context.Context, shardID string, d *domain.WebhookDelivery, successEventPayload []byte, eventID string) error {
	args := m.Called(ctx, shardID, d, successEventPayload, eventID)
	return args.Error(0)
}

func (m *MockRepository) FailDeliveryAndRouteToDLQ(ctx context.Context, shardID string, d *domain.WebhookDelivery, errorMsg string, failEventPayload []byte, eventID string, firstFailedAt time.Time, lastFailedAt time.Time) error {
	args := m.Called(ctx, shardID, d, errorMsg, failEventPayload, eventID, firstFailedAt, lastFailedAt)
	return args.Error(0)
}

func (m *MockRepository) ScheduleRetry(ctx context.Context, shardID string, d *domain.WebhookDelivery, failEventPayload []byte, eventID string) error {
	args := m.Called(ctx, shardID, d, failEventPayload, eventID)
	return args.Error(0)
}

func (m *MockRepository) FetchPendingRetries(ctx context.Context, shardID string, limit int) ([]*domain.WebhookDelivery, error) {
	args := m.Called(ctx, shardID, limit)
	return args.Get(0).([]*domain.WebhookDelivery), args.Error(1)
}

func (m *MockRepository) GetAvailableShardIDs() []string {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).([]string)
}

func (m *MockRepository) RouteToGlobalDLQ(ctx context.Context, payload []byte, errorMsg string) error {
	args := m.Called(ctx, payload, errorMsg)
	return args.Error(0)
}

type MockHTTPClient struct{ mock.Mock }

func (m *MockHTTPClient) Post(ctx context.Context, merchantID string, url string, payload []byte, signature, timestamp, eventID string, attempt int) (int, error) {
	args := m.Called(ctx, merchantID, url, payload, signature, timestamp, eventID, attempt)
	return args.Int(0), args.Error(1)
}

func makeTestPayload(eventID string) []byte {
	env := &eventsv1.EventEnvelope{EventId: eventID}
	bytes, _ := proto.Marshal(env)
	return bytes
}

func TestWebhookService_HandleMessage_UnmarshalError(t *testing.T) {
	mockRepo := new(MockRepository)
	svc := app.NewWebhookService(mockRepo, new(MockHTTPClient), zap.NewNop())

	mockRepo.On("RouteToGlobalDLQ", mock.Anything, []byte("{invalid json}"), mock.Anything).Return(nil)

	err := svc.HandleMessage(context.Background(), "merch_1", []byte("{invalid json}"))

	require.NoError(t, err)
	mockRepo.AssertExpectations(t)
}

func TestWebhookService_HandleMessage_MerchantNotFoundRoutesToGlobalDLQ(t *testing.T) {
	mockRepo := new(MockRepository)
	svc := app.NewWebhookService(mockRepo, new(MockHTTPClient), zap.NewNop())
	payload := makeTestPayload("ev_1")

	// 1. Mock: GetMerchantConfig returns nil, nil (not found)
	mockRepo.On("GetMerchantConfig", mock.Anything, "merch_1").Return((*domain.Merchant)(nil), nil)

	// 2. Mock: RouteToGlobalDLQ should be called
	mockRepo.On("RouteToGlobalDLQ", mock.Anything, payload, domain.ErrorMerchantLookupFailed).Return(nil)

	err := svc.HandleMessage(context.Background(), "merch_1", payload)

	require.NoError(t, err) // Returns nil to commit offset
	mockRepo.AssertExpectations(t)
}

func TestWebhookService_HandleMessage_SkipsInactiveMerchant(t *testing.T) {
	mockRepo := new(MockRepository)
	mockClient := new(MockHTTPClient)
	svc := app.NewWebhookService(mockRepo, mockClient, zap.NewNop())
	payload := makeTestPayload("ev_1")

	mockRepo.On("GetMerchantConfig", mock.Anything, "merch_1").Return(&domain.Merchant{
		ID:     "merch_1",
		Status: domain.StatusFrozen,
	}, nil)

	err := svc.HandleMessage(context.Background(), "merch_1", payload)

	require.NoError(t, err)
	mockRepo.AssertExpectations(t)
	mockClient.AssertNotCalled(t, "Post", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	mockRepo.AssertNotCalled(t, "CompleteDelivery", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestWebhookService_HandleMessage_DeliverySuccess(t *testing.T) {
	mockRepo := new(MockRepository)
	mockClient := new(MockHTTPClient)
	svc := app.NewWebhookService(mockRepo, mockClient, zap.NewNop())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go svc.StartFastLaneWorkers(ctx, 1)() // Start 1 worker

	payload := makeTestPayload("ev_1")

	mockRepo.On("GetMerchantConfig", mock.Anything, "merch_1").Return(&domain.Merchant{
		ID:            "merch_1",
		WebhookURL:    "https://example.com/hook",
		WebhookSecret: "secret",
		Status:        domain.StatusActive,
		ShardID:       "shard_a",
	}, nil)
	mockRepo.On("CreatePendingDelivery", mock.Anything, "shard_a", mock.Anything).Return(nil)
	mockClient.On("Post", mock.Anything, "merch_1", "https://example.com/hook", mock.Anything, mock.Anything, mock.Anything, "ev_1", 1).Return(200, nil)
	mockRepo.On("CompleteDelivery", mock.Anything, "shard_a", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	err := svc.HandleMessage(context.Background(), "merch_1", payload)

	require.NoError(t, err)

	// Wait a bit for the async fast-lane to complete
	time.Sleep(50 * time.Millisecond)

	mockRepo.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestWebhookService_HandleMessage_DeliveryFailureSchedulesRetry(t *testing.T) {
	mockRepo := new(MockRepository)
	mockClient := new(MockHTTPClient)
	svc := app.NewWebhookService(mockRepo, mockClient, zap.NewNop())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go svc.StartFastLaneWorkers(ctx, 1)()

	payload := makeTestPayload("ev_1")

	mockRepo.On("GetMerchantConfig", mock.Anything, "merch_1").Return(&domain.Merchant{
		ID:            "merch_1",
		WebhookURL:    "https://example.com/hook",
		WebhookSecret: "secret",
		Status:        domain.StatusActive,
		ShardID:       "shard_a",
	}, nil)
	mockRepo.On("CreatePendingDelivery", mock.Anything, "shard_a", mock.Anything).Return(nil)
	mockClient.On("Post", mock.Anything, "merch_1", "https://example.com/hook", mock.Anything, mock.Anything, mock.Anything, "ev_1", 1).Return(0, &platform.HttpError{Status: 502, Err: errors.New("bad gateway")})
	mockRepo.On("ScheduleRetry", mock.Anything, "shard_a", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	err := svc.HandleMessage(context.Background(), "merch_1", payload)

	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	mockRepo.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestWebhookService_HandleMessage_Concurrent(t *testing.T) {
	mockRepo := new(MockRepository)
	mockClient := new(MockHTTPClient)
	svc := app.NewWebhookService(mockRepo, mockClient, zap.NewNop())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go svc.StartFastLaneWorkers(ctx, 5)()

	payload := makeTestPayload("ev_1")

	mockRepo.On("GetMerchantConfig", mock.Anything, "merch_1").Return(&domain.Merchant{
		ID:            "merch_1",
		WebhookURL:    "https://example.com/hook",
		WebhookSecret: "secret",
		Status:        domain.StatusActive,
		ShardID:       "shard_a",
	}, nil)
	mockRepo.On("CreatePendingDelivery", mock.Anything, "shard_a", mock.Anything).Return(nil)
	mockClient.On("Post", mock.Anything, "merch_1", "https://example.com/hook", mock.Anything, mock.Anything, mock.Anything, "ev_1", 1).Return(200, nil)
	mockRepo.On("CompleteDelivery", mock.Anything, "shard_a", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	const goroutines = 10
	errChan := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			errChan <- svc.HandleMessage(context.Background(), "merch_1", payload)
		}()
	}

	for i := 0; i < goroutines; i++ {
		err := <-errChan
		require.NoError(t, err)
	}

	time.Sleep(100 * time.Millisecond)

	mockRepo.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}
