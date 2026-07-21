package app_test

import (
	"context"
	"errors"
	"testing"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/app"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/domain"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func makeTestPayload(eventID string) []byte {
	env := &eventsv1.EventEnvelope{EventId: eventID}
	bytes, _ := proto.Marshal(env)
	return bytes
}

func TestWebhookService_HandleMessage_UnmarshalError(t *testing.T) {
	svc := app.NewWebhookService(new(MockRepository), new(MockHTTPClient), newMockBreakerRegistry(), zap.NewNop())

	err := svc.HandleMessage(context.Background(), "merch_1", []byte("{invalid json}"))

	require.Error(t, err)
}

func TestWebhookService_HandleMessage_MerchantNotFoundRoutesToDLQ(t *testing.T) {
	mockRepo := new(MockRepository)

	svc := app.NewWebhookService(mockRepo, new(MockHTTPClient), newMockBreakerRegistry(), zap.NewNop())

	payload := makeTestPayload("ev_1")

	mockRepo.On("GetMerchantConfig", mock.Anything, "merch_1").Return((*domain.Merchant)(nil), nil)
	mockRepo.On("GetAvailableShardIDs").Return([]string{"shard_a"})
	mockRepo.On("RouteToDLQ", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	err := svc.HandleMessage(context.Background(), "merch_1", payload)

	require.NoError(t, err)
	mockRepo.AssertExpectations(t)
}

func TestWebhookService_HandleMessage_SkipsInactiveMerchant(t *testing.T) {
	mockRepo := new(MockRepository)
	mockClient := new(MockHTTPClient)

	svc := app.NewWebhookService(mockRepo, mockClient, newMockBreakerRegistry(), zap.NewNop())

	payload := makeTestPayload("ev_1")

	mockRepo.On("GetMerchantConfig", mock.Anything, "merch_1").Return(&domain.Merchant{
		ID:     "merch_1",
		Status: domain.StatusFrozen,
	}, nil)

	err := svc.HandleMessage(context.Background(), "merch_1", payload)

	require.NoError(t, err)
	mockRepo.AssertExpectations(t)
	mockClient.AssertNotCalled(t, "Post", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	mockRepo.AssertNotCalled(t, "SaveDelivery", mock.Anything, mock.Anything, mock.Anything)
}

func TestWebhookService_HandleMessage_DeliverySuccess(t *testing.T) {
	mockRepo := new(MockRepository)
	mockClient := new(MockHTTPClient)

	svc := app.NewWebhookService(mockRepo, mockClient, newMockBreakerRegistry(), zap.NewNop())

	payload := makeTestPayload("ev_1")

	mockRepo.On("GetMerchantConfig", mock.Anything, "merch_1").Return(&domain.Merchant{
		ID:            "merch_1",
		WebhookURL:    "https://example.com/hook",
		WebhookSecret: "secret",
		Status:        domain.StatusActive,
		ShardID:       "shard_a",
	}, nil)
	mockClient.On("Post", mock.Anything, "https://example.com/hook", mock.Anything, mock.Anything, "ev_1", 1).Return(200, nil)
	mockRepo.On("SaveDelivery", mock.Anything, "shard_a", mock.Anything).Return(nil)
	mockRepo.On("RecordEvent", mock.Anything, "shard_a", mock.Anything, domain.EventTypeWebhookDelivered, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	err := svc.HandleMessage(context.Background(), "merch_1", payload)

	require.NoError(t, err)
	mockRepo.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestWebhookService_HandleMessage_DeliveryFailureSchedulesRetry(t *testing.T) {
	mockRepo := new(MockRepository)
	mockClient := new(MockHTTPClient)

	svc := app.NewWebhookService(mockRepo, mockClient, newMockBreakerRegistry(), zap.NewNop())

	payload := makeTestPayload("ev_1")

	mockRepo.On("GetMerchantConfig", mock.Anything, "merch_1").Return(&domain.Merchant{
		ID:            "merch_1",
		WebhookURL:    "https://example.com/hook",
		WebhookSecret: "secret",
		Status:        domain.StatusActive,
		ShardID:       "shard_a",
	}, nil)
	mockClient.On("Post", mock.Anything, "https://example.com/hook", mock.Anything, mock.Anything, "ev_1", 1).Return(0, errors.New("connection reset"))
	mockRepo.On("SaveDelivery", mock.Anything, "shard_a", mock.MatchedBy(func(d *domain.WebhookDelivery) bool {
		return d.Status == domain.StatusPending
	})).Return(nil)

	err := svc.HandleMessage(context.Background(), "merch_1", payload)

	require.NoError(t, err)
	mockRepo.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestWebhookService_HandleMessage_Concurrent(t *testing.T) {
	mockRepo := new(MockRepository)
	mockClient := new(MockHTTPClient)

	svc := app.NewWebhookService(mockRepo, mockClient, newMockBreakerRegistry(), zap.NewNop())

	payload := makeTestPayload("ev_1")

	mockRepo.On("GetMerchantConfig", mock.Anything, "merch_1").Return(&domain.Merchant{
		ID:            "merch_1",
		WebhookURL:    "https://example.com/hook",
		WebhookSecret: "secret",
		Status:        domain.StatusActive,
		ShardID:       "shard_a",
	}, nil)
	mockClient.On("Post", mock.Anything, "https://example.com/hook", mock.Anything, mock.Anything, "ev_1", 1).Return(200, nil)
	mockRepo.On("SaveDelivery", mock.Anything, "shard_a", mock.Anything).Return(nil)
	mockRepo.On("RecordEvent", mock.Anything, "shard_a", mock.Anything, domain.EventTypeWebhookDelivered, mock.Anything, mock.Anything, mock.Anything).Return(nil)

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

	mockRepo.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}
