package app_test

import (
	"context"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/adapter/outbound/resilience"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/domain"
	"github.com/stretchr/testify/mock"
)

func newMockBreakerRegistry() *resilience.BreakerRegistry {
	reg := resilience.NewBreakerRegistry()
	_ = reg.For("test-merchant")
	return reg
}

type MockRepository struct{ mock.Mock }

func (m *MockRepository) GetMerchantConfig(ctx context.Context, merchantID string) (*domain.Merchant, error) {
	args := m.Called(ctx, merchantID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*domain.Merchant), args.Error(1)
}

func (m *MockRepository) SaveDelivery(ctx context.Context, shardID string, d *domain.WebhookDelivery) error {
	args := m.Called(ctx, shardID, d)
	return args.Error(0)
}

func (m *MockRepository) FetchPendingRetries(ctx context.Context, shardID string, limit int) ([]*domain.WebhookDelivery, error) {
	args := m.Called(ctx, shardID, limit)
	return args.Get(0).([]*domain.WebhookDelivery), args.Error(1)
}

func (m *MockRepository) RecordEvent(ctx context.Context, shardID string, eventID, eventType, aggregateType, aggregateID string, payload []byte) error {
	args := m.Called(ctx, shardID, eventID, eventType, aggregateType, aggregateID, payload)
	return args.Error(0)
}

func (m *MockRepository) RouteToDLQ(ctx context.Context, shardID string, source string, payload []byte, errorMsg string, attemptCount int, firstFailedAt, lastFailedAt time.Time) error {
	args := m.Called(ctx, shardID, source, payload, errorMsg, attemptCount, firstFailedAt, lastFailedAt)
	return args.Error(0)
}

func (m *MockRepository) GetAvailableShardIDs() []string {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).([]string)
}

type MockHTTPClient struct{ mock.Mock }

func (m *MockHTTPClient) Post(ctx context.Context, url string, payload []byte, signature, eventID string, attempt int) (int, error) {
	args := m.Called(ctx, url, payload, signature, eventID, attempt)
	return args.Int(0), args.Error(1)
}
