package resilience

import (
	"context"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/port"
)

type repositoryResilience struct {
	next port.Repository
	cbs  *platform.DBCircuitBreakers
}

func NewRepositoryResilience(next port.Repository, cbs *platform.DBCircuitBreakers) port.Repository {
	return &repositoryResilience{next: next, cbs: cbs}
}

func (r *repositoryResilience) GetMerchantConfig(ctx context.Context, merchantID string) (*domain.Merchant, error) {
	// Merchants are in the global DB, which has its own CB name
	res, err := r.cbs.Merchants().Execute(func() (interface{}, error) {
		return r.next.GetMerchantConfig(ctx, merchantID)
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	return res.(*domain.Merchant), nil
}

func (r *repositoryResilience) SaveDelivery(ctx context.Context, shardID string, d *domain.WebhookDelivery) error {
	_, err := r.cbs.ShardRW(shardID).Execute(func() (interface{}, error) {
		return nil, r.next.SaveDelivery(ctx, shardID, d)
	})
	return err
}

func (r *repositoryResilience) FetchPendingRetries(ctx context.Context, shardID string, limit int) ([]*domain.WebhookDelivery, error) {
	res, err := r.cbs.ShardRW(shardID).Execute(func() (interface{}, error) {
		return r.next.FetchPendingRetries(ctx, shardID, limit)
	})
	if err != nil {
		return nil, err
	}
	return res.([]*domain.WebhookDelivery), nil
}

func (r *repositoryResilience) RecordEvent(ctx context.Context, shardID string, eventID, eventType, aggregateType, aggregateID string, payload []byte) error {
	_, err := r.cbs.ShardRW(shardID).Execute(func() (interface{}, error) {
		return nil, r.next.RecordEvent(ctx, shardID, eventID, eventType, aggregateType, aggregateID, payload)
	})
	return err
}

func (r *repositoryResilience) RouteToDLQ(ctx context.Context, shardID string, source string, payload []byte, errorMsg string, attemptCount int, firstFailedAt, lastFailedAt time.Time) error {
	_, err := r.cbs.ShardRW(shardID).Execute(func() (interface{}, error) {
		return nil, r.next.RouteToDLQ(ctx, shardID, source, payload, errorMsg, attemptCount, firstFailedAt, lastFailedAt)
	})
	return err
}

func (r *repositoryResilience) GetAvailableShardIDs() []string {
	return r.next.GetAvailableShardIDs()
}
