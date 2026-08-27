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
	res, err := r.cbs.Merchants().Execute(ctx, func() (interface{}, error) {
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

func (r *repositoryResilience) CreatePendingDelivery(ctx context.Context, shardID string, d *domain.WebhookDelivery) error {
	_, err := r.cbs.ShardRW(shardID).Execute(ctx, func() (interface{}, error) {
		return nil, r.next.CreatePendingDelivery(ctx, shardID, d)
	})
	return err
}

func (r *repositoryResilience) CompleteDelivery(ctx context.Context, shardID string, d *domain.WebhookDelivery, successEventPayload []byte, eventID string) error {
	_, err := r.cbs.ShardRW(shardID).Execute(ctx, func() (interface{}, error) {
		return nil, r.next.CompleteDelivery(ctx, shardID, d, successEventPayload, eventID)
	})
	return err
}

func (r *repositoryResilience) FailDeliveryAndRouteToDLQ(ctx context.Context, shardID string, d *domain.WebhookDelivery, errorMsg string, failEventPayload []byte, eventID string, firstFailedAt time.Time, lastFailedAt time.Time) error {
	_, err := r.cbs.ShardRW(shardID).Execute(ctx, func() (interface{}, error) {
		return nil, r.next.FailDeliveryAndRouteToDLQ(ctx, shardID, d, errorMsg, failEventPayload, eventID, firstFailedAt, lastFailedAt)
	})
	return err
}

func (r *repositoryResilience) ScheduleRetry(ctx context.Context, shardID string, d *domain.WebhookDelivery, failEventPayload []byte, eventID string) error {
	_, err := r.cbs.ShardRW(shardID).Execute(ctx, func() (interface{}, error) {
		return nil, r.next.ScheduleRetry(ctx, shardID, d, failEventPayload, eventID)
	})
	return err
}

func (r *repositoryResilience) FetchPendingRetries(ctx context.Context, shardID string, limit int) ([]*domain.WebhookDelivery, error) {
	res, err := r.cbs.ShardRW(shardID).Execute(ctx, func() (interface{}, error) {
		return r.next.FetchPendingRetries(ctx, shardID, limit)
	})
	if err != nil {
		return nil, err
	}
	return res.([]*domain.WebhookDelivery), nil
}

func (r *repositoryResilience) GetAvailableShardIDs() []string {
	return r.next.GetAvailableShardIDs()
}

func (r *repositoryResilience) RouteToGlobalDLQ(ctx context.Context, payload []byte, topic string, key string, errorMsg string) error {
	// Write to global DLQ using the merchants circuit breaker
	_, err := r.cbs.Merchants().Execute(ctx, func() (interface{}, error) {
		return nil, r.next.RouteToGlobalDLQ(ctx, payload, topic, key, errorMsg)
	})
	return err
}
