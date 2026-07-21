package port

import (
	"context"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/domain"
)

type Repository interface {
	GetMerchantConfig(ctx context.Context, merchantID string) (*domain.Merchant, error)
	SaveDelivery(ctx context.Context, shardID string, d *domain.WebhookDelivery) error
	FetchPendingRetries(ctx context.Context, shardID string, limit int) ([]*domain.WebhookDelivery, error)
	RecordEvent(ctx context.Context, shardID string, eventID, eventType, aggregateType, aggregateID string, payload []byte) error
	RouteToDLQ(ctx context.Context, shardID string, source string, payload []byte, errorMsg string, attemptCount int, firstFailedAt, lastFailedAt time.Time) error
	GetAvailableShardIDs() []string
}

type WebhookApp interface {
	HandleMessage(ctx context.Context, merchantID string, payload []byte) error
}

type HTTPClient interface {
	Post(ctx context.Context, url string, payload []byte, signature, eventID string, attempt int) (int, error)
}
