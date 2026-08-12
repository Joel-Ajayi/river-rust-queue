package port

import (
	"context"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/domain"
	"github.com/segmentio/kafka-go"
)

type BreakerRegistry interface {
	For(merchantID string) *platform.WebhookResilience
}

type Repository interface {
	GetMerchantConfig(ctx context.Context, merchantID string) (*domain.Merchant, error)
	CreatePendingDelivery(ctx context.Context, shardID string, delivery *domain.WebhookDelivery) error
	CompleteDelivery(ctx context.Context, shardID string, delivery *domain.WebhookDelivery, successEventPayload []byte, eventID string) error
	FailDeliveryAndRouteToDLQ(ctx context.Context, shardID string, delivery *domain.WebhookDelivery, errorMsg string, failEventPayload []byte, eventID string, firstFailedAt time.Time, lastFailedAt time.Time) error
	ScheduleRetry(ctx context.Context, shardID string, delivery *domain.WebhookDelivery, failEventPayload []byte, eventID string) error
	FetchPendingRetries(ctx context.Context, shardID string, limit int) ([]*domain.WebhookDelivery, error)
	GetAvailableShardIDs() []string
	RouteToGlobalDLQ(ctx context.Context, payload []byte, errorMsg string) error
}

type WebhookApp interface {
	HandleMessage(ctx context.Context, merchantID string, payload []byte) error
	RetryScheduler(ctx context.Context) error
	RouteToGlobalDLQ(ctx context.Context, payload []byte, errorMsg string) error
}

type HTTPClient interface {
	Post(ctx context.Context, merchantID string, url string, payload []byte, signature, timestamp, eventID string, attempt int) (int, error)
}

type KafkaReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

type Consumer interface {
	Consume(ctx context.Context) error
}
