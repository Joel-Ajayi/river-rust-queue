package observability

import (
	"context"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/port"
)

// -- Repository Decorator --

type repositoryMetrics struct {
	next port.Repository
}

func NewRepositoryMetrics(next port.Repository) port.Repository {
	return &repositoryMetrics{next: next}
}

func (m *repositoryMetrics) GetMerchantConfig(ctx context.Context, merchantID string) (*domain.Merchant, error) {
	merchant, err := m.next.GetMerchantConfig(ctx, merchantID)
	if err != nil && platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure {
		platform.RecordInfrastructureError(ctx, platform.ComponentMerchantDirectory) // Same DB conceptually
	}
	return merchant, err
}

func (m *repositoryMetrics) SaveDelivery(ctx context.Context, shardID string, d *domain.WebhookDelivery) error {
	err := m.next.SaveDelivery(ctx, shardID, d)
	if err != nil && platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure {
		platform.RecordInfrastructureError(ctx, platform.ComponentWebhookStore)
	}
	return err
}

func (m *repositoryMetrics) FetchPendingRetries(ctx context.Context, shardID string, limit int) ([]*domain.WebhookDelivery, error) {
	deliveries, err := m.next.FetchPendingRetries(ctx, shardID, limit)
	if err != nil && platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure {
		platform.RecordInfrastructureError(ctx, platform.ComponentWebhookStore)
	}
	return deliveries, err
}

func (m *repositoryMetrics) RecordEvent(ctx context.Context, shardID string, eventID, eventType, aggregateType, aggregateID string, payload []byte) error {
	err := m.next.RecordEvent(ctx, shardID, eventID, eventType, aggregateType, aggregateID, payload)
	if err != nil && platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure {
		platform.RecordInfrastructureError(ctx, platform.ComponentWebhookStore)
	}
	return err
}

func (m *repositoryMetrics) RouteToDLQ(ctx context.Context, shardID string, source string, payload []byte, errorMsg string, attemptCount int, firstFailedAt, lastFailedAt time.Time) error {
	err := m.next.RouteToDLQ(ctx, shardID, source, payload, errorMsg, attemptCount, firstFailedAt, lastFailedAt)
	if err != nil && platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure {
		platform.RecordInfrastructureError(ctx, platform.ComponentDLQStore)
	}
	return err
}

func (m *repositoryMetrics) GetAvailableShardIDs() []string {
	return m.next.GetAvailableShardIDs()
}

// -- WebhookApp Decorator --

type webhookAppMetrics struct {
	next port.WebhookApp
}

func NewWebhookAppMetrics(next port.WebhookApp) port.WebhookApp {
	return &webhookAppMetrics{next: next}
}

func (m *webhookAppMetrics) HandleMessage(ctx context.Context, merchantID string, payload []byte) error {
	start := time.Now()
	err := m.next.HandleMessage(ctx, merchantID, payload)
	duration := time.Since(start)

	platform.RecordConsumerMsgDuration(ctx, platform.TopicNotify, duration)
	if err != nil && platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure {
		platform.RecordInfrastructureError(ctx, platform.ComponentWebhookHandler)
	}
	return err
}
