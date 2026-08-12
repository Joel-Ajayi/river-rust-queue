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

func (m *repositoryMetrics) CreatePendingDelivery(ctx context.Context, shardID string, d *domain.WebhookDelivery) error {
	err := m.next.CreatePendingDelivery(ctx, shardID, d)
	if err != nil && platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure {
		platform.RecordInfrastructureError(ctx, platform.ComponentWebhookStore)
	}
	return err
}

func (m *repositoryMetrics) CompleteDelivery(ctx context.Context, shardID string, d *domain.WebhookDelivery, successEventPayload []byte, eventID string) error {
	err := m.next.CompleteDelivery(ctx, shardID, d, successEventPayload, eventID)
	if err != nil && platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure {
		platform.RecordInfrastructureError(ctx, platform.ComponentWebhookStore)
	}
	return err
}

func (m *repositoryMetrics) FailDeliveryAndRouteToDLQ(ctx context.Context, shardID string, d *domain.WebhookDelivery, errorMsg string, failEventPayload []byte, eventID string, firstFailedAt time.Time, lastFailedAt time.Time) error {
	err := m.next.FailDeliveryAndRouteToDLQ(ctx, shardID, d, errorMsg, failEventPayload, eventID, firstFailedAt, lastFailedAt)
	if err != nil && platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure {
		platform.RecordInfrastructureError(ctx, platform.ComponentDLQStore)
	}
	return err
}

func (m *repositoryMetrics) ScheduleRetry(ctx context.Context, shardID string, d *domain.WebhookDelivery, failEventPayload []byte, eventID string) error {
	err := m.next.ScheduleRetry(ctx, shardID, d, failEventPayload, eventID)
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

func (r *repositoryMetrics) GetAvailableShardIDs() []string {
	return r.next.GetAvailableShardIDs()
}

func (m *repositoryMetrics) RouteToGlobalDLQ(ctx context.Context, payload []byte, errorMsg string) error {
	err := m.next.RouteToGlobalDLQ(ctx, payload, errorMsg)
	if err != nil && platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure {
		platform.RecordInfrastructureError(ctx, platform.ComponentDLQStore)
	}
	return err
}

// -- WebhookApp Decorator --

type webhookAppMetrics struct {
	next port.WebhookApp
}

func NewWebhookAppMetrics(next port.WebhookApp) port.WebhookApp {
	return &webhookAppMetrics{next: next}
}

func (m *webhookAppMetrics) HandleMessage(ctx context.Context, merchantID string, payload []byte) error {
	err := m.next.HandleMessage(ctx, merchantID, payload)
	if err != nil && platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure {
		platform.RecordInfrastructureError(ctx, platform.ComponentWebhookHandler)
	}
	return err
}

func (w *webhookAppMetrics) RetryScheduler(ctx context.Context) error {
	return w.next.RetryScheduler(ctx)
}

func (w *webhookAppMetrics) RouteToGlobalDLQ(ctx context.Context, payload []byte, errorMsg string) error {
	return w.next.RouteToGlobalDLQ(ctx, payload, errorMsg)
}
