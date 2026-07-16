package observability

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/kong-sync-worker/internal/core/port"
)

type KongGatewayMetrics struct {
	inner port.KongGateway
}

var _ port.KongGateway = (*KongGatewayMetrics)(nil)

func NewKongGatewayMetrics(inner port.KongGateway) *KongGatewayMetrics {
	return &KongGatewayMetrics{inner: inner}
}

func (m *KongGatewayMetrics) SyncConsumer(ctx context.Context, merchantID string) error {
	err := m.inner.SyncConsumer(ctx, merchantID)
	if err != nil {
		platform.RecordInfrastructureError(ctx, platform.ComponentKongGateway)
	}
	return err
}

func (m *KongGatewayMetrics) SyncRateLimitPlugin(ctx context.Context, merchantID, tier string) error {
	err := m.inner.SyncRateLimitPlugin(ctx, merchantID, tier)
	if err != nil {
		platform.RecordInfrastructureError(ctx, platform.ComponentKongGateway)
	}
	return err
}

func (m *KongGatewayMetrics) SyncJWTCredential(ctx context.Context, merchantID string) error {
	err := m.inner.SyncJWTCredential(ctx, merchantID)
	if err != nil {
		platform.RecordInfrastructureError(ctx, platform.ComponentKongGateway)
	}
	return err
}
