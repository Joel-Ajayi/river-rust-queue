package observability

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/kong-sync-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/kong-sync-worker/internal/core/port"
)

type MerchantDirectoryMetrics struct {
	inner port.MerchantDirectory
}

var _ port.MerchantDirectory = (*MerchantDirectoryMetrics)(nil)

func NewMerchantDirectoryMetrics(inner port.MerchantDirectory) *MerchantDirectoryMetrics {
	return &MerchantDirectoryMetrics{inner: inner}
}

func (m *MerchantDirectoryMetrics) GetActiveMerchants(ctx context.Context) ([]domain.Merchant, error) {
	merchants, err := m.inner.GetActiveMerchants(ctx)
	if err != nil {
		platform.RecordInfrastructureError(ctx, platform.ComponentMerchantDirectory)
	}
	return merchants, err
}
