package resilience

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/kong-sync-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/kong-sync-worker/internal/core/port"
)

type MerchantDirectoryCB struct {
	inner port.MerchantDirectory
	cbs   *platform.DBCircuitBreakers
}

var _ port.MerchantDirectory = (*MerchantDirectoryCB)(nil)

func NewMerchantDirectoryCB(inner port.MerchantDirectory, cbs *platform.DBCircuitBreakers) *MerchantDirectoryCB {
	return &MerchantDirectoryCB{inner: inner, cbs: cbs}
}

func (cb *MerchantDirectoryCB) GetActiveMerchants(ctx context.Context) ([]domain.Merchant, error) {
	res, err := cb.cbs.Merchants().Execute(func() (interface{}, error) {
		return cb.inner.GetActiveMerchants(ctx)
	})
	if err != nil {
		return nil, err
	}
	return res.([]domain.Merchant), nil
}
