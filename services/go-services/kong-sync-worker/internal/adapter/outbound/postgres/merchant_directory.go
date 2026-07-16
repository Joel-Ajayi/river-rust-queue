package postgres

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/kong-sync-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/kong-sync-worker/internal/core/port"
)

// MerchantDirectory reads the global merchants database.
type MerchantDirectory struct {
	pools *platform.ShardPools
}

// compile time interface implementation check
var _ port.MerchantDirectory = (*MerchantDirectory)(nil)

// NewMerchantDirectory builds the adapter over the shared connection pools.
func NewMerchantDirectory(pools *platform.ShardPools) *MerchantDirectory {
	return &MerchantDirectory{pools}
}

// GetActiveMerchants returns all active merchants.
func (md *MerchantDirectory) GetActiveMerchants(ctx context.Context) ([]domain.Merchant, error) {
	rows, err := md.pools.MerchantsPoolRO().Query(ctx, "SELECT id, tier FROM merchants WHERE status = $1", platform.MerchantStatusActive)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var merchants []domain.Merchant
	for rows.Next() {
		var id, tier string
		if err := rows.Scan(&id, &tier); err != nil {
			return nil, err
		}
		merchants = append(merchants, domain.Merchant{
			ID:     id,
			Tier:   tier,
			Status: platform.MerchantStatusActive,
		})
	}
	
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return merchants, nil
}
