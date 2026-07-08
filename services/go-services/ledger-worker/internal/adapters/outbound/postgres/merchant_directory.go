package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/ports"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
	"time"
)

// MerchantDirectory reads the global merchants database.
type MerchantDirectory struct {
	pools  *platform.ShardPools
	logger *zap.Logger
}

// compile time interface implementation check
var _ ports.MerchantDirectory = (*MerchantDirectory)(nil)

// NewMerchantDirectory builds the adapter over the shared connection pools.
func NewMerchantDirectory(pools *platform.ShardPools, logger *zap.Logger) *MerchantDirectory {
	return &MerchantDirectory{pools: pools, logger: logger}
}

// ShardFor returns the shard owning an active merchant.
func (md *MerchantDirectory) ShardFor(ctx context.Context, merchantID string) (string, error) {
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var shardID string
	err := md.pools.MerchantsPool().QueryRow(
		qCtx,
		`SELECT shard_id FROM merchants WHERE id = $1 AND status = $2`,
		merchantID, platform.MerchantStatusActive,
	).Scan(&shardID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", fmt.Errorf("merchant %q not found or not active", merchantID)
		}
		return "", err
	}

	return shardID, nil
}

