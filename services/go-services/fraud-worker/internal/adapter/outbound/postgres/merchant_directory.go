package postgres

import (
	"context"
	"errors"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/core/port"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

type MerchantDirectory struct {
	pools  *platform.ShardPools
	logger *zap.Logger
}

var _ port.MerchantDirectory = (*MerchantDirectory)(nil)

func NewMerchantDirectory(pools *platform.ShardPools, logger *zap.Logger) *MerchantDirectory {
	return &MerchantDirectory{pools: pools, logger: logger}
}

func (md *MerchantDirectory) ShardFor(ctx context.Context, merchantID string) (string, error) {
	var shardID string
	err := md.pools.MerchantsPoolRO().QueryRow(
		ctx,
		`SELECT shard_id FROM merchants WHERE id = $1 AND status = $2`,
		merchantID, platform.MerchantStatusActive,
	).Scan(&shardID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", domain.ErrMerchantInactiveOrNotFound
		}
		return "", err
	}

	return shardID, nil
}
