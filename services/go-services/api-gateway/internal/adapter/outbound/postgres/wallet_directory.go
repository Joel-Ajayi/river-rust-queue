package postgres

import (
	"context"
	"errors"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/port"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/jackc/pgx/v5"
)

type WalletDirectory struct {
	pools port.ShardPools
}

// compile time interface implementation check
var _ port.WalletDirectory = (*WalletDirectory)(nil)

func NewWalletDirectory(pools *platform.ShardPools) *WalletDirectory {
	return &WalletDirectory{pools: pools}
}

func (d *WalletDirectory) CheckWalletOwnership(ctx context.Context, shardID, walletID, merchantID string) error {
	pool, err := d.pools.ShardPool(shardID)
	if err != nil {
		return err
	}

	var ownerID string
	err = pool.QueryRow(ctx, "SELECT merchant_id FROM wallets WHERE id = $1", walletID).Scan(&ownerID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrWalletNotOwned
		}
		return err
	}

	if ownerID != merchantID {
		return domain.ErrWalletNotOwned
	}

	return nil
}
