package postgres

import (
	"context"
	"errors"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/port"
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
	pool, err := d.pools.ShardPoolRO(shardID)
	if err != nil {
		return err
	}

	var ownerID string
	err = pool.QueryRow(ctx, "SELECT merchant_id FROM wallets WHERE id = $1", walletID).Scan(&ownerID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrWalletNotFound
		}
		return err
	}

	if ownerID != merchantID {
		return domain.ErrWalletNotOwned
	}

	return nil
}

func (d *WalletDirectory) GetBalance(ctx context.Context, shardID, walletID string) (int64, string, error) {
	// Zero-Downtime Reads: Use the read-only pool pointing to <cluster-name>-ro
	pool, err := d.pools.ShardPoolRO(shardID)
	if err != nil {
		return 0, "", err
	}

	var balance int64
	var currency string

	err = pool.QueryRow(ctx, `
		SELECT COALESCE(wbc.balance, 0::bigint), w.currency 
		FROM wallets w 
		LEFT JOIN wallet_balance_cache wbc ON w.id = wbc.wallet_id 
		WHERE w.id = $1`, walletID).Scan(&balance, &currency)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, "", domain.ErrWalletNotFound
		}
		return 0, "", err
	}

	return balance, currency, nil
}
