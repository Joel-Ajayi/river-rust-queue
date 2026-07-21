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

// compile time interface implementation checks.
var (
	_ port.WalletDirectory = (*WalletDirectory)(nil)
	_ port.WalletStore     = (*WalletDirectory)(nil)
)

func NewWalletDirectory(pools *platform.ShardPools) *WalletDirectory {
	return &WalletDirectory{pools: pools}
}

// CreateWallet creates a new customer or system wallet and initializes its balance cache.
func (d *WalletDirectory) CreateWallet(ctx context.Context, shardID, walletID, merchantID, currency, walletType string) error {
	pool, err := d.pools.ShardPool(shardID)
	if err != nil {
		return err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO wallets (id, merchant_id, currency, wallet_type, status)
		VALUES ($1, $2, $3, $4, $5)
	`, walletID, merchantID, currency, walletType, platform.WalletStatusActive)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO wallet_balance_cache (wallet_id, balance, last_entry_id, updated_at)
		VALUES ($1, 0, 0, NOW())
	`, walletID)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// FindFiatVault finds the platform fiat vault wallet on the specified shard.
func (d *WalletDirectory) FindFiatVault(ctx context.Context, shardID, currency string) (string, error) {
	pool, err := d.pools.ShardPoolRO(shardID)
	if err != nil {
		return "", err
	}

	var vaultID string
	err = pool.QueryRow(ctx, `
		SELECT id FROM wallets
		WHERE wallet_type = $1 AND currency = $2 AND status = $3
		LIMIT 1
	`, platform.WalletTypeFiatVault, currency, platform.WalletStatusActive).Scan(&vaultID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", domain.ErrWalletNotFound
		}
		return "", err
	}

	return vaultID, nil
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
