package port

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

// WalletUseCase is a driving port for wallet actions.
type WalletUseCase interface {
	CreateWallet(ctx context.Context, merchantID, currency string) (string, error)
	Deposit(ctx context.Context, t domain.Transfer, idempKey string) (domain.SubmitResult, error)
}

// -- Outgoing ports --

// WalletDirectory is a driven port for verifying wallet information on a shard.
type WalletDirectory interface {
	CheckWalletOwnership(ctx context.Context, shardID, walletID, merchantID string) error
	GetBalance(ctx context.Context, shardID, walletID string) (int64, string, error)
}

// ShardPools is driven port for looking up pool for the given shard ID or an error if unknown.
type ShardPools interface {
	ShardPool(shardId string) (*pgxpool.Pool, error)
	ShardPoolRO(shardId string) (*pgxpool.Pool, error)
	AllShardPools() map[string]*pgxpool.Pool
}

// WalletStore is a driven port for wallet persistence and queries on shards.
type WalletStore interface {
	CreateWallet(ctx context.Context, shardID, walletID, merchantID, currency, walletType string) error
	FindFiatVault(ctx context.Context, shardID, currency string) (string, error)
}
