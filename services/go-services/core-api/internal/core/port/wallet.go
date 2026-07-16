package port

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

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
