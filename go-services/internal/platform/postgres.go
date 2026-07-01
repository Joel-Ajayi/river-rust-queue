package platform

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

const (
	// Merchant Statuses
	MerchantStatusActive = "active"
	MerchantStatusFrozen = "frozen"
	MerchantStatusClosed = "closed"

	// Merchant Shard Status
	ShardStatusActive    = "active"
	ShardStatusMigrating = "migrating"

	// Wallet Statuses
	WalletStatusActive = "active"
	WalletStatusFrozen = "frozen"
	WalletStatusClosed = "closed"
)

// ShardPools manages pgx connection pools keyed by shard ID.
type ShardPools struct {
	merchants *pgxpool.Pool
	shards    map[string]*pgxpool.Pool
	mu        sync.RWMutex
}

// NewShardPools creates pools for the global merchants DB and each shard.
func NewShardPools(ctx context.Context, cfg *Config, log *zap.Logger) (*ShardPools, error) {
	sp := &ShardPools{shards: make(map[string]*pgxpool.Pool)}

	pool, err := pgxpool.New(ctx, cfg.MerchantsDBURI)
	if err != nil {
		return nil, fmt.Errorf("merchants db pool: %w", err)
	}
	sp.merchants = pool
	log.Info("connected to merchants-db")

	for shardID, uri := range cfg.ShardURIs {
		pool, err := pgxpool.New(ctx, uri)
		if err != nil {
			sp.Close()
			return nil, fmt.Errorf("shard %s pool: %w", shardID, err)
		}
		sp.shards[shardID] = pool
		log.Info("connected to shard", zap.String("shard_id", shardID))
	}

	return sp, nil
}

func (sp *ShardPools) MerchantsPool() *pgxpool.Pool { return sp.merchants }

// ShardPool returns the pool for the given shard ID or an error if unknown.
func (sp *ShardPools) ShardPool(shardID string) (*pgxpool.Pool, error) {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	pool, ok := sp.shards[shardID]
	if !ok {
		return nil, fmt.Errorf("unknown shard: %q", shardID)
	}
	return pool, nil
}

// Ping verifies connectivity to all pools (readiness probe).
func (sp *ShardPools) Ping(ctx context.Context) error {
	if err := sp.merchants.Ping(ctx); err != nil {
		return fmt.Errorf("merchants db ping: %w", err)
	}
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	for id, pool := range sp.shards {
		if err := pool.Ping(ctx); err != nil {
			return fmt.Errorf("shard %s ping: %w", id, err)
		}
	}
	return nil
}

// Close shuts down all connection pools.
func (sp *ShardPools) Close() {
	if sp.merchants != nil {
		sp.merchants.Close()
	}
	sp.mu.Lock()
	defer sp.mu.Unlock()
	for _, pool := range sp.shards {
		pool.Close()
	}
}
