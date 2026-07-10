package platform

import (
	"context"
	"fmt"
	"net/url"
	"strings"
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
)

// ShardPools manages pgx connection pools keyed by shard ID.
type ShardPools struct {
	merchants   *pgxpool.Pool
	roMerchants *pgxpool.Pool
	shards      map[string]*pgxpool.Pool
	roShards  map[string]*pgxpool.Pool
	mu        sync.RWMutex
}

// NewShardPools creates pools for the global merchants DB and each shard.
func NewShardPools(ctx context.Context, cfg *Config, log *zap.Logger) (*ShardPools, error) {
	pgLog := log.Named(LogComponentPostgres)

	sp := &ShardPools{
		shards:   make(map[string]*pgxpool.Pool),
		roShards: make(map[string]*pgxpool.Pool),
	}

	pool, err := pgxpool.New(ctx, cfg.MerchantsDBURI)
	if err != nil {
		return nil, err
	}
	sp.merchants = pool

	roURI := cfg.MerchantsDBURI
	if u, err := url.Parse(cfg.MerchantsDBURI); err == nil {
		u.Host = strings.Replace(u.Host, "-rw", "-ro", 1)
		roURI = u.String()
	}
	roPool, err := pgxpool.New(ctx, roURI)
	if err != nil {
		sp.Close()
		return nil, err
	}
	sp.roMerchants = roPool
	pgLog.Info("Connected to merchants database", zap.String(LogFieldEvent, LogEventMerchantsDBConnected))

	for shardID, uri := range cfg.ShardURIs {
		shardLog := pgLog.With(zap.String(LogFieldShardID, shardID))

		// Read-Write Pool
		pool, err := pgxpool.New(ctx, uri)
		if err != nil {
			sp.Close()
			return nil, err
		}
		sp.shards[shardID] = pool

		// Read-Only Pool (Zero Downtime Reads pattern)
		roURI := uri
		if u, err := url.Parse(uri); err == nil {
			u.Host = strings.Replace(u.Host, "-rw", "-ro", 1)
			roURI = u.String()
		}
		roPool, err := pgxpool.New(ctx, roURI)
		if err != nil {
			sp.Close()
			return nil, err
		}
		sp.roShards[shardID] = roPool

		shardLog.Info("Connected to database shard", zap.String(LogFieldEvent, LogEventShardDBConnected))
	}

	return sp, nil
}

func (sp *ShardPools) MerchantsPool() *pgxpool.Pool { return sp.merchants }

func (sp *ShardPools) MerchantsPoolRO() *pgxpool.Pool { return sp.roMerchants }

func (sp *ShardPools) AllShardPools() map[string]*pgxpool.Pool {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	res := make(map[string]*pgxpool.Pool, len(sp.shards))
	for k, v := range sp.shards {
		res[k] = v
	}
	return res
}

// ShardPool returns the Read-Write pool for the given shard ID.
func (sp *ShardPools) ShardPool(shardID string) (*pgxpool.Pool, error) {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	pool, ok := sp.shards[shardID]
	if !ok {
		return nil, fmt.Errorf("unknown shard: %q", shardID)
	}
	return pool, nil
}

// ShardPoolRO returns the Read-Only pool for the given shard ID.
func (sp *ShardPools) ShardPoolRO(shardID string) (*pgxpool.Pool, error) {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	pool, ok := sp.roShards[shardID]
	if !ok {
		return nil, fmt.Errorf("unknown shard: %q", shardID)
	}
	return pool, nil
}

// Return Available Shard IDs
func (sp *ShardPools) GetAvailableShardIDs() []string {
	sp.mu.RLock()
	defer sp.mu.RUnlock()

	var shardIDs []string
	for shardID := range sp.shards {
		shardIDs = append(shardIDs, shardID)
	}
	return shardIDs
}

// Ping verifies connectivity to all pools (readiness probe).
func (sp *ShardPools) Ping(ctx context.Context) error {
	if err := sp.merchants.Ping(ctx); err != nil {
		return err
	}
	if sp.roMerchants != nil {
		if err := sp.roMerchants.Ping(ctx); err != nil {
			return err
		}
	}
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	for _, pool := range sp.shards {
		if err := pool.Ping(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Close shuts down all connection pools.
func (sp *ShardPools) Close() {
	if sp.merchants != nil {
		sp.merchants.Close()
	}
	if sp.roMerchants != nil {
		sp.roMerchants.Close()
	}
	sp.mu.Lock()
	defer sp.mu.Unlock()
	for _, pool := range sp.shards {
		pool.Close()
	}
	for _, pool := range sp.roShards {
		pool.Close()
	}
}
