package platform

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

var (
	ErrUnknownShard       = errors.New("unknown shard")
	ErrCBMerchantsOpen    = errors.New("merchants circuit breaker is open")
	ErrCBRWOpen           = errors.New("shard RW circuit breaker is open")
	ErrCBROpen            = errors.New("shard RO circuit breaker is open")
	ErrReconciliationHeld = errors.New("reconciliation lock is already held by another runner")
)

// ShardPools manages pgx connection pools keyed by shard ID.
type ShardPools struct {
	merchants   *pgxpool.Pool
	roMerchants *pgxpool.Pool
	shards      map[string]*pgxpool.Pool
	roShards    map[string]*pgxpool.Pool
	hashRing    *HashRing
	mu          sync.RWMutex
}

const (
	DBMaxConnIdleTime   = 10 * time.Minute
	DBMaxConnLifetime   = 1 * time.Hour
	MerchantsDBMaxConns = 5
	MerchantsROMaxConns = 2
	ShardRWMaxConns     = 20
	ShardROMaxConns     = 5
	DBHostRWSuffix      = "-rw"
	DBHostROSuffix      = "-ro"
	DefaultShardLetters = "ABCDE"
)

func createPool(ctx context.Context, uri string, maxConns int32) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(uri)
	if err != nil {
		return nil, err
	}
	config.MaxConns = maxConns
	config.MaxConnIdleTime = DBMaxConnIdleTime
	config.MaxConnLifetime = DBMaxConnLifetime
	return pgxpool.NewWithConfig(ctx, config)
}

// NewShardPools creates pools for the global merchants DB and each shard.
func NewShardPools(ctx context.Context, cfg *Config, log *zap.Logger) (*ShardPools, error) {
	pgLog := log.Named(LogComponentPostgres)

	sp := &ShardPools{
		shards:   make(map[string]*pgxpool.Pool),
		roShards: make(map[string]*pgxpool.Pool),
	}

	pool, err := createPool(ctx, cfg.MerchantsDBURI, MerchantsDBMaxConns)
	if err != nil {
		return nil, err
	}
	sp.merchants = pool

	roURI := cfg.MerchantsDBURI
	if u, err := url.Parse(cfg.MerchantsDBURI); err != nil {
		pgLog.Warn("failed to parse merchants DB URI for RO pool, falling back to RW URI", zap.String(LogFieldEvent, "merchants_ro_uri_parse_failed"), zap.Error(err))
	} else {
		u.Host = strings.Replace(u.Host, DBHostRWSuffix, DBHostROSuffix, 1)
		roURI = u.String()
	}
	roPool, err := createPool(ctx, roURI, MerchantsROMaxConns)
	if err != nil {
		sp.Close()
		return nil, err
	}
	sp.roMerchants = roPool
	pgLog.Info("Connected to merchants database", zap.String(LogFieldEvent, LogEventMerchantsDBConnected))

	for shardID, uri := range cfg.ShardURIs {
		shardLog := pgLog.With(zap.String(LogFieldShardID, shardID))

		// Read-Write Pool
		pool, err := createPool(ctx, uri, ShardRWMaxConns)
		if err != nil {
			sp.Close()
			return nil, err
		}
		sp.shards[shardID] = pool

		// Read-Only Pool (Zero Downtime Reads pattern)
		roURI := uri
		if u, err := url.Parse(uri); err != nil {
			shardLog.Warn("failed to parse shard URI for RO pool, falling back to RW URI", zap.String(LogFieldEvent, "shard_ro_uri_parse_failed"), zap.Error(err))
		} else {
			u.Host = strings.Replace(u.Host, DBHostRWSuffix, DBHostROSuffix, 1)
			roURI = u.String()
		}
		roPool, err := createPool(ctx, roURI, ShardROMaxConns)
		if err != nil {
			sp.Close()
			return nil, err
		}
		sp.roShards[shardID] = roPool

		shardLog.Info("Connected to database shard", zap.String(LogFieldEvent, LogEventShardDBConnected))
	}

	// Build consistent hash ring from configured shard IDs
	shardIDs := make([]string, 0, len(sp.shards))
	for sid := range sp.shards {
		shardIDs = append(shardIDs, sid)
	}
	sp.hashRing = NewHashRing(shardIDs, HashRingDefaultVNodes)

	return sp, nil
}

func (sp *ShardPools) MerchantsPool() *pgxpool.Pool   { return sp.merchants }
func (sp *ShardPools) MerchantsPoolRO() *pgxpool.Pool { return sp.roMerchants }
func (sp *ShardPools) HashRing() *HashRing            { return sp.hashRing }

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
		return nil, fmt.Errorf("%w: %q", ErrUnknownShard, shardID)
	}
	return pool, nil
}

// ShardPoolRO returns the Read-Only pool for the given shard ID.
func (sp *ShardPools) ShardPoolRO(shardID string) (*pgxpool.Pool, error) {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	pool, ok := sp.roShards[shardID]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownShard, shardID)
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
