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
	DBHostRWSuffix      = "-rw"
	DBHostROSuffix      = "-ro"
	DefaultShardLetters = "ABCDE"
)

func createPool(ctx context.Context, uri string, maxConns int32, maxIdleTime, maxLifetime time.Duration) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(uri)
	if err != nil {
		return nil, err
	}
	config.MaxConns = maxConns
	config.MaxConnIdleTime = maxIdleTime
	config.MaxConnLifetime = maxLifetime
	return pgxpool.NewWithConfig(ctx, config)
}

// NewShardPools creates pools for the global merchants DB and each shard.
func NewShardPools(ctx context.Context, cfg *Config, log *zap.Logger) (*ShardPools, error) {
	pgLog := log.Named(LogComponentPostgres)

	maxIdleTime := time.Duration(cfg.GlobalCapacity.PGConnMaxIdleTimeMs) * time.Millisecond
	maxLifetime := time.Duration(cfg.GlobalCapacity.PGConnMaxLifetimeMs) * time.Millisecond

	sp := &ShardPools{
		shards:   make(map[string]*pgxpool.Pool),
		roShards: make(map[string]*pgxpool.Pool),
	}

	// Per-pod merchants RW cap (engine-derived, per-service env var).
	// Falls back to a sensible default of 4 (1 baseline + 3 from workers).
	merchantsMaxConns := int32(cfg.Capacity.PGMerchantsRWMaxConns)
	if merchantsMaxConns <= 0 {
		merchantsMaxConns = int32(cfg.Capacity.DBPoolSize)
		if merchantsMaxConns <= 0 {
			merchantsMaxConns = 4
		}
	}

	pool, err := createPool(ctx, cfg.MerchantsDBURI, merchantsMaxConns, maxIdleTime, maxLifetime)
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

	merchantsROMaxConns := int32(cfg.GlobalCapacity.PGMerchantsROMaxConns)

	// Skip RO pool creation if the service does not request any RO conns
	// (avoids keeping idle connections/FDS open for nothing — issue 22).
	if merchantsROMaxConns > 0 {
		roPool, err := createPool(ctx, roURI, merchantsROMaxConns, maxIdleTime, maxLifetime)
		if err != nil {
			sp.Close()
			return nil, err
		}
		sp.roMerchants = roPool
	}
	pgLog.Info("Connected to merchants database", zap.String(LogFieldEvent, LogEventMerchantsDBConnected))

	for shardID, uri := range cfg.ShardURIs {
		shardLog := pgLog.With(zap.String(LogFieldShardID, shardID))

		// Per-pod per-shard RW cap from the engine (per-service env var map).
		// Falls back to the per-pod DBPoolSize if not set for this shard.
		shardRWMaxConns := int32(cfg.Capacity.DBPoolSize)
		if cap, ok := cfg.Capacity.PGShardRWMaxConns[shardID]; ok && cap > 0 {
			shardRWMaxConns = int32(cap)
		}

		// Read-Write Pool
		pool, err := createPool(ctx, uri, shardRWMaxConns, maxIdleTime, maxLifetime)
		if err != nil {
			sp.Close()
			return nil, err
		}
		sp.shards[shardID] = pool

		// Read-Only Pool (Zero Downtime Reads pattern).
		// Skipped entirely when the service requests zero RO conns to avoid
		// idle connections and FDs (see issue 22).
		shardROMaxConns := int32(cfg.GlobalCapacity.PGShardROMaxConns)
		if shardROMaxConns > 0 {
			roURI := uri
			if u, err := url.Parse(uri); err != nil {
				shardLog.Warn("failed to parse shard URI for RO pool, falling back to RW URI", zap.String(LogFieldEvent, "shard_ro_uri_parse_failed"), zap.Error(err))
			} else {
				u.Host = strings.Replace(u.Host, DBHostRWSuffix, DBHostROSuffix, 1)
				roURI = u.String()
			}
			roPool, err := createPool(ctx, roURI, shardROMaxConns, maxIdleTime, maxLifetime)
			if err != nil {
				sp.Close()
				return nil, err
			}
			sp.roShards[shardID] = roPool
		}

		shardLog.Info("Connected to database shard", zap.String(LogFieldEvent, LogEventShardDBConnected))
	}

	// Build consistent hash ring from configured shard IDs
	shardIDs := make([]string, 0, len(sp.shards))
	for sid := range sp.shards {
		shardIDs = append(shardIDs, sid)
	}
	sp.hashRing = NewHashRing(shardIDs, cfg.GlobalCapacity.KetamaVNodes)

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
// Returns ErrUnknownShard if the service has no RO pool configured
// (use_ro_pool == false in the service config).
func (sp *ShardPools) ShardPoolRO(shardID string) (*pgxpool.Pool, error) {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	pool, ok := sp.roShards[shardID]
	if !ok || pool == nil {
		return nil, fmt.Errorf("%w: %q (RO pool not provisioned for this service)", ErrUnknownShard, shardID)
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
