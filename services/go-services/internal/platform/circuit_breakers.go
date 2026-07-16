package platform

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/segmentio/kafka-go"
	"github.com/sony/gobreaker"
)

// IsTerminalFunc classifies business errors so the
// DB breaker registry treats them as pool-health successes, not failures.
type IsTerminalFunc func(err error) bool

// Circuit breaker pool identity strings, centralised for consistent metric labels.
const (
	CBNameMerchantsGlobal = "merchants-global"
	CBNameShardRWPrefix   = "shard-rw-"
	CBNameShardROPrefix   = "shard-ro-"
	CBNameKafkaEgress     = "kafka-egress"
)

// cbGauge values for breaker state metrics: 0=closed, 1=half-open, 2=open.
const (
	cbGaugeClosed   int64 = 0
	cbGaugeHalfOpen int64 = 1
	cbGaugeOpen     int64 = 2
)

// --- Circuit Breaker ---
type CircuitBreakerConfig struct {
	Name          string
	MaxRequests   uint32
	Timeout       time.Duration
	Interval      time.Duration
	MaxFails      uint32
	MinRequests   uint32
	ErrorRate     float64
	IsSuccessful  func(error) bool
	OnStateChange func(name string, from gobreaker.State, to gobreaker.State)
}

// newCircuitBreaker creates a standardized gobreaker.CircuitBreaker for the platform.
func newCircuitBreaker(cfg CircuitBreakerConfig) *gobreaker.CircuitBreaker {
	return gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        cfg.Name,
		MaxRequests: cfg.MaxRequests, // Max requests in half-open state
		Interval:    cfg.Interval,    // how often the failure counters are reset while the circuit is Closed
		Timeout:     cfg.Timeout,     //
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			// Error rate is better for high-volume systems because it reflects overall service health.
			if cfg.ErrorRate > 0 {
				if counts.Requests >= cfg.MinRequests {
					failureRate := float64(counts.TotalFailures) / float64(counts.Requests)
					return failureRate >= cfg.ErrorRate
				}
				return false
			}
			// Consecutive failures is useful for low-traffic services.
			return counts.ConsecutiveFailures >= cfg.MaxFails
		},
		IsSuccessful: func(err error) bool {
			if err == nil {
				return true
			}
			if cfg.IsSuccessful != nil {
				return cfg.IsSuccessful(err)
			}
			return false
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			var stateVal int64
			switch to {
			case gobreaker.StateClosed:
				stateVal = 0
			case gobreaker.StateHalfOpen:
				stateVal = 1
			case gobreaker.StateOpen:
				stateVal = 2
			}
			RecordCircuitBreakerState(context.Background(), name, stateVal)

			if cfg.OnStateChange != nil {
				cfg.OnStateChange(name, from, to)
			}
		},
	})
}

// dbIsSuccessfulPolicy: terminal business errors + safe ACID transaction failures
// (e.g. deadlock, serialization) count as success (the DB is up and doing its job).
// Everything else (timeouts, pool exhaustion, network errors) is a pool-health failure.
func dbIsSuccessfulPolicy(isTerminal IsTerminalFunc) func(error) bool {
	return func(err error) bool {
		if err == nil {
			return true
		}
		if isTerminal != nil && isTerminal(err) {
			return true
		}

		// Timeouts (Go context) are signs of overload -> FAIL
		if errors.Is(err, context.DeadlineExceeded) {
			return false
		}

		// Network/socket level timeouts -> FAIL
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return false
		}

		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			switch pgErr.Code {
			case pgerrcode.DeadlockDetected, pgerrcode.SerializationFailure:
				// The DB successfully processed the query and protected ACID guarantees.
				// This is a healthy response.
				return true
			case pgerrcode.InsufficientResources, pgerrcode.TooManyConnections:
				// DB is overloaded/exhausted -> FAIL
				return false
			default:
				// For other PG structural errors (e.g., syntax error),
				// they are application faults, but the DB is healthy.
				// We don't trip the breaker for bad queries.
				return true
			}
		}

		// Connection drops / unhandled errors -> FAIL
		return false
	}
}

// kafkaEgressIsSuccessful: only returns success if the message was delivered.
// Any timeouts, network exceptions, or leader elections are failures.
func kafkaEgressIsSuccessful(err error) bool {
	if err == nil {
		return true
	}

	// If it's a context timeout, Kafka is overloaded -> FAIL
	if errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	var kErr kafka.Error
	if errors.As(err, &kErr) {
		// All Kafka network/timeout/temporary errors are signs of distress -> FAIL
		return false
	}

	return false
}

// DBCircuitBreakers implements the per-pool breaker registry: one instance
// per physical connection pool (merchants-global + one-per-shard RW + one-per-shard RO).
type DBCircuitBreakers struct {
	mu         sync.RWMutex
	merchants  *gobreaker.CircuitBreaker
	shardsRW   map[string]*gobreaker.CircuitBreaker
	shardsRO   map[string]*gobreaker.CircuitBreaker
	isTerminal IsTerminalFunc
	cbConfig   CircuitBreakerConfig // Base settings for these pools
}

// NewDBCircuitBreakers builds one breaker for the merchants pool, one per shard RW, and one per shard RO.
// All breakers are pre-created at startup for deterministic observability.
func NewDBCircuitBreakers(merchantPoolName string, shardIDs []string, isTerminal IsTerminalFunc, cbConfig CircuitBreakerConfig) *DBCircuitBreakers {
	reg := &DBCircuitBreakers{
		shardsRW:   make(map[string]*gobreaker.CircuitBreaker, len(shardIDs)),
		shardsRO:   make(map[string]*gobreaker.CircuitBreaker, len(shardIDs)),
		isTerminal: isTerminal,
		cbConfig:   cbConfig,
	}
	if merchantPoolName == "" {
		merchantPoolName = CBNameMerchantsGlobal
	}
	reg.merchants = reg.newDBBreaker(merchantPoolName)
	RecordCircuitBreakerState(context.Background(), merchantPoolName, cbGaugeClosed)
	for _, sid := range shardIDs {
		if sid == "" {
			continue
		}

		rwName := CBNameShardRWPrefix + sid
		reg.shardsRW[sid] = reg.newDBBreaker(rwName)
		RecordCircuitBreakerState(context.Background(), rwName, cbGaugeClosed)

		roName := CBNameShardROPrefix + sid
		reg.shardsRO[sid] = reg.newDBBreaker(roName)
		RecordCircuitBreakerState(context.Background(), roName, cbGaugeClosed)
	}
	return reg
}

// newDBBreaker builds a gobreaker with the shared DB success policy + metrics.
func (r *DBCircuitBreakers) newDBBreaker(name string) *gobreaker.CircuitBreaker {
	cfg := r.cbConfig
	cfg.Name = name
	cfg.IsSuccessful = dbIsSuccessfulPolicy(r.isTerminal)
	cfg.OnStateChange = recordBreakerStateChange
	return newCircuitBreaker(cfg)
}

// recordBreakerStateChange records state gauge and open/half-open counters.
func recordBreakerStateChange(name string, from gobreaker.State, to gobreaker.State) {
	var stateVal int64
	switch to {
	case gobreaker.StateClosed:
		stateVal = cbGaugeClosed
	case gobreaker.StateHalfOpen:
		stateVal = cbGaugeHalfOpen
	case gobreaker.StateOpen:
		stateVal = cbGaugeOpen
	}
	RecordCircuitBreakerState(context.Background(), name, stateVal)
	if to == gobreaker.StateOpen {
		RecordCircuitBreakerOpen(context.Background(), name)
		if from == gobreaker.StateHalfOpen {
			RecordCircuitBreakerHalfOpenFailure(context.Background(), name)
		}
	}
}

// Merchants returns the single breaker for the global merchants pool.
func (r *DBCircuitBreakers) Merchants() *gobreaker.CircuitBreaker {
	return r.merchants
}

// ShardRW returns the single breaker for the given shard's RW pool.
func (r *DBCircuitBreakers) ShardRW(shardID string) *gobreaker.CircuitBreaker {
	r.mu.RLock()
	cb, ok := r.shardsRW[shardID]
	r.mu.RUnlock()
	if ok {
		return cb
	}
	// Should not happen - all shards pre-created at startup
	r.mu.Lock()
	defer r.mu.Unlock()
	if cb, ok := r.shardsRW[shardID]; ok {
		return cb
	}
	cb = r.newDBBreaker(CBNameShardRWPrefix + shardID)
	r.shardsRW[shardID] = cb
	return cb
}

// ShardRO returns the single breaker for the given shard's RO pool.
func (r *DBCircuitBreakers) ShardRO(shardID string) *gobreaker.CircuitBreaker {
	r.mu.RLock()
	cb, ok := r.shardsRO[shardID]
	r.mu.RUnlock()
	if ok {
		return cb
	}
	// Should not happen - all shards pre-created at startup
	r.mu.Lock()
	defer r.mu.Unlock()
	if cb, ok := r.shardsRO[shardID]; ok {
		return cb
	}
	cb = r.newDBBreaker(CBNameShardROPrefix + shardID)
	r.shardsRO[shardID] = cb
	return cb
}

// KafkaCircuitBreaker wraps the single Kafka egress breaker for outbox-relay.
type KafkaCircuitBreaker struct {
	cb *gobreaker.CircuitBreaker
}

// NewKafkaCircuitBreaker returns the singleton Kafka egress breaker.
func NewKafkaCircuitBreaker(name string, cbConfig CircuitBreakerConfig) *KafkaCircuitBreaker {
	if name == "" {
		name = CBNameKafkaEgress
	}
	cbConfig.Name = name
	cbConfig.IsSuccessful = kafkaEgressIsSuccessful
	cbConfig.OnStateChange = recordBreakerStateChange

	RecordCircuitBreakerState(context.Background(), name, cbGaugeClosed)

	return &KafkaCircuitBreaker{
		cb: newCircuitBreaker(cbConfig),
	}
}

// Breaker returns the underlying gobreaker instance.
func (k *KafkaCircuitBreaker) Breaker() *gobreaker.CircuitBreaker { return k.cb }
