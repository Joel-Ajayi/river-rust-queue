package platform

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/circuitbreaker"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/segmentio/kafka-go"
)

// CircuitBreaker wraps a failsafe-go circuitbreaker.CircuitBreaker[any] with helper methods.
type CircuitBreaker struct {
	cb circuitbreaker.CircuitBreaker[any]
}

func (c *CircuitBreaker) Execute(fn func() (any, error)) (any, error) {
	return failsafe.NewExecutor[any](c.cb).Get(fn)
}

func (c *CircuitBreaker) ExecuteVoid(fn func() error) error {
	return failsafe.NewExecutor[any](c.cb).Run(fn)
}

func (c *CircuitBreaker) State() circuitbreaker.State {
	return c.cb.State()
}

func (c *CircuitBreaker) IsOpen() bool {
	return c.cb.IsOpen()
}

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

// CBGauge values for breaker state metrics: 0=closed, 1=half-open, 2=open.
const (
	CBGaugeClosed   int64 = 0
	CBGaugeHalfOpen int64 = 1
	CBGaugeOpen     int64 = 2
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
	OnStateChange func(name string, from circuitbreaker.State, to circuitbreaker.State)
}

// newCircuitBreaker creates a standardized failsafe-go CircuitBreaker for the platform.
func newCircuitBreaker(cfg CircuitBreakerConfig) *CircuitBreaker {
	builder := circuitbreaker.Builder[any]()

	if cfg.MaxFails > 0 {
		builder.WithFailureThreshold(uint(cfg.MaxFails))
	} else if cfg.MinRequests > 0 {
		builder.WithFailureThresholdRatio(uint(cfg.MinRequests), 10)
	}

	if cfg.Timeout > 0 {
		builder.WithDelay(cfg.Timeout)
	}

	if cfg.IsSuccessful != nil {
		builder.HandleIf(func(result any, err error) bool {
			return err != nil && !cfg.IsSuccessful(err)
		})
	}

	builder.OnStateChanged(func(e circuitbreaker.StateChangedEvent) {
		var stateVal int64
		switch e.NewState {
		case circuitbreaker.ClosedState:
			stateVal = CBGaugeClosed
		case circuitbreaker.HalfOpenState:
			stateVal = CBGaugeHalfOpen
		case circuitbreaker.OpenState:
			stateVal = CBGaugeOpen
		}
		RecordCircuitBreakerState(context.Background(), cfg.Name, stateVal)

		if cfg.OnStateChange != nil {
			cfg.OnStateChange(cfg.Name, e.OldState, e.NewState)
		}
	})

	return &CircuitBreaker{cb: builder.Build()}
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
	merchants  *CircuitBreaker
	shardsRW   map[string]*CircuitBreaker
	shardsRO   map[string]*CircuitBreaker
	isTerminal IsTerminalFunc
	cbConfig   CircuitBreakerConfig // Base settings for these pools
}

// NewDBCircuitBreakers builds one breaker for the merchants pool, one per shard RW, and one per shard RO.
// All breakers are pre-created at startup for deterministic observability.
func NewDBCircuitBreakers(merchantPoolName string, shardIDs []string, isTerminal IsTerminalFunc, cbConfig CircuitBreakerConfig) *DBCircuitBreakers {
	reg := &DBCircuitBreakers{
		shardsRW:   make(map[string]*CircuitBreaker, len(shardIDs)),
		shardsRO:   make(map[string]*CircuitBreaker, len(shardIDs)),
		isTerminal: isTerminal,
		cbConfig:   cbConfig,
	}
	if merchantPoolName == "" {
		merchantPoolName = CBNameMerchantsGlobal
	}
	reg.merchants = reg.newDBBreaker(merchantPoolName)
	RecordCircuitBreakerState(context.Background(), merchantPoolName, CBGaugeClosed)
	for _, sid := range shardIDs {
		if sid == "" {
			continue
		}

		rwName := CBNameShardRWPrefix + sid
		reg.shardsRW[sid] = reg.newDBBreaker(rwName)
		RecordCircuitBreakerState(context.Background(), rwName, CBGaugeClosed)

		roName := CBNameShardROPrefix + sid
		reg.shardsRO[sid] = reg.newDBBreaker(roName)
		RecordCircuitBreakerState(context.Background(), roName, CBGaugeClosed)
	}
	return reg
}

// newDBBreaker builds a circuitbreaker with the shared DB success policy + metrics.
func (r *DBCircuitBreakers) newDBBreaker(name string) *CircuitBreaker {
	cfg := r.cbConfig
	cfg.Name = name
	cfg.IsSuccessful = dbIsSuccessfulPolicy(r.isTerminal)
	cfg.OnStateChange = recordBreakerStateChange
	return newCircuitBreaker(cfg)
}

// recordBreakerStateChange records state gauge and open/half-open counters.
func recordBreakerStateChange(name string, from circuitbreaker.State, to circuitbreaker.State) {
	var stateVal int64
	switch to {
	case circuitbreaker.ClosedState:
		stateVal = CBGaugeClosed
	case circuitbreaker.HalfOpenState:
		stateVal = CBGaugeHalfOpen
	case circuitbreaker.OpenState:
		stateVal = CBGaugeOpen
	}
	RecordCircuitBreakerState(context.Background(), name, stateVal)
	if to == circuitbreaker.OpenState {
		RecordCircuitBreakerOpen(context.Background(), name)
		if from == circuitbreaker.HalfOpenState {
			RecordCircuitBreakerHalfOpenFailure(context.Background(), name)
		}
	}
}

// Merchants returns the single breaker for the global merchants pool.
func (r *DBCircuitBreakers) Merchants() *CircuitBreaker {
	return r.merchants
}

// ShardRW returns the single breaker for the given shard's RW pool.
func (r *DBCircuitBreakers) ShardRW(shardID string) *CircuitBreaker {
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
func (r *DBCircuitBreakers) ShardRO(shardID string) *CircuitBreaker {
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
	cb *CircuitBreaker
}

// NewKafkaCircuitBreaker returns the singleton Kafka egress breaker.
func NewKafkaCircuitBreaker(name string, cbConfig CircuitBreakerConfig) *KafkaCircuitBreaker {
	if name == "" {
		name = CBNameKafkaEgress
	}
	cbConfig.Name = name
	cbConfig.IsSuccessful = kafkaEgressIsSuccessful
	cbConfig.OnStateChange = recordBreakerStateChange

	RecordCircuitBreakerState(context.Background(), name, CBGaugeClosed)

	return &KafkaCircuitBreaker{
		cb: newCircuitBreaker(cbConfig),
	}
}

// Breaker returns the underlying failsafe-go circuitbreaker instance.
func (k *KafkaCircuitBreaker) Breaker() *CircuitBreaker { return k.cb }
