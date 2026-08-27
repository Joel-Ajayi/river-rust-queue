package platform

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/bulkhead"
	"github.com/failsafe-go/failsafe-go/circuitbreaker"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"sync/atomic"
)

// CircuitBreaker wraps a failsafe circuit breaker.
type CircuitBreaker struct {
	cb        circuitbreaker.CircuitBreaker[any]
	latestCtx atomic.Pointer[context.Context]
}

func (c *CircuitBreaker) Execute(ctx context.Context, fn func() (any, error)) (any, error) {
	c.latestCtx.Store(&ctx)
	return failsafe.With[any](c.cb).Get(fn)
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
	CBNameRedis           = "redis"
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
	Logger        *zap.Logger
	MaxRequests   uint32
	Timeout       time.Duration
	Interval      time.Duration
	MaxFails      uint32
	MinRequests   uint32
	ErrorRate     float64
	IsSuccessful  func(error) bool
	OnStateChange func(ctx context.Context, logger *zap.Logger, name string, from circuitbreaker.State, to circuitbreaker.State)
}

// newCircuitBreaker creates a standardized failsafe-go CircuitBreaker for the platform.
func newCircuitBreaker(cfg CircuitBreakerConfig) *CircuitBreaker {
	c := &CircuitBreaker{}
	builder := circuitbreaker.NewBuilder[any]()

	switch {
	case cfg.ErrorRate > 0 && cfg.MinRequests > 0 && cfg.Interval > 0:
		builder.WithFailureRateThreshold(cfg.ErrorRate, uint(cfg.MinRequests), cfg.Interval)
	case cfg.ErrorRate > 0 && cfg.MinRequests > 0:
		builder.WithFailureThresholdRatio(uint(cfg.ErrorRate*float64(cfg.MinRequests)), uint(cfg.MinRequests))
	case cfg.MaxFails > 0:
		builder.WithFailureThreshold(uint(cfg.MaxFails))
	case cfg.MinRequests > 0:
		builder.WithFailureThresholdRatio(uint(0.50*float64(cfg.MinRequests)), uint(cfg.MinRequests))
	}

	if cfg.MaxRequests > 0 {
		builder.WithSuccessThreshold(uint(cfg.MaxRequests))
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
		if cfg.OnStateChange != nil {
			ctxPtr := c.latestCtx.Load()
			ctx := context.Background()
			if ctxPtr != nil {
				ctx = *ctxPtr
			}
			cfg.OnStateChange(ctx, cfg.Logger, cfg.Name, e.OldState, e.NewState)
		}
	})

	c.cb = builder.Build()
	return c
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

		// Caller went away, request aborted, etc. No info on DB health -> FAIL
		if errors.Is(err, context.Canceled) {
			return true
		}

		// Timeouts are signs of overload -> FAIL
		if errors.Is(err, context.DeadlineExceeded) {
			// Usually means the DB or network was too slow.
			return false
		}

		// Network/socket level timeouts -> FAIL
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return false
		}

		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			switch {
			case strings.HasPrefix(pgErr.Code, "08"):
				// Connection exception
				return false

			case pgErr.Code == pgerrcode.TooManyConnections:
				return false

			case pgErr.Code == pgerrcode.InsufficientResources:
				return false

			case pgErr.Code == pgerrcode.CannotConnectNow:
				return false

			case pgErr.Code == pgerrcode.OutOfMemory:
				return false

			case pgErr.Code == pgerrcode.DiskFull:
				return false

			case pgErr.Code == pgerrcode.SerializationFailure,
				pgErr.Code == pgerrcode.DeadlockDetected:
				return true

			default:
				// All query/data/application errors are healthy DB responses.
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

	// Caller went away, request aborted, etc. No info on DB health -> FAIL
	if errors.Is(err, context.Canceled) {
		return true
	}

	// Timeouts are signs of overload -> FAIL
	if errors.Is(err, context.DeadlineExceeded) {
		// Usually means the DB or network was too slow.
		return false
	}

	var kErr kafka.Error
	if errors.As(err, &kErr) {
		// All Kafka network/timeout/temporary errors are signs of distress -> FAIL
		return false
	}

	return false
}

// WebhookResilience wraps a Bulkhead and a CircuitBreaker to limit concurrency and fail fast.
type WebhookResilience struct {
	executor failsafe.Executor[any]
	cb       circuitbreaker.CircuitBreaker[any]
}

func (w *WebhookResilience) Execute(ctx context.Context, fn func() (any, error)) (any, error) {
	return w.executor.Get(fn)
}

func (w *WebhookResilience) IsOpen() bool {
	return w.cb.IsOpen()
}

func webhookIsSuccessfulPolicy(isTerminal func(error) bool) func(error) bool {
	return func(err error) bool {
		if err == nil {
			return true
		}
		class := ClassifyError(err, isTerminal)
		if class == ClassificationTerminal || class == ClassificationPoison {
			return true
		}
		return false
	}
}

// NewWebhookResilience creates a bulkhead and circuit breaker for webhook egress.
func NewWebhookResilience(name string, maxConcurrency uint, isTerminal func(error) bool, cbConfig CircuitBreakerConfig, logger *zap.Logger) *WebhookResilience {
	cbConfig.Name = name
	cbConfig.Logger = logger
	cbConfig.IsSuccessful = webhookIsSuccessfulPolicy(isTerminal)
	cbConfig.OnStateChange = recordBreakerStateChange

	cb := newCircuitBreaker(cbConfig).cb
	bh := bulkhead.New[any](maxConcurrency)

	return &WebhookResilience{
		executor: failsafe.With[any](bh, cb),
		cb:       cb,
	}
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
func NewDBCircuitBreakers(merchantPoolName string, shardIDs []string, isTerminal IsTerminalFunc, cbConfig CircuitBreakerConfig, logger *zap.Logger) *DBCircuitBreakers {
	cbConfig.Logger = logger
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
func recordBreakerStateChange(ctx context.Context, logger *zap.Logger, name string, from circuitbreaker.State, to circuitbreaker.State) {
	var stateVal int64
	switch to {
	case circuitbreaker.ClosedState:
		stateVal = CBGaugeClosed
	case circuitbreaker.HalfOpenState:
		stateVal = CBGaugeHalfOpen
	case circuitbreaker.OpenState:
		stateVal = CBGaugeOpen
	}
	if to == circuitbreaker.OpenState && logger != nil {

	}
	RecordCircuitBreakerState(ctx, name, stateVal)
	if to == circuitbreaker.OpenState {
		RecordCircuitBreakerOpen(ctx, name)
		if from == circuitbreaker.HalfOpenState {
			RecordCircuitBreakerHalfOpenFailure(ctx, name)
		}
		if logger != nil {
			logger.Warn("Circuit breaker opened", zap.String(LogFieldComponent, "circuit-breaker"), zap.String("circuit_breaker_name", name))
		}
	} else if to == circuitbreaker.ClosedState && from != circuitbreaker.ClosedState {
		if logger != nil {
			logger.Info("Circuit breaker closed", zap.String(LogFieldComponent, "circuit-breaker"), zap.String("circuit_breaker_name", name))
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
	// Lazy-create path: emit the initial state metric so the breaker is
	// visible on the dashboard from creation (see issue 37). This should
	// only happen for shards added after startup.
	r.mu.Lock()
	defer r.mu.Unlock()
	if cb, ok := r.shardsRW[shardID]; ok {
		return cb
	}
	cb = r.newDBBreaker(CBNameShardRWPrefix + shardID)
	r.shardsRW[shardID] = cb
	RecordCircuitBreakerState(context.Background(), CBNameShardRWPrefix+shardID, 0)
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

// redisIsSuccessful: velocity counters are pure infrastructure — there are no
// business errors to excuse.
func redisIsSuccessful(err error) bool {
	if err == nil {
		return true
	}

	if errors.Is(err, context.Canceled) {
		return true
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return false
	}

	return false
}

// RedisCircuitBreaker wraps the single Redis breaker shared by all velocity calls.
type RedisCircuitBreaker struct {
	cb *CircuitBreaker
}

// NewRedisCircuitBreaker returns the singleton Redis breaker.
func NewRedisCircuitBreaker(name string, cbConfig CircuitBreakerConfig, logger *zap.Logger) *RedisCircuitBreaker {
	if name == "" {
		name = CBNameRedis
	}
	cbConfig.Name = name
	cbConfig.Logger = logger
	cbConfig.IsSuccessful = redisIsSuccessful
	cbConfig.OnStateChange = recordBreakerStateChange

	RecordCircuitBreakerState(context.Background(), name, CBGaugeClosed)

	return &RedisCircuitBreaker{
		cb: newCircuitBreaker(cbConfig),
	}
}

// Execute runs fn guarded by the Redis breaker. While the breaker is open,
// calls fail fast without touching the Redis client.
func (r *RedisCircuitBreaker) Execute(ctx context.Context, fn func() (any, error)) (any, error) {
	return r.cb.Execute(ctx, fn)
}

// Breaker returns the underlying failsafe-go circuitbreaker instance.
func (r *RedisCircuitBreaker) Breaker() *CircuitBreaker { return r.cb }
