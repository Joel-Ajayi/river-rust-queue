package platform

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/circuitbreaker"
	"github.com/failsafe-go/failsafe-go/retrypolicy"
)

var ErrRetryBudgetExhausted = errors.New("retry budget exhausted")

// RetryBudget implements a Token Bucket Retry Budget.
// When the budget is exhausted during an outage, retries fail fast to prevent load amplification.
type RetryBudget struct {
	mu        sync.Mutex
	tokens    int64
	minTokens int64
	maxTokens int64
	ratio     int64 // successes needed to earn 1 token (e.g. 1/0.10 = 10)
	count     int64 // success counter
}

// NewRetryBudget creates a volume-derived RetryBudget with min baseline floor, max ceiling, and budget fraction.
func NewRetryBudget(minTokens, maxTokens int64, budgetFraction float64) *RetryBudget {
	if minTokens < 1 {
		minTokens = 2
	}
	if maxTokens < minTokens {
		maxTokens = 100
	}
	ratio := int64(10)
	if budgetFraction > 0 {
		ratio = int64(math.Ceil(1.0 / budgetFraction))
	}
	if ratio < 1 {
		ratio = 1
	}
	return &RetryBudget{
		tokens:    minTokens,
		minTokens: minTokens,
		maxTokens: maxTokens,
		ratio:     ratio,
	}
}

// RecordSuccess deposits a token into the bucket once ratio successes accumulate.
func (b *RetryBudget) RecordSuccess() {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.count++
	if b.count >= b.ratio {
		b.count = 0
		if b.tokens < b.maxTokens {
			b.tokens++
		}
	}
}

// TryAcquire attempts to spend 1 retry token. Returns true if granted, false if budget is exhausted.
func (b *RetryBudget) TryAcquire() bool {
	if b == nil {
		return true // Default open if no budget specified
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.tokens > 0 {
		b.tokens--
		return true
	}
	return false
}

// Global default platform retry budget for services.
var defaultPlatformRetryBudget = NewRetryBudget(10, 100, 0.10)

// --- Exponential Backoff with Full Jitter ---

// RetryConfig defines the parameters for Exponential Backoff and Full Jitter
type RetryConfig struct {
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
	Budget     *RetryBudget
}

// DLQRetryConfig builds the per-service retry budget for durable DLQ writes from
// CapacityConfig, centralizing the DLQ-persistence retry policy in one place.
func DLQRetryConfig(c CapacityConfig) RetryConfig {
	return RetryConfig{
		MaxRetries: c.DLQMaxRetries,
		BaseDelay:  time.Duration(c.DLQBaseDelayMs) * time.Millisecond,
		MaxDelay:   time.Duration(c.DLQCapDelayMs) * time.Millisecond,
	}
}

// NewRetryPolicy builds a failsafe-go RetryPolicy using exponential backoff and Full Jitter.
func NewRetryPolicy[T any](cfg RetryConfig, isFailure func(error) bool) retrypolicy.RetryPolicy[T] {
	builder := retrypolicy.NewBuilder[T]().
		WithMaxRetries(cfg.MaxRetries).
		WithBackoff(cfg.BaseDelay, cfg.MaxDelay).
		WithJitterFactor(1.0)

	if isFailure != nil {
		builder.HandleIf(func(result T, err error) bool {
			return err != nil && isFailure(err)
		})
	}
	return builder.Build()
}

// ExecuteWithJitter executes the given function with Exponential Backoff, Full Jitter, and Token Bucket Retry Budget protection.
func ExecuteWithJitter(ctx context.Context, cfg RetryConfig, fn func(exec failsafe.Execution[any]) error) error {
	budget := cfg.Budget
	if budget == nil {
		budget = defaultPlatformRetryBudget
	}

	policy := NewRetryPolicy[any](cfg, func(err error) bool {
		if errors.Is(err, circuitbreaker.ErrOpen) {
			return false // Fail fast on open CB
		}
		if errors.Is(err, ErrConsumerFetchDeadline) {
			return false // Don't retry past the session deadline
		}
		// Enforce Token Bucket Retry Budget before permitting a retry attempt
		if !budget.TryAcquire() {
			RecordRetryBudgetExhausted(ctx) // B1: surface budget denial in rrq.retry.budget.exhausted
			return false                    // Retry budget exhausted -> fail fast to DLQ!
		}
		classification := ClassifyError(err, nil)
		return classification == ClassificationTransient || classification == ClassificationInfrastructure
	})

	err := failsafe.With[any](policy).WithContext(ctx).RunWithExecution(fn)
	if err == nil {
		budget.RecordSuccess()
	}
	return err
}
