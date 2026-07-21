package platform

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/retrypolicy"
)

// --- Exponential Backoff with Full Jitter ---

// RetryConfig defines the parameters for Exponential Backoff with Full Jitter
type RetryConfig struct {
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
}

// NewRetryPolicy builds a failsafe-go RetryPolicy using exponential backoff and jitter.
func NewRetryPolicy[T any](cfg RetryConfig, isTransient func(error) bool) retrypolicy.RetryPolicy[T] {
	builder := retrypolicy.Builder[T]().
		WithMaxRetries(cfg.MaxRetries).
		WithBackoff(cfg.BaseDelay, cfg.MaxDelay).
		WithJitterFactor(0.25)

	if isTransient != nil {
		builder.HandleIf(func(result T, err error) bool {
			return err != nil && isTransient(err)
		})
	}
	return builder.Build()
}

// ExecuteWithJitter executes the given function with Exponential Backoff and Full Jitter using failsafe-go.
func ExecuteWithJitter(ctx context.Context, cfg RetryConfig, fn func() error) error {
	policy := NewRetryPolicy[any](cfg, isTransientError)
	return failsafe.NewExecutor[any](policy).WithContext(ctx).Run(fn)
}

// CalculateJitterBackoff calculates the exponential backoff with full jitter for a given attempt.
func CalculateJitterBackoff(attempt int, baseDelay, maxDelay time.Duration) time.Duration {
	shiftAttempt := attempt
	if shiftAttempt > 30 {
		shiftAttempt = 30
	}

	backoff := baseDelay * (1 << shiftAttempt)
	if backoff > maxDelay || backoff <= 0 {
		backoff = maxDelay
	}

	// Equal Jitter: base backoff/2 + random(backoff/2)
	// Guarantees a minimum delay to prevent rapid hammering.
	half := backoff / 2
	jitter := time.Duration(0)
	if half > 0 {
		jitter = time.Duration(rand.Int64N(int64(half)))
	}

	jitterDelay := half + jitter

	if jitterDelay < baseDelay {
		jitterDelay = baseDelay
	}
	return jitterDelay
}
