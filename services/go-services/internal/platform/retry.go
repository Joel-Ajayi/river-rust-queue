package platform

import (
	"context"
	"errors"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/circuitbreaker"
	"github.com/failsafe-go/failsafe-go/retrypolicy"
)

// --- Exponential Backoff with Full Jitter ---

// RetryConfig defines the parameters for Exponential Backoff and Full Jitter
type RetryConfig struct {
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
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

// ExecuteWithJitter executes the given function with Exponential Backoff and Full Jitter using failsafe-go.
func ExecuteWithJitter(ctx context.Context, cfg RetryConfig, fn func(exec failsafe.Execution[any]) error) error {
	policy := NewRetryPolicy[any](cfg, func(err error) bool {
		if errors.Is(err, circuitbreaker.ErrOpen) {
			return false // Fail fast on open CB
		}
		if errors.Is(err, ErrConsumerFetchDeadline) {
			return false // Don't retry past the session deadline
		}
		classification := ClassifyError(err, nil)
		return classification == ClassificationTransient || classification == ClassificationInfrastructure
	})

	return failsafe.With[any](policy).WithContext(ctx).RunWithExecution(fn)
}
