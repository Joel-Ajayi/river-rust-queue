package platform

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/sony/gobreaker"
)

// --- Exponential Backoff with Full Jitter ---

// RetryConfig defines the parameters for Exponential Backoff with Full Jitter
type RetryConfig struct {
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
}

// ExecuteWithJitter executes the given function with Exponential Backoff and Full Jitter (AWS Pattern).
// isTransientError is a function that returns true ONLY if the error is a mathematically safe, temporary condition (e.g. Deadlock).
func ExecuteWithJitter(ctx context.Context, cfg RetryConfig, isTransientError func(error) bool, fn func() error) error {
	var lastErr error

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		// 1. Execute the payment/database operation
		err := fn()
		if err == nil {
			return nil
		}

		// Strict Error Taxonomy: If we do not have an explicit transient error check, or if the error is NOT transient,
		// we must FAIL FAST immediately. We do NOT retry infrastructure outages (Connection Refused) or Business Errors.
		if isTransientError == nil || !isTransientError(err) {
			return err
		}

		// Record the transient infrastructure error for alerting
		RecordInfrastructureError(context.Background())

		lastErr = err
		if attempt == cfg.MaxRetries {
			break
		}

		// 2. Safely calculate exponential backoff without integer overflow risks
		shiftAttempt := attempt
		if shiftAttempt > 30 { // Cap the shift power to 30 to stay safely within int64 bounds
			shiftAttempt = 30
		}

		backoff := cfg.BaseDelay * (1 << shiftAttempt)
		if backoff > cfg.MaxDelay || backoff <= 0 {
			backoff = cfg.MaxDelay
		}

		// 3. Apply AWS Full Jitter pattern
		var jitterDelay time.Duration
		if backoff > 0 {
			// rand.Int64N handles concurrency scale perfectly without global mutexes
			jitterDelay = time.Duration(rand.Int64N(int64(backoff)))
		}

		// 4. Memory-safe non-leaking timer allocation
		if jitterDelay <= 0 {
			jitterDelay = cfg.BaseDelay
		}

		timer := time.NewTimer(jitterDelay)
		select {
		case <-ctx.Done():
			timer.Stop() // Clean up timer instantly to prevent memory leaks
			return ctx.Err()
		case <-timer.C:
			// Timer fired cleanly, continue loop to next attempt
		}
	}

	return lastErr
}

// --- Circuit Breaker ---
type CircuitBreakerConfig struct {
	Name          string
	MaxRequests   uint32
	Timeout       time.Duration
	MaxFails      uint32
	IsSuccessful  func(error) bool
	OnStateChange func(name string, from gobreaker.State, to gobreaker.State)
}

// NewCircuitBreaker creates a standardized gobreaker.CircuitBreaker for the platform.
func NewCircuitBreaker(cfg CircuitBreakerConfig) *gobreaker.CircuitBreaker {
	return gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        cfg.Name,
		MaxRequests: cfg.MaxRequests,
		Interval:    0,
		Timeout:     cfg.Timeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
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
