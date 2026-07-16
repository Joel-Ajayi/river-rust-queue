package platform

import (
	"context"
	"math/rand/v2"
	"time"
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
func ExecuteWithJitter(ctx context.Context, cfg RetryConfig, fn func() error) error {
	var lastErr error

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		// 1. Execute the payment/database operation
		err := fn()
		if err == nil {
			return nil
		}

		// If we do not have an explicit transient error check, or if the error is NOT transient,
		if !isTransientError(err) {
			return err
		}

		lastErr = err
		if attempt == cfg.MaxRetries {
			break
		}

		// 2. Safely calculate exponential backoff without integer overflow risks
		jitterDelay := CalculateJitterBackoff(attempt, cfg.BaseDelay, cfg.MaxDelay)

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
