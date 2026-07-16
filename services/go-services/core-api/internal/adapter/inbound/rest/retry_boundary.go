package rest

import (
	"context"
	"errors"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/sony/gobreaker"
)

// retryBoundaryConfig is the single retry budget for Flow A handlers.
// Sub-second base/max — long sleeps hold goroutines and amplify tail.
var retryBoundaryConfig = platform.RetryConfig{
	MaxRetries: 3,
	BaseDelay:  10 * time.Millisecond,
	MaxDelay:   200 * time.Millisecond,
}

// retryBoundary is the Single retry boundary for Flow A (Synchronous).
// fresh on each attempt (re-validate, re-resolve shard, re-evaluate CB).
func retryBoundary(ctx context.Context, fn func() error) error {
	return platform.ExecuteWithJitter(ctx, retryBoundaryConfig, fn)
}

// domain 503 sentinel. writeError then maps ErrServiceUnavailable → 503.
// Terminal/transient/infra errors pass through untouched.
func mapHTTPError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
		return domain.ErrServiceUnavailable
	}
	return err
}
