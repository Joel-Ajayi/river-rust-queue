package rest

import (
	"context"
	"errors"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/circuitbreaker"
)

// retryBoundary is the Single retry boundary for Synchronous flow.
func (s *Server) retryBoundary(ctx context.Context, fn func(ctx context.Context, exec failsafe.Execution[any]) error) error {
	return platform.ExecuteWithJitter(ctx, s.retryConfig, func(exec failsafe.Execution[any]) error {
		attemptCtx, cancel := context.WithTimeout(ctx, s.attemptTimeout)
		defer cancel()
		return fn(attemptCtx, exec)
	})
}

// domain 503 sentinel. writeError then maps ErrServiceUnavailable → 503.
// Terminal/transient/infra errors pass through untouched.
func mapHTTPError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, circuitbreaker.ErrOpen) {
		return domain.ErrServiceUnavailable
	}
	return err
}
