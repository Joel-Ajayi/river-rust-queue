package resilience

import (
	"context"
	"sync"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/domain"
	"github.com/sony/gobreaker"
)

type BreakerRegistry struct {
	breakers sync.Map
}

func NewBreakerRegistry() *BreakerRegistry {
	return &BreakerRegistry{}
}

func (r *BreakerRegistry) For(merchantID string) *gobreaker.CircuitBreaker {
	// Look up the existing breaker.
	if cb, ok := r.breakers.Load(merchantID); ok {
		return cb.(*gobreaker.CircuitBreaker)
	}

	// Create a new one.
	st := gobreaker.Settings{
		Name:          domain.PrefixWebhook + merchantID,
		MaxRequests:   domain.BreakerMaxRequests,
		Interval:      domain.BreakerResetWindow,
		Timeout:       domain.BreakerCooldown,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= domain.BreakerConsecutiveFailures
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			var stateVal int64
			switch to {
			case gobreaker.StateClosed:
				stateVal = platform.CBGaugeClosed
			case gobreaker.StateHalfOpen:
				stateVal = platform.CBGaugeHalfOpen
			case gobreaker.StateOpen:
				stateVal = platform.CBGaugeOpen
			}
			ctx := context.Background()
			platform.RecordCircuitBreakerState(ctx, name, stateVal)
			if to == gobreaker.StateOpen {
				platform.RecordCircuitBreakerOpen(ctx, name)
				if from == gobreaker.StateHalfOpen {
					platform.RecordCircuitBreakerHalfOpenFailure(ctx, name)
				}
			}
		},
	}
	
	newCB := gobreaker.NewCircuitBreaker(st)
	actualCB, _ := r.breakers.LoadOrStore(merchantID, newCB)
	return actualCB.(*gobreaker.CircuitBreaker)
}
