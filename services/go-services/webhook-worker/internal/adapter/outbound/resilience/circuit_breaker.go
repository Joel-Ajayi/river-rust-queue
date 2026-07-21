package resilience

import (
	"sync"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/domain"
)

type BreakerRegistry struct {
	breakers sync.Map
}

func NewBreakerRegistry() *BreakerRegistry {
	return &BreakerRegistry{}
}

func (r *BreakerRegistry) For(merchantID string) *platform.CircuitBreaker {
	if cb, ok := r.breakers.Load(merchantID); ok {
		return cb.(*platform.CircuitBreaker)
	}

	cfg := platform.CircuitBreakerConfig{
		Name:        domain.PrefixWebhook + merchantID,
		MaxRequests: domain.BreakerMaxRequests,
		Interval:    domain.BreakerResetWindow,
		Timeout:     domain.BreakerCooldown,
		MaxFails:    domain.BreakerConsecutiveFailures,
	}

	newCB := platform.NewDBCircuitBreakers(domain.PrefixWebhook+merchantID, nil, nil, cfg).Merchants()
	actualCB, _ := r.breakers.LoadOrStore(merchantID, newCB)
	return actualCB.(*platform.CircuitBreaker)
}
