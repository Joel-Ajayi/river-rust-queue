package resilience

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/port"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/domain"
	"go.uber.org/zap"
)

type breakerEntry struct {
	cb         *platform.WebhookResilience
	lastAccess atomic.Int64
}

type BreakerRegistry struct {
	breakers sync.Map
	logger   *zap.Logger
	cfg      *platform.Config
}

func NewBreakerRegistry(logger *zap.Logger, cfg *platform.Config) *BreakerRegistry {
	return &BreakerRegistry{logger: logger, cfg: cfg}
}

func (r *BreakerRegistry) For(merchantID string) *platform.WebhookResilience {
	now := time.Now().Unix()
	if val, ok := r.breakers.Load(merchantID); ok {
		entry := val.(*breakerEntry)
		entry.lastAccess.Store(now)
		return entry.cb
	}

	capCfg := r.cfg.Capacity
	cfg := platform.CircuitBreakerConfig{
		Name:        domain.PrefixWebhook + merchantID,
		MaxRequests: uint32(capCfg.CBHalfOpenProbes),
		Interval:    time.Duration(capCfg.CBIntervalMs) * time.Millisecond,
		Timeout:     time.Duration(capCfg.CBTimeoutMs) * time.Millisecond,
		MaxFails:    uint32(capCfg.CBMaxFails),
	}

	maxConc := capCfg.WebhookMaxConcurrency
	if maxConc <= 0 {
		maxConc = domain.WebhookMaxConcurrency // sensible default
	}
	newCB := platform.NewWebhookResilience(domain.PrefixWebhook+merchantID, uint(maxConc), domain.IsTerminalError, cfg, r.logger)
	entry := &breakerEntry{cb: newCB}
	entry.lastAccess.Store(now)

	actualVal, _ := r.breakers.LoadOrStore(merchantID, entry)
	actualEntry := actualVal.(*breakerEntry)
	actualEntry.lastAccess.Store(now)
	return actualEntry.cb
}

// CleanupEvicted removes circuit breakers that have not been accessed for longer than maxIdle.
func (r *BreakerRegistry) CleanupEvicted(maxIdle time.Duration) {
	cutoff := time.Now().Add(-maxIdle).Unix()
	r.breakers.Range(func(key, value any) bool {
		entry := value.(*breakerEntry)
		if entry.lastAccess.Load() < cutoff && !entry.cb.IsOpen() {
			r.breakers.Delete(key)
		}
		return true
	})
}

// http client decorator that uses circuit breaker to\n
// prevent overwhelming a misbehaving merchant
type httpClientResilience struct {
	next     port.HTTPClient
	breakers *BreakerRegistry
}

func NewHTTPClientResilience(next port.HTTPClient, breakers *BreakerRegistry) port.HTTPClient {
	return &httpClientResilience{next: next, breakers: breakers}
}

func (c *httpClientResilience) Post(ctx context.Context, merchantID string, url string, payload []byte, signature, timestamp, eventID string, attempt int) (int, error) {
	breaker := c.breakers.For(merchantID)

	result, err := breaker.Execute(ctx, func() (interface{}, error) {
		return c.next.Post(ctx, merchantID, url, payload, signature, timestamp, eventID, attempt)
	})

	if err != nil {
		return 0, err
	}
	return result.(int), nil
}
