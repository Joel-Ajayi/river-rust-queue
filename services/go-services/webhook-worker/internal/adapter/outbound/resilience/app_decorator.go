package resilience

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/port"
	"github.com/failsafe-go/failsafe-go"
	"go.uber.org/zap"
)

type webhookAppResilience struct {
	next     port.WebhookApp
	retryCfg platform.RetryConfig
	logger   *zap.Logger
}

func NewWebhookAppResilience(next port.WebhookApp, retryCfg platform.RetryConfig, logger *zap.Logger) port.WebhookApp {
	return &webhookAppResilience{
		next:     next,
		retryCfg: retryCfg,
		logger:   logger,
	}
}

func (a *webhookAppResilience) HandleMessage(ctx context.Context, merchantID string, topic string, key string, payload []byte) error {
	return a.next.HandleMessage(ctx, merchantID, topic, key, payload)
}

func (a *webhookAppResilience) RouteToGlobalDLQ(ctx context.Context, payload []byte, topic string, key string, errorMsg string) error {
	return a.next.RouteToGlobalDLQ(ctx, payload, topic, key, errorMsg)
}

func (a *webhookAppResilience) RetryScheduler(ctx context.Context) error {
	cfg := a.retryCfg
	cfg.MaxRetries = -1
	return platform.ExecuteWithJitter(ctx, cfg, func(exec failsafe.Execution[any]) error {
		if err := a.next.RetryScheduler(ctx); err != nil && err != context.Canceled {
			a.logger.Error(platform.LogEventRetrySchedulerStopped, zap.Error(err))
			return err
		}
		return nil
	})
}
