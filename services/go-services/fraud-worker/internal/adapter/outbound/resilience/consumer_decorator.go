package resilience

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/core/port"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/failsafe-go/failsafe-go"
	"go.uber.org/zap"
)

type consumerResilience struct {
	next     port.Consumer
	retryCfg platform.RetryConfig
	logger   *zap.Logger
}

func NewConsumerResilience(next port.Consumer, retryCfg platform.RetryConfig, logger *zap.Logger) port.Consumer {
	return &consumerResilience{
		next:     next,
		retryCfg: retryCfg,
		logger:   logger,
	}
}

func (c *consumerResilience) Consume(ctx context.Context) error {
	cfg := c.retryCfg
	cfg.MaxRetries = -1 // infinite restart loop
	return platform.ExecuteWithJitter(ctx, cfg, func(exec failsafe.Execution[any]) error {
		if err := c.next.Consume(ctx); err != nil && err != context.Canceled {
			c.logger.Error(platform.LogEventKafkaConsumerStopped, zap.Error(err))
			return err
		}
		return nil
	})
}
