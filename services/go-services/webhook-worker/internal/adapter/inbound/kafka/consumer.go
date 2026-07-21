package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/port"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type Consumer struct {
	reader *kafka.Reader
	app    port.WebhookApp
	logger *zap.Logger
}

func NewConsumer(reader *kafka.Reader, app port.WebhookApp, logger *zap.Logger) *Consumer {
	return &Consumer{
		reader: reader,
		app:    app,
		logger: logger,
	}
}

func (c *Consumer) Consume(ctx context.Context) error {
	attempt := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			platform.LoggerWithTrace(ctx, c.logger).Error(platform.LogEventKafkaFetchFailed, zap.Error(err))
			c.sleepBackoff(ctx, &attempt)
			continue
		}

		merchantID := string(msg.Key)
		
		tracedCtx := platform.InjectTraceIntoContext(ctx, &msg)
		
		processCtx, cancel := context.WithTimeout(tracedCtx, domain.ServerShutdownTimeout)

		err = c.processMessage(processCtx, msg, func() error {
			return c.app.HandleMessage(processCtx, merchantID, msg.Value)
		})
		cancel()

		if err != nil {
			c.sleepBackoff(ctx, &attempt)
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			platform.LoggerWithTrace(ctx, c.logger).Error(platform.LogEventKafkaCommitFailed, zap.Error(err))
			c.sleepBackoff(ctx, &attempt)
			continue
		}
		platform.RecordConsumerCommit(ctx, msg.Topic)

		attempt = 0
	}
}

func (c *Consumer) sleepBackoff(ctx context.Context, attempt *int) {
	jitterDelay := platform.CalculateJitterBackoff(*attempt, platform.ConsumerBackoffMinDelay, platform.ConsumerBackoffMaxDelay)
	timer := time.NewTimer(jitterDelay)
	select {
	case <-ctx.Done():
		timer.Stop()
	case <-timer.C:
	}
	platform.RecordConsumerBackoffDuration(ctx, platform.ComponentConsumer, jitterDelay)
	*attempt++
}

func (c *Consumer) processMessage(ctx context.Context, msg kafka.Message, processFn func() error) (err error) {
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		if r := recover(); r != nil {
			var panicErr error
			if e, ok := r.(error); ok {
				panicErr = e
			} else {
				panicErr = fmt.Errorf("%w: %v", domain.ErrPanic, r)
			}
			platform.LoggerWithTrace(ctx, c.logger).Error(platform.LogEventPanicRecovered, zap.Any(platform.LogFieldPanic, r), zap.Duration(platform.LogFieldDuration, duration))
			_ = panicErr
			err = nil
		}
	}()

	if err := processFn(); err != nil {
		if domain.IsTerminalError(err) {
			platform.LoggerWithTrace(ctx, c.logger).Warn(platform.LogEventTerminalBusinessError, zap.Error(err))
			return nil // offset committed, message dropped/DLQ'd by app logic
		}

		platform.RecordInfrastructureError(ctx, platform.ComponentConsumerProcessing)
		return err
	}

	return nil
}
