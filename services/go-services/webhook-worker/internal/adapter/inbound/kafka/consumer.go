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
	reader port.KafkaReader
	app    port.WebhookApp
	logger *zap.Logger
}

func NewConsumer(reader port.KafkaReader, app port.WebhookApp, logger *zap.Logger) *Consumer {
	return &Consumer{
		reader: reader,
		app:    app,
		logger: logger,
	}
}

func (c *Consumer) Consume(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			return err
		}

		merchantID := string(msg.Key)
		tracedCtx := platform.InjectTraceIntoContext(ctx, &msg)
		processCtx, cancel := context.WithTimeout(tracedCtx, domain.ServerShutdownTimeout)

		procErr := c.processMessage(processCtx, msg, func() error {
			return c.app.HandleMessage(processCtx, merchantID, msg.Value)
		})
		cancel()

		if procErr != nil {
			// If HandleMessage returns an error, it is an infrastructure failure (e.g. DB outage).
			return procErr
		}

		if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
			platform.LoggerWithTrace(ctx, c.logger).Error(platform.LogEventKafkaCommitFailed, zap.Error(commitErr))
			return commitErr
		}
		platform.RecordConsumerCommit(ctx, msg.Topic)
	}
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

			// Route to global DLQ to isolate the poison pill
			c.app.RouteToGlobalDLQ(ctx, msg.Value, panicErr.Error())

			// Return nil so the offset is committed and the consumer can move past the poison pill
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
