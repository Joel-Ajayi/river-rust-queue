package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform/platform_consumer"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/port"
	"github.com/failsafe-go/failsafe-go"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

func WebhookHandler(app port.WebhookApp, retryCfg platform.RetryConfig, logger *zap.Logger) platform_consumer.MessageHandler {
	return func(ctx context.Context, msg kafka.Message) error {
		merchantID := string(msg.Key)
		start := time.Now()
		var attemptCount int

		// Use passed retryCfg

		err := platform.ExecuteWithJitter(ctx, retryCfg, func(exec failsafe.Execution[any]) error {
			attemptCount++
			if ctx.Err() != nil {
				return ctx.Err()
			}

			defer func() {
				if r := recover(); r != nil {
					platform.LoggerWithTrace(ctx, logger).Error(platform.LogEventPanicRecoveredDLQ,
						zap.Any(platform.LogFieldPanic, r))
					app.RouteToGlobalDLQ(ctx, msg.Value,
						fmt.Sprintf("%v: %v", domain.ErrPanic, r))
				}
			}()

			if procErr := app.HandleMessage(ctx, merchantID, msg.Value); procErr != nil {
				classification := platform.ClassifyError(procErr, domain.IsTerminalError)

				switch classification {
				case platform.ClassificationTerminal, platform.ClassificationPoison:
					if dlqErr := app.RouteToGlobalDLQ(ctx, msg.Value, procErr.Error()); dlqErr != nil {
						platform.LoggerWithTrace(ctx, logger).Error(platform.LogEventDLQWriteFailed, zap.Error(dlqErr))
					}
					platform.LogCanonicalEvent(ctx, logger, platform.ServiceNameWebhookWorker, platform.CanonicalLogLine{
						Event:        platform.EventWebhookFailed,
						Status:       platform.StatusDLQ,
						MerchantID:   merchantID,
						RetryCount:   attemptCount - 1,
						ErrorMessage: procErr.Error(),
						DurationMs:   float64(time.Since(start).Microseconds()) / 1000.0,
					})
					return nil
				}

				platform.RecordInfrastructureError(ctx, platform.ComponentConsumerProcessing)
				platform.LoggerWithTrace(ctx, logger).Warn(platform.LogEventConsumerFetchRetry,
					zap.String(platform.LogFieldErrorType, string(classification)),
					zap.Int(platform.LogFieldPartition, msg.Partition),
					zap.Int64(platform.LogFieldOffset, msg.Offset),
					zap.Int(platform.LogFieldRetryCount, attemptCount),
					zap.Int(platform.LogFieldAttempt, attemptCount),
					zap.Error(procErr),
				)
				return procErr
			}

			duration := float64(time.Since(start).Microseconds()) / 1000.0
			platform.LogCanonicalEvent(ctx, logger, platform.ServiceNameWebhookWorker, platform.CanonicalLogLine{
				Event:      platform.EventWebhookDelivered,
				Status:     platform.StatusSuccess,
				MerchantID: merchantID,
				RetryCount: attemptCount - 1,
				DurationMs: duration,
			})
			return nil
		})

		if err != nil {
			platform.LoggerWithTrace(ctx, logger).Warn("Transient retry budget exhausted, routing to DLQ to unblock partition",
				zap.String(platform.LogFieldTopic, msg.Topic),
				zap.Int(platform.LogFieldPartition, msg.Partition),
				zap.Int64(platform.LogFieldOffset, msg.Offset),
				zap.Int(platform.LogFieldRetryCount, attemptCount),
				zap.Error(err),
			)
			if dlqErr := app.RouteToGlobalDLQ(ctx, msg.Value, err.Error()); dlqErr != nil {
				platform.LoggerWithTrace(ctx, logger).Error(platform.LogEventDLQWriteFailed, zap.Error(dlqErr))
				return err // Return original error to halt partition only if DLQ write fails
			}
			return nil
		}
		return nil
	}
}
