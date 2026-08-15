package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/core/port"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform/platform_consumer"
	"github.com/failsafe-go/failsafe-go"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type ConsumerManager struct {
	logger      *zap.Logger
	reader      *kafka.Reader
	handler     port.JobHandler
	dlqStore    port.DLQStore
	merchantDir port.MerchantDirectory
	pools       *platform.ShardPools
	cfg         *platform.Config
	retryBudget *platform.RetryBudget
}

func NewConsumerManager(
	logger *zap.Logger,
	reader *kafka.Reader,
	handler port.JobHandler,
	dlqStore port.DLQStore,
	merchantDir port.MerchantDirectory,
	pools *platform.ShardPools,
	cfg *platform.Config,
) *ConsumerManager {
	budget := platform.NewRetryBudget(
		int64(cfg.Capacity.RetryBudgetMinTokens),
		int64(cfg.Capacity.RetryBudgetMaxTokens),
		cfg.Capacity.RetryBudgetFraction,
	)
	return &ConsumerManager{
		logger:      logger,
		reader:      reader,
		handler:     handler,
		dlqStore:    dlqStore,
		merchantDir: merchantDir,
		pools:       pools,
		cfg:         cfg,
		retryBudget: budget,
	}
}

func (m *ConsumerManager) consume(ctx context.Context) error {
	cfgForConsumer := platform_consumer.NewConsumerConfigFromCapacity(m.cfg)
	cfgForConsumer.OnPanicDLQ = func(ctx context.Context, msg kafka.Message, reason error) error {
		return m.routeToDLQ(ctx, msg, reason)
	}

	readerAdapter := &readerAdapter{reader: m.reader}

	pipeline := platform_consumer.NewConsumerPipeline(readerAdapter, m.messageHandler(), cfgForConsumer, m.logger)
	return pipeline.Consume(ctx)
}

type readerAdapter struct {
	reader *kafka.Reader
}

func (a *readerAdapter) FetchMessage(ctx context.Context) (kafka.Message, error) {
	return a.reader.FetchMessage(ctx)
}

func (a *readerAdapter) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	return a.reader.CommitMessages(ctx, msgs...)
}

func (m *ConsumerManager) messageHandler() platform_consumer.MessageHandler {
	return func(ctx context.Context, msg kafka.Message) error {
		envelope, err := platform.UnmarshalEnvelope(msg.Value)
		if err != nil {
			if dlqErr := m.routeToDLQ(ctx, msg, fmt.Errorf("%w: %w", domain.ErrUnmarshal, err)); dlqErr != nil {
				m.logger.Error(platform.LogEventDLQWriteFailed, zap.Error(dlqErr))
			}
			return nil
		}

		var attemptCount int

		retryCfg := platform.RetryConfig{
			MaxRetries: int(m.cfg.Capacity.MaxRetries),
			BaseDelay:  time.Duration(m.cfg.Capacity.BackoffBaseMs) * time.Millisecond,
			MaxDelay:   time.Duration(m.cfg.Capacity.BackoffCapMs) * time.Millisecond,
			Budget:     m.retryBudget,
		}

		start := time.Now()
		logger := platform.LoggerWithTrace(ctx, m.logger)

		payload := envelope.GetJobRequested()
		err = platform.ExecuteWithJitter(ctx, retryCfg, func(exec failsafe.Execution[any]) error {
			attemptCount++

			if ctx.Err() != nil {
				return ctx.Err()
			}

			procErr := m.handler.ProcessJob(ctx, payload, envelope.EventId, envelope.OccurredAt.AsTime().UnixMilli())
			if procErr != nil {
				classification := platform.ClassifyError(procErr, domain.IsTerminalError)

				switch classification {
				case platform.ClassificationPoison, platform.ClassificationTerminal:
					if dlqErr := m.routeToDLQ(ctx, msg, procErr); dlqErr != nil {
						logger.Error(platform.LogEventDLQWriteFailed, zap.Error(dlqErr))
					}
					platform.RecordDLQIngestion(ctx, platform.ServiceNameFraudWorker)
					logger.Warn(platform.LogEventTerminalBusinessError,
						zap.String(platform.LogFieldTopic, msg.Topic),
						zap.Int(platform.LogFieldPartition, msg.Partition),
						zap.Int64(platform.LogFieldOffset, msg.Offset),
						zap.ByteString(platform.LogFieldKey, msg.Key),
						zap.Error(procErr),
					)
					return nil
				}

				platform.RecordInfrastructureError(ctx, platform.ComponentConsumerProcessing)
				// Infrastructure-driven DLQ — distinguish from business-rule
				// rejections in dashboards (see issue 36).
				platform.RecordDLQInfraFlood(ctx, platform.ServiceNameFraudWorker)
				logger.Warn(platform.LogEventConsumerFetchRetry,
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
			platform.LogCanonicalEvent(ctx, m.logger, platform.ServiceNameFraudWorker, platform.CanonicalLogLine{
				Event:      platform.EventFraudVelocityCheck,
				Status:     platform.StatusSuccess,
				JobID:      payload.JobId,
				MerchantID: payload.MerchantId,
				RetryCount: attemptCount - 1,
				DurationMs: duration,
			})
			return nil
		})

		if err != nil {
			logger.Warn("Transient retry budget exhausted, routing to DLQ to unblock partition",
				zap.String(platform.LogFieldTopic, msg.Topic),
				zap.Int(platform.LogFieldPartition, msg.Partition),
				zap.Int64(platform.LogFieldOffset, msg.Offset),
				zap.Int(platform.LogFieldRetryCount, attemptCount),
				zap.Error(err),
			)
			if dlqErr := m.routeToDLQ(ctx, msg, err); dlqErr != nil {
				logger.Error(platform.LogEventDLQWriteFailed, zap.Error(dlqErr))
				return err // Return original error to halt partition only if DLQ write fails
			}
			return nil
		}
		return nil
	}
}

func (m *ConsumerManager) Stop() {
	_ = m.reader.Close()
}

func (m *ConsumerManager) Consume(ctx context.Context) error {
	return m.consume(ctx)
}

func (m *ConsumerManager) routeToDLQ(ctx context.Context, msg kafka.Message, reasonErr error) error {
	classification := platform.ClassifyError(reasonErr, domain.IsTerminalError)

	traceID, spanID := platform.ExtractTraceFromMessageHeaders(&msg)

	entry := platform.NewDLQEntry(
		platform.DLQSourceFraud, msg.Topic, string(msg.Key), msg.Value,
		fmt.Sprintf("%s"+platform.DLQOriginSep+"%d"+platform.DLQOriginSep+"%d", msg.Topic, msg.Partition, msg.Offset),
		fmt.Sprintf("%s: %s", classification, reasonErr.Error()),
		classification, msg.Time, time.Now(), traceID, spanID,
	)

	// Retry of the DLQ write itself lives in platform.WriteDLQEntryWithRetry
	// (per-service budget, error-classified).
	if err := m.dlqStore.WriteDLQEntry(ctx, entry); err != nil {
		platform.LoggerWithTrace(ctx, m.logger).Error(platform.LogEventDLQWriteExhausted,
			zap.Error(err),
			zap.String(platform.LogFieldDLQID, entry.GetId()),
			zap.String(platform.LogFieldTopic, msg.Topic),
			zap.ByteString(platform.LogFieldKey, msg.Key),
		)
		return err
	}

	return nil
}
