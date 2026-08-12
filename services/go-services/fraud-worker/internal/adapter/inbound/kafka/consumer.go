package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/core/port"
	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform/pii"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform/platform_consumer"
	"github.com/failsafe-go/failsafe-go"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type ConsumerManager struct {
	logger      *zap.Logger
	reader      *kafka.Reader
	handler     port.JobHandler
	dlqStore    port.DLQStore
	merchantDir port.MerchantDirectory
	pools       *platform.ShardPools
	cfg         *platform.Config
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
	return &ConsumerManager{
		logger:      logger,
		reader:      reader,
		handler:     handler,
		dlqStore:    dlqStore,
		merchantDir: merchantDir,
		pools:       pools,
		cfg:         cfg,
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
		var envelope eventsv1.EventEnvelope
		if err := proto.Unmarshal(msg.Value, &envelope); err != nil {
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
		}

		start := time.Now()
		logger := platform.LoggerWithTrace(ctx, m.logger)

		payload := envelope.GetJobRequested()
		err := platform.ExecuteWithJitter(ctx, retryCfg, func(exec failsafe.Execution[any]) error {
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
			platform.RecordDLQIngestion(ctx, platform.ServiceNameFraudWorker)
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

	shardID := m.pools.GetAvailableShardIDs()[0]
	var envelope eventsv1.EventEnvelope
	if err := proto.Unmarshal(msg.Value, &envelope); err == nil {
		payload := envelope.GetJobRequested()
		if payload != nil {
			if s, err := m.merchantDir.ShardFor(ctx, payload.MerchantId); err == nil {
				shardID = s
			}
		}
	}

	maskedPayload := pii.Mask(msg.Value)
	entry := domain.DLQEntry{
		ID:                  platform.NewEventID(),
		Source:              platform.DLQSourceFraud,
		OriginalPayload:     maskedPayload,
		ErrorMessage:        string(classification) + ": " + reasonErr.Error(),
		ErrorClassification: string(classification),
		AttemptCount:        0,
		FirstFailedAt:       msg.Time,
		LastFailedAt:        time.Now(),
		Status:              platform.DLQStatusOpen,
		TraceID:             traceID,
		SpanID:              spanID,
	}

	dlqRetryCfg := platform.RetryConfig{
		MaxRetries: m.cfg.Capacity.DLQMaxRetries,
		BaseDelay:  time.Duration(m.cfg.Capacity.DLQBaseDelayMs) * time.Millisecond,
		MaxDelay:   time.Duration(m.cfg.Capacity.DLQCapDelayMs) * time.Millisecond,
	}

	err := platform.ExecuteWithJitter(ctx, dlqRetryCfg, func(exec failsafe.Execution[any]) error {
		return m.dlqStore.WriteDLQEntry(ctx, shardID, entry)
	})
	if err != nil {
		platform.LoggerWithTrace(ctx, m.logger).Error(platform.LogEventDLQWriteExhausted, zap.Error(err), zap.String(platform.LogFieldTopic, msg.Topic), zap.ByteString(platform.LogFieldKey, msg.Key))
		return err
	}

	return nil
}
