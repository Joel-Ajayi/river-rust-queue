package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform/pii"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform/platform_consumer"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/port"
	"github.com/failsafe-go/failsafe-go"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type ConsumerManager struct {
	logger        *zap.Logger
	jobReader     *kafka.Reader
	xshardReaders []*kafka.Reader
	jobHandler    port.JobHandler
	sagaHandler   port.SagaHandler
	dlqStore      port.DLQStore
	directory     port.MerchantDirectory
	pools         *platform.ShardPools
	cfg           *platform.Config
}

func NewConsumerManager(
	logger *zap.Logger,
	jobReader *kafka.Reader,
	xshardReaders []*kafka.Reader,
	jobHandler port.JobHandler,
	sagaHandler port.SagaHandler,
	dlqStore port.DLQStore,
	directory port.MerchantDirectory,
	pools *platform.ShardPools,
	cfg *platform.Config,
) *ConsumerManager {
	return &ConsumerManager{
		logger:        logger,
		jobReader:     jobReader,
		xshardReaders: xshardReaders,
		jobHandler:    jobHandler,
		sagaHandler:   sagaHandler,
		dlqStore:      dlqStore,
		directory:     directory,
		pools:         pools,
		cfg:           cfg,
	}
}

func (m *ConsumerManager) Shutdown() {
	if m.jobReader != nil {
		_ = m.jobReader.Close()
	}
	for _, r := range m.xshardReaders {
		if r != nil {
			_ = r.Close()
		}
	}
}

func (m *ConsumerManager) Consume(ctx context.Context) error {
	cfg := platform_consumer.NewConsumerConfigFromCapacity(m.cfg)

	// Shared semaphore for both pipelines (job + xshard)
	sharedSem := make(chan struct{}, cfg.WorkerPoolSize)

	jobReaderAdapter := &readerAdapter{reader: m.jobReader}
	jobHandler := m.jobHandler
	sagaHandler := m.sagaHandler
	dlqStore := m.dlqStore
	directory := m.directory
	pools := m.pools

	cfg.OnPanicDLQ = func(ctx context.Context, msg kafka.Message, reason error) error {
		return m.routeToDLQ(ctx, msg, reason, dlqStore, directory, pools)
	}

	jobPipeline := platform_consumer.NewConsumerPipeline(
		jobReaderAdapter, m.jobMessageHandler(nil, jobHandler, sagaHandler, dlqStore, directory, pools),
		cfg, m.logger,
	)
	jobPipeline.SetSharedSemaphore(sharedSem)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := jobPipeline.Consume(ctx); err != nil && !errors.Is(err, context.Canceled) {
			m.logger.Error(platform.LogEventKafkaConsumerStopped, zap.Error(err))
		}
	}()

	for _, r := range m.xshardReaders {
		xshardAdapter := &readerAdapter{reader: r}
		xsPipeline := platform_consumer.NewConsumerPipeline(
			xshardAdapter, m.xshardMessageHandler(sagaHandler, dlqStore, directory, pools),
			cfg, m.logger,
		)
		xsPipeline.SetSharedSemaphore(sharedSem)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := xsPipeline.Consume(ctx); err != nil && !errors.Is(err, context.Canceled) {
				m.logger.Error(platform.LogEventKafkaConsumerStopped, zap.Error(err))
			}
		}()
	}

	wg.Wait()
	return nil
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

func (m *ConsumerManager) jobMessageHandler(_ *kafka.Reader, jobHandler port.JobHandler, _ port.SagaHandler, dlqStore port.DLQStore, directory port.MerchantDirectory, pools *platform.ShardPools) platform_consumer.MessageHandler {
	return func(ctx context.Context, msg kafka.Message) error {
		var envelope eventsv1.EventEnvelope
		if err := proto.Unmarshal(msg.Value, &envelope); err != nil {
			if dlqErr := m.routeToDLQ(ctx, msg, fmt.Errorf("%w: %w", domain.ErrUnmarshal, err), dlqStore, directory, pools); dlqErr != nil {
				platform.LoggerWithTrace(ctx, m.logger).Error(platform.LogEventPoisonDLQWriteFailed, zap.Error(dlqErr))
			}
			return nil
		}

		if payload := envelope.GetJobRequested(); payload != nil {
			return m.processWithRetry(ctx, msg, func() error {
				return jobHandler.ProcessJob(ctx, payload)
			}, domain.IsTerminalError, dlqStore, directory, pools)
		}

		return nil
	}
}

func (m *ConsumerManager) xshardMessageHandler(sagaHandler port.SagaHandler, dlqStore port.DLQStore, directory port.MerchantDirectory, pools *platform.ShardPools) platform_consumer.MessageHandler {
	return func(ctx context.Context, msg kafka.Message) error {
		var envelope eventsv1.EventEnvelope
		if err := proto.Unmarshal(msg.Value, &envelope); err != nil {
			if dlqErr := m.routeToDLQ(ctx, msg, fmt.Errorf("%w: %w", domain.ErrUnmarshal, err), dlqStore, directory, pools); dlqErr != nil {
				platform.LoggerWithTrace(ctx, m.logger).Error(platform.LogEventPoisonDLQWriteFailed, zap.Error(dlqErr))
			}
			return nil
		}

		return m.processWithRetry(ctx, msg, func() error {
			switch {
			case envelope.GetXshardTransferRequested() != nil:
				return sagaHandler.HandleXShardRequested(ctx, envelope.GetXshardTransferRequested())
			case envelope.GetXshardTransferSettled() != nil:
				return sagaHandler.HandleXShardSettled(ctx, envelope.GetXshardTransferSettled())
			case envelope.GetXshardTransferFailed() != nil:
				return sagaHandler.HandleXShardFailed(ctx, envelope.GetXshardTransferFailed())
			}
			return domain.ErrInvalidBody
		}, domain.IsTerminalError, dlqStore, directory, pools)
	}
}

func (m *ConsumerManager) processWithRetry(ctx context.Context, msg kafka.Message, processFn func() error, isTerminal func(error) bool, dlqStore port.DLQStore, directory port.MerchantDirectory, pools *platform.ShardPools) error {
	start := time.Now()
	logger := platform.LoggerWithTrace(ctx, m.logger)

	retryCfg := platform.RetryConfig{
		MaxRetries: m.cfg.Capacity.MaxRetries,
		BaseDelay:  time.Duration(m.cfg.Capacity.BackoffBaseMs) * time.Millisecond,
		MaxDelay:   time.Duration(m.cfg.Capacity.BackoffCapMs) * time.Millisecond,
	}

	var attemptCount int
	err := platform.ExecuteWithJitter(ctx, retryCfg, func(exec failsafe.Execution[any]) error {
		attemptCount++
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err := processFn(); err != nil {
			classification := platform.ClassifyError(err, isTerminal)

			if classification == platform.ClassificationPoison || classification == platform.ClassificationTerminal {
				if dlqErr := m.routeToDLQ(ctx, msg, err, dlqStore, directory, pools); dlqErr != nil {
					logger.Error(platform.LogEventDLQWriteFailed, zap.Error(dlqErr))
				}
				platform.RecordDLQIngestion(ctx, platform.ServiceNameLedgerWorker)
				logger.Warn(platform.LogEventTerminalBusinessError,
					zap.String(platform.LogFieldTopic, msg.Topic),
					zap.Int(platform.LogFieldPartition, msg.Partition),
					zap.Int64(platform.LogFieldOffset, msg.Offset),
					zap.ByteString(platform.LogFieldKey, msg.Key),
					zap.Error(err),
				)
				return nil
			}

			platform.RecordInfrastructureError(ctx, platform.ComponentConsumerProcessing)
			logger.Warn(platform.LogEventConsumerFetchRetry,
				zap.String(platform.LogFieldErrorType, string(classification)),
				zap.Int(platform.LogFieldPartition, msg.Partition),
				zap.Int64(platform.LogFieldOffset, msg.Offset),
				zap.Int(platform.LogFieldRetryCount, attemptCount),
				zap.Int(platform.LogFieldAttempt, attemptCount),
				zap.Error(err),
			)
			return err
		}

		duration := float64(time.Since(start).Microseconds()) / 1000.0
		platform.LogCanonicalEvent(ctx, m.logger, platform.ServiceNameLedgerWorker, platform.CanonicalLogLine{
			Event:      platform.EventTransferCompleted,
			Status:     platform.StatusSuccess,
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
		if dlqErr := m.routeToDLQ(ctx, msg, err, dlqStore, directory, pools); dlqErr != nil {
			logger.Error(platform.LogEventDLQWriteFailed, zap.Error(dlqErr))
			return err // Return original error to halt partition only if DLQ write fails
		}
		platform.RecordDLQIngestion(ctx, platform.ServiceNameLedgerWorker)
		return nil
	}

	return nil
}

func (m *ConsumerManager) routeToDLQ(ctx context.Context, msg kafka.Message, reasonErr error, dlqStore port.DLQStore, directory port.MerchantDirectory, pools *platform.ShardPools) error {
	classification := platform.ClassifyError(reasonErr, domain.IsTerminalError)

	traceID, spanID := platform.ExtractTraceFromMessageHeaders(&msg)

	shardID := pools.GetAvailableShardIDs()[0]
	var envelope eventsv1.EventEnvelope
	if err := proto.Unmarshal(msg.Value, &envelope); err == nil {
		switch {
		case envelope.GetJobRequested() != nil:
			merchantID := envelope.GetJobRequested().MerchantId
			if s, err := directory.ShardFor(ctx, merchantID); err == nil {
				shardID = s
			}
		case envelope.GetXshardTransferRequested() != nil:
			shardID = envelope.GetXshardTransferRequested().SrcShard
		case envelope.GetXshardTransferSettled() != nil:
			shardID = envelope.GetXshardTransferSettled().DstShard
		case envelope.GetXshardTransferFailed() != nil:
			shardID = envelope.GetXshardTransferFailed().SrcShard
		}
	}

	maskedPayload := pii.Mask(msg.Value)
	entry := domain.DLQEntry{
		ID:                  platform.NewEventID(),
		Source:              domain.DLQSourceLedger,
		OriginalPayload:     maskedPayload,
		ErrorMessage:        fmt.Sprintf("[%s] %s", classification, reasonErr.Error()),
		ErrorClassification: string(classification),
		AttemptCount:        0,
		FirstFailedAt:       msg.Time,
		LastFailedAt:        time.Now(),
		Status:              domain.DLQStatusOpen,
		TraceID:             traceID,
		SpanID:              spanID,
	}

	dlqRetryCfg := platform.RetryConfig{
		MaxRetries: m.cfg.Capacity.DLQMaxRetries,
		BaseDelay:  time.Duration(m.cfg.Capacity.DLQBaseDelayMs) * time.Millisecond,
		MaxDelay:   time.Duration(m.cfg.Capacity.DLQCapDelayMs) * time.Millisecond,
	}

	err := platform.ExecuteWithJitter(ctx, dlqRetryCfg, func(exec failsafe.Execution[any]) error {
		return dlqStore.WriteDLQEntry(ctx, shardID, entry)
	})
	if err != nil {
		platform.LoggerWithTrace(ctx, m.logger).Error(platform.LogEventDLQWriteExhausted, zap.Error(err), zap.String(platform.LogFieldTopic, msg.Topic), zap.ByteString(platform.LogFieldKey, msg.Key))
		return err
	}

	platform.RecordDLQIngestion(ctx, platform.ServiceNameLedgerWorker)
	return nil
}
