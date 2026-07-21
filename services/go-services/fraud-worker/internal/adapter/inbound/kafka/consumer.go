package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/core/port"
	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform/pii"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type ConsumerManager struct {
	logger       *zap.Logger
	reader       *kafka.Reader
	handler      port.JobHandler
	dlqStore     port.DLQStore
	merchantDir  port.MerchantDirectory
	pools        *platform.ShardPools
	wg           sync.WaitGroup
	shutdownCh   chan struct{}
	shutdownOnce sync.Once
}

func NewConsumerManager(
	logger *zap.Logger,
	reader *kafka.Reader,
	handler port.JobHandler,
	dlqStore port.DLQStore,
	merchantDir port.MerchantDirectory,
	pools *platform.ShardPools,
) *ConsumerManager {
	return &ConsumerManager{
		logger:      logger,
		reader:      reader,
		handler:     handler,
		dlqStore:    dlqStore,
		merchantDir: merchantDir,
		pools:       pools,
		shutdownCh:  make(chan struct{}),
	}
}

const (
	retryBackoff        = 1 * time.Second
	processTimeout      = 5 * time.Second
	initialAttemptCount = 1
)

func (m *ConsumerManager) Start(ctx context.Context) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.logger.Info(platform.LogEventServerStarted)

		for {
			select {
			case <-ctx.Done():
				return
			case <-m.shutdownCh:
				return
			default:
			}

			msg, err := m.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				time.Sleep(retryBackoff)
				continue
			}

			var envelope eventsv1.EventEnvelope
			if err := proto.Unmarshal(msg.Value, &envelope); err != nil {
				m.logger.Error(platform.LogEventPoisonPill, zap.Error(err))
				if dlqErr := m.routeToDLQ(ctx, msg, fmt.Errorf("%w: %w", domain.ErrUnmarshal, err)); dlqErr != nil {
					m.logger.Error(platform.LogEventDLQWriteFailed, zap.Error(dlqErr))
				} else {
					_ = m.reader.CommitMessages(ctx, msg)
					platform.RecordConsumerCommit(ctx, msg.Topic)
				}
				continue
			}

			payload := envelope.GetJobRequested()
			if payload == nil {
				_ = m.reader.CommitMessages(ctx, msg)
				platform.RecordConsumerCommit(ctx, msg.Topic)
				continue
			}

			tracedCtx := platform.InjectTraceIntoContext(ctx, &msg)
			processCtx, cancel := context.WithTimeout(tracedCtx, processTimeout)
			err = m.processMessage(processCtx, msg, func() error {
				return m.handler.ProcessJob(processCtx, payload, envelope.EventId, envelope.OccurredAt.AsTime().UnixMilli())
			})
			cancel()

			if err != nil {
				time.Sleep(retryBackoff)
				continue
			}

			_ = m.reader.CommitMessages(ctx, msg)
			platform.RecordConsumerCommit(ctx, msg.Topic)
		}
	}()
}

func (m *ConsumerManager) processMessage(ctx context.Context, msg kafka.Message, processFn func() error) (err error) {
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
			platform.LoggerWithTrace(ctx, m.logger).Error(platform.LogEventPanicRecovered, zap.Any(platform.LogFieldPanic, r), zap.Duration(platform.LogFieldDuration, duration))
			if dlqErr := m.routeToDLQ(ctx, msg, fmt.Errorf("%w: %w", domain.ErrPanic, panicErr)); dlqErr != nil {
				platform.LoggerWithTrace(ctx, m.logger).Error(platform.LogEventDLQWriteFailed, zap.Error(dlqErr))
			}
			err = nil
		}
	}()

	if err := processFn(); err != nil {
		if domain.IsTerminalError(err) {
			if dlqErr := m.routeToDLQ(ctx, msg, err); dlqErr != nil {
				platform.LoggerWithTrace(ctx, m.logger).Error(platform.LogEventDLQWriteFailed, zap.Error(dlqErr))
			}
			return nil
		}
		platform.RecordInfrastructureError(ctx, platform.ComponentConsumerProcessing)
		return err
	}
	return nil
}

func (m *ConsumerManager) Stop() {
	m.shutdownOnce.Do(func() {
		close(m.shutdownCh)
		_ = m.reader.Close()
		m.wg.Wait()
	})
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
		AttemptCount:        initialAttemptCount,
		FirstFailedAt:       time.Now(),
		LastFailedAt:        time.Now(),
		Status:              platform.DLQStatusOpen,
		TraceID:             traceID,
		SpanID:              spanID,
	}

	return m.dlqStore.WriteDLQEntry(ctx, shardID, entry)
}
