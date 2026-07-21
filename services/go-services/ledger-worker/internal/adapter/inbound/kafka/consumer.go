package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform/pii"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/port"
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

	wg           sync.WaitGroup
	shutdownCh   chan struct{}
	shutdownOnce sync.Once
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
		shutdownCh:    make(chan struct{}),
	}
}

func (m *ConsumerManager) Start(ctx context.Context) {
	m.wg.Add(1)
	go m.consumeJobs(ctx)

	for _, r := range m.xshardReaders {
		m.wg.Add(1)
		go m.consumeXShardJob(ctx, r)
	}
}

func (m *ConsumerManager) Wait() {
	m.wg.Wait()
	close(m.shutdownCh)
}

func (m *ConsumerManager) Shutdown() {
	m.shutdownOnce.Do(func() {
		// Close readers first to unblock any FetchMessage calls
		if m.jobReader != nil {
			_ = m.jobReader.Close()
		}
		for _, r := range m.xshardReaders {
			if r != nil {
				_ = r.Close()
			}
		}
		close(m.shutdownCh)
	})
}

// consumeJobs polls for job requested events and processes them with proper backoff
func (m *ConsumerManager) consumeJobs(ctx context.Context) {
	defer m.wg.Done()

	attempt := 0
	processTimeout := domain.ConsumerProcessTimeout

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.shutdownCh:
			return
		default:
		}

		msg, err := m.jobReader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			m.sleepBackoff(ctx, &attempt)
			continue
		}

		var envelope eventsv1.EventEnvelope
		if err := proto.Unmarshal(msg.Value, &envelope); err != nil {
			if dlqErr := m.routeToDLQ(ctx, msg, fmt.Errorf("%w: %w", domain.ErrUnmarshal, err)); dlqErr != nil {
				platform.LoggerWithTrace(ctx, m.logger).Error(platform.LogEventPoisonDLQWriteFailed, zap.Error(dlqErr))
			} else {
				m.jobReader.CommitMessages(ctx, msg)
				platform.RecordConsumerCommit(ctx, msg.Topic)
			}
			attempt = 0
			continue
		}

		if payload := envelope.GetJobRequested(); payload != nil {
			// Extract trace context from Kafka headers and inject into processCtx
			tracedCtx := platform.InjectTraceIntoContext(ctx, &msg)

			processCtx, cancel := context.WithTimeout(tracedCtx, processTimeout)
			err := m.processMessage(processCtx, msg, func() error {
				return m.jobHandler.ProcessJob(processCtx, payload)
			})
			cancel()

			if err != nil {
				m.sleepBackoff(ctx, &attempt)
				continue
			}
		}

		if err := m.jobReader.CommitMessages(ctx, msg); err != nil {
			m.sleepBackoff(ctx, &attempt)
			continue
		}
		platform.RecordConsumerCommit(ctx, msg.Topic)

		attempt = 0
	}
}

// consumeXShardJob polls for xshard saga events
func (m *ConsumerManager) consumeXShardJob(ctx context.Context, r *kafka.Reader) {
	defer m.wg.Done()

	attempt := 0
	processTimeout := domain.ConsumerProcessTimeout

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.shutdownCh:
			return
		default:
		}

		msg, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			m.sleepBackoff(ctx, &attempt)
			continue
		}

		var envelope eventsv1.EventEnvelope
		if err := proto.Unmarshal(msg.Value, &envelope); err != nil {
			if dlqErr := m.routeToDLQ(ctx, msg, fmt.Errorf("%w: %w", domain.ErrUnmarshal, err)); dlqErr != nil {
			} else {
				r.CommitMessages(ctx, msg)
				platform.RecordConsumerCommit(ctx, msg.Topic)
			}
			attempt = 0
			continue
		}

		// Extract trace context from Kafka headers and inject into processCtx
		tracedCtx := platform.InjectTraceIntoContext(ctx, &msg)

		processCtx, cancel := context.WithTimeout(tracedCtx, processTimeout)
		err = m.processMessage(processCtx, msg, func() error {
			switch {
			case envelope.GetXshardTransferRequested() != nil:
				return m.sagaHandler.HandleXShardRequested(processCtx, envelope.GetXshardTransferRequested())
			case envelope.GetXshardTransferSettled() != nil:
				return m.sagaHandler.HandleXShardSettled(processCtx, envelope.GetXshardTransferSettled())
			case envelope.GetXshardTransferFailed() != nil:
				return m.sagaHandler.HandleXShardFailed(processCtx, envelope.GetXshardTransferFailed())
			}
			return domain.ErrInvalidBody
		})
		cancel()

		if err != nil {
			m.sleepBackoff(ctx, &attempt)
			continue
		}

		if err := r.CommitMessages(ctx, msg); err != nil {
			m.sleepBackoff(ctx, &attempt)
			continue
		}
		platform.RecordConsumerCommit(ctx, msg.Topic)

		attempt = 0
	}
}

// sleepBackoff sleeps with exponential backoff + full jitter, respecting context cancellation
func (m *ConsumerManager) sleepBackoff(ctx context.Context, attempt *int) {
	jitterDelay := platform.CalculateJitterBackoff(*attempt, domain.ConsumerBackoffMinDelay, domain.ConsumerBackoffMaxDelay)
	timer := time.NewTimer(jitterDelay)
	select {
	case <-ctx.Done():
		timer.Stop()
	case <-m.shutdownCh:
		timer.Stop()
	case <-timer.C:
	}
	platform.RecordConsumerBackoffDuration(ctx, platform.ComponentConsumer, jitterDelay)
	*attempt++
}

// processMessage executes the handler with panic recovery and terminal error handling.
// Returns nil for terminal errors (offset should be committed) and infra errors for backoff.
func (m *ConsumerManager) processMessage(ctx context.Context, msg kafka.Message, processFn func() error) (err error) {
	start := time.Now()

	// Panic recovery to ensure that a panic in the handler does not crash the consumer.
	defer func() {
		duration := time.Since(start)
		if r := recover(); r != nil {
			var panicErr error
			if e, ok := r.(error); ok {
				panicErr = e
			} else {
				panicErr = fmt.Errorf("%w: %v", domain.ErrPanic, r)
			}
			platform.LoggerWithTrace(ctx, m.logger).Error(platform.LogEventPanicRecoveredDLQ, zap.Any(platform.LogFieldPanic, r), zap.Duration(platform.LogFieldDuration, duration))
			if dlqErr := m.routeToDLQ(ctx, msg, fmt.Errorf("%w: %w", domain.ErrPanic, panicErr)); dlqErr != nil {
				platform.LoggerWithTrace(ctx, m.logger).Error(platform.LogEventPanicDLQWriteFailed, zap.Error(dlqErr))
			}
			err = nil
			_ = panicErr
		}
	}()

	if err := processFn(); err != nil {
		if domain.IsTerminalError(err) {
			// Check if this is a cross-shard message
			isXShard := msg.Topic != platform.TopicJobs

			if isXShard {
				platform.LoggerWithTrace(ctx, m.logger).Warn(platform.LogEventCrossShardTerminalDLQ,
					zap.Error(err),
					zap.String(platform.LogFieldTopic, msg.Topic),
					zap.ByteString(platform.LogFieldKey, msg.Key),
				)
				if dlqErr := m.routeToDLQ(ctx, msg, fmt.Errorf("%w: %w", domain.ErrCrossShardTerminal, err)); dlqErr != nil {
					platform.LoggerWithTrace(ctx, m.logger).Error(platform.LogEventCrossShardDLQWriteFailed, zap.Error(dlqErr))
				}
				return nil // Offset committed, no backoff
			}

			//Local transfer terminal error -> record failure, commit offset, NO DLQ
			return nil // Offset will be committed, no backoff
		}

		// Infrastructure error (non-terminal) - bubble up for backoff
		platform.RecordInfrastructureError(ctx, platform.ComponentConsumerProcessing)
		return err
	}

	return nil
}

// routeToDLQ writes a message to the dead letter queue with full context.
func (m *ConsumerManager) routeToDLQ(ctx context.Context, msg kafka.Message, reasonErr error) error {
	// 1. Classify error
	classification := platform.ClassifyError(reasonErr, domain.IsTerminalError)

	// 2. Extract trace context from Kafka headers
	traceID, spanID := platform.ExtractTraceFromMessageHeaders(&msg)

	// 3. Extract shard ID from message payload
	shardID := m.pools.GetAvailableShardIDs()[0]
	var envelope eventsv1.EventEnvelope
	if err := proto.Unmarshal(msg.Value, &envelope); err == nil {
		switch {
		case envelope.GetJobRequested() != nil:
			merchantID := envelope.GetJobRequested().MerchantId
			if s, err := m.directory.ShardFor(ctx, merchantID); err == nil {
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

	// 4. PII mask the payload before storage and Create DLQ entry
	maskedPayload := pii.Mask(msg.Value)
	entry := domain.DLQEntry{
		ID:                  platform.NewEventID(),
		Source:              domain.DLQSourceLedger,
		OriginalPayload:     maskedPayload,
		ErrorMessage:        fmt.Sprintf("[%s] %s", classification, reasonErr.Error()),
		ErrorClassification: string(classification),
		AttemptCount:        0, // Will be incremented by caller if needed
		FirstFailedAt:       msg.Time,
		LastFailedAt:        time.Now(),
		Status:              domain.DLQStatusOpen,
		TraceID:             traceID,
		SpanID:              spanID,
	}

	// Retry DLQ write with exponential backoff
	var lastErr error
	for i := 0; i < domain.ConsumerDLQMaxRetries; i++ {
		if err := m.dlqStore.WriteDLQEntry(ctx, shardID, entry); err != nil {
			lastErr = err
			platform.LoggerWithTrace(ctx, m.logger).Error(platform.LogEventDLQRetryFailed, zap.Error(err), zap.Int(platform.LogFieldAttempt, i+1), zap.String(platform.LogFieldTopic, msg.Topic))
			delay := platform.CalculateJitterBackoff(i, domain.ConsumerDLQRetryBaseDelay, domain.ConsumerDLQMaxBackoff)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
			continue
		}
		platform.RecordDLQIngestion(ctx, platform.ServiceNameLedgerWorker)
		return nil
	}

	platform.LoggerWithTrace(ctx, m.logger).Error(platform.LogEventDLQWriteExhausted, zap.Error(lastErr), zap.String(platform.LogFieldTopic, msg.Topic), zap.ByteString(platform.LogFieldKey, msg.Key))
	return lastErr
}
