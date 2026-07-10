package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/app"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/ports"
	"github.com/segmentio/kafka-go"
	"github.com/sony/gobreaker"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
)

type ConsumerManager struct {
	logger        *zap.Logger
	jobReader     *kafka.Reader
	xshardReaders []*kafka.Reader
	processor     *app.Processor
	xshardService *app.XShardService
	dlqStore      ports.DLQStore
	pools         *platform.ShardPools

	cb          *gobreaker.CircuitBreaker
	cbOpenCount metric.Int64Counter
	cbHalfFail  metric.Int64Counter
}

func NewConsumerManager(
	logger *zap.Logger,
	jobReader *kafka.Reader,
	xshardReaders []*kafka.Reader,
	processor *app.Processor,
	xshardService *app.XShardService,
	dlqStore ports.DLQStore,
	pools *platform.ShardPools,
) *ConsumerManager {
	meter := otel.Meter("rrq.ledger")
	cbOpenCount, _ := meter.Int64Counter("rrq_circuit_breaker_open_total")
	cbHalfFail, _ := meter.Int64Counter("rrq_circuit_breaker_half_open_failure")

	cb := platform.NewCircuitBreaker(platform.CircuitBreakerConfig{
		Name:        "PostgresDB",
		MaxRequests: 1,               // Number of requests allowed in Half-Open state
		Timeout:     5 * time.Second, // Time in Open state before transitioning to Half-Open
		MaxFails:    3,
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			logger.Warn("Circuit Breaker State Changed", zap.String("name", name), zap.String("from", from.String()), zap.String("to", to.String()))
			if to == gobreaker.StateOpen {
				cbOpenCount.Add(context.Background(), 1)
			}
			if from == gobreaker.StateHalfOpen && to == gobreaker.StateOpen {
				cbHalfFail.Add(context.Background(), 1)
			}
		},
	})

	return &ConsumerManager{
		logger:        logger,
		jobReader:     jobReader,
		xshardReaders: xshardReaders,
		processor:     processor,
		xshardService: xshardService,
		dlqStore:      dlqStore,
		pools:         pools,
		cb:            cb,
		cbOpenCount:   cbOpenCount,
		cbHalfFail:    cbHalfFail,
	}
}

func (m *ConsumerManager) Start(ctx context.Context) {
	go m.consumeJobs(ctx)
	for _, r := range m.xshardReaders {
		go m.consumeXShard(ctx, r)
	}
}

func (m *ConsumerManager) checkCircuitBreaker(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		_, err := m.cb.Execute(func() (interface{}, error) {
			allDown := true
			for _, shardID := range m.pools.GetAvailableShardIDs() {
				pool, err := m.pools.ShardPool(shardID)
				if err == nil && pool.Ping(ctx) == nil {
					allDown = false
					break
				}
			}
			if allDown {
				return nil, fmt.Errorf("all databases unreachable")
			}
			return nil, nil
		})

		if err != nil {
			if err == gobreaker.ErrOpenState || err == gobreaker.ErrTooManyRequests {
				time.Sleep(1 * time.Second)
			} else {
				time.Sleep(500 * time.Millisecond)
			}
		} else {
			return // DB is up, proceed
		}
	}
}

// poll for job requested events and process them
func (m *ConsumerManager) consumeJobs(ctx context.Context) {
	for {
		m.checkCircuitBreaker(ctx)

		msg, err := m.jobReader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			m.logger.Error("failed to fetch job message", zap.Error(err))
			continue
		}

		var envelope eventsv1.EventEnvelope
		if err := protojson.Unmarshal(msg.Value, &envelope); err != nil {
			m.logger.Error("failed to unmarshal job event (poison message)", zap.Error(err))
			if dlqErr := m.routeToDLQ(ctx, msg, fmt.Sprintf("Unmarshal error: %v", err)); dlqErr != nil {
				m.logger.Fatal("Failed to write poison message to DLQ. Crashing to prevent data loss", zap.Error(dlqErr))
			}
			m.jobReader.CommitMessages(ctx, msg)
			continue
		}

		if payload := envelope.GetJobRequested(); payload != nil {
			if err := m.processWithRetry(ctx, msg, func() error {
				return m.processor.ProcessJob(ctx, payload)
			}); err != nil {
				m.logger.Fatal("Failed to process message and write to DLQ. Crashing to prevent data loss", zap.Error(err))
			}
		}

		// Consumer ACK the message to prevent redelivery
		if err := m.jobReader.CommitMessages(ctx, msg); err != nil {
			m.logger.Error("failed to commit job message", zap.Error(err))
		}
	}
}

// poll for xshard sagas
func (m *ConsumerManager) consumeXShard(ctx context.Context, r *kafka.Reader) {
	for {
		m.checkCircuitBreaker(ctx)

		msg, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			m.logger.Error("failed to fetch xshard message", zap.Error(err))
			continue
		}

		var envelope eventsv1.EventEnvelope
		if err := protojson.Unmarshal(msg.Value, &envelope); err != nil {
			m.logger.Error("failed to unmarshal xshard event (poison message)", zap.Error(err))
			if dlqErr := m.routeToDLQ(ctx, msg, fmt.Sprintf("Unmarshal error: %v", err)); dlqErr != nil {
				m.logger.Fatal("Failed to write poison message to DLQ. Crashing to prevent data loss", zap.Error(dlqErr))
			}
			r.CommitMessages(ctx, msg)
			continue
		}

		if err := m.processWithRetry(ctx, msg, func() error {
			switch {
			case envelope.GetXshardTransferRequested() != nil:
				return m.xshardService.HandleXShardRequested(ctx, envelope.GetXshardTransferRequested())
			case envelope.GetXshardTransferSettled() != nil:
				return m.xshardService.HandleXShardSettled(ctx, envelope.GetXshardTransferSettled())
			case envelope.GetXshardTransferFailed() != nil:
				return m.xshardService.HandleXShardFailed(ctx, envelope.GetXshardTransferFailed())
			}
			return fmt.Errorf("unknown xshard payload")
		}); err != nil {
			m.logger.Fatal("Failed to process xshard message and write to DLQ. Crashing to prevent data loss", zap.Error(err))
		}

		if err := r.CommitMessages(ctx, msg); err != nil {
			m.logger.Error("failed to commit xshard job message", zap.Error(err))
		}
	}
}

// processWithRetry executes the given function with strict inline exponential backoff and jitter.
// If it exhausts the retry budget or encounters a poison message, it writes to the DLQ.
func (m *ConsumerManager) processWithRetry(ctx context.Context, msg kafka.Message, processFn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			m.logger.Error("Recovered from panic, routing to DLQ", zap.Any("panic", r))
			panicErr := fmt.Errorf("panic during processing: %v", r)
			if dlqErr := m.routeToDLQ(ctx, msg, panicErr.Error()); dlqErr != nil {
				m.logger.Fatal("Failed to write poison panic message to DLQ. Crashing to prevent data loss", zap.Error(dlqErr))
			}
			err = nil // Successfully routed to DLQ, so we return nil to allow offset commit
		}
	}()

	err = platform.ExecuteWithJitter(ctx, platform.RetryConfig{
		MaxRetries: 10,
		BaseDelay:  50 * time.Millisecond,
		MaxDelay:   1 * time.Second,
	}, platform.IsTransientError, func() error {
		processErr := processFn()
		if processErr != nil {
			m.logger.Warn("Transient error processing message, strict blocking retry triggered",
				zap.Error(processErr),
				zap.String("topic", msg.Topic),
				zap.ByteString("key", msg.Key),
			)
		}
		return processErr
	})

	if err != nil {
		m.logger.Error("Exhausted inline retry budget, routing to DLQ as poison pill",
			zap.Error(err),
			zap.String("topic", msg.Topic),
			zap.ByteString("key", msg.Key),
		)
		return m.routeToDLQ(ctx, msg, fmt.Sprintf("Exhausted retries. Last error: %v", err))
	}

	return nil
}

func (m *ConsumerManager) routeToDLQ(ctx context.Context, msg kafka.Message, reason string) error {
	platform.RecordDLQIngestion(ctx, platform.ServiceNameLedgerWorker)

	// Attempt to extract shardID. If we can't find one, fallback to a default shard.
	shardID := m.pools.GetAvailableShardIDs()[0]

	var envelope eventsv1.EventEnvelope
	if err := protojson.Unmarshal(msg.Value, &envelope); err == nil {
		var merchantID string
		if payload := envelope.GetJobRequested(); payload != nil {
			merchantID = payload.MerchantId
		} else if payload := envelope.GetXshardTransferRequested(); payload != nil {
			merchantID = payload.MerchantId
		}

		if merchantID != "" {
			if s, err := m.processor.Directory().ShardFor(ctx, merchantID); err == nil {
				shardID = s
			}
		}
	}

	// Ensure payload is valid JSON for Postgres JSONB (in case of malformed protobuf bytes)
	var originalPayload []byte
	if json.Valid(msg.Value) {
		originalPayload = msg.Value
	} else {
		wrapped := map[string]interface{}{
			"error": "invalid_json_bytes",
			"raw":   msg.Value,
		}
		originalPayload, _ = json.Marshal(wrapped)
	}

	var traceID, spanID string
	for _, h := range msg.Headers {
		if h.Key == "traceparent" {
			// W3C traceparent format: 00-traceID-spanID-flags
			parts := strings.Split(string(h.Value), "-")
			if len(parts) >= 3 {
				traceID = parts[1]
				spanID = parts[2]
			}
			break
		}
	}

	entry := domain.DLQEntry{
		ID:              string(msg.Key), // Use kafka key as DLQ ID
		Source:          domain.DLQSourceLedger,
		OriginalPayload: originalPayload,
		ErrorMessage:    reason,
		AttemptCount:    10, // Assuming it exhausted the inline budget
		TraceID:         traceID,
		SpanID:          spanID,
		FirstFailedAt:   msg.Time,
		LastFailedAt:    time.Now(),
		Status:          domain.DLQStatusOpen,
	}

	if entry.ID == "" {
		entry.ID = platform.NewEventID()
	}

	if err := m.dlqStore.WriteDLQEntry(ctx, shardID, entry); err != nil {
		m.logger.Error("Failed to write to DLQ. Message permanently dropped!", zap.Error(err), zap.String("topic", msg.Topic), zap.ByteString("key", msg.Key))
		return err
	}
	return nil
}
