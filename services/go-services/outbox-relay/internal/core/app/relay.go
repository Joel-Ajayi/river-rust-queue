package app

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/port"
	"go.uber.org/zap"
)

type RelayService struct {
	store          port.EventStore
	publisher      port.EventPublisher
	log            *zap.Logger
	draining       chan struct{}
	wakeCh         chan struct{}
	shardID        string
	pollInterval   atomic.Int64 // nanoseconds; adjusted by the Kafka buffer monitor
	processTimeout time.Duration
	fetchBatchSize int
	maxPayloadSize int
}

type RelayServiceConfig struct {
	ProcessTimeout time.Duration
	FetchBatchSize int
	PollInterval   time.Duration
	MaxPayloadSize int
}

func NewRelayService(store port.EventStore, publisher port.EventPublisher, log *zap.Logger, shardID string, cfg RelayServiceConfig) *RelayService {
	s := &RelayService{
		store:          store,
		publisher:      publisher,
		log:            log,
		draining:       make(chan struct{}),
		wakeCh:         make(chan struct{}, 1),
		shardID:        shardID,
		processTimeout: cfg.ProcessTimeout,
		fetchBatchSize: cfg.FetchBatchSize,
		maxPayloadSize: cfg.MaxPayloadSize,
	}
	s.pollInterval.Store(int64(cfg.PollInterval))
	return s
}

// SetDraining signals the relay to stop starting new polls after the current batch completes.
func (s *RelayService) SetDraining() {
	close(s.draining)
}

// SetPollInterval updates the poll interval (used by the Kafka buffer monitor).
func (s *RelayService) SetPollInterval(d time.Duration) {
	s.pollInterval.Store(int64(d))
	select {
	case s.wakeCh <- struct{}{}:
	default:
	}
}

// ShardID returns the shard this relay is responsible for.
func (s *RelayService) ShardID() string {
	return s.shardID
}

func (s *RelayService) Start(ctx context.Context, shardID string) error {
	platform.LoggerWithTrace(ctx, s.log).Info(platform.LogEventRelayServiceStarted, zap.String(platform.LogFieldShardID, shardID))

	timer := time.NewTimer(time.Duration(s.pollInterval.Load()))
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			platform.LoggerWithTrace(ctx, s.log).Info(platform.LogEventRelayServiceShutdown, zap.String(platform.LogFieldShardID, shardID))
			return nil
		case <-s.draining:
			platform.LoggerWithTrace(ctx, s.log).Info(platform.LogEventRelayServiceDraining, zap.String(platform.LogFieldShardID, shardID))
			return nil
		case <-s.wakeCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(time.Duration(s.pollInterval.Load()))
		case <-timer.C:
			start := time.Now()
			if err := s.processBatch(ctx, shardID); err != nil {
				s.log.Error(platform.LogEventRelayBatchProcessFailed, zap.String(platform.LogFieldShardID, shardID), zap.Error(err))
			}
			elapsed := time.Since(start)
			interval := time.Duration(s.pollInterval.Load())
			nextDelay := interval - elapsed
			if nextDelay < 0 {
				nextDelay = 0
			}
			timer.Reset(nextDelay)
		}
	}
}

func (s *RelayService) processBatch(ctx context.Context, shardID string) (err error) {
	batchCtx, cancel := context.WithTimeout(ctx, s.processTimeout)
	defer cancel()

	// Panic recovery at the batch level — `GetOldestUnpublishedEventAge` runs
	// BEFORE the per-event panic recovery in processEvents can engage, so a
	// panic here would propagate up to Start's error log without any DLQ.
	// We count the panic via RecordOutboxPanic so it shows up in alerting
	defer func() {
		if r := recover(); r != nil {
			platform.RecordOutboxPanic(batchCtx, shardID)
			platform.LoggerWithTrace(batchCtx, s.log).Error(platform.LogEventPanicRecoveredDLQ,
				zap.String(platform.LogFieldShardID, shardID),
				zap.Any(platform.LogFieldPanic, r))
			err = fmt.Errorf("panic in processBatch: %v", r)
		}
	}()

	// Record outbox lag metric.
	lag, lagErr := s.store.GetOldestUnpublishedEventAge(batchCtx, shardID)
	if lagErr != nil {
		return lagErr
	}
	platform.RecordOutboxLag(batchCtx, shardID, lag)

	// Process a batch of unpublished events within the transaction.
	return s.store.ProcessUnpublishedEvents(batchCtx, shardID, s.fetchBatchSize, s.processEvents)
}
