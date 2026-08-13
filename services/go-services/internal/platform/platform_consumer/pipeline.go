package platform_consumer

import (
	"context"
	"errors"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"go.uber.org/zap"
)

// ConsumerPipeline orchestrates Fetcher + WorkerPool + CommitCoordinator.
type ConsumerPipeline struct {
	reader      ConsumerReader
	handler     MessageHandler
	config      ConsumerConfig
	logger      *zap.Logger
	coordinator *CommitCoordinator
	fetcher     *Fetcher
	workerPool  *WorkerPool
}

func NewConsumerPipeline(reader ConsumerReader, handler MessageHandler, cfg ConsumerConfig, logger *zap.Logger) *ConsumerPipeline {
	cc := NewCommitCoordinator(reader, cfg, logger)
	f := NewFetcher(reader, cc, handler, cfg, logger)
	cfg.Logger = logger
	wp := NewWorkerPool(cfg.WorkerPoolSize, cc, cfg.ProcessTimeout, cfg.DLQWriteTimeout, cfg.OnPanicDLQ, f, cfg.SharedSemaphore, logger)
	wp.UpdatePartitions(f.TaskChans())

	return &ConsumerPipeline{
		reader:      reader,
		handler:     handler,
		config:      cfg,
		logger:      logger,
		coordinator: cc,
		fetcher:     f,
		workerPool:  wp,
	}
}

// SetSharedSemaphore replaces the worker pool's internal semaphore with the
// given one, allowing multiple pipelines on the same service to share a single
// concurrency budget
func (p *ConsumerPipeline) SetSharedSemaphore(sem chan struct{}) {
	p.workerPool.SetSemaphore(sem)
}

// Consume runs the full pipeline. Implements Consumer interface.
func (p *ConsumerPipeline) Consume(ctx context.Context) error {
	p.coordinator.Start(ctx)
	defer p.coordinator.Shutdown(context.Background(), p.config.CommitFlushTimeout)

	go p.refreshChannels(ctx)
	go p.recordBackpressure(ctx)

	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		p.workerPool.Run(ctx)
	}()

	fetchErr := p.fetcher.Run(ctx)

	// Shutdown: drain workers and flush offsets.
	drainCtx, cancel := context.WithTimeout(context.Background(), p.config.DrainTimeout)
	defer cancel()

	// Flush any remaining offsets in the coordinator before waiting for workers to finish.
	p.coordinator.Flush(drainCtx)

	select {
	case <-workerDone:
	case <-drainCtx.Done():
		platform.LoggerWithTrace(ctx, p.logger).Warn(platform.LogEventConsumerDrainTimeout,
			zap.Duration(platform.LogFieldPollMs, p.config.DrainTimeout),
		)
	}

	if fetchErr != nil && !errors.Is(fetchErr, context.Canceled) {
		return fetchErr
	}
	return nil
}

// update the worker pool channels periodically to account for new partitions being assigned or revoked.
func (p *ConsumerPipeline) refreshChannels(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.fetcher.notifyChannelAdded:
			p.workerPool.UpdatePartitions(p.fetcher.TaskChans())
		}
	}
}

// recordBackpressure periodically samples task channel fill ratios and
// commit coordinator queue depths for backpressure monitoring.
func (p *ConsumerPipeline) recordBackpressure(ctx context.Context) {
	ticker := time.NewTicker(p.config.ChannelRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, pc := range p.fetcher.TaskChans() {
				if pc == nil || pc.Chan == nil {
					continue
				}
				ratio := float64(len(pc.Chan)) / float64(cap(pc.Chan))
				platform.RecordTaskChannelFillRatio(ctx, pc.Topic, pc.Partition, ratio)
			}
			p.coordinator.mu.RLock()
			for _, ps := range p.coordinator.partitions {
				ps.mu.Lock()
				depth := int64(len(ps.uncommitted))
				ps.mu.Unlock()
				platform.RecordCommitCoordinatorQueueDepth(ctx, ps.topic, ps.partition, depth)
			}
			p.coordinator.mu.RUnlock()
		}
	}
}
