package platform_consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.uber.org/zap"
)

// WorkerPool runs per-partition sequential goroutines, bounded by a
// counting semaphore of capacity `concurrency`.
type WorkerPool struct {
	concurrency     int
	coordinator     *CommitCoordinator
	processTimeout  time.Duration
	dlqWriteTimeout time.Duration // outer DLQ write deadline (engine-derived, == SessionMs)
	sem             chan struct{} // counting semaphore, capacity = concurrency
	onPanicDLQ      DLQRouteFn    // routes panicking messages to the DLQ
	logger          *zap.Logger   // logger for panic-recovery events
	fetcher         *Fetcher      // used to release pending-bytes after processing (issue 35)

	mu      sync.Mutex
	ctx     context.Context                     // set by Run
	active  map[partitionKey]context.CancelFunc // running partition goroutines
	pending map[partitionKey]*PartitionTaskChan // buffered before Run is called
	wg      sync.WaitGroup                      // tracks all partition goroutines
}

func NewWorkerPool(concurrency int, cc *CommitCoordinator, processTimeout time.Duration, dlqWriteTimeout time.Duration, onPanicDLQ DLQRouteFn, fetcher *Fetcher, sharedSem chan struct{}, logger *zap.Logger) *WorkerPool {
	sem := sharedSem
	if sem == nil {
		sem = make(chan struct{}, concurrency)
	}
	return &WorkerPool{
		concurrency:     concurrency,
		coordinator:     cc,
		processTimeout:  processTimeout,
		dlqWriteTimeout: dlqWriteTimeout,
		sem:             sem,
		onPanicDLQ:      onPanicDLQ,
		fetcher:         fetcher,
		logger:          logger,
		active:          make(map[partitionKey]context.CancelFunc),
		pending:         make(map[partitionKey]*PartitionTaskChan),
	}
}

// SetSemaphore replaces the pool's internal semaphore with the given one.
// Used by SetSharedSemaphore to share concurrency across pipelines.
func (wp *WorkerPool) SetSemaphore(sem chan struct{}) {
	wp.sem = sem
}

// UpdatePartitions diffs incoming partition channels against active goroutines.
// New partitions get a dedicated sequential goroutine; removed partitions are cancelled.
// Safe to call before or after Run.
func (wp *WorkerPool) UpdatePartitions(chans []*PartitionTaskChan) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	incoming := make(map[partitionKey]*PartitionTaskChan, len(chans))
	for _, pc := range chans {
		key := partitionKey{Topic: pc.Topic, Partition: pc.Partition}
		incoming[key] = pc
	}

	if wp.ctx == nil {
		// Run hasn't started yet — buffer for later.
		wp.pending = incoming
		return
	}

	wp.syncLocked(incoming)
}

// Run stores the pipeline context, starts goroutines for any buffered
// partitions, and blocks until ctx is cancelled and all goroutines drain.
func (wp *WorkerPool) Run(ctx context.Context) {
	wp.mu.Lock()
	wp.ctx = ctx
	if len(wp.pending) > 0 {
		wp.syncLocked(wp.pending)
		wp.pending = nil
	}
	wp.mu.Unlock()

	// Block until the pipeline shuts down.
	<-ctx.Done()

	wp.wg.Wait()
}

// syncLocked starts/stops partition goroutines to match the incoming set.
func (wp *WorkerPool) syncLocked(incoming map[partitionKey]*PartitionTaskChan) {
	// Spawn goroutines for new partitions.
	for key, pc := range incoming {
		if _, exists := wp.active[key]; exists {
			continue
		}
		loopCtx, cancel := context.WithCancel(wp.ctx)
		wp.active[key] = cancel
		wp.wg.Add(1)
		go wp.partitionLoop(loopCtx, pc)
	}

	// Cancel goroutines for removed partitions (defensive — revocations).
	for key, cancel := range wp.active {
		if _, exists := incoming[key]; !exists {
			cancel()
			delete(wp.active, key)
		}
	}
}

// partitionLoop drains a single partition channel sequentially.
// It acquires a semaphore slot before processing each message, ensuring
// bounded total concurrency.
func (wp *WorkerPool) partitionLoop(ctx context.Context, pc *PartitionTaskChan) {
	defer wp.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case task, ok := <-pc.Chan:
			if !ok {
				return
			}

			// Acquire semaphore — blocks if all concurrency slots are taken.
			select {
			case wp.sem <- struct{}{}:
			case <-ctx.Done():
				return
			}

			wp.processTask(task)

			// Release semaphore slot.
			<-wp.sem

			// Decrement the Fetcher's pending-bytes counter so the next
			// blocked fetch can proceed. (See issue 35.)
			if wp.fetcher != nil {
				wp.fetcher.ReleasePendingBytes(int64(len(task.Msg.Value)))
			}
		}
	}
}

// processTask executes a single task with tracing, timeout, and panic recovery.
func (wp *WorkerPool) processTask(task *ConsumerTask) {
	spanCtx, span := platform.GetTracer().Start(task.Ctx, platform.SpanProcessMessage)
	defer span.End()
	span.SetAttributes(
		attribute.String(platform.MetricLabelTopic, task.Msg.Topic),
		attribute.Int(platform.MetricLabelPartition, task.Msg.Partition),
		attribute.Int64(platform.LogFieldOffset, task.Msg.Offset),
	)

	defer func() {
		if r := recover(); r != nil {
			panicErr := fmt.Errorf("%w: %v", platform.ErrConsumerPanic, r)
			span.RecordError(panicErr)
			span.SetStatus(codes.Error, fmt.Sprintf("%v", r))
			platform.RecordConsumerPanic(spanCtx, task.Msg.Topic)
			// Route the message to the DLQ first so the poison event is preserved
			// even if the commit-coordinator behavior changes (see issue 2).
			if wp.onPanicDLQ != nil {
				// Outer DLQ write deadline (engine-derived, == SessionMs).
				dlqCtx, dlqCancel := context.WithTimeout(context.Background(), wp.dlqWriteTimeout)
				if dlqErr := wp.onPanicDLQ(dlqCtx, task.Msg, panicErr); dlqErr != nil {
					platform.LoggerWithTrace(spanCtx, wp.logger).Error(
						platform.LogEventDLQWriteFailed,
						zap.Error(dlqErr),
						zap.String(platform.MetricLabelTopic, task.Msg.Topic),
						zap.Int(platform.MetricLabelPartition, task.Msg.Partition),
						zap.Int64(platform.LogFieldOffset, task.Msg.Offset),
					)
				}
				dlqCancel()
			}
			wp.coordinator.Report(ConsumerResult{
				Partition: task.Msg.Partition,
				Offset:    task.Msg.Offset,
				Err:       panicErr,
				Msg:       task.Msg,
			})
		}
	}()

	processCtx, cancel := context.WithTimeout(spanCtx, wp.processTimeout)
	defer cancel()
	err := task.Process(processCtx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		// Issue 2 (Option B): the handler returned an error after retries were
		// exhausted. Route the message to the DLQ before reporting to the commit
		// coordinator so the poison event is preserved (at-least-once
		// delivery: the offset will still be committed, but the message is
		// not lost).
		if wp.onPanicDLQ != nil {
			// Outer DLQ write deadline (engine-derived, == SessionMs).
			// A fresh context (not processCtx) so a tight ProcessTimeout
			// doesn't kill a DLQ write already in progress.
			dlqCtx, dlqCancel := context.WithTimeout(context.Background(), wp.dlqWriteTimeout)
			if dlqErr := wp.onPanicDLQ(dlqCtx, task.Msg, err); dlqErr != nil {
				platform.LoggerWithTrace(spanCtx, wp.logger).Error(
					platform.LogEventDLQWriteFailed,
					zap.Error(dlqErr),
					zap.String(platform.MetricLabelTopic, task.Msg.Topic),
					zap.Int(platform.MetricLabelPartition, task.Msg.Partition),
					zap.Int64(platform.LogFieldOffset, task.Msg.Offset),
				)
			}
			dlqCancel()
		}
	}
	wp.coordinator.Report(ConsumerResult{
		Partition: task.Msg.Partition,
		Offset:    task.Msg.Offset,
		Err:       err,
		Msg:       task.Msg,
	})
}
