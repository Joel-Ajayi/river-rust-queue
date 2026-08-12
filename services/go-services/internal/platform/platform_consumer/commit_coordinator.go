package platform_consumer

import (
	"context"
	"sync"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// partitionState tracks messages ready to be committed per partition.
type partitionState struct {
	mu          sync.Mutex
	partition   int
	topic       string
	uncommitted []kafka.Message
}

// CommitCoordinator provides per-partition FIFO offset commits.
type CommitCoordinator struct {
	mu            sync.RWMutex
	partitions    map[int]*partitionState
	reader        ConsumerReader
	resultsCh     chan ConsumerResult
	stopCh        chan struct{}
	done          chan struct{}
	logger        *zap.Logger
	batchCap      int
	minCapFrac    float64
	flushInterval time.Duration
}

func NewCommitCoordinator(reader ConsumerReader, cfg ConsumerConfig, logger *zap.Logger) *CommitCoordinator {
	return &CommitCoordinator{
		partitions:    make(map[int]*partitionState),
		reader:        reader,
		resultsCh:     make(chan ConsumerResult, cfg.CommitBatchCapacity),
		stopCh:        make(chan struct{}),
		done:          make(chan struct{}),
		logger:        logger,
		batchCap:      cfg.CommitBatchCapacity,
		minCapFrac:    cfg.MinCommitCapPer,
		flushInterval: cfg.CommitFlushInterval,
	}
}

func (c *CommitCoordinator) partitionState(p int, topic string) *partitionState {
	c.mu.Lock()
	defer c.mu.Unlock()
	ps, ok := c.partitions[p]
	if !ok {
		ps = &partitionState{
			partition:   p,
			topic:       topic,
			uncommitted: make([]kafka.Message, 0, c.batchCap),
		}
		c.partitions[p] = ps

	}
	return ps
}

// Report sends a result to the coordinator.
func (c *CommitCoordinator) Report(r ConsumerResult) {
	c.resultsCh <- r
}

func (c *CommitCoordinator) processResults(ctx context.Context) {
	defer close(c.done)

	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.Flush(ctx)
		case r := <-c.resultsCh:
			c.handleResult(ctx, r)
		}
	}
}

// commit flushes all ready messages for a partition.
func (c *CommitCoordinator) commit(ctx context.Context, ps *partitionState) {
	if len(ps.uncommitted) == 0 {
		return
	}

	toCommit := ps.uncommitted

	if err := c.reader.CommitMessages(ctx, toCommit...); err != nil {
		platform.LoggerWithTrace(ctx, c.logger).Error(platform.LogEventConsumerCommitFailed,
			zap.Error(err),
			zap.Int(platform.LogFieldPartition, ps.partition),
			zap.Int64(platform.LogFieldOffset, toCommit[len(toCommit)-1].Offset),
			zap.Int(platform.LogFieldCount, len(toCommit)),
		)
	} else {
		topics := make(map[string]struct{})
		for _, msg := range toCommit {
			topics[msg.Topic] = struct{}{}
		}
		platform.RecordConsumerCommitWithPartition(ctx, topics, ps.partition)
	}

	// Reset slice, retaining capacity
	ps.uncommitted = ps.uncommitted[:0]
}

func (c *CommitCoordinator) handleResult(ctx context.Context, r ConsumerResult) {
	ps := c.partitionState(r.Partition, r.Msg.Topic)
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.uncommitted = append(ps.uncommitted, r.Msg)
	minCommitLen := int(c.minCapFrac * float64(c.batchCap))
	if len(ps.uncommitted) > minCommitLen {
		c.commit(ctx, ps)
	}
}

// Flush commits all remaining uncommitted offsets.
func (c *CommitCoordinator) Flush(ctx context.Context) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, ps := range c.partitions {
		ps.mu.Lock()
		c.commit(ctx, ps)
		ps.mu.Unlock()
	}
}

func (c *CommitCoordinator) Start(ctx context.Context) {
	go c.processResults(ctx)
}

func (c *CommitCoordinator) Shutdown(ctx context.Context, flushTimeout time.Duration) {
	close(c.stopCh)

	shutdownCtx, cancel := context.WithTimeout(ctx, flushTimeout)
	defer cancel()

	select {
	case <-c.done:
	case <-shutdownCtx.Done():
	}
	// Drain any remaining results from the buffer.
	for {
		select {
		case r := <-c.resultsCh:
			c.handleResult(shutdownCtx, r)
		default:
			c.Flush(shutdownCtx)
			return
		}
	}
}
