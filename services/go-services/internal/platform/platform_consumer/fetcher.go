package platform_consumer

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// Fetcher fetches messages and routes them to per-partition task channels.
type Fetcher struct {
	reader             ConsumerReader
	coordinator        *CommitCoordinator
	handler            MessageHandler
	config             ConsumerConfig
	taskChans          map[partitionKey]*PartitionTaskChan
	taskChanMu         sync.RWMutex
	notifyChannelAdded chan struct{}
	logger             *zap.Logger
	pendingBytes       atomic.Int64 // approximate sum of bytes pending in all task channels
}

type partitionKey struct {
	Topic     string
	Partition int
}

// PartitionTaskChan bundles a task channel with its partition's topic.
type PartitionTaskChan struct {
	Chan      chan *ConsumerTask
	Partition int
	Topic     string
}

func NewFetcher(reader ConsumerReader, cc *CommitCoordinator, handler MessageHandler, cfg ConsumerConfig, logger *zap.Logger) *Fetcher {
	return &Fetcher{
		reader:             reader,
		coordinator:        cc,
		handler:            handler,
		config:             cfg,
		taskChans:          make(map[partitionKey]*PartitionTaskChan),
		logger:             logger,
		notifyChannelAdded: make(chan struct{}, 1), // prevent blocking if no one is listening
	}
}

func (f *Fetcher) getTaskChan(partition int, topic string) chan *ConsumerTask {
	f.taskChanMu.Lock()
	defer f.taskChanMu.Unlock()

	key := partitionKey{
		Topic:     topic,
		Partition: partition,
	}

	pc, ok := f.taskChans[key]
	if !ok {
		ch := make(chan *ConsumerTask, f.config.PartitionChannelSize)
		pc = &PartitionTaskChan{Chan: ch, Partition: partition, Topic: topic}
		f.taskChans[key] = pc

		// non-blocking send. If the channel is full
		select {
		case f.notifyChannelAdded <- struct{}{}:
		default:
		}
	}

	return pc.Chan
}

// TaskChans returns all partition task channel bundles with their
// partition and topic metadata.
func (f *Fetcher) TaskChans() []*PartitionTaskChan {
	f.taskChanMu.RLock()
	defer f.taskChanMu.RUnlock()
	var chans []*PartitionTaskChan
	for _, pc := range f.taskChans {
		chans = append(chans, pc)
	}
	return chans
}

// wrapHandler creates a Process function from the MessageHandler, propagating
// the traced context from the Fetcher into the worker goroutine.
func (f *Fetcher) wrapHandler(msg kafka.Message) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		return f.handler(ctx, msg)
	}
}

func (f *Fetcher) FetchMessage(ctx context.Context) (kafka.Message, error) {
	msg, err := f.reader.FetchMessage(ctx)
	if err != nil {
		if ctx.Err() == nil {
			platform.LoggerWithTrace(ctx, f.logger).Warn(platform.LogEventConsumerFetchRetry,
				zap.Error(err),
			)
			platform.RecordInfrastructureError(ctx, platform.ComponentConsumer)
		}
	}
	return msg, err
}

// Run fetches and routes messages. Blocks until ctx is cancelled.
func (f *Fetcher) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Fetch Message with deadline-bounded retry and backoff,
		// since the fetcher is the only source of messages for the worker pool.
		// Retries are capped by FetchRetryDeadline (derived from SessionMs).
		msg, err := f.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}

		// 2. Process the message in a worker goroutine.
		ch := f.getTaskChan(msg.Partition, msg.Topic)
		tracedCtx := platform.InjectTraceIntoContext(ctx, &msg)
		task := &ConsumerTask{
			Msg:     msg,
			Process: f.wrapHandler(msg),
			Ctx:     tracedCtx,
		}

		// push to channel, blocking if the channel is full (backpressure).
		// Also enforce a buffer-bytes cap to bound heap usage when large
		// payloads are in flight (see issue 35).
		msgBytes := int64(len(msg.Value))
		for {
			cap := f.config.MaxPendingBytes
			if cap > 0 && f.pendingBytes.Load()+msgBytes > cap {
				// Wait until workers drain enough that we can accept this message.
				// Short sleep, then recheck.
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(100 * time.Millisecond):
				}
				continue
			}
			select {
			case ch <- task:
				f.pendingBytes.Add(msgBytes)
			case <-ctx.Done():
				return ctx.Err()
			}
			break
		}
	}
}

// ReleasePendingBytes should be called by the worker pool after processing a
// task to decrement the cumulative pending-bytes counter on the Fetcher.
func (f *Fetcher) ReleasePendingBytes(n int64) {
	if n > 0 {
		f.pendingBytes.Add(-n)
	}
}
