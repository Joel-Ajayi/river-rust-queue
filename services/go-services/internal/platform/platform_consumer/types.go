package platform_consumer

import (
	"context"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// ConsumerTask is a unit of work for the shared pipeline.
type ConsumerTask struct {
	Msg     kafka.Message
	Process func(ctx context.Context) error
	Ctx     context.Context // traced context propagated from Fetcher
}

// ConsumerResult is reported by workers to the CommitCoordinator.
type ConsumerResult struct {
	Partition int
	Offset    int64
	Err       error
	Msg       kafka.Message
}

type MessageHandler func(ctx context.Context, msg kafka.Message) error

// DLQRouteFn routes a message to the dead-letter queue. .
type DLQRouteFn func(ctx context.Context, msg kafka.Message, reason error) error

// ConsumerConfig configures the shared pipeline.
type ConsumerConfig struct {
	WorkerPoolSize int           // Little's Law: L = λ_pod × W_p95
	ProcessTimeout time.Duration // per-message processing deadline
	Logger         *zap.Logger   // logger for pipeline-internal events

	// Engine-derived runtime (platform configmap)
	MinCommitCapPer        float64       // minimum commit batch capacity fraction to trigger commit
	ChannelRefreshInterval time.Duration // channel refresh interval
	DrainTimeout           time.Duration // drain on shutdown
	CommitFlushTimeout     time.Duration // offset flush on shutdown
	CommitFlushInterval    time.Duration // periodic background offset flush
	CommitBatchCapacity    int           // commit batch capacity
	PartitionChannelSize   int           // partition channel capacity

	// OnPanicDLQ routes a message to the DLQ on worker panic OR when
	// processing returns an error after retries are exhausted. If nil, those
	// messages are only reported to the commit coordinator (legacy behavior).
	OnPanicDLQ DLQRouteFn

	// DLQWriteTimeout is the hard outer deadline for DLQ writes (panic
	// recovery or retry-exhausted path). Engine-derived: == SessionMs.
	DLQWriteTimeout time.Duration

	// MaxPendingBytes caps the cumulative byte size of all messages buffered
	// in the task channels. The fetcher will block when the cap is hit.
	// 0 means no cap (legacy behavior).
	MaxPendingBytes int64

	// SharedSemaphore is an optional shared concurrency limiter. When non-nil,
	// multiple pipelines in the same service share a single semaphore so total
	// concurrency is bounded by WorkerPoolSize across all topics (not per
	// pipeline). This prevents N× over-subscription when a service consumes
	// multiple Kafka topics.
	SharedSemaphore chan struct{}
}

// NewConsumerConfigFromCapacity builds the pipeline config from platform-derived bounds.
func NewConsumerConfigFromCapacity(cfg *platform.Config) ConsumerConfig {
	g := cfg.GlobalCapacity
	return ConsumerConfig{
		WorkerPoolSize:         int(cfg.Capacity.WorkerPoolSize),
		ProcessTimeout:         time.Duration(cfg.Capacity.RequestTimeoutMs) * time.Millisecond,
		MinCommitCapPer:        g.ConsumerMinCommitCapFrac,
		ChannelRefreshInterval: time.Duration(g.ConsumerChannelRefreshMs) * time.Millisecond,
		DrainTimeout:           time.Duration(g.ConsumerDrainTimeoutMs) * time.Millisecond,
		CommitFlushTimeout:     time.Duration(g.ConsumerCommitFlushTimeoutMs) * time.Millisecond,
		CommitFlushInterval:    time.Duration(g.ConsumerCommitFlushIntervalMs) * time.Millisecond,
		CommitBatchCapacity:    g.ConsumerCommitBatchCapacity,
		PartitionChannelSize:   g.ConsumerPartitionChannelSize,
		DLQWriteTimeout:        time.Duration(cfg.Capacity.DLQWriteTimeoutMs) * time.Millisecond,
	}
}

// ConsumerReader is the Kafka reader interface for the pipeline.
type ConsumerReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}
