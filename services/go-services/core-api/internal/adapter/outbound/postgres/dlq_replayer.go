package postgres

import (
	"context"
	"fmt"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type dlqReplayer struct {
	cfg     *platform.Config
	pools   *platform.ShardPools
	writers map[string]*kafka.Writer
	logger  *zap.Logger
}

// NewDLQReplayer creates a Postgres-backed DLQReplayer adapter.
func NewDLQReplayer(cfg *platform.Config, pools *platform.ShardPools, logger *zap.Logger) *dlqReplayer {
	return &dlqReplayer{
		cfg:     cfg,
		pools:   pools,
		writers: make(map[string]*kafka.Writer),
		logger:  logger,
	}
}

// selectPool always returns the global merchants DB where all DLQ entries now reside.
func (r *dlqReplayer) selectPool() (*pgxpool.Pool, error) {
	pool := r.pools.MerchantsPool()
	if pool == nil {
		return nil, platform.ErrMerchantsPoolNotInitialized
	}
	return pool, nil
}

// ReplayDLQ republishes open DLQ entries to their original Kafka topics (global
// merchants DB for webhook, shard DB otherwise), via source_topic or fallback.
func (r *dlqReplayer) ReplayDLQ(ctx context.Context, source string, limit int) (int, error) {
	if limit <= 0 {
		limit = platform.DefaultDLQBatchLimit
	}

	pool, err := r.selectPool()
	if err != nil {
		return 0, err
	}

	items, err := platform.ReadDLQEntries(ctx, pool, source, limit)
	if err != nil {
		return 0, fmt.Errorf("failed to query dlq_entries: %w", err)
	}

	replayedCount := 0
	log := platform.LoggerWithTrace(ctx, r.logger)

	for _, e := range items {
		t := platform.DLQReplayTarget{
			ID:              e.GetId(),
			Source:          e.GetSource(),
			SourceTopic:     e.GetSourceTopic(),
			OriginalKey:     e.GetOriginalKey(),
			OriginalPayload: e.GetOriginalPayload(),
		}
		if err := r.replayEntry(ctx, pool, t); err != nil {
			log.Error(platform.LogEventDLQReplayFailed,
				zap.String(platform.LogFieldDLQID, t.ID),
				zap.Error(err),
			)
			continue
		}
		replayedCount++
	}

	log.Info(platform.LogEventDLQReplayCompleted,
		zap.String(platform.LogFieldSource, source),
		zap.Int(platform.LogFieldCount, replayedCount),
	)

	return replayedCount, nil
}

// ReplayDLQEntry republishes a single open DLQ entry by id and marks it replayed.
// Publish + mark are not atomic; at-least-once, safe under idempotent consumers.
func (r *dlqReplayer) ReplayDLQEntry(ctx context.Context, source string, id string) (platform.DLQEntrySummary, error) {
	pool, err := r.selectPool()
	if err != nil {
		return platform.DLQEntrySummary{}, err
	}

	target, err := platform.GetDLQReplayTarget(ctx, pool, id)
	if err != nil {
		return platform.DLQEntrySummary{}, err
	}

	if err := r.replayEntry(ctx, pool, target); err != nil {
		return platform.DLQEntrySummary{}, fmt.Errorf("replay dlq %s: %w", id, err)
	}

	return platform.DLQEntrySummary{
		ID:          target.ID,
		Source:      target.Source,
		SourceTopic: target.SourceTopic,
		OriginalKey: target.OriginalKey,
		Status:      platform.DLQStatusReplayed,
	}, nil
}

// ListDLQEntries returns operator-facing summaries for open DLQ entries,
// newest-first. Scoped to one pool (shard or global) — pass shard_id accordingly.
func (r *dlqReplayer) ListDLQEntries(ctx context.Context, source string, status string, limit int, offset int) ([]platform.DLQEntrySummary, error) {
	if limit <= 0 {
		limit = platform.DefaultDLQBatchLimit
	}

	pool, err := r.selectPool()
	if err != nil {
		return nil, err
	}

	entries, err := platform.ListDLQEntries(ctx, pool, source, status, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to list dlq_entries: %w", err)
	}
	return entries, nil
}

// replayEntry republishes one DLQ entry to its topic/key and marks it replayed.
func (r *dlqReplayer) replayEntry(ctx context.Context, pool *pgxpool.Pool, t platform.DLQReplayTarget) error {
	topic := platform.ResolveReplayTopic(t.Source, t.SourceTopic)
	writer := r.writerFor(topic)

	key := t.OriginalKey
	if key == "" {
		key = t.ID
	}

	msg := kafka.Message{Key: []byte(key), Value: t.OriginalPayload}
	if err := writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("publish dlq %s to %s: %w", t.ID, topic, err)
	}
	if err := platform.MarkDLQReplayedByID(ctx, pool, t.ID); err != nil {
		return fmt.Errorf("mark replayed dlq %s: %w", t.ID, err)
	}
	return nil
}

// writerFor lazily creates one Kafka writer per target topic.
func (r *dlqReplayer) writerFor(topic string) *kafka.Writer {
	if w, ok := r.writers[topic]; ok {
		return w
	}
	w := platform.NewKafkaWriter(r.cfg, r.cfg.KafkaBrokers, topic, 0, 0, r.logger)
	r.writers[topic] = w
	return w
}
