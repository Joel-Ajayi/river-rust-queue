package postgres

import (
	"context"
	"fmt"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

const (
	// DefaultDLQBatchLimit is the default batch size limit for DLQ replay queries.
	DefaultDLQBatchLimit = 100

	// querySelectOpenDLQEntries fetches open DLQ items from dlq_entries.
	querySelectOpenDLQEntries = `
		SELECT id, source, original_payload, attempt_count
		FROM dlq_entries
		WHERE status = $1 AND ($2 = '' OR source = $2)
		ORDER BY created_at ASC
		LIMIT $3`

	// queryUpdateDLQStatus marks replayed DLQ items in dlq_entries.
	queryUpdateDLQStatus = `
		UPDATE dlq_entries
		SET status = $1, replayed_at = NOW()
		WHERE id = $2`
)

type dlqReplayer struct {
	pools  *platform.ShardPools
	writer *kafka.Writer
	logger *zap.Logger
}

// NewDLQReplayer creates a Postgres-backed DLQReplayer adapter.
func NewDLQReplayer(pools *platform.ShardPools, writer *kafka.Writer, logger *zap.Logger) *dlqReplayer {
	return &dlqReplayer{
		pools:  pools,
		writer: writer,
		logger: logger,
	}
}

// ReplayDLQ queries open DLQ entries from the specified shard and republishes them to Kafka.
func (r *dlqReplayer) ReplayDLQ(ctx context.Context, shardID string, source string, limit int) (int, error) {
	if limit <= 0 {
		limit = DefaultDLQBatchLimit
	}

	pool, err := r.pools.ShardPool(shardID)
	if err != nil {
		return 0, fmt.Errorf("shard pool not found for %s: %w", shardID, err)
	}

	rows, err := pool.Query(ctx, querySelectOpenDLQEntries, platform.DLQStatusOpen, source, limit)
	if err != nil {
		return 0, fmt.Errorf("failed to query dlq_entries: %w", err)
	}
	defer rows.Close()

	type dlqItem struct {
		id      string
		source  string
		payload []byte
	}
	var items []dlqItem

	for rows.Next() {
		var item dlqItem
		var attempts int
		if err := rows.Scan(&item.id, &item.source, &item.payload, &attempts); err != nil {
			return 0, fmt.Errorf("failed to scan dlq_entries row: %w", err)
		}
		items = append(items, item)
	}

	replayedCount := 0
	log := platform.LoggerWithTrace(ctx, r.logger)

	for _, item := range items {
		msg := kafka.Message{
			Key:   []byte(item.id),
			Value: item.payload,
		}

		if r.writer != nil {
			if err := r.writer.WriteMessages(ctx, msg); err != nil {
				if log != nil {
					log.Error(platform.LogEventDLQReplayFailed,
						zap.String(platform.LogFieldDLQID, item.id),
						zap.Error(err),
					)
				}
				continue
			}
		}

		_, err := pool.Exec(ctx, queryUpdateDLQStatus, platform.DLQStatusReplayed, item.id)
		if err != nil {
			if log != nil {
				log.Error(platform.LogEventDLQUpdateFailed,
					zap.String(platform.LogFieldDLQID, item.id),
					zap.Error(err),
				)
			}
			continue
		}
		replayedCount++
	}

	if log != nil {
		log.Info(platform.LogEventDLQReplayCompleted,
			zap.String(platform.LogFieldShardID, shardID),
			zap.Int(platform.LogFieldCount, replayedCount),
		)
	}

	return replayedCount, nil
}
