package postgres

import (
	"context"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/ports"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

type dlqStore struct {
	pools  *platform.ShardPools
	logger *zap.Logger
}

var _ ports.DLQStore = (*dlqStore)(nil)

func NewDLQStore(pools *platform.ShardPools, logger *zap.Logger) *dlqStore {
	return &dlqStore{
		pools:  pools,
		logger: logger,
	}
}

func (s *dlqStore) WriteDLQEntry(ctx context.Context, shardID string, entry domain.DLQEntry) error {
	pool, err := s.pools.ShardPool(shardID)
	if err != nil {
		return err
	}

	txCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	tx, err := pool.BeginTx(txCtx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(txCtx)

	_, err = tx.Exec(txCtx, `
		INSERT INTO dlq_entries (id, source, original_payload, error_message, attempt_count, trace_id, span_id, first_failed_at, last_failed_at, status, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
		ON CONFLICT (id) DO UPDATE SET
			error_message = EXCLUDED.error_message,
			attempt_count = EXCLUDED.attempt_count,
			trace_id = EXCLUDED.trace_id,
			span_id = EXCLUDED.span_id,
			last_failed_at = EXCLUDED.last_failed_at,
			status = EXCLUDED.status
	`,
		entry.ID,
		entry.Source,
		entry.OriginalPayload,
		entry.ErrorMessage,
		entry.AttemptCount,
		entry.TraceID,
		entry.SpanID,
		entry.FirstFailedAt,
		entry.LastFailedAt,
		string(entry.Status),
	)
	if err != nil {
		return err
	}

	if err := tx.Commit(txCtx); err != nil {
		return err
	}

	s.logger.Info("DLQ entry written successfully", zap.String("entry_id", entry.ID), zap.String("shard_id", shardID))
	return nil
}
