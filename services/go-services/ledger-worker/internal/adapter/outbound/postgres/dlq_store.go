package postgres

import (
	"context"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/port"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

type dlqStore struct {
	pools *platform.ShardPools
}

var _ port.DLQStore = (*dlqStore)(nil)

func NewDLQStore(pools *platform.ShardPools, _ *zap.Logger) *dlqStore {
	return &dlqStore{
		pools: pools,
	}
}

func (s *dlqStore) WriteDLQEntry(ctx context.Context, shardID string, entry domain.DLQEntry) error {
	pool, err := s.pools.ShardPool(shardID)
	if err != nil {
		return err
	}

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO dlq_entries (id, source, original_payload, error_message, error_classification, attempt_count, first_failed_at, last_failed_at, status, trace_id, span_id, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW())
		ON CONFLICT (id) DO UPDATE SET
			error_message = EXCLUDED.error_message,
			attempt_count = EXCLUDED.attempt_count + 1,
			last_failed_at = EXCLUDED.last_failed_at,
			status = EXCLUDED.status
	`,
		entry.ID,
		entry.Source,
		entry.OriginalPayload,
		entry.ErrorMessage,
		entry.ErrorClassification,
		entry.AttemptCount,
		entry.FirstFailedAt,
		entry.LastFailedAt,
		string(entry.Status),
		entry.TraceID,
		entry.SpanID,
	)
	if err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	return nil
}
