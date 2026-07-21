package postgres

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/core/port"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"go.uber.org/zap"
)

type dlqStore struct {
	pools  *platform.ShardPools
	logger *zap.Logger
}

var _ port.DLQStore = (*dlqStore)(nil)

func NewDLQStore(pools *platform.ShardPools, logger *zap.Logger) *dlqStore {
	return &dlqStore{pools: pools, logger: logger}
}

func (s *dlqStore) WriteDLQEntry(ctx context.Context, shardID string, entry domain.DLQEntry) error {
	pool, err := s.pools.ShardPool(shardID)
	if err != nil {
		return err
	}

	traceID := entry.TraceID
	spanID := entry.SpanID

	_, err = pool.Exec(ctx, `
		INSERT INTO dlq_entries (id, source, original_payload, error_message, error_classification, attempt_count, first_failed_at, last_failed_at, status, trace_id, span_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULLIF($10, ''), NULLIF($11, ''))
	`, entry.ID, entry.Source, entry.OriginalPayload, entry.ErrorMessage, entry.ErrorClassification, entry.AttemptCount, entry.FirstFailedAt, entry.LastFailedAt, entry.Status, traceID, spanID)
	if err != nil {
		return err
	}

	platform.RecordDLQIngestion(ctx, platform.ServiceNameFraudWorker)
	return nil
}
