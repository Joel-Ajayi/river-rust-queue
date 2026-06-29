package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/port"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/jackc/pgx/v5/pgxpool"
)

// JobStore writes jobs and their outbox events on a merchant's shard.
type JobStore struct {
	pools   *platform.ShardPools
	newEvID func() string
}

var _ port.JobStore = (*JobStore)(nil)

// NewJobStore builds the adapter; newEvID mints outbox event ids.
func NewJobStore(pools *platform.ShardPools, newEvID func() string) *JobStore {
	return &JobStore{pools: pools, newEvID: newEvID}
}

// ClaimAndRecord performs the idempotent accept: one transaction that claims the
// (merchant, key) pair and, only on first sight, records the job + outbox event.
func (s *JobStore) ClaimAndRecord(
	ctx context.Context,
	shardID string,
	job domain.Job,
	t domain.Transfer,
	idempotencyKey string,
	requestHash string,
) (domain.SubmitResult, error) {
	pool, err := s.pools.ShardPool(shardID)
	if err != nil {
		return domain.SubmitResult{}, err
	}
	return s.claim(ctx, pool, shardID, job, t, idempotencyKey, requestHash)
}

func (s *JobStore) claim(
	ctx context.Context,
	pool *pgxpool.Pool,
	_ string,
	job domain.Job,
	t domain.Transfer,
	idempotencyKey string,
	requestHash string,
) (domain.SubmitResult, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return domain.SubmitResult{}, err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx,
		`INSERT INTO jobs (id, merchant_id, idempotency_key, request_hash, type, status, created_at)
		 VALUES ($1, $2, $3, $4, 'transfer', 'pending', $5)
		 ON CONFLICT (merchant_id, idempotency_key) DO NOTHING`,
		job.ID, t.MerchantID, idempotencyKey, requestHash, time.Now(),
	)
	if err != nil {
		return domain.SubmitResult{}, err
	}

	if tag.RowsAffected() == 0 {
		var existing domain.Job
		var existingHash string
		if err := tx.QueryRow(ctx,
			`SELECT id, request_hash, status FROM jobs WHERE merchant_id = $1 AND idempotency_key = $2`,
			t.MerchantID, idempotencyKey,
		).Scan(&existing.ID, &existingHash, &existing.Status); err != nil {
			return domain.SubmitResult{}, err
		}
		if existingHash != requestHash {
			return domain.SubmitResult{}, domain.ErrIdempotencyConflict
		}
		return domain.SubmitResult{Job: existing, AlreadyExisted: true}, nil
	}

	payload, _ := json.Marshal(map[string]any{
		"job_id":          job.ID,
		"merchant_id":     t.MerchantID,
		"idempotency_key": idempotencyKey,
		"job_type":        "transfer",
		"data": map[string]any{
			"from_wallet": t.FromWallet,
			"to_wallet":   t.ToWallet,
			"amount":      t.Amount,
			"currency":    t.Currency,
			"reference":   t.Reference,
		},
	})

	if _, err := tx.Exec(ctx,
		`INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id,
		 correlation_id, payload, occurred_at, publish_topic)
		 VALUES ($1, 'job.requested', 'job', $2, $2, $3, $4, 'jobs')`,
		s.newEvID(), job.ID, payload, time.Now(),
	); err != nil {
		return domain.SubmitResult{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.SubmitResult{}, err
	}
	return domain.SubmitResult{Job: job}, nil
}
