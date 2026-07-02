package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/port"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/jackc/pgx/v5"
)

type JobStore struct {
	pools port.ShardPools
}

// compile time interface implementation check
var _ port.JobStore = (*JobStore)(nil)

func NewJobStore(pools *platform.ShardPools) *JobStore {
	return &JobStore{pools}
}

func (s *JobStore) ClaimAndRecord(ctx context.Context, shardId string, job domain.Job, t domain.Transfer, idempKey string) (domain.SubmitResult, error) {
	// a. get merchant pool and begin transaction
	pool, err := s.pools.ShardPool(shardId)
	if err != nil {
		return domain.SubmitResult{}, err
	}

	// Begin atomic transaction
	tx, err := pool.Begin(ctx)
	if err != nil {
		return domain.SubmitResult{}, err
	}
	defer tx.Rollback(ctx)

	// b. try to insert Job(UNIQUE merchant_id, idempotency_key)
	tag, err := tx.Exec(ctx,
		`INSERT INTO jobs (id, merchant_id, idempotency_key, request_hash, type, status, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (merchant_id, idempotency_key) DO NOTHING`,
		job.ID, job.MerchantID, job.IdempotencyKey, job.PayloadHash, job.Type, job.Status, time.Now())
	if err != nil {
		return domain.SubmitResult{}, err
	}

	// c. ALREADY EXISTS! We must fetch the existing job to see if the hash matches.
	if tag.RowsAffected() == 0 {
		var existing domain.Job
		if err := tx.QueryRow(ctx,
			`SELECT id, request_hash, status FROM jobs WHERE merchant_id = $1 AND idempotency_key=$2`,
			job.MerchantID, job.IdempotencyKey,
		).Scan(&existing.ID, &existing.PayloadHash, &existing.Status); err != nil {
			return domain.SubmitResult{}, err
		}

		if existing.PayloadHash != job.PayloadHash {
			return domain.SubmitResult{}, domain.ErrIdempotencyConflict
		}

		// Replay successful!
		return domain.SubmitResult{Job: existing, AlreadyExisted: true}, nil
	}

	// c. We successfully claimed the key! Now, write the Outbox Event.
	payload, _ := json.Marshal(t)
	eventID := platform.NewEventID()

	if _, err = tx.Exec(ctx, `
		INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, correlation_id, payload, occurred_at, publish_topic)
		VALUES ($1, $4, $5, $2, $2, $3, NOW(), $6)`,
		eventID, job.ID, payload, platform.EventTypeJobRequested, platform.AggregateTypeJob, platform.TopicJobs,
	); err != nil {
		return domain.SubmitResult{}, err
	}

	// d. Commit the transaction! Both the Job and Event are saved atomically.
	if err := tx.Commit(ctx); err != nil {
		return domain.SubmitResult{}, err
	}

	return domain.SubmitResult{Job: job}, nil
}

func (s *JobStore) GetJob(ctx context.Context, shardID, jobID string) (domain.Job, error) {
	pool, err := s.pools.ShardPool(shardID)
	if err != nil {
		return domain.Job{}, err
	}

	var job domain.Job
	err = pool.QueryRow(ctx, `
		SELECT id, merchant_id, idempotency_key, type, request_hash, status, failure_reason, created_at, completed_at
		FROM jobs WHERE id = $1`, jobID).Scan(
		&job.ID,
		&job.MerchantID,
		&job.IdempotencyKey,
		&job.Type,
		&job.PayloadHash,
		&job.Status,
		&job.FailureReason,
		&job.CreatedAt,
		&job.CompletedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Job{}, domain.ErrJobNotFound
		}
		return domain.Job{}, err
	}

	return job, nil
}
