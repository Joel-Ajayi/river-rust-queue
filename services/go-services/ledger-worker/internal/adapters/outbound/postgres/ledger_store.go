package postgres

import (
	"context"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/ports"
)

type LedgerStore struct {
	pools  *platform.ShardPools
	logger *zap.Logger
}

var _ ports.LedgerStore = (*LedgerStore)(nil)

func NewLedgerStore(pools *platform.ShardPools, logger *zap.Logger) *LedgerStore {
	return &LedgerStore{pools: pools, logger: logger}
}

func (s *LedgerStore) PostTransfer(ctx context.Context, shardID string, transfer domain.Transfer) error {
	// 1. Get shard pool
	pool, err := s.pools.ShardPool(shardID)
	if err != nil {
		return err
	}

	txCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	tx, err := pool.BeginTx(txCtx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer tx.Rollback(txCtx)

	// 2. Prevent self-transfers
	if transfer.FromWallet == transfer.ToWallet {
		return domain.ErrSelfTransfer
	}

	// 3. Lock job to ensure idempotency and prevent concurrent processing
	var jobStatus string
	err = tx.QueryRow(txCtx, `SELECT status FROM jobs WHERE id = $1 FOR UPDATE`, transfer.JobID).Scan(&jobStatus)
	if err != nil {
		if err == pgx.ErrNoRows {
			// If job doesn't exist on this shard, it's an orchestration error
			s.logger.Error("Job not found on shard", zap.String("job_id", transfer.JobID))
			return nil // Ack to kafka to drop it
		}
		return err
	}
	if jobStatus != string(domain.JobStatusPending) {
		s.logger.Info("Job already processed, skipping PostTransfer", zap.String("job_id", transfer.JobID), zap.String("status", jobStatus))
		return nil
	}

	// 4. Lock wallets in sorted order to prevent deadlocks
	wallets := []string{transfer.FromWallet, transfer.ToWallet}
	slices.Sort(wallets)

	rows, err := tx.Query(txCtx, `
		SELECT id, currency, status FROM wallets WHERE id = ANY($1) ORDER BY id FOR UPDATE
	`, wallets)
	if err != nil {
		return err
	}
	defer rows.Close()

	// 4. Validate wallet existence, currencies, and statuses
	foundWallets := 0
	for rows.Next() {
		var id, currency, status string
		if err := rows.Scan(&id, &currency, &status); err != nil {
			return err
		}
		foundWallets++
		if status == string(domain.WalletStatusFrozen) {
			return domain.ErrWalletFrozen
		}
		if status == string(domain.WalletStatusClosed) {
			return domain.ErrWalletClosed
		}
		if currency != transfer.Currency {
			return domain.ErrCurrencyMismatch
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if foundWallets != 2 {
		return domain.ErrWalletNotFound
	}

	// 5. Balance check (under lock — I2)
	var fromBal, toBal int64
	err = tx.QueryRow(txCtx, `SELECT COALESCE(SUM(amount), 0) FROM ledger_entries WHERE wallet_id=$1`, transfer.FromWallet).Scan(&fromBal)
	if err != nil {
		return err
	}
	if fromBal < transfer.Amount {
		return domain.ErrInsufficientBalance
	}

	err = tx.QueryRow(txCtx, `SELECT COALESCE(SUM(amount), 0) FROM ledger_entries WHERE wallet_id=$1`, transfer.ToWallet).Scan(&toBal)
	if err != nil {
		return err
	}

	// 6. Record transfer — ON CONFLICT DO NOTHING handles redelivery idempotency
	tag, err := tx.Exec(txCtx, `
		INSERT INTO transfers (id, job_id, from_wallet, to_wallet, amount, currency, status, posted_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
		ON CONFLICT (id) DO NOTHING
	`, transfer.ID, transfer.JobID, transfer.FromWallet, transfer.ToWallet, transfer.Amount, transfer.Currency, string(domain.TransferStateCompleted))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		s.logger.Info("idempotency guard hit on PostTransfer redelivery", zap.String("transfer_id", transfer.ID))
		return nil // Duplicate redelivery — already completed
	}

	// 7. Insert ledger entries
	// Debit leg (negative amount)
	_, err = tx.Exec(txCtx, `
		INSERT INTO ledger_entries (wallet_id, transfer_id, leg, amount, balance_after, created_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
	`, transfer.FromWallet, transfer.ID, string(domain.LegDebit), -transfer.Amount, fromBal-transfer.Amount)
	if err != nil {
		return err
	}

	// Credit leg (positive amount)
	_, err = tx.Exec(txCtx, `
		INSERT INTO ledger_entries (wallet_id, transfer_id, leg, amount, balance_after, created_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
	`, transfer.ToWallet, transfer.ID, string(domain.LegCredit), transfer.Amount, toBal+transfer.Amount)
	if err != nil {
		return err
	}

	// 8. Update job status
	_, err = tx.Exec(txCtx, `
		UPDATE jobs SET status = $1, completed_at = NOW() WHERE id = $2
	`, domain.JobStatusCompleted, transfer.JobID)
	if err != nil {
		return err
	}

	// 9. Emit Outbox Event
	eventID := platform.NewEventID()
	now := time.Now()

	envelope := &eventsv1.EventEnvelope{
		EventId:       eventID,
		EventType:     string(platform.EventTypeTransferCompleted),
		AggregateType: string(platform.AggregateTypeTransfer),
		AggregateId:   transfer.ID,
		CorrelationId: transfer.JobID,
		OccurredAt:    timestamppb.New(now),
		Payload: &eventsv1.EventEnvelope_TransferCompleted{
			TransferCompleted: &eventsv1.TransferCompletedPayload{
				JobId:      transfer.JobID,
				TransferId: transfer.ID,
				MerchantId: transfer.MerchantID,
				FromWallet: transfer.FromWallet,
				ToWallet:   transfer.ToWallet,
				Amount:     transfer.Amount,
				Currency:   transfer.Currency,
			},
		},
	}
	marshaler := protojson.MarshalOptions{EmitUnpopulated: true}
	payloadBytes, err := marshaler.Marshal(envelope)
	if err != nil {
		return err
	}

	_, err = tx.Exec(txCtx, `
		INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, correlation_id, payload, occurred_at, publish_topic)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, eventID, string(platform.EventTypeTransferCompleted), string(platform.AggregateTypeTransfer), transfer.ID, transfer.JobID, payloadBytes, now, platform.TopicNotify)
	if err != nil {
		return err
	}

	if err := tx.Commit(txCtx); err != nil {
		return err
	}
	s.logger.Info("PostTransfer committed", zap.String("transfer_id", transfer.ID), zap.String("job_id", transfer.JobID))
	return nil
}

// FailTransfer records a terminal business error (e.g. insufficient balance, frozen wallet).
func (s *LedgerStore) FailTransfer(ctx context.Context, shardID string, transfer domain.Transfer, reason string) error {
	// 1. Get shard pool
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

	// 2. Record the transfer as failed — ON CONFLICT handles redelivery idempotency
	tag, err := tx.Exec(txCtx, `
		INSERT INTO transfers (id, job_id, from_wallet, to_wallet, amount, currency, status, failure_reason, posted_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
		ON CONFLICT (id) DO NOTHING
	`, transfer.ID, transfer.JobID, transfer.FromWallet, transfer.ToWallet, transfer.Amount, transfer.Currency, string(domain.TransferStateFailed), reason)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		s.logger.Info("idempotency guard hit on FailTransfer redelivery", zap.String("transfer_id", transfer.ID))
		return nil // Duplicate redelivery — already recorded
	}

	// 3. Update job status to failed
	_, err = tx.Exec(txCtx, `
		UPDATE jobs SET status = $1, failure_reason = $2, completed_at = NOW() WHERE id = $3
	`, domain.JobStatusFailed, reason, transfer.JobID)
	if err != nil {
		return err
	}

	// 4. Emit transfer.failed event to outbox (notify topic)
	eventID := platform.NewEventID()
	now := time.Now()

	envelope := &eventsv1.EventEnvelope{
		EventId:       eventID,
		EventType:     string(platform.EventTypeTransferFailed),
		AggregateType: string(platform.AggregateTypeTransfer),
		AggregateId:   transfer.ID,
		CorrelationId: transfer.JobID,
		OccurredAt:    timestamppb.New(now),
		Payload: &eventsv1.EventEnvelope_TransferFailed{
			TransferFailed: &eventsv1.TransferFailedPayload{
				JobId:      transfer.JobID,
				TransferId: transfer.ID,
				MerchantId: transfer.MerchantID,
				Reason:     reason,
			},
		},
	}
	marshaler := protojson.MarshalOptions{EmitUnpopulated: true}
	payloadBytes, err := marshaler.Marshal(envelope)
	if err != nil {
		return err
	}

	_, err = tx.Exec(txCtx, `
		INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, correlation_id, payload, occurred_at, publish_topic)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, eventID, string(platform.EventTypeTransferFailed), string(platform.AggregateTypeTransfer), transfer.ID, transfer.JobID, payloadBytes, now, platform.TopicNotify)
	if err != nil {
		return err
	}

	if err := tx.Commit(txCtx); err != nil {
		return err
	}
	s.logger.Info("FailTransfer committed", zap.String("transfer_id", transfer.ID), zap.String("reason", reason))
	return nil
}
