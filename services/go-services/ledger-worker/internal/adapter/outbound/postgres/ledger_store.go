package postgres

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/port"
)

type LedgerStore struct {
	pools *platform.ShardPools
}

var _ port.LedgerStore = (*LedgerStore)(nil)

func NewLedgerStore(pools *platform.ShardPools, _ *zap.Logger) *LedgerStore {
	return &LedgerStore{
		pools: pools,
	}
}

func (s *LedgerStore) PostTransfer(ctx context.Context, shardID string, transfer domain.Transfer) error {
	// 1. Get shard pool
	pool, err := s.pools.ShardPool(shardID)
	if err != nil {
		return err
	}

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// 2. Prevent self-transfers
	if transfer.FromWallet == transfer.ToWallet {
		return domain.ErrSelfTransfer
	}

	// 3. Lock job to ensure idempotency and prevent concurrent processing
	var jobStatus string
	err = tx.QueryRow(ctx, `SELECT status FROM jobs WHERE id = $1 FOR UPDATE`, transfer.JobID).Scan(&jobStatus)
	if err != nil {
		if err == pgx.ErrNoRows {
			// If job doesn't exist on this shard, it's an orchestration error
			return nil // Ack to kafka to drop it
		}
		return err
	}
	if jobStatus != platform.JobStatusPending {
		return nil
	}

	// 4. Lock wallets in sorted order to prevent deadlocks
	wallets := []string{transfer.FromWallet, transfer.ToWallet}
	slices.Sort(wallets)

	rows, err := tx.Query(ctx, `
		SELECT id, currency, status, wallet_type FROM wallets WHERE id = ANY($1) ORDER BY id FOR UPDATE
	`, wallets)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Validate wallet existence, currencies, statuses, and type mapping.
	foundWallets := 0
	walletTypes := make(map[string]string)
	for rows.Next() {
		var id, currency, status, wtype string
		if err := rows.Scan(&id, &currency, &status, &wtype); err != nil {
			return err
		}
		foundWallets++
		walletTypes[id] = wtype
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
	if foundWallets != platform.LedgerTransferWalletCount {
		return domain.ErrWalletNotFound
	}

	// Lock and get cached balances in sorted order to prevent deadlocks.
	balances := make(map[string]int64)
	for _, walletID := range wallets {
		var bal int64
		err = tx.QueryRow(ctx, `
			SELECT balance FROM wallet_balance_cache WHERE wallet_id = $1 FOR UPDATE
		`, walletID).Scan(&bal)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// Initialize by summing existing ledger entries (bootstrapping step).
				err = tx.QueryRow(ctx, `
					SELECT COALESCE(SUM(amount), 0) FROM ledger_entries WHERE wallet_id = $1
				`, walletID).Scan(&bal)
				if err != nil {
					return err
				}
				// Insert bootstrapped balance into cache and lock it.
				_, err = tx.Exec(ctx, `
					INSERT INTO wallet_balance_cache (wallet_id, balance, last_entry_id, updated_at)
					VALUES ($1, $2, 0, NOW())
					ON CONFLICT (wallet_id) DO UPDATE SET balance = EXCLUDED.balance, updated_at = NOW()
				`, walletID, bal)
				if err != nil {
					return err
				}
				// Lock again to ensure consistency.
				err = tx.QueryRow(ctx, `
					SELECT balance FROM wallet_balance_cache WHERE wallet_id = $1 FOR UPDATE
				`, walletID).Scan(&bal)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
		balances[walletID] = bal
	}

	fromBal := balances[transfer.FromWallet]
	toBal := balances[transfer.ToWallet]

	fromWType := walletTypes[transfer.FromWallet]
	if !domain.IsSystemWallet(fromWType) && fromBal < transfer.Amount {
		return domain.ErrInsufficientBalance
	}

	// 6. Record transfer — ON CONFLICT DO NOTHING handles redelivery idempotency
	tag, err := tx.Exec(ctx, `
		INSERT INTO transfers (id, job_id, from_wallet, to_wallet, amount, currency, status, posted_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
		ON CONFLICT (id) DO NOTHING
	`, transfer.ID, transfer.JobID, transfer.FromWallet, transfer.ToWallet, transfer.Amount, transfer.Currency, platform.TransferStatusCompleted)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return nil // Duplicate redelivery — already completed
	}

	// 7. Insert ledger entries
	// Debit leg (negative amount)
	_, err = tx.Exec(ctx, `
		INSERT INTO ledger_entries (wallet_id, transfer_id, leg, amount, balance_after, created_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
	`, transfer.FromWallet, transfer.ID, string(domain.LegDebit), -transfer.Amount, fromBal-transfer.Amount)
	if err != nil {
		return err
	}

	// Credit leg (positive amount)
	_, err = tx.Exec(ctx, `
		INSERT INTO ledger_entries (wallet_id, transfer_id, leg, amount, balance_after, created_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
	`, transfer.ToWallet, transfer.ID, string(domain.LegCredit), transfer.Amount, toBal+transfer.Amount)
	if err != nil {
		return err
	}

	// Update cached balances
	for walletID := range balances {
		var delta int64
		if walletID == transfer.FromWallet {
			delta = -transfer.Amount
		} else {
			delta = transfer.Amount
		}
		_, err = tx.Exec(ctx, `
			UPDATE wallet_balance_cache SET balance = balance + $1, updated_at = NOW() WHERE wallet_id = $2
		`, delta, walletID)
		if err != nil {
			return err
		}
	}

	// 8. Update job status
	_, err = tx.Exec(ctx, `
		UPDATE jobs SET status = $1, completed_at = NOW() WHERE id = $2
	`, platform.JobStatusCompleted, transfer.JobID)
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
		Traceparent:   platform.ExtractTraceparent(ctx),
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

	_, err = tx.Exec(ctx, `
		INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, correlation_id, payload, occurred_at, publish_topic)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, eventID, string(platform.EventTypeTransferCompleted), string(platform.AggregateTypeTransfer), transfer.ID, transfer.JobID, payloadBytes, now, platform.TopicNotify)
	if err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

// FailTransfer records a terminal business error (e.g. insufficient balance, frozen wallet).
func (s *LedgerStore) FailTransfer(ctx context.Context, shardID string, transfer domain.Transfer, reason string) error {
	// 1. Get shard pool
	pool, err := s.pools.ShardPool(shardID)
	if err != nil {
		return err
	}

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// 2. Record the transfer as failed — ON CONFLICT handles redelivery idempotency
	tag, err := tx.Exec(ctx, `
		INSERT INTO transfers (id, job_id, from_wallet, to_wallet, amount, currency, status, failure_reason, posted_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
		ON CONFLICT (id) DO NOTHING
	`, transfer.ID, transfer.JobID, transfer.FromWallet, transfer.ToWallet, transfer.Amount, transfer.Currency, platform.TransferStatusFailed, reason)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return nil // Duplicate redelivery — already recorded
	}

	// 3. Update job status to failed
	_, err = tx.Exec(ctx, `
		UPDATE jobs SET status = $1, failure_reason = $2, completed_at = NOW() WHERE id = $3
	`, platform.JobStatusFailed, reason, transfer.JobID)
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
		Traceparent:   platform.ExtractTraceparent(ctx),
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

	_, err = tx.Exec(ctx, `
		INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, correlation_id, payload, occurred_at, publish_topic)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, eventID, string(platform.EventTypeTransferFailed), string(platform.AggregateTypeTransfer), transfer.ID, transfer.JobID, payloadBytes, now, platform.TopicNotify)
	if err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}
