package postgres

import (
	"context"
	"errors"
	"time"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/port"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type CrossShardStore struct {
	pools *platform.ShardPools
}

var _ port.CrossShardStore = (*CrossShardStore)(nil)

func NewCrossShardStore(pools *platform.ShardPools, _ *zap.Logger) *CrossShardStore {
	return &CrossShardStore{
		pools: pools,
	}
}

func (s *CrossShardStore) DebitToClearingAccount(ctx context.Context, srcShard, dstShard, jobID string, transfer domain.Transfer) error {
	pool, err := s.pools.ShardPool(srcShard)
	if err != nil {
		return err
	}

	// Removed hardcoded timeout

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// 1a. Lock job to ensure idempotency and prevent concurrent processing
	var jobStatus string
	err = tx.QueryRow(ctx, `SELECT status FROM jobs WHERE id = $1 FOR UPDATE`, jobID).Scan(&jobStatus)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil
		}
		return err
	}
	if jobStatus != platform.JobStatusPending {
		return nil
	}

	// Look up the clearing wallet for this shard by wallet_type and currency (read-only)
	var clearingWallet string
	err = tx.QueryRow(ctx, `SELECT id FROM wallets WHERE wallet_type = $1 AND currency = $2`, string(domain.WalletTypeSystem), transfer.Currency).Scan(&clearingWallet)
	if err != nil {
		if err == pgx.ErrNoRows {
			return domain.ErrWalletNotFound
		}
		return err
	}

	// 3. Lock only the customer FromWallet
	var fromCurrency, fromStatus, fromWType string
	err = tx.QueryRow(ctx, `SELECT currency, status, wallet_type FROM wallets WHERE id = $1 FOR UPDATE`, transfer.FromWallet).Scan(&fromCurrency, &fromStatus, &fromWType)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrWalletNotFound
		}
		return err
	}
	if fromStatus == string(domain.WalletStatusFrozen) {
		return domain.ErrWalletFrozen
	}
	if fromStatus == string(domain.WalletStatusClosed) {
		return domain.ErrWalletClosed
	}
	if fromCurrency != transfer.Currency {
		return domain.ErrCurrencyMismatch
	}

	// Lock and get cached balance for customer FromWallet
	var fromBal int64
	err = tx.QueryRow(ctx, `
		SELECT balance FROM wallet_balance_cache WHERE wallet_id = $1 FOR UPDATE
	`, transfer.FromWallet).Scan(&fromBal)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Initialize by summing existing ledger entries (bootstrapping step).
			err = tx.QueryRow(ctx, `
				SELECT COALESCE(SUM(amount), 0) FROM ledger_entries WHERE wallet_id = $1
			`, transfer.FromWallet).Scan(&fromBal)
			if err != nil {
				return err
			}
			// Insert bootstrapped balance into cache and lock it.
			_, err = tx.Exec(ctx, `
				INSERT INTO wallet_balance_cache (wallet_id, balance, last_entry_id, updated_at)
				VALUES ($1, $2, 0, NOW())
				ON CONFLICT (wallet_id) DO UPDATE SET balance = EXCLUDED.balance, updated_at = NOW()
			`, transfer.FromWallet, fromBal)
			if err != nil {
				return err
			}
			// Lock again to ensure consistency.
			err = tx.QueryRow(ctx, `
				SELECT balance FROM wallet_balance_cache WHERE wallet_id = $1 FOR UPDATE
			`, transfer.FromWallet).Scan(&fromBal)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	if !domain.IsSystemWallet(fromWType) && fromBal < transfer.Amount {
		return domain.ErrInsufficientBalance
	}

	// 5. Record transfer — ON CONFLICT DO NOTHING handles redelivery idempotency
	tag, err := tx.Exec(ctx, `
		INSERT INTO transfers (id, job_id, from_wallet, to_wallet, amount, currency, status, posted_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
		ON CONFLICT (id) DO NOTHING
	`, transfer.ID, jobID, transfer.FromWallet, transfer.ToWallet, transfer.Amount, transfer.Currency, platform.TransferStatusCompleted)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return nil // Duplicate redelivery — already completed
	}

	// 6. Insert ledger entries (Full double-entry bookkeeping)
	// debit FromWallet
	_, err = tx.Exec(ctx, `INSERT INTO ledger_entries (wallet_id, transfer_id, leg, amount, balance_after, created_at) VALUES ($1, $2, $3, $4, $5, NOW())`,
		transfer.FromWallet, transfer.ID, string(domain.LegDebit), -transfer.Amount, fromBal-transfer.Amount)
	if err != nil {
		return err
	}
	// credit ClearingWallet (audit entry)
	_, err = tx.Exec(ctx, `INSERT INTO ledger_entries (wallet_id, transfer_id, leg, amount, balance_after, created_at) VALUES ($1, $2, $3, $4, $5, NOW())`,
		clearingWallet, transfer.ID, string(domain.LegCredit), transfer.Amount, 0)
	if err != nil {
		return err
	}

	// Update cached balance ONLY for customer FromWallet (clearing wallet bypasses cache lock)
	_, err = tx.Exec(ctx, `
		UPDATE wallet_balance_cache SET balance = balance - $1, updated_at = NOW() WHERE wallet_id = $2
	`, transfer.Amount, transfer.FromWallet)
	if err != nil {
		return err
	}

	// 6. Record saga
	var existingState string
	err = tx.QueryRow(ctx, `
		INSERT INTO cross_shard_transfer (transfer_id, job_id, src_shard, dst_shard, from_wallet, to_wallet, amount, currency, state)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (transfer_id) DO NOTHING
		RETURNING state
	`, transfer.ID, jobID, srcShard, dstShard, transfer.FromWallet, transfer.ToWallet, transfer.Amount, transfer.Currency, platform.XShardTransferStatusPending).Scan(&existingState)
	if err != nil {
		return err
	}

	// 7. Emit intent to destination shard
	eventID := platform.NewEventID()
	now := time.Now()

	envelope := &eventsv1.EventEnvelope{
		EventId:       eventID,
		EventType:     string(platform.EventTypeXShardTransferRequested),
		AggregateType: string(platform.AggregateTypeXShardTransfer),
		AggregateId:   transfer.ID,
		CorrelationId: jobID,
		OccurredAt:    timestamppb.New(now),
		Traceparent:   platform.ExtractTraceparent(ctx),
		Payload: &eventsv1.EventEnvelope_XshardTransferRequested{
			XshardTransferRequested: &eventsv1.XShardTransferRequestedPayload{
				TransferId: transfer.ID,
				JobId:      jobID,
				MerchantId: transfer.MerchantID,
				SrcShard:   srcShard,
				DstShard:   dstShard,
				FromWallet: transfer.FromWallet,
				ToWallet:   transfer.ToWallet,
				Amount:     transfer.Amount,
				Currency:   transfer.Currency,
				Status:     platform.TransferStatusPending,
			},
		},
	}
	payloadBytes, err := platform.MarshalEnvelope(envelope)
	if err != nil {
		return err
	}
	publishTopic := platform.TopicXShardPrefix + dstShard

	_, err = tx.Exec(ctx, `
		INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, correlation_id, payload, occurred_at, publish_topic)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, eventID, string(platform.EventTypeXShardTransferRequested), string(platform.AggregateTypeXShardTransfer), transfer.ID, jobID, payloadBytes, now, publishTopic)
	if err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

func (s *CrossShardStore) CreditFromClearingAccount(ctx context.Context, intent *eventsv1.XShardTransferRequestedPayload) error {
	pool, err := s.pools.ShardPool(intent.DstShard)
	if err != nil {
		return err
	}

	// Removed hardcoded timeout

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	emitFailure := func(reason string) error {
		eventID := platform.NewEventID()
		now := time.Now()
		envelope := &eventsv1.EventEnvelope{
			EventId:       eventID,
			EventType:     string(platform.EventTypeXShardTransferFailed),
			AggregateType: string(platform.AggregateTypeXShardTransfer),
			AggregateId:   intent.TransferId,
			CorrelationId: intent.JobId,
			OccurredAt:    timestamppb.New(now),
			Traceparent:   platform.ExtractTraceparent(ctx),
			Payload: &eventsv1.EventEnvelope_XshardTransferFailed{
				XshardTransferFailed: &eventsv1.XShardTransferFailedPayload{
					TransferId: intent.TransferId,
					SrcShard:   intent.SrcShard,
					DstShard:   intent.DstShard,
					Reason:     reason,
				},
			},
		}
		payloadBytes, err := platform.MarshalEnvelope(envelope)
		if err != nil {
			return err
		}
		publishTopic := platform.TopicXShardPrefix + intent.SrcShard
		_, err = tx.Exec(ctx, `
			INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, correlation_id, payload, occurred_at, publish_topic)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		`, eventID, string(platform.EventTypeXShardTransferFailed), string(platform.AggregateTypeXShardTransfer), intent.TransferId, intent.JobId, payloadBytes, now, publishTopic)
		if err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	// 2. Resolve the Destination Merchant ID and its Clearing Wallet (read-only)
	var destMerchantID string
	err = tx.QueryRow(ctx, "SELECT merchant_id FROM wallets WHERE id=$1", intent.ToWallet).Scan(&destMerchantID)
	if err != nil {
		if err == pgx.ErrNoRows {
			return emitFailure(domain.ErrWalletNotFound.Error())
		}
		return err
	}

	var clearingWallet string
	err = tx.QueryRow(ctx, `SELECT id FROM wallets WHERE wallet_type = $1 AND currency = $2`, string(domain.WalletTypeSystem), intent.Currency).Scan(&clearingWallet)
	if err != nil {
		if err == pgx.ErrNoRows {
			return emitFailure(domain.ErrWalletNotFound.Error())
		}
		return err
	}

	// 3. Lock only customer destination ToWallet
	var toCurrency, toStatus string
	err = tx.QueryRow(ctx, `SELECT currency, status FROM wallets WHERE id = $1 FOR UPDATE`, intent.ToWallet).Scan(&toCurrency, &toStatus)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return emitFailure(domain.ErrWalletNotFound.Error())
		}
		return err
	}
	if toStatus == string(domain.WalletStatusFrozen) {
		return emitFailure(domain.ErrWalletFrozen.Error())
	}
	if toStatus == string(domain.WalletStatusClosed) {
		return emitFailure(domain.ErrWalletClosed.Error())
	}
	if toCurrency != intent.Currency {
		return emitFailure(domain.ErrCurrencyMismatch.Error())
	}

	// 4. Lock and get cached balance for customer destination ToWallet
	var toBal int64
	err = tx.QueryRow(ctx, `
		SELECT balance FROM wallet_balance_cache WHERE wallet_id = $1 FOR UPDATE
	`, intent.ToWallet).Scan(&toBal)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Initialize by summing existing ledger entries (bootstrapping step)
			err = tx.QueryRow(ctx, `
				SELECT COALESCE(SUM(amount), 0) FROM ledger_entries WHERE wallet_id = $1
			`, intent.ToWallet).Scan(&toBal)
			if err != nil {
				return emitFailure(err.Error())
			}
			// Insert the bootstrapped balance into cache and lock it
			_, err = tx.Exec(ctx, `
				INSERT INTO wallet_balance_cache (wallet_id, balance, last_entry_id, updated_at)
				VALUES ($1, $2, 0, NOW())
				ON CONFLICT (wallet_id) DO UPDATE SET balance = EXCLUDED.balance, updated_at = NOW()
			`, intent.ToWallet, toBal)
			if err != nil {
				return emitFailure(err.Error())
			}
			// Lock again to ensure consistency
			err = tx.QueryRow(ctx, `
				SELECT balance FROM wallet_balance_cache WHERE wallet_id = $1 FOR UPDATE
			`, intent.ToWallet).Scan(&toBal)
			if err != nil {
				return emitFailure(err.Error())
			}
		} else {
			return emitFailure(err.Error())
		}
	}

	// 5. Record Final Transfer State on Destination Shard (ON CONFLICT DO NOTHING handles redelivery)
	tag, err := tx.Exec(ctx, `
		INSERT INTO transfers (id, job_id, from_wallet, to_wallet, amount, currency, status, posted_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
		ON CONFLICT (id) DO NOTHING
	`, intent.TransferId, intent.JobId, intent.FromWallet, intent.ToWallet, intent.Amount, intent.Currency, platform.TransferStatusCompleted)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return nil // Duplicate redelivery
	}

	// 6. Insert Ledger Entries (Debit clearing, Credit receiver)
	_, err = tx.Exec(ctx, `INSERT INTO ledger_entries (wallet_id, transfer_id, leg, amount, balance_after, created_at) VALUES ($1, $2, $3, $4, $5, NOW())`, clearingWallet, intent.TransferId, string(domain.LegDebit), -intent.Amount, 0)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO ledger_entries (wallet_id, transfer_id, leg, amount, balance_after, created_at) VALUES ($1, $2, $3, $4, $5, NOW())`, intent.ToWallet, intent.TransferId, string(domain.LegCredit), intent.Amount, toBal+intent.Amount)
	if err != nil {
		return err
	}

	// Update cached balance ONLY for customer destination ToWallet (clearing wallet bypasses cache lock)
	_, err = tx.Exec(ctx, `
		UPDATE wallet_balance_cache SET balance = balance + $1, updated_at = NOW() WHERE wallet_id = $2
	`, intent.Amount, intent.ToWallet)
	if err != nil {
		return err
	}

	// 7. Emit settled event back to source shard
	eventID := platform.NewEventID()
	now := time.Now()

	envelope := &eventsv1.EventEnvelope{
		EventId:       eventID,
		EventType:     string(platform.EventTypeXShardTransferSettled),
		AggregateType: string(platform.AggregateTypeXShardTransfer),
		AggregateId:   intent.TransferId,
		CorrelationId: intent.JobId,
		OccurredAt:    timestamppb.New(now),
		Traceparent:   platform.ExtractTraceparent(ctx),
		Payload: &eventsv1.EventEnvelope_XshardTransferSettled{
			XshardTransferSettled: &eventsv1.XShardTransferSettledPayload{
				TransferId: intent.TransferId,
				SrcShard:   intent.SrcShard,
				DstShard:   intent.DstShard,
			},
		},
	}
	payloadBytes, err := platform.MarshalEnvelope(envelope)
	if err != nil {
		return err
	}
	publishTopic := platform.TopicXShardPrefix + intent.SrcShard

	_, err = tx.Exec(ctx, `
		INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, correlation_id, payload, occurred_at, publish_topic)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, eventID, string(platform.EventTypeXShardTransferSettled), string(platform.AggregateTypeXShardTransfer), intent.TransferId, intent.JobId, payloadBytes, now, publishTopic)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// CountUnresolvedSagas returns the count of cross_shard_transfer rows on every shard that are still pending and older than thresholdMs.
func (s *CrossShardStore) CountUnresolvedSagas(ctx context.Context, thresholdMs int64) (int64, error) {
	shardIDs := s.pools.GetAvailableShardIDs()
	if len(shardIDs) == 0 {
		return 0, nil
	}
	var total int64
	for _, shardID := range shardIDs {
		pool, err := s.pools.ShardPool(shardID)
		if err != nil {
			continue
		}
		var n int64
		err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM cross_shard_transfer WHERE state = $1 AND created_at < NOW() - ($2::bigint * INTERVAL '1 millisecond')`, platform.XShardTransferStatusPending, thresholdMs).Scan(&n)
		if err != nil {
			continue
		}
		total += n
	}
	return total, nil
}

func (s *CrossShardStore) SettleCrossShardTransfer(ctx context.Context, srcShard string, transferID string) (int64, string, error) {
	pool, err := s.pools.ShardPool(srcShard)
	if err != nil {
		return 0, "", err
	}

	// A4.3: capture saga start for rrq_business_saga_duration_seconds histogram.
	sagaStart := time.Now()

	// Removed hardcoded timeout
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, "", err
	}
	defer tx.Rollback(ctx)

	// 1. Idempotency Check & Retrieve Saga info (singular table)
	var state string
	var jobID string
	err = tx.QueryRow(ctx, `SELECT state, job_id FROM cross_shard_transfer WHERE transfer_id=$1 FOR UPDATE`, transferID).Scan(&state, &jobID)
	if err != nil {
		return 0, "", err // If pgx.ErrNoRows, it's a fatal error because the saga must exist on the source shard
	}
	if state == platform.XShardTransferStatusCompleted {
		return 0, "", nil // Already settled
	}

	// 2. Update Saga State
	_, err = tx.Exec(ctx, `UPDATE cross_shard_transfer SET state=$1, settled_at=NOW() WHERE transfer_id=$2`, platform.XShardTransferStatusCompleted, transferID)
	if err != nil {
		return 0, "", err
	}

	// 3. Update Job State
	_, err = tx.Exec(ctx, `UPDATE jobs SET status=$1, completed_at=NOW() WHERE id=$2`, platform.JobStatusCompleted, jobID)
	if err != nil {
		return 0, "", err
	}

	// 4. Retrieve Source Merchant ID to build the TransferEventPayload
	var fromWallet, toWallet, currency string
	var amount int64
	err = tx.QueryRow(ctx, `SELECT from_wallet, to_wallet, amount, currency FROM cross_shard_transfer WHERE transfer_id=$1`, transferID).Scan(&fromWallet, &toWallet, &amount, &currency)
	if err != nil {
		return 0, "", err
	}

	var merchantID string
	err = tx.QueryRow(ctx, "SELECT merchant_id FROM wallets WHERE id=$1", fromWallet).Scan(&merchantID)
	if err != nil {
		return 0, "", err
	}

	// 5. Emit Final Transfer Completed Event (Unified TransferEventPayload, platform.TopicNotify)
	eventID := platform.NewEventID()
	now := time.Now()

	envelope := &eventsv1.EventEnvelope{
		EventId:       eventID,
		EventType:     string(platform.EventTypeTransferCompleted),
		AggregateType: string(platform.AggregateTypeTransfer),
		AggregateId:   transferID,
		CorrelationId: jobID,
		OccurredAt:    timestamppb.New(now),
		Traceparent:   platform.ExtractTraceparent(ctx),
		Payload: &eventsv1.EventEnvelope_TransferCompleted{
			TransferCompleted: &eventsv1.TransferCompletedPayload{
				JobId:      jobID,
				TransferId: transferID,
				MerchantId: merchantID,
				FromWallet: fromWallet,
				ToWallet:   toWallet,
				Amount:     amount,
				Currency:   currency,
			},
		},
	}
	payloadBytes, err := platform.MarshalEnvelope(envelope)
	if err != nil {
		return 0, "", err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, correlation_id, payload, occurred_at, publish_topic)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, eventID, string(platform.EventTypeTransferCompleted), string(platform.AggregateTypeTransfer), transferID, jobID, payloadBytes, now, platform.TopicNotify)
	if err != nil {
		return 0, "", err
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, "", err
	}

	// A4.3: record saga duration only on successful commit.
	platform.RecordBusinessSagaDuration(ctx, time.Since(sagaStart))
	return amount, currency, nil
}

func (s *CrossShardStore) ReverseCrossShardTransfer(ctx context.Context, srcShard, transferID, reason string) error {
	pool, err := s.pools.ShardPool(srcShard)
	if err != nil {
		return err
	}

	// Removed hardcoded timeout

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// 1. Idempotency Check
	var existingState string
	var jobID, fromWallet, toWallet, currency string
	var amount int64
	err = tx.QueryRow(ctx, `SELECT state, job_id, from_wallet, to_wallet, amount, currency FROM cross_shard_transfer WHERE transfer_id=$1 FOR UPDATE`,
		transferID).Scan(&existingState, &jobID, &fromWallet, &toWallet, &amount, &currency)
	if err != nil {
		return err
	}
	if existingState == platform.XShardTransferStatusReversed {
		return nil // Already reversed
	}

	// 2. Resolve the Source Merchant ID and its Clearing Wallet (read-only)
	var merchantID string
	err = tx.QueryRow(ctx, "SELECT merchant_id FROM wallets WHERE id=$1", fromWallet).Scan(&merchantID)
	if err != nil {
		if err == pgx.ErrNoRows {
			return domain.ErrWalletNotFound
		}
		return err
	}

	var clearingWallet string
	err = tx.QueryRow(ctx, `SELECT id FROM wallets WHERE wallet_type = $1 AND currency = $2`,
		string(domain.WalletTypeSystem), currency).Scan(&clearingWallet)
	if err != nil {
		if err == pgx.ErrNoRows {
			return domain.ErrWalletNotFound
		}
		return err
	}

	// 3. Lock and get cached balance only for customer fromWallet
	var fromBal int64
	err = tx.QueryRow(ctx, `
		SELECT balance FROM wallet_balance_cache WHERE wallet_id = $1 FOR UPDATE
	`, fromWallet).Scan(&fromBal)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Initialize by summing existing ledger entries (bootstrapping step)
			err = tx.QueryRow(ctx, `
				SELECT COALESCE(SUM(amount), 0) FROM ledger_entries WHERE wallet_id = $1
			`, fromWallet).Scan(&fromBal)
			if err != nil {
				return err
			}
			// Insert the bootstrapped balance into cache and lock it
			_, err = tx.Exec(ctx, `
				INSERT INTO wallet_balance_cache (wallet_id, balance, last_entry_id, updated_at)
				VALUES ($1, $2, 0, NOW())
				ON CONFLICT (wallet_id) DO UPDATE SET balance = EXCLUDED.balance, updated_at = NOW()
			`, fromWallet, fromBal)
			if err != nil {
				return err
			}
			// Lock again to ensure consistency
			err = tx.QueryRow(ctx, `
				SELECT balance FROM wallet_balance_cache WHERE wallet_id = $1 FOR UPDATE
			`, fromWallet).Scan(&fromBal)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	// 5. Record the reversal in the transfers table (creates a new parent ID for the ledger entries)
	revTransferID := domain.ReversalPrefix + transferID
	_, err = tx.Exec(ctx, `
		INSERT INTO transfers (id, job_id, from_wallet, to_wallet, amount, currency, status, failure_reason, posted_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
	`, revTransferID, jobID, clearingWallet, fromWallet, amount, currency, platform.TransferStatusCompleted, reason)
	if err != nil {
		return err
	}

	// 6. Reverse Ledger Entries: Debit clearing wallet, Credit fromWallet (returning the funds)
	_, err = tx.Exec(ctx, `INSERT INTO ledger_entries (wallet_id, transfer_id, leg, amount, balance_after, created_at) VALUES ($1, $2, $3, $4, $5, NOW())`,
		clearingWallet, revTransferID, string(domain.LegDebit), -amount, 0)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO ledger_entries (wallet_id, transfer_id, leg, amount, balance_after, created_at) VALUES ($1, $2, $3, $4, $5, NOW())`,
		fromWallet, revTransferID, string(domain.LegCredit), amount, fromBal+amount)
	if err != nil {
		return err
	}

	// Update cached balance ONLY for customer fromWallet
	_, err = tx.Exec(ctx, `
		UPDATE wallet_balance_cache SET balance = balance + $1, updated_at = NOW() WHERE wallet_id = $2
	`, amount, fromWallet)
	if err != nil {
		return err
	}

	// 6. Update saga state to reversed
	_, err = tx.Exec(ctx, `UPDATE cross_shard_transfer SET state=$1, reason=$2, settled_at=NOW() WHERE transfer_id=$3`,
		platform.XShardTransferStatusReversed, reason, transferID)
	if err != nil {
		return err
	}

	// 7. Update job state to failed
	_, err = tx.Exec(ctx, `UPDATE jobs SET status=$1, failure_reason=$2, completed_at=NOW() WHERE id=$3`,
		platform.JobStatusFailed, reason, jobID)
	if err != nil {
		return err
	}

	// 6. Emit unified transfer.failed to outbox
	eventID := platform.NewEventID()
	now := time.Now()

	envelope := &eventsv1.EventEnvelope{
		EventId:       eventID,
		EventType:     string(platform.EventTypeTransferFailed),
		AggregateType: string(platform.AggregateTypeTransfer),
		AggregateId:   transferID,
		CorrelationId: jobID,
		OccurredAt:    timestamppb.New(now),
		Traceparent:   platform.ExtractTraceparent(ctx),
		Payload: &eventsv1.EventEnvelope_TransferFailed{
			TransferFailed: &eventsv1.TransferFailedPayload{
				JobId:      jobID,
				TransferId: transferID,
				MerchantId: merchantID,
				Reason:     reason,
			},
		},
	}
	payloadBytes, err := platform.MarshalEnvelope(envelope)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, correlation_id, payload, occurred_at, publish_topic)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, eventID, string(platform.EventTypeTransferFailed), string(platform.AggregateTypeTransfer), transferID, jobID, payloadBytes, now, platform.TopicNotify)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}
