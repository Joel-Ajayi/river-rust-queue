package postgres

import (
	"context"
	"slices"
	"time"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/ports"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type CrossShardStore struct {
	pools  *platform.ShardPools
	logger *zap.Logger
}

var _ ports.CrossShardStore = (*CrossShardStore)(nil)

func NewCrossShardStore(pools *platform.ShardPools, logger *zap.Logger) *CrossShardStore {
	return &CrossShardStore{pools: pools, logger: logger}
}

func (s *CrossShardStore) DebitToClearingAccount(ctx context.Context, srcShard, dstShard, jobID string, transfer domain.Transfer) error {
	pool, err := s.pools.ShardPool(srcShard)
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

	// 1a. Lock job to ensure idempotency and prevent concurrent processing
	var jobStatus string
	err = tx.QueryRow(txCtx, `SELECT status FROM jobs WHERE id = $1 FOR UPDATE`, jobID).Scan(&jobStatus)
	if err != nil {
		if err == pgx.ErrNoRows {
			s.logger.Error("Job not found on shard", zap.String("job_id", jobID))
			return nil
		}
		return err
	}
	if jobStatus != string(domain.JobStatusPending) {
		s.logger.Info("Job already processed, skipping DebitToClearingAccount", zap.String("job_id", jobID), zap.String("status", jobStatus))
		return nil
	}

	// 2. Look up the clearing wallet for the source merchant (lives on this shard)
	var clearingWallet string
	err = tx.QueryRow(txCtx, `SELECT id FROM wallets WHERE merchant_id = $1 AND wallet_type = $2 AND currency = $3`, transfer.MerchantID, string(domain.WalletTypeSystem), transfer.Currency).Scan(&clearingWallet)
	if err != nil {
		if err == pgx.ErrNoRows {
			return domain.ErrWalletNotFound
		}
		return err
	}

	// 3. Lock wallets in sorted order to prevent deadlocks
	wallets := []string{transfer.FromWallet, clearingWallet}
	slices.Sort(wallets)

	rows, err := tx.Query(txCtx, `SELECT id, currency, status FROM wallets WHERE id = ANY($1) ORDER BY id FOR UPDATE`, wallets)
	if err != nil {
		return err
	}
	defer rows.Close()

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

	// 4. Balance checks
	var fromBal, clearingBal int64
	err = tx.QueryRow(txCtx, `SELECT COALESCE(SUM(amount), 0) FROM ledger_entries WHERE wallet_id = $1`, transfer.FromWallet).Scan(&fromBal)
	if err != nil {
		return err
	}
	if fromBal < transfer.Amount {
		return domain.ErrInsufficientBalance
	}

	err = tx.QueryRow(txCtx, `SELECT COALESCE(SUM(amount), 0) FROM ledger_entries WHERE wallet_id = $1`, clearingWallet).Scan(&clearingBal)
	if err != nil {
		return err
	}

	// 5. Record transfer — ON CONFLICT DO NOTHING handles redelivery idempotency
	tag, err := tx.Exec(txCtx, `
		INSERT INTO transfers (id, job_id, from_wallet, to_wallet, amount, currency, status, posted_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
		ON CONFLICT (id) DO NOTHING
	`, transfer.ID, jobID, transfer.FromWallet, transfer.ToWallet, transfer.Amount, transfer.Currency, string(domain.TransferStateCompleted))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		s.logger.Info("idempotency hit on DebitToClearingAccount", zap.String("transfer_id", transfer.ID))
		return nil // Duplicate redelivery — already completed
	}

	// 6. Insert ledger entries
	// debit FromWallet
	_, err = tx.Exec(txCtx, `INSERT INTO ledger_entries (wallet_id, transfer_id, leg, amount, balance_after, created_at) VALUES ($1, $2, $3, $4, $5, NOW())`,
		transfer.FromWallet, transfer.ID, string(domain.LegDebit), -transfer.Amount, fromBal-transfer.Amount)
	if err != nil {
		return err
	}
	// credit ClearingWallet
	_, err = tx.Exec(txCtx, `INSERT INTO ledger_entries (wallet_id, transfer_id, leg, amount, balance_after, created_at) VALUES ($1, $2, $3, $4, $5, NOW())`,
		clearingWallet, transfer.ID, string(domain.LegCredit), transfer.Amount, clearingBal+transfer.Amount)
	if err != nil {
		return err
	}

	// 6. Record saga
	var existingState string
	err = tx.QueryRow(txCtx, `
		INSERT INTO cross_shard_transfer (transfer_id, job_id, src_shard, dst_shard, from_wallet, to_wallet, amount, currency, state)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (transfer_id) DO NOTHING
		RETURNING state
	`, transfer.ID, jobID, srcShard, dstShard, transfer.FromWallet, transfer.ToWallet, transfer.Amount, transfer.Currency, string(domain.XShardTransferStatePending)).Scan(&existingState)
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
				Status:     string(domain.TransferStatePending),
			},
		},
	}
	marshaler := protojson.MarshalOptions{EmitUnpopulated: true}
	payloadBytes, err := marshaler.Marshal(envelope)
	if err != nil {
		return err
	}
	publishTopic := platform.TopicXShardPrefix + dstShard

	_, err = tx.Exec(txCtx, `
		INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, correlation_id, payload, occurred_at, publish_topic)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, eventID, string(platform.EventTypeXShardTransferRequested), string(platform.AggregateTypeXShardTransfer), transfer.ID, jobID, payloadBytes, now, publishTopic)
	if err != nil {
		return err
	}

	if err := tx.Commit(txCtx); err != nil {
		return err
	}
	s.logger.Info("DebitToClearingAccount committed", zap.String("transfer_id", transfer.ID))
	return nil
}

func (s *CrossShardStore) CreditFromClearingAccount(ctx context.Context, intent *eventsv1.XShardTransferRequestedPayload) error {
	s.logger.Info("Starting CreditFromClearingAccount", zap.String("transfer_id", intent.TransferId))
	pool, err := s.pools.ShardPool(intent.DstShard)
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
			Payload: &eventsv1.EventEnvelope_XshardTransferFailed{
				XshardTransferFailed: &eventsv1.XShardTransferFailedPayload{
					TransferId: intent.TransferId,
					SrcShard:   intent.SrcShard,
					DstShard:   intent.DstShard,
					Reason:     reason,
				},
			},
		}
		payloadBytes, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(envelope)
		if err != nil {
			return err
		}
		publishTopic := platform.TopicXShardPrefix + intent.SrcShard
		_, err = tx.Exec(txCtx, `
			INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, correlation_id, payload, occurred_at, publish_topic)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		`, eventID, string(platform.EventTypeXShardTransferFailed), string(platform.AggregateTypeXShardTransfer), intent.TransferId, intent.JobId, payloadBytes, now, publishTopic)
		if err != nil {
			return err
		}
		return tx.Commit(txCtx)
	}

	// 2. Resolve the Destination Merchant ID and its Clearing Wallet
	var destMerchantID string
	err = tx.QueryRow(txCtx, "SELECT merchant_id FROM wallets WHERE id=$1", intent.ToWallet).Scan(&destMerchantID)
	if err != nil {
		if err == pgx.ErrNoRows {
			return emitFailure(domain.ErrWalletNotFound.Error())
		}
		return err
	}

	var clearingWallet string
	err = tx.QueryRow(txCtx, `SELECT id FROM wallets WHERE merchant_id = $1 AND wallet_type = $2 AND currency = $3`, destMerchantID, string(domain.WalletTypeSystem), intent.Currency).Scan(&clearingWallet)
	if err != nil {
		if err == pgx.ErrNoRows {
			return emitFailure(domain.ErrWalletNotFound.Error())
		}
		return err
	}

	// 3. Lock Wallets (Destination Merchant's Clearing Wallet + Destination Merchant's Receiving Wallet)
	wallets := []string{intent.ToWallet, clearingWallet}
	slices.Sort(wallets)

	rows, err := tx.Query(txCtx, `SELECT id, currency, status FROM wallets WHERE id = ANY($1) ORDER BY id FOR UPDATE`, wallets)
	if err != nil {
		return err
	}
	defer rows.Close()

	foundWallets := 0
	for rows.Next() {
		var id, currency, status string
		if err := rows.Scan(&id, &currency, &status); err != nil {
			return err
		}
		foundWallets++
		if status == string(domain.WalletStatusFrozen) {
			return emitFailure(domain.ErrWalletFrozen.Error())
		}
		if status == string(domain.WalletStatusClosed) {
			return emitFailure(domain.ErrWalletClosed.Error())
		}
		if currency != intent.Currency {
			return emitFailure(domain.ErrCurrencyMismatch.Error())
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if foundWallets != 2 {
		return emitFailure(domain.ErrWalletNotFound.Error())
	}

	// 4. Balance checks
	var clearingBal, toBal int64
	err = tx.QueryRow(txCtx, `SELECT COALESCE(SUM(amount), 0) FROM ledger_entries WHERE wallet_id = $1`, clearingWallet).Scan(&clearingBal)
	if err != nil {
		return err
	}
	err = tx.QueryRow(txCtx, `SELECT COALESCE(SUM(amount), 0) FROM ledger_entries WHERE wallet_id = $1`, intent.ToWallet).Scan(&toBal)
	if err != nil {
		return err
	}

	// 5. Record Final Transfer State on Destination Shard (ON CONFLICT DO NOTHING handles redelivery)
	tag, err := tx.Exec(txCtx, `
		INSERT INTO transfers (id, job_id, from_wallet, to_wallet, amount, currency, status, posted_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
		ON CONFLICT (id) DO NOTHING
	`, intent.TransferId, intent.JobId, intent.FromWallet, intent.ToWallet, intent.Amount, intent.Currency, string(domain.TransferStateCompleted))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		s.logger.Info("idempotency hit on CreditFromClearingAccount", zap.String("transfer_id", intent.TransferId))
		return nil // Duplicate redelivery
	}

	// 6. Insert Ledger Entries (Debit clearing, Credit receiver)
	_, err = tx.Exec(txCtx, `INSERT INTO ledger_entries (wallet_id, transfer_id, leg, amount, balance_after, created_at) VALUES ($1, $2, $3, $4, $5, NOW())`, clearingWallet, intent.TransferId, string(domain.LegDebit), -intent.Amount, clearingBal-intent.Amount)
	if err != nil {
		return err
	}
	_, err = tx.Exec(txCtx, `INSERT INTO ledger_entries (wallet_id, transfer_id, leg, amount, balance_after, created_at) VALUES ($1, $2, $3, $4, $5, NOW())`, intent.ToWallet, intent.TransferId, string(domain.LegCredit), intent.Amount, toBal+intent.Amount)
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
		Payload: &eventsv1.EventEnvelope_XshardTransferSettled{
			XshardTransferSettled: &eventsv1.XShardTransferSettledPayload{
				TransferId: intent.TransferId,
				SrcShard:   intent.SrcShard,
				DstShard:   intent.DstShard,
			},
		},
	}
	marshaler := protojson.MarshalOptions{EmitUnpopulated: true}
	payloadBytes, err := marshaler.Marshal(envelope)
	if err != nil {
		return err
	}
	publishTopic := platform.TopicXShardPrefix + intent.SrcShard

	_, err = tx.Exec(txCtx, `
		INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, correlation_id, payload, occurred_at, publish_topic)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, eventID, string(platform.EventTypeXShardTransferSettled), string(platform.AggregateTypeXShardTransfer), intent.TransferId, intent.JobId, payloadBytes, now, publishTopic)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *CrossShardStore) SettleCrossShardTransfer(ctx context.Context, srcShard string, transferID string) error {
	pool, err := s.pools.ShardPool(srcShard)
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

	// 1. Idempotency Check & Retrieve Saga info (singular table)
	var state string
	var jobID string
	err = tx.QueryRow(txCtx, `SELECT state, job_id FROM cross_shard_transfer WHERE transfer_id=$1 FOR UPDATE`, transferID).Scan(&state, &jobID)
	if err != nil {
		return err // If pgx.ErrNoRows, it's a fatal error because the saga must exist on the source shard
	}
	if state == string(domain.XShardTransferStateCompleted) {
		s.logger.Info("idempotency hit on SettleCrossShardTransfer", zap.String("transfer_id", transferID))
		return nil // Already settled
	}

	// 2. Update Saga State
	_, err = tx.Exec(txCtx, `UPDATE cross_shard_transfer SET state=$1, settled_at=NOW() WHERE transfer_id=$2`, string(domain.XShardTransferStateCompleted), transferID)
	if err != nil {
		return err
	}

	// 3. Update Job State
	_, err = tx.Exec(txCtx, `UPDATE jobs SET status=$1, completed_at=NOW() WHERE id=$2`, string(domain.JobStatusCompleted), jobID)
	if err != nil {
		return err
	}

	// 4. Retrieve Source Merchant ID to build the TransferEventPayload
	var fromWallet, toWallet, currency string
	var amount int64
	err = tx.QueryRow(txCtx, `SELECT from_wallet, to_wallet, amount, currency FROM cross_shard_transfer WHERE transfer_id=$1`, transferID).Scan(&fromWallet, &toWallet, &amount, &currency)
	if err != nil {
		return err
	}

	var merchantID string
	err = tx.QueryRow(txCtx, "SELECT merchant_id FROM wallets WHERE id=$1", fromWallet).Scan(&merchantID)
	if err != nil {
		return err
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
	marshaler := protojson.MarshalOptions{EmitUnpopulated: true}
	payloadBytes, err := marshaler.Marshal(envelope)
	if err != nil {
		return err
	}

	_, err = tx.Exec(txCtx, `
		INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, correlation_id, payload, occurred_at, publish_topic)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, eventID, string(platform.EventTypeTransferCompleted), string(platform.AggregateTypeTransfer), transferID, jobID, payloadBytes, now, platform.TopicNotify)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *CrossShardStore) ReverseCrossShardTransfer(ctx context.Context, srcShard, transferID, reason string) error {
	pool, err := s.pools.ShardPool(srcShard)
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

	// 1. Idempotency Check
	var existingState string
	var jobID, fromWallet, toWallet, currency string
	var amount int64
	err = tx.QueryRow(txCtx, `SELECT state, job_id, from_wallet, to_wallet, amount, currency FROM cross_shard_transfer WHERE transfer_id=$1 FOR UPDATE`,
		transferID).Scan(&existingState, &jobID, &fromWallet, &toWallet, &amount, &currency)
	if err != nil {
		return err
	}
	if existingState == string(domain.XShardTransferStateReversed) {
		s.logger.Info("idempotency hit on ReverseCrossShardTransfer", zap.String("transfer_id", transferID))
		return nil // Already reversed
	}

	// 2. Resolve the Source Merchant ID and its Clearing Wallet
	var merchantID string
	err = tx.QueryRow(txCtx, "SELECT merchant_id FROM wallets WHERE id=$1", fromWallet).Scan(&merchantID)
	if err != nil {
		if err == pgx.ErrNoRows {
			return domain.ErrWalletNotFound
		}
		return err
	}

	var clearingWallet string
	err = tx.QueryRow(txCtx, `SELECT id FROM wallets WHERE merchant_id = $1 AND wallet_type = $2 AND currency = $3`,
		merchantID, string(domain.WalletTypeSystem), currency).Scan(&clearingWallet)
	if err != nil {
		if err == pgx.ErrNoRows {
			return domain.ErrWalletNotFound
		}
		return err
	}

	// 3. Lock clearing wallet and fromWallet to reverse funds
	wallets := []string{fromWallet, clearingWallet}
	slices.Sort(wallets)

	rows, err := tx.Query(txCtx, `SELECT id FROM wallets WHERE id = ANY($1) ORDER BY id FOR UPDATE`, wallets)
	if err != nil {
		return err
	}
	for rows.Next() {
		// Drain cursor to hold FOR UPDATE locks
		var walletID string
		if err := rows.Scan(&walletID); err != nil {
			rows.Close()
			return err
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	// 4. Balance checks
	var clearingBal, fromBal int64
	err = tx.QueryRow(txCtx, `SELECT COALESCE(SUM(amount), 0) FROM ledger_entries WHERE wallet_id = $1`, clearingWallet).Scan(&clearingBal)
	if err != nil {
		return err
	}
	err = tx.QueryRow(txCtx, `SELECT COALESCE(SUM(amount), 0) FROM ledger_entries WHERE wallet_id = $1`, fromWallet).Scan(&fromBal)
	if err != nil {
		return err
	}

	// 5. Record the reversal in the transfers table (creates a new parent ID for the ledger entries)
	revTransferID := domain.ReversalPrefix + transferID
	_, err = tx.Exec(txCtx, `
		INSERT INTO transfers (id, job_id, from_wallet, to_wallet, amount, currency, status, failure_reason, posted_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
	`, revTransferID, jobID, clearingWallet, fromWallet, amount, currency, string(domain.TransferStateCompleted), reason)
	if err != nil {
		return err
	}

	// 6. Reverse Ledger Entries: Debit clearing wallet, Credit fromWallet (returning the funds)
	_, err = tx.Exec(txCtx, `INSERT INTO ledger_entries (wallet_id, transfer_id, leg, amount, balance_after, created_at) VALUES ($1, $2, $3, $4, $5, NOW())`,
		clearingWallet, revTransferID, string(domain.LegDebit), -amount, clearingBal-amount)
	if err != nil {
		return err
	}
	_, err = tx.Exec(txCtx, `INSERT INTO ledger_entries (wallet_id, transfer_id, leg, amount, balance_after, created_at) VALUES ($1, $2, $3, $4, $5, NOW())`,
		fromWallet, revTransferID, string(domain.LegCredit), amount, fromBal+amount)
	if err != nil {
		return err
	}

	// 6. Update saga state to reversed
	_, err = tx.Exec(txCtx, `UPDATE cross_shard_transfer SET state=$1, reason=$2, settled_at=NOW() WHERE transfer_id=$3`,
		string(domain.XShardTransferStateReversed), reason, transferID)
	if err != nil {
		return err
	}

	// 7. Update job state to failed
	_, err = tx.Exec(txCtx, `UPDATE jobs SET status=$1, failure_reason=$2, completed_at=NOW() WHERE id=$3`,
		string(domain.JobStatusFailed), reason, jobID)
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
		Payload: &eventsv1.EventEnvelope_TransferFailed{
			TransferFailed: &eventsv1.TransferFailedPayload{
				JobId:      jobID,
				TransferId: transferID,
				MerchantId: merchantID,
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
	`, eventID, string(platform.EventTypeTransferFailed), string(platform.AggregateTypeTransfer), transferID, jobID, payloadBytes, now, platform.TopicNotify)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}
