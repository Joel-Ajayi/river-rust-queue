package postgres

import (
	"context"
	"errors"
	"time"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/recon-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/recon-worker/internal/core/port"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	reconAdvisoryLockKey = 20260718
)

type ReconRepository struct {
	pools    *platform.ShardPools
	lockConn *pgxpool.Conn
}

var _ port.ReconRepository = (*ReconRepository)(nil)

func NewReconRepository(pools *platform.ShardPools) *ReconRepository {
	return &ReconRepository{pools: pools}
}

func (r *ReconRepository) AcquireLock(ctx context.Context) (bool, error) {
	conn, err := r.pools.MerchantsPool().Acquire(ctx)
	if err != nil {
		return false, err
	}
	r.lockConn = conn

	var locked bool
	// Use arbitrary 64-bit integer hash for rrq advisory lock key
	err = conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", reconAdvisoryLockKey).Scan(&locked)
	if err != nil {
		conn.Release()
		r.lockConn = nil
		return false, err
	}
	if !locked {
		conn.Release()
		r.lockConn = nil
	}
	return locked, nil
}

func (r *ReconRepository) ReleaseLock(ctx context.Context) error {
	if r.lockConn == nil {
		return nil
	}
	defer func() {
		r.lockConn.Release()
		r.lockConn = nil
	}()

	var unlocked bool
	err := r.lockConn.QueryRow(ctx, "SELECT pg_advisory_unlock($1)", reconAdvisoryLockKey).Scan(&unlocked)
	return err
}

func (r *ReconRepository) GetShardSum(ctx context.Context, shardID string, cutoff time.Time) (int64, error) {
	pool, err := r.pools.ShardPoolRO(shardID)
	if err != nil {
		return 0, err
	}

	var sum int64
	err = pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(amount), 0) FROM ledger_entries WHERE created_at < $1
	`, cutoff).Scan(&sum)
	return sum, err
}

func (r *ReconRepository) FindAffectedWallets(ctx context.Context, shardID string, start, cutoff time.Time) ([]string, error) {
	pool, err := r.pools.ShardPoolRO(shardID)
	if err != nil {
		return nil, err
	}

	rows, err := pool.Query(ctx, `
		SELECT DISTINCT wallet_id FROM ledger_entries WHERE created_at >= $1 AND created_at < $2
	`, start, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var walletIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		walletIDs = append(walletIDs, id)
	}
	return walletIDs, rows.Err()
}

func (r *ReconRepository) CheckWallet(ctx context.Context, shardID string, walletID string, cutoff time.Time) (*domain.Discrepancy, error) {
	pool, err := r.pools.ShardPoolRO(shardID)
	if err != nil {
		return nil, err
	}

	rows, err := pool.Query(ctx, `
		SELECT amount, balance_after FROM ledger_entries 
		WHERE wallet_id = $1 AND created_at < $2 
		ORDER BY id ASC
	`, walletID, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var derived int64
	for rows.Next() {
		var amount, balanceAfter int64
		if err := rows.Scan(&amount, &balanceAfter); err != nil {
			return nil, err
		}
		derived += amount
		if balanceAfter != derived {
			return &domain.Discrepancy{
				Kind:           domain.DiscrepancyKindBalanceAfter,
				WalletID:       walletID,
				DerivedBalance: derived,
				StoredBalance:  balanceAfter,
			}, nil
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var cached int64
	err = pool.QueryRow(ctx, `
		SELECT balance FROM wallet_balance_cache WHERE wallet_id = $1
	`, walletID).Scan(&cached)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			if derived != 0 {
				return &domain.Discrepancy{
					Kind:           domain.DiscrepancyKindProjectionDrift,
					WalletID:       walletID,
					DerivedBalance: derived,
					CachedBalance:  0,
					Delta:          -derived,
				}, nil
			}
			return nil, nil
		}
		return nil, err
	}

	if derived != cached {
		return &domain.Discrepancy{
			Kind:           domain.DiscrepancyKindProjectionDrift,
			WalletID:       walletID,
			DerivedBalance: derived,
			CachedBalance:  cached,
			Delta:          cached - derived,
		}, nil
	}

	return nil, nil
}

func (r *ReconRepository) CheckTransferLegs(ctx context.Context, shardID string, cutoff time.Time) ([]string, error) {
	pool, err := r.pools.ShardPoolRO(shardID)
	if err != nil {
		return nil, err
	}

	rows, err := pool.Query(ctx, `
		SELECT transfer_id FROM ledger_entries 
		WHERE created_at < $1 
		GROUP BY transfer_id 
		HAVING SUM(amount) != 0 OR COUNT(*) != 2
	`, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var transferIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		transferIDs = append(transferIDs, id)
	}
	return transferIDs, rows.Err()
}

func (r *ReconRepository) PersistReport(ctx context.Context, shardID string, report *domain.Report) error {
	pool, err := r.pools.ShardPool(shardID)
	if err != nil {
		return err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	now := time.Now()

	// Write reconciliation.completed event
	completedEventID := platform.NewEventID()
	envelope := &eventsv1.EventEnvelope{
		EventId:       completedEventID,
		EventType:     string(platform.EventTypeReconciliationCompleted),
		AggregateType: string(platform.AggregateTypeReconciliation),
		AggregateId:   report.RunID,
		OccurredAt:    timestamppb.New(now),
		Traceparent:   platform.ExtractTraceparent(ctx),
		Payload: &eventsv1.EventEnvelope_ReconciliationCompleted{
			ReconciliationCompleted: &eventsv1.ReconciliationCompletedPayload{
				RunId:              report.RunID,
				WindowStart:        timestamppb.New(report.WindowStart),
				WindowEnd:          timestamppb.New(report.WindowEnd),
				LedgerSum:          report.GlobalSum,
				WalletsChecked:     int32(report.WalletsChecked),
				DiscrepanciesFound: int32(len(report.Discrepancies)),
				DurationSeconds:    report.DurationSeconds,
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
	`, completedEventID, string(platform.EventTypeReconciliationCompleted), string(platform.AggregateTypeReconciliation), report.RunID, report.RunID, payloadBytes, now, platform.TopicNotify)
	if err != nil {
		return err
	}

	// Write reconciliation.alert events
	for _, d := range report.Discrepancies {
		alertEventID := platform.NewEventID()
		alertEnvelope := &eventsv1.EventEnvelope{
			EventId:       alertEventID,
			EventType:     string(platform.EventTypeReconciliationAlert),
			AggregateType: string(platform.AggregateTypeReconciliation),
			AggregateId:   report.RunID,
			OccurredAt:    timestamppb.New(now),
			Traceparent:   platform.ExtractTraceparent(ctx),
			Payload: &eventsv1.EventEnvelope_ReconciliationAlert{
				ReconciliationAlert: &eventsv1.ReconciliationAlertPayload{
					RunId:          report.RunID,
					Kind:           d.Kind,
					WalletId:       d.WalletID,
					DerivedBalance: d.DerivedBalance,
					CachedBalance:  d.CachedBalance,
					Delta:          d.Delta,
				},
			},
		}

		alertPayloadBytes, err := marshaler.Marshal(alertEnvelope)
		if err != nil {
			return err
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, correlation_id, payload, occurred_at, publish_topic)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		`, alertEventID, string(platform.EventTypeReconciliationAlert), string(platform.AggregateTypeReconciliation), report.RunID, report.RunID, alertPayloadBytes, now, platform.TopicNotify)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}
