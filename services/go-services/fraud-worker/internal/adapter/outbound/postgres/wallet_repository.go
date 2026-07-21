package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/core/port"
	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type WalletRepository struct {
	pools  *platform.ShardPools
	logger *zap.Logger
}

var _ port.WalletRepository = (*WalletRepository)(nil)

func NewWalletRepository(pools *platform.ShardPools, logger *zap.Logger) *WalletRepository {
	return &WalletRepository{pools: pools, logger: logger}
}

func (r *WalletRepository) GetWalletStatus(ctx context.Context, shardID string, walletID string) (string, error) {
	pool, err := r.pools.ShardPoolRO(shardID)
	if err != nil {
		return "", err
	}

	var status string
	err = pool.QueryRow(ctx, `SELECT status FROM wallets WHERE id = $1`, walletID).Scan(&status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		return "", err
	}

	return status, nil
}

func (r *WalletRepository) FreezeWallet(ctx context.Context, shardID string, walletID string, reason string) error {
	pool, err := r.pools.ShardPool(shardID)
	if err != nil {
		return err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Idempotent update: only succeed if currently active.
	result, err := tx.Exec(ctx, `
		UPDATE wallets
		SET status = $2, frozen_reason = $3
		WHERE id = $1 AND status = $4
	`, walletID, platform.MerchantStatusFrozen, reason, platform.MerchantStatusActive)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return nil
	}

	// Emit event
	eventID := platform.NewEventID()
	now := time.Now()

	envelope := &eventsv1.EventEnvelope{
		EventId:       eventID,
		EventType:     string(platform.EventTypeWalletFrozen),
		AggregateType: string(platform.AggregateTypeWallet),
		AggregateId:   walletID,
		OccurredAt:    timestamppb.New(now),
		Traceparent:   platform.ExtractTraceparent(ctx),
		Payload: &eventsv1.EventEnvelope_WalletFrozen{
			WalletFrozen: &eventsv1.WalletFrozenPayload{
				WalletId: walletID,
				Reason:   reason,
				FrozenBy: "system",
			},
		},
	}

	marshaler := protojson.MarshalOptions{EmitUnpopulated: true}
	payloadBytes, err := marshaler.Marshal(envelope)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, payload, occurred_at, publish_topic)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, eventID, string(platform.EventTypeWalletFrozen), string(platform.AggregateTypeWallet), walletID, payloadBytes, now, platform.TopicNotify)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}
