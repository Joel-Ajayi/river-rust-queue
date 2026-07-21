package app

import (
	"context"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/port"
	"go.uber.org/zap"
)

type XShardService struct {
	xshardStore port.CrossShardStore
	logger      *zap.Logger
}

func NewXShardService(logger *zap.Logger, xshardStore port.CrossShardStore) *XShardService {
	return &XShardService{
		xshardStore: xshardStore,
		logger:      logger,
	}
}

func (s *XShardService) HandleXShardRequested(ctx context.Context, payload *eventsv1.XShardTransferRequestedPayload) error {
	return s.xshardStore.CreditFromClearingAccount(ctx, payload)
}

func (s *XShardService) HandleXShardSettled(ctx context.Context, payload *eventsv1.XShardTransferSettledPayload) error {
	err := s.xshardStore.SettleCrossShardTransfer(ctx, payload.SrcShard, payload.TransferId)
	if err != nil {
		return err
	}

	// Business metrics: saga completed + successful cross-shard transfer
	platform.RecordBusinessSagaCompleted(ctx)
	platform.RecordBusinessTransfer(ctx, platform.TransferMetricSuccess, platform.ShardTypeCross)

	// Canonical log: saga completed (cross-shard transfer fully settled)
	platform.LogCanonicalEvent(ctx, s.logger, platform.ServiceNameLedgerWorker, platform.CanonicalLogLine{
		Event:      platform.EventSagaCompleted,
		Status:     platform.StatusSuccess,
		TransferID: payload.TransferId,
	})
	return nil
}

func (s *XShardService) HandleXShardFailed(ctx context.Context, payload *eventsv1.XShardTransferFailedPayload) error {
	err := s.xshardStore.ReverseCrossShardTransfer(ctx, payload.SrcShard, payload.TransferId, payload.Reason)
	if err != nil {
		return err
	}

	// Business metrics: failed cross-shard transfer + decline
	platform.RecordBusinessTransfer(ctx, platform.TransferMetricFailed, platform.ShardTypeCross)
	platform.RecordBusinessDecline(ctx, platform.DeclineReasonSagaCompensated)

	// Canonical log: saga compensated/failed
	platform.LogCanonicalEvent(ctx, s.logger, platform.ServiceNameLedgerWorker, platform.CanonicalLogLine{
		Event:        platform.EventSagaCompensated,
		Status:       platform.StatusFailed,
		TransferID:   payload.TransferId,
		ErrorCode:    platform.ErrorCodeSagaFailed,
		ErrorMessage: payload.Reason,
	})
	return nil
}
