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
	// 1. Credit destination wallet from cross-shard clearing account in destination shard
	if err := s.xshardStore.CreditFromClearingAccount(ctx, payload); err != nil {
		platform.LoggerWithTrace(ctx, s.logger).Error(platform.LogEventSagaFailed,
			zap.String(platform.LogFieldTransferID, payload.TransferId),
			zap.String(platform.LogFieldDstShard, payload.DstShard),
			zap.Error(err),
		)
		return err
	}
	return nil
}

func (s *XShardService) HandleXShardSettled(ctx context.Context, payload *eventsv1.XShardTransferSettledPayload) error {
	// 1. Settle source shard clearing account and mark cross-shard transfer completed
	amount, currency, err := s.xshardStore.SettleCrossShardTransfer(ctx, payload.SrcShard, payload.TransferId)
	if err != nil {
		platform.LoggerWithTrace(ctx, s.logger).Error(platform.LogEventSagaFailed,
			zap.String(platform.LogFieldTransferID, payload.TransferId),
			zap.String(platform.LogFieldSrcShard, payload.SrcShard),
			zap.Error(err),
		)
		return err
	}

	// 2. Record Gross Transaction Value and saga completion metrics
	platform.RecordBusinessGTV(ctx, amount, currency)
	platform.RecordBusinessSagaCompleted(ctx)
	platform.RecordBusinessTransfer(ctx, platform.TransferMetricSuccess, platform.ShardTypeCross)

	// 3. Emit canonical transaction log for settled cross-shard saga
	platform.LogCanonicalEvent(ctx, s.logger, platform.ServiceNameLedgerWorker, platform.CanonicalLogLine{
		Event:      platform.EventSagaCompleted,
		Status:     platform.StatusSuccess,
		TransferID: payload.TransferId,
	})
	return nil
}

func (s *XShardService) HandleXShardFailed(ctx context.Context, payload *eventsv1.XShardTransferFailedPayload) error {
	// 1. Compensate and refund source wallet on source shard
	err := s.xshardStore.ReverseCrossShardTransfer(ctx, payload.SrcShard, payload.TransferId, payload.Reason)
	if err != nil {
		platform.LoggerWithTrace(ctx, s.logger).Error(platform.LogEventSagaFailed,
			zap.String(platform.LogFieldTransferID, payload.TransferId),
			zap.String(platform.LogFieldSrcShard, payload.SrcShard),
			zap.Error(err),
		)
		return err
	}

	// 2. Record business failure metrics and saga compensation decline reason
	platform.RecordBusinessTransfer(ctx, platform.TransferMetricFailed, platform.ShardTypeCross)
	platform.RecordBusinessDecline(ctx, platform.DeclineReasonSagaCompensated)

	// 3. Emit canonical transaction log for compensated cross-shard saga
	platform.LogCanonicalEvent(ctx, s.logger, platform.ServiceNameLedgerWorker, platform.CanonicalLogLine{
		Event:        platform.EventSagaCompensated,
		Status:       platform.StatusFailed,
		TransferID:   payload.TransferId,
		ErrorCode:    platform.ErrorCodeSagaFailed,
		ErrorMessage: payload.Reason,
	})
	return nil
}
