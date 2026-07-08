package app

import (
	"context"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/ports"
	"go.uber.org/zap"
)

type XShardService struct {
	logger      *zap.Logger
	xshardStore ports.CrossShardStore
}

func NewXShardService(logger *zap.Logger, xshardStore ports.CrossShardStore) *XShardService {
	return &XShardService{
		logger:      logger,
		xshardStore: xshardStore,
	}
}

func (s *XShardService) HandleXShardRequested(ctx context.Context, payload *eventsv1.XShardTransferRequestedPayload) error {
	s.logger.Info("Handling XShardTransferRequested", zap.String("transfer_id", payload.TransferId))

	return s.xshardStore.CreditFromClearingAccount(ctx, payload)
}

func (s *XShardService) HandleXShardSettled(ctx context.Context, payload *eventsv1.XShardTransferSettledPayload) error {
	s.logger.Info("Handling XShardTransferSettled", zap.String("transfer_id", payload.TransferId))
	return s.xshardStore.SettleCrossShardTransfer(ctx, payload.SrcShard, payload.TransferId)
}

func (s *XShardService) HandleXShardFailed(ctx context.Context, payload *eventsv1.XShardTransferFailedPayload) error {
	s.logger.Info("Handling XShardTransferFailed", zap.String("transfer_id", payload.TransferId))
	return s.xshardStore.ReverseCrossShardTransfer(ctx, payload.SrcShard, payload.TransferId, payload.Reason)
}
