package app

import (
	"context"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/port"
	"go.uber.org/zap"
)

type XShardService struct {
	xshardStore port.CrossShardStore
}

func NewXShardService(_ *zap.Logger, xshardStore port.CrossShardStore) *XShardService {
	return &XShardService{
		xshardStore: xshardStore,
	}
}

func (s *XShardService) HandleXShardRequested(ctx context.Context, payload *eventsv1.XShardTransferRequestedPayload) error {
	return s.xshardStore.CreditFromClearingAccount(ctx, payload)
}

func (s *XShardService) HandleXShardSettled(ctx context.Context, payload *eventsv1.XShardTransferSettledPayload) error {
	return s.xshardStore.SettleCrossShardTransfer(ctx, payload.SrcShard, payload.TransferId)
}

func (s *XShardService) HandleXShardFailed(ctx context.Context, payload *eventsv1.XShardTransferFailedPayload) error {
	return s.xshardStore.ReverseCrossShardTransfer(ctx, payload.SrcShard, payload.TransferId, payload.Reason)
}
