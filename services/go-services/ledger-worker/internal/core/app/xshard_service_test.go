package app_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/app"
	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestXShardSettled_Success(t *testing.T) {
	mockXshard := new(MockCrossShardStore)
	logger := zap.NewNop()

	svc := app.NewXShardService(logger, mockXshard)

	mockXshard.On("SettleCrossShardTransfer", mock.Anything, "shard_a", "tf_123").Return(nil)

	err := svc.HandleXShardSettled(context.Background(), &eventsv1.XShardTransferSettledPayload{
		SrcShard:   "shard_a",
		TransferId: "tf_123",
	})

	require.NoError(t, err)
	mockXshard.AssertExpectations(t)
}

func TestXShardSettled_StoreError(t *testing.T) {
	mockXshard := new(MockCrossShardStore)
	logger := zap.NewNop()

	svc := app.NewXShardService(logger, mockXshard)

	mockXshard.On("SettleCrossShardTransfer", mock.Anything, "shard_a", "tf_123").Return(errors.New("db error"))

	err := svc.HandleXShardSettled(context.Background(), &eventsv1.XShardTransferSettledPayload{
		SrcShard:   "shard_a",
		TransferId: "tf_123",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "db error")
	mockXshard.AssertExpectations(t)
}

func TestXShardFailed_Compensates(t *testing.T) {
	mockXshard := new(MockCrossShardStore)
	logger := zap.NewNop()

	svc := app.NewXShardService(logger, mockXshard)

	mockXshard.On("ReverseCrossShardTransfer", mock.Anything, "shard_a", "tf_123", "timeout").Return(nil)

	err := svc.HandleXShardFailed(context.Background(), &eventsv1.XShardTransferFailedPayload{
		SrcShard:   "shard_a",
		TransferId: "tf_123",
		Reason:     "timeout",
	})

	require.NoError(t, err)
	mockXshard.AssertExpectations(t)
}

func TestXShardFailed_StoreError(t *testing.T) {
	mockXshard := new(MockCrossShardStore)
	logger := zap.NewNop()

	svc := app.NewXShardService(logger, mockXshard)

	mockXshard.On("ReverseCrossShardTransfer", mock.Anything, "shard_a", "tf_123", "reason").Return(errors.New("db error"))

	err := svc.HandleXShardFailed(context.Background(), &eventsv1.XShardTransferFailedPayload{
		SrcShard:   "shard_a",
		TransferId: "tf_123",
		Reason:     "reason",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "db error")
	mockXshard.AssertExpectations(t)
}

func TestXShardRequested_Credits(t *testing.T) {
	mockXshard := new(MockCrossShardStore)
	logger := zap.NewNop()

	svc := app.NewXShardService(logger, mockXshard)

	payload := &eventsv1.XShardTransferRequestedPayload{
		SrcShard: "shard_a",
	}
	mockXshard.On("CreditFromClearingAccount", mock.Anything, payload).Return(nil)

	err := svc.HandleXShardRequested(context.Background(), payload)

	require.NoError(t, err)
	mockXshard.AssertExpectations(t)
}
