package app_test

import (
	"context"
	"errors"
	"testing"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	mocks "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/testutil/mocks"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/app"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func newTestPayload(merchantID, fromWallet, toMerchant, toWallet string, amount int64, currency string) *eventsv1.JobRequestedPayload {
	return &eventsv1.JobRequestedPayload{
		JobType:    platform.JobTypeTransfer,
		JobId:      platform.NewJobID(),
		MerchantId: merchantID,
		Data: &eventsv1.JobRequestedPayload_TransferData{
			TransferData: &eventsv1.TransferData{
				FromWallet:   fromWallet,
				ToWallet:     toWallet,
				Amount:       amount,
				Currency:     currency,
				ToMerchantId: toMerchant,
			},
		},
	}
}

func TestProcessJob_SameShardSuccess(t *testing.T) {
	mockLedger := new(MockLedgerStore)
	mockXshard := new(MockCrossShardStore)
	mockDir := new(mocks.MockMerchantDirectory)
	logger := zap.NewNop()

	svc := app.NewJobService(logger, mockLedger, mockXshard, mockDir)

	payload := newTestPayload("merch_1", "wal_a", "merch_1", "wal_b", 1000, "USD")

	mockDir.On("ShardFor", mock.Anything, "merch_1").Return("shard_a", nil)
	mockLedger.On("PostTransfer", mock.Anything, "shard_a", mock.MatchedBy(func(t domain.Transfer) bool {
		return t.Amount == 1000 && t.Currency == "USD" && t.FromWallet == "wal_a" && t.ToWallet == "wal_b"
	})).Return(nil)

	err := svc.ProcessJob(context.Background(), payload)

	require.NoError(t, err)
	mockDir.AssertExpectations(t)
	mockLedger.AssertExpectations(t)
}

func TestProcessJob_CrossShardInitiatesSaga(t *testing.T) {
	mockLedger := new(MockLedgerStore)
	mockXshard := new(MockCrossShardStore)
	mockDir := new(mocks.MockMerchantDirectory)
	logger := zap.NewNop()

	svc := app.NewJobService(logger, mockLedger, mockXshard, mockDir)

	payload := newTestPayload("merch_1", "wal_a", "merch_2", "wal_b", 1000, "USD")

	mockDir.On("ShardFor", mock.Anything, "merch_1").Return("shard_a", nil)
	mockDir.On("ShardFor", mock.Anything, "merch_2").Return("shard_b", nil)
	mockXshard.On("DebitToClearingAccount", mock.Anything, "shard_a", "shard_b", payload.JobId, mock.Anything).Return(nil)

	err := svc.ProcessJob(context.Background(), payload)

	require.NoError(t, err)
	mockDir.AssertExpectations(t)
	mockXshard.AssertExpectations(t)
}

func TestProcessJob_DeclineOnTerminalError(t *testing.T) {
	mockLedger := new(MockLedgerStore)
	mockXshard := new(MockCrossShardStore)
	mockDir := new(mocks.MockMerchantDirectory)
	logger := zap.NewNop()

	svc := app.NewJobService(logger, mockLedger, mockXshard, mockDir)

	payload := newTestPayload("merch_1", "wal_a", "merch_1", "wal_b", 1000, "USD")

	mockDir.On("ShardFor", mock.Anything, "merch_1").Return("shard_a", nil)
	mockLedger.On("PostTransfer", mock.Anything, "shard_a", mock.Anything).Return(domain.ErrInsufficientBalance)
	mockLedger.On("FailTransfer", mock.Anything, "shard_a", mock.Anything, mock.Anything).Return(nil)

	err := svc.ProcessJob(context.Background(), payload)

	require.NoError(t, err) // terminal errors are consumed (recorded as decline)
	mockLedger.AssertExpectations(t)
	mockDir.AssertExpectations(t)
}

func TestProcessJob_TransientErrorReturns(t *testing.T) {
	mockLedger := new(MockLedgerStore)
	mockXshard := new(MockCrossShardStore)
	mockDir := new(mocks.MockMerchantDirectory)
	logger := zap.NewNop()

	svc := app.NewJobService(logger, mockLedger, mockXshard, mockDir)

	payload := newTestPayload("merch_1", "wal_a", "merch_1", "wal_b", 1000, "USD")

	mockDir.On("ShardFor", mock.Anything, "merch_1").Return("shard_a", nil)
	mockLedger.On("PostTransfer", mock.Anything, "shard_a", mock.Anything).Return(errors.New("connection refused"))

	err := svc.ProcessJob(context.Background(), payload)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection refused")
	mockLedger.AssertExpectations(t)
}

func TestProcessJob_InvalidJobType(t *testing.T) {
	svc := app.NewJobService(zap.NewNop(), new(MockLedgerStore), new(MockCrossShardStore), new(mocks.MockMerchantDirectory))

	err := svc.ProcessJob(context.Background(), &eventsv1.JobRequestedPayload{
		JobType: "unknown_type",
	})

	assert.ErrorIs(t, err, domain.ErrInvalidJobType)
}

func TestProcessJob_MissingTransferData(t *testing.T) {
	svc := app.NewJobService(zap.NewNop(), new(MockLedgerStore), new(MockCrossShardStore), new(mocks.MockMerchantDirectory))

	err := svc.ProcessJob(context.Background(), &eventsv1.JobRequestedPayload{
		JobType: platform.JobTypeTransfer,
	})

	assert.ErrorIs(t, err, domain.ErrMissingTransferData)
}

func TestProcessJob_FailTransferError(t *testing.T) {
	mockLedger := new(MockLedgerStore)
	mockDir := new(mocks.MockMerchantDirectory)
	logger := zap.NewNop()

	svc := app.NewJobService(logger, mockLedger, new(MockCrossShardStore), mockDir)

	payload := newTestPayload("merch_1", "wal_a", "merch_1", "wal_b", 1000, "USD")

	mockDir.On("ShardFor", mock.Anything, "merch_1").Return("shard_a", nil)
	mockLedger.On("PostTransfer", mock.Anything, "shard_a", mock.Anything).Return(domain.ErrInsufficientBalance)
	mockLedger.On("FailTransfer", mock.Anything, "shard_a", mock.Anything, mock.Anything).Return(errors.New("db error"))

	err := svc.ProcessJob(context.Background(), payload)

	require.Error(t, err) // FailTransfer error propagates
	assert.Contains(t, err.Error(), "db error")
	mockLedger.AssertExpectations(t)
}

func TestProcessJob_Concurrent(t *testing.T) {
	mockLedger := new(MockLedgerStore)
	mockXshard := new(MockCrossShardStore)
	mockDir := new(mocks.MockMerchantDirectory)
	logger := zap.NewNop()

	svc := app.NewJobService(logger, mockLedger, mockXshard, mockDir)
	payload := newTestPayload("merch_1", "wal_a", "merch_1", "wal_b", 1000, "USD")

	mockDir.On("ShardFor", mock.Anything, "merch_1").Return("shard_a", nil)
	mockLedger.On("PostTransfer", mock.Anything, "shard_a", mock.Anything).Return(nil)

	const goroutines = 10
	errChan := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			errChan <- svc.ProcessJob(context.Background(), payload)
		}()
	}

	for i := 0; i < goroutines; i++ {
		err := <-errChan
		require.NoError(t, err)
	}

	mockDir.AssertExpectations(t)
	mockLedger.AssertExpectations(t)
}
