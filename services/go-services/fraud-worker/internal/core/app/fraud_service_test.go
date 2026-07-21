package app_test

import (
	"context"
	"testing"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/core/app"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/core/domain"
	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	mocks "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/testutil/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestFraudService_ProcessJob_TripsThreshold(t *testing.T) {
	logger := zap.NewNop()
	mockRedis := new(mocks.MockRedisStore)
	mockRepo := new(mocks.MockWalletRepository)
	mockMerch := new(mocks.MockMerchantDirectory)

	rules := []domain.VelocityRule{
		{
			Name:          "test_rule",
			WindowSeconds: 60,
			Threshold:     5,
			Reason:        "Velocity high",
		},
	}

	service := app.NewFraudService(logger, mockRepo, mockRedis, mockMerch, rules)
	ctx := context.Background()

	mockMerch.On("ShardFor", mock.Anything, "merch_1").Return("shard_a", nil)
	mockRedis.On("UpdateVelocity", mock.Anything, "wallet_1", "event_1", int64(1000), 60).Return(5, nil)
	mockRepo.On("GetWalletStatus", mock.Anything, "shard_a", "wallet_1").Return(platform.WalletStatusActive, nil)
	mockRepo.On("FreezeWallet", mock.Anything, "shard_a", "wallet_1", "Velocity high").Return(nil)

	payload := &eventsv1.JobRequestedPayload{
		JobType:    platform.JobTypeTransfer,
		MerchantId: "merch_1",
		Data: &eventsv1.JobRequestedPayload_TransferData{
			TransferData: &eventsv1.TransferData{
				FromWallet: "wallet_1",
			},
		},
	}
	err := service.ProcessJob(ctx, payload, "event_1", 1000)
	require.NoError(t, err)

	mockMerch.AssertExpectations(t)
	mockRedis.AssertExpectations(t)
	mockRepo.AssertExpectations(t)
}

func TestFraudService_ProcessJob_BelowThreshold(t *testing.T) {
	logger := zap.NewNop()
	mockRedis := new(mocks.MockRedisStore)
	mockRepo := new(mocks.MockWalletRepository)
	mockMerch := new(mocks.MockMerchantDirectory)

	rules := []domain.VelocityRule{
		{
			Name:          "test_rule",
			WindowSeconds: 60,
			Threshold:     5,
			Reason:        "Velocity high",
		},
	}

	service := app.NewFraudService(logger, mockRepo, mockRedis, mockMerch, rules)
	ctx := context.Background()

	mockMerch.On("ShardFor", mock.Anything, "merch_1").Return("shard_a", nil)
	mockRedis.On("UpdateVelocity", mock.Anything, "wallet_1", "event_1", int64(1000), 60).Return(3, nil)

	payload := &eventsv1.JobRequestedPayload{
		JobType:    platform.JobTypeTransfer,
		MerchantId: "merch_1",
		Data: &eventsv1.JobRequestedPayload_TransferData{
			TransferData: &eventsv1.TransferData{
				FromWallet: "wallet_1",
			},
		},
	}
	err := service.ProcessJob(ctx, payload, "event_1", 1000)
	require.NoError(t, err)

	mockMerch.AssertExpectations(t)
	mockRedis.AssertExpectations(t)
	mockRepo.AssertExpectations(t)
}

func TestFraudService_ProcessJob_NonTransferJobType(t *testing.T) {
	logger := zap.NewNop()
	mockRedis := new(mocks.MockRedisStore)
	mockRepo := new(mocks.MockWalletRepository)
	mockMerch := new(mocks.MockMerchantDirectory)

	service := app.NewFraudService(logger, mockRepo, mockRedis, mockMerch, nil)
	ctx := context.Background()

	payload := &eventsv1.JobRequestedPayload{
		JobType:    "deposit",
		MerchantId: "merch_1",
	}
	err := service.ProcessJob(ctx, payload, "event_1", 1000)
	require.NoError(t, err)
}

func TestFraudService_ProcessJob_MerchantLookupError(t *testing.T) {
	logger := zap.NewNop()
	mockRedis := new(mocks.MockRedisStore)
	mockRepo := new(mocks.MockWalletRepository)
	mockMerch := new(mocks.MockMerchantDirectory)

	service := app.NewFraudService(logger, mockRepo, mockRedis, mockMerch, nil)
	ctx := context.Background()

	mockMerch.On("ShardFor", mock.Anything, "merch_1").Return("", domain.ErrMerchantInactiveOrNotFound)

	payload := &eventsv1.JobRequestedPayload{
		JobType:    platform.JobTypeTransfer,
		MerchantId: "merch_1",
		Data: &eventsv1.JobRequestedPayload_TransferData{
			TransferData: &eventsv1.TransferData{
				FromWallet: "wallet_1",
			},
		},
	}
	err := service.ProcessJob(ctx, payload, "event_1", 1000)
	require.Error(t, err)
	require.Equal(t, domain.ErrMerchantInactiveOrNotFound, err)
}

func TestFraudService_ProcessJob_Concurrent(t *testing.T) {
	logger := zap.NewNop()
	mockRedis := new(mocks.MockRedisStore)
	mockRepo := new(mocks.MockWalletRepository)
	mockMerch := new(mocks.MockMerchantDirectory)

	rules := []domain.VelocityRule{
		{
			Name:          "test_rule",
			WindowSeconds: 60,
			Threshold:     10,
			Reason:        "Velocity high",
		},
	}

	service := app.NewFraudService(logger, mockRepo, mockRedis, mockMerch, rules)
	ctx := context.Background()

	mockMerch.On("ShardFor", mock.Anything, "merch_1").Return("shard_a", nil)
	mockRedis.On("UpdateVelocity", mock.Anything, "wallet_1", mock.Anything, int64(1000), 60).Return(1, nil)

	payload := &eventsv1.JobRequestedPayload{
		JobType:    platform.JobTypeTransfer,
		MerchantId: "merch_1",
		Data: &eventsv1.JobRequestedPayload_TransferData{
			TransferData: &eventsv1.TransferData{
				FromWallet: "wallet_1",
			},
		},
	}

	const goroutines = 10
	errChan := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		go func(eventID string) {
			errChan <- service.ProcessJob(ctx, payload, eventID, 1000)
		}(platform.NewEventID())
	}

	for i := 0; i < goroutines; i++ {
		err := <-errChan
		require.NoError(t, err)
	}

	mockMerch.AssertExpectations(t)
	mockRedis.AssertExpectations(t)
}
