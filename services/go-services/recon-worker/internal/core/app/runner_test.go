//go:build integration

package app_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/recon-worker/internal/core/app"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/recon-worker/internal/core/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

type MockReconRepository struct {
	mock.Mock
}

func (m *MockReconRepository) AcquireLock(ctx context.Context) (bool, error) {
	args := m.Called(ctx)
	return args.Bool(0), args.Error(1)
}

func (m *MockReconRepository) ReleaseLock(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockReconRepository) GetShardSum(ctx context.Context, shardID string, cutoff time.Time) (int64, error) {
	args := m.Called(ctx, shardID, cutoff)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockReconRepository) FindAffectedWallets(ctx context.Context, shardID string, start, cutoff time.Time) ([]string, error) {
	args := m.Called(ctx, shardID, start, cutoff)
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockReconRepository) CheckWallet(ctx context.Context, shardID string, walletID string, cutoff time.Time) (*domain.Discrepancy, error) {
	args := m.Called(ctx, shardID, walletID, cutoff)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*domain.Discrepancy), args.Error(1)
}

func (m *MockReconRepository) CheckTransferLegs(ctx context.Context, shardID string, cutoff time.Time) ([]string, error) {
	args := m.Called(ctx, shardID, cutoff)
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockReconRepository) PersistReport(ctx context.Context, shardID string, report *domain.Report) error {
	args := m.Called(ctx, shardID, report)
	return args.Error(0)
}

func TestRunner_Run_Success(t *testing.T) {
	logger := zap.NewNop()
	mockRepo := new(MockReconRepository)

	cfg := &platform.Config{
		ShardURIs: map[string]string{
			"shard_a": "postgres://localhost:5432/shard_a",
		},
	}
	pools, _ := platform.NewShardPools(context.Background(), cfg, logger)

	runner := app.NewRunner(logger, mockRepo, 2, pools)

	ctx := context.Background()
	start := time.Now().Add(-24 * time.Hour)
	end := time.Now()

	// Expectations
	mockRepo.On("AcquireLock", mock.Anything).Return(true, nil)
	mockRepo.On("ReleaseLock", mock.Anything).Return(nil)
	mockRepo.On("GetShardSum", mock.Anything, "shard_a", mock.Anything).Return(int64(0), nil)
	mockRepo.On("CheckTransferLegs", mock.Anything, "shard_a", mock.Anything).Return([]string{}, nil)
	mockRepo.On("FindAffectedWallets", mock.Anything, "shard_a", mock.Anything, mock.Anything).Return([]string{"wallet_1"}, nil)
	mockRepo.On("CheckWallet", mock.Anything, "shard_a", "wallet_1", mock.Anything).Return((*domain.Discrepancy)(nil), nil)
	mockRepo.On("PersistReport", mock.Anything, "shard_a", mock.Anything).Return(nil)

	report, err := runner.Run(ctx, start, end)

	assert.NoError(t, err)
	assert.NotNil(t, report)
	assert.Equal(t, 0, len(report.Discrepancies))
	assert.Equal(t, 1, report.WalletsChecked)

	mockRepo.AssertExpectations(t)
}

func TestRunner_Run_LockNotAcquired(t *testing.T) {
	logger := zap.NewNop()
	mockRepo := new(MockReconRepository)

	cfg := &platform.Config{
		ShardURIs: map[string]string{
			"shard_a": "postgres://localhost:5432/shard_a",
		},
	}
	pools, _ := platform.NewShardPools(context.Background(), cfg, logger)

	runner := app.NewRunner(logger, mockRepo, 2, pools)

	mockRepo.On("AcquireLock", mock.Anything).Return(false, nil)

	_, err := runner.Run(context.Background(), time.Now().Add(-1*time.Hour), time.Now())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "lock is already held")
	mockRepo.AssertExpectations(t)
}

func TestRunner_Run_LockAcquireError(t *testing.T) {
	logger := zap.NewNop()
	mockRepo := new(MockReconRepository)

	cfg := &platform.Config{
		ShardURIs: map[string]string{
			"shard_a": "postgres://localhost:5432/shard_a",
		},
	}
	pools, _ := platform.NewShardPools(context.Background(), cfg, logger)

	runner := app.NewRunner(logger, mockRepo, 2, pools)

	mockRepo.On("AcquireLock", mock.Anything).Return(false, errors.New("db error"))

	_, err := runner.Run(context.Background(), time.Now().Add(-1*time.Hour), time.Now())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "reconciliation lock")
	mockRepo.AssertExpectations(t)
}

func TestRunner_Run_ShardSumImbalance(t *testing.T) {
	logger := zap.NewNop()
	mockRepo := new(MockReconRepository)

	cfg := &platform.Config{
		ShardURIs: map[string]string{
			"shard_a": "postgres://localhost:5432/shard_a",
		},
	}
	pools, _ := platform.NewShardPools(context.Background(), cfg, logger)

	runner := app.NewRunner(logger, mockRepo, 2, pools)

	ctx := context.Background()
	start := time.Now().Add(-24 * time.Hour)
	end := time.Now()

	mockRepo.On("AcquireLock", mock.Anything).Return(true, nil)
	mockRepo.On("ReleaseLock", mock.Anything).Return(nil)
	mockRepo.On("GetShardSum", mock.Anything, "shard_a", mock.Anything).Return(int64(500), nil)
	mockRepo.On("CheckTransferLegs", mock.Anything, "shard_a", mock.Anything).Return([]string{}, nil)
	mockRepo.On("FindAffectedWallets", mock.Anything, "shard_a", mock.Anything, mock.Anything).Return([]string{}, nil)
	mockRepo.On("PersistReport", mock.Anything, "shard_a", mock.Anything).Return(nil)

	report, err := runner.Run(ctx, start, end)

	assert.NoError(t, err)
	assert.Len(t, report.Discrepancies, 1)
	assert.Equal(t, int64(500), report.GlobalSum)
	mockRepo.AssertExpectations(t)
}

func TestRunner_Run_LegImbalance(t *testing.T) {
	logger := zap.NewNop()
	mockRepo := new(MockReconRepository)

	cfg := &platform.Config{
		ShardURIs: map[string]string{
			"shard_a": "postgres://localhost:5432/shard_a",
		},
	}
	pools, _ := platform.NewShardPools(context.Background(), cfg, logger)

	runner := app.NewRunner(logger, mockRepo, 2, pools)

	ctx := context.Background()
	start := time.Now().Add(-24 * time.Hour)
	end := time.Now()

	mockRepo.On("AcquireLock", mock.Anything).Return(true, nil)
	mockRepo.On("ReleaseLock", mock.Anything).Return(nil)
	mockRepo.On("GetShardSum", mock.Anything, "shard_a", mock.Anything).Return(int64(0), nil)
	mockRepo.On("CheckTransferLegs", mock.Anything, "shard_a", mock.Anything).Return([]string{"tf_123"}, nil)
	mockRepo.On("FindAffectedWallets", mock.Anything, "shard_a", mock.Anything, mock.Anything).Return([]string{}, nil)
	mockRepo.On("PersistReport", mock.Anything, "shard_a", mock.Anything).Return(nil)

	report, err := runner.Run(ctx, start, end)

	assert.NoError(t, err)
	assert.Len(t, report.Discrepancies, 1)
	assert.Equal(t, domain.DiscrepancyKindLegImbalance, report.Discrepancies[0].Kind)
	assert.Equal(t, "tf_123", report.Discrepancies[0].TransferID)
	mockRepo.AssertExpectations(t)
}

func TestRunner_Run_WalletDiscrepancy(t *testing.T) {
	logger := zap.NewNop()
	mockRepo := new(MockReconRepository)

	cfg := &platform.Config{
		ShardURIs: map[string]string{
			"shard_a": "postgres://localhost:5432/shard_a",
		},
	}
	pools, _ := platform.NewShardPools(context.Background(), cfg, logger)

	runner := app.NewRunner(logger, mockRepo, 2, pools)

	ctx := context.Background()
	start := time.Now().Add(-24 * time.Hour)
	end := time.Now()

	mockRepo.On("AcquireLock", mock.Anything).Return(true, nil)
	mockRepo.On("ReleaseLock", mock.Anything).Return(nil)
	mockRepo.On("GetShardSum", mock.Anything, "shard_a", mock.Anything).Return(int64(0), nil)
	mockRepo.On("CheckTransferLegs", mock.Anything, "shard_a", mock.Anything).Return([]string{}, nil)
	mockRepo.On("FindAffectedWallets", mock.Anything, "shard_a", mock.Anything, mock.Anything).Return([]string{"wallet_1"}, nil)
	mockRepo.On("CheckWallet", mock.Anything, "shard_a", "wallet_1", mock.Anything).Return(&domain.Discrepancy{
		Kind:           domain.DiscrepancyKindBalanceAfter,
		WalletID:      "wallet_1",
		DerivedBalance: -100,
	}, nil)
	mockRepo.On("PersistReport", mock.Anything, "shard_a", mock.Anything).Return(nil)

	report, err := runner.Run(ctx, start, end)

	assert.NoError(t, err)
	assert.Len(t, report.Discrepancies, 1)
	assert.Equal(t, "wallet_1", report.Discrepancies[0].WalletID)
	mockRepo.AssertExpectations(t)
}

func TestRunner_Run_PersistError(t *testing.T) {
	logger := zap.NewNop()
	mockRepo := new(MockReconRepository)

	cfg := &platform.Config{
		ShardURIs: map[string]string{
			"shard_a": "postgres://localhost:5432/shard_a",
		},
	}
	pools, _ := platform.NewShardPools(context.Background(), cfg, logger)

	runner := app.NewRunner(logger, mockRepo, 2, pools)

	ctx := context.Background()
	start := time.Now().Add(-24 * time.Hour)
	end := time.Now()

	mockRepo.On("AcquireLock", mock.Anything).Return(true, nil)
	mockRepo.On("ReleaseLock", mock.Anything).Return(nil)
	mockRepo.On("GetShardSum", mock.Anything, "shard_a", mock.Anything).Return(int64(0), nil)
	mockRepo.On("CheckTransferLegs", mock.Anything, "shard_a", mock.Anything).Return([]string{}, nil)
	mockRepo.On("FindAffectedWallets", mock.Anything, "shard_a", mock.Anything, mock.Anything).Return([]string{}, nil)
	mockRepo.On("PersistReport", mock.Anything, "shard_a", mock.Anything).Return(errors.New("disk full"))

	_, err := runner.Run(ctx, start, end)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "persist report")
	mockRepo.AssertExpectations(t)
}
