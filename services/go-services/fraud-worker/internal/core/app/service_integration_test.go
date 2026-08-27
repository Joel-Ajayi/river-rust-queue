package app

import (
	"context"
	"testing"
	_ "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/testutil"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/core/domain"
	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"go.uber.org/zap"
)

// === Mock Port Implementations ===

type mockWalletRepository struct {
	statusFunc func(ctx context.Context, shardID, walletID string) (string, error)
	freezeFunc func(ctx context.Context, shardID, walletID, reason string) error
}

func (m *mockWalletRepository) GetWalletStatus(ctx context.Context, shardID, walletID string) (string, error) {
	if m.statusFunc != nil {
		return m.statusFunc(ctx, shardID, walletID)
	}
	return platform.WalletStatusActive, nil
}

func (m *mockWalletRepository) FreezeWallet(ctx context.Context, shardID, walletID, reason string) error {
	if m.freezeFunc != nil {
		return m.freezeFunc(ctx, shardID, walletID, reason)
	}
	return nil
}

type mockRedisStore struct {
	count int
	err   error
}

func (m *mockRedisStore) UpdateVelocity(ctx context.Context, walletID, eventID string, timestamp int64, windowMs int) (int, error) {
	return m.count, m.err
}

type mockMerchantDirectory struct{}

func (m *mockMerchantDirectory) ShardFor(ctx context.Context, merchantID string) (string, error) {
	return "shard-a", nil
}

func setupFraudService(redis *mockRedisStore, repo *mockWalletRepository) *FraudService {
	logger, _ := zap.NewDevelopment()
	rules := []domain.VelocityRule{
		{
			Name:      "test_window_rule",
			WindowMs:  60000,
			Threshold: 5,
			Reason:    "high transfer velocity",
		},
	}
	return NewFraudService(logger, repo, redis, &mockMerchantDirectory{}, rules)
}

// === Service Integration Tests ===

func TestFraud_ProcessJob_UnderThreshold(t *testing.T) {
	redis := &mockRedisStore{count: 2} // Below threshold of 5
	repo := &mockWalletRepository{}
	svc := setupFraudService(redis, repo)

	payload := &eventsv1.JobRequestedPayload{
		JobId:      platform.NewJobID(),
		JobType:    platform.JobTypeTransfer,
		MerchantId: "merch-123",
		Data: &eventsv1.JobRequestedPayload_TransferData{
			TransferData: &eventsv1.TransferData{
				FromWallet: "merch-123.w1",
				ToWallet:   "merch-123.w2",
				Amount:     100,
				Currency:   "USD",
			},
		},
	}

	err := svc.ProcessJob(context.Background(), payload, platform.NewEventID(), time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("expected nil error for under-threshold velocity check, got %v", err)
	}
}

func TestFraud_ProcessJob_OverThresholdFreezesWallet(t *testing.T) {
	frozenCalled := false
	redis := &mockRedisStore{count: 10} // Exceeds threshold of 5
	repo := &mockWalletRepository{
		freezeFunc: func(ctx context.Context, shardID, walletID, reason string) error {
			frozenCalled = true
			return nil
		},
	}
	svc := setupFraudService(redis, repo)

	payload := &eventsv1.JobRequestedPayload{
		JobId:      platform.NewJobID(),
		JobType:    platform.JobTypeTransfer,
		MerchantId: "merch-123",
		Data: &eventsv1.JobRequestedPayload_TransferData{
			TransferData: &eventsv1.TransferData{
				FromWallet: "merch-123.w1",
				ToWallet:   "merch-123.w2",
				Amount:     100,
				Currency:   "USD",
			},
		},
	}

	err := svc.ProcessJob(context.Background(), payload, platform.NewEventID(), time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("expected nil error when wallet is frozen due to velocity check, got %v", err)
	}
	if !frozenCalled {
		t.Fatalf("expected FreezeWallet to be called when velocity threshold exceeded")
	}
}
