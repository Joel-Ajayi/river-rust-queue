package app

import (
	"context"
	"testing"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/domain"
	"go.uber.org/zap"
)

// === Mock Implementations ===

type mockLedgerStore struct {
	postTransferFunc func(ctx context.Context, shardID string, t domain.Transfer) error
	failTransferFunc func(ctx context.Context, shardID string, t domain.Transfer, reason string) error
}

func (m *mockLedgerStore) PostTransfer(ctx context.Context, shardID string, t domain.Transfer) error {
	if m.postTransferFunc != nil {
		return m.postTransferFunc(ctx, shardID, t)
	}
	return nil
}

func (m *mockLedgerStore) FailTransfer(ctx context.Context, shardID string, t domain.Transfer, reason string) error {
	if m.failTransferFunc != nil {
		return m.failTransferFunc(ctx, shardID, t, reason)
	}
	return nil
}

type mockCrossShardStore struct {
	debitFunc   func(ctx context.Context, srcShard, dstShard, jobID string, t domain.Transfer) error
	creditFunc  func(ctx context.Context, intent *eventsv1.XShardTransferRequestedPayload) error
	settleFunc  func(ctx context.Context, srcShard, transferID string) (int64, string, error)
	reverseFunc func(ctx context.Context, srcShard, transferID, reason string) error
}

func (m *mockCrossShardStore) DebitToClearingAccount(ctx context.Context, srcShard, dstShard, jobID string, t domain.Transfer) error {
	if m.debitFunc != nil {
		return m.debitFunc(ctx, srcShard, dstShard, jobID, t)
	}
	return nil
}

func (m *mockCrossShardStore) CreditFromClearingAccount(ctx context.Context, intent *eventsv1.XShardTransferRequestedPayload) error {
	if m.creditFunc != nil {
		return m.creditFunc(ctx, intent)
	}
	return nil
}

func (m *mockCrossShardStore) SettleCrossShardTransfer(ctx context.Context, srcShard, transferID string) (int64, string, error) {
	if m.settleFunc != nil {
		return m.settleFunc(ctx, srcShard, transferID)
	}
	return 1000, "USD", nil
}

func (m *mockCrossShardStore) ReverseCrossShardTransfer(ctx context.Context, srcShard, transferID, reason string) error {
	if m.reverseFunc != nil {
		return m.reverseFunc(ctx, srcShard, transferID, reason)
	}
	return nil
}

type mockDirectory struct {
	shards map[string]string
}

func (m *mockDirectory) ShardFor(ctx context.Context, merchantID string) (string, error) {
	if s, ok := m.shards[merchantID]; ok {
		return s, nil
	}
	return "shard-a", nil
}

func setupJobService(directory *mockDirectory, ledger *mockLedgerStore, xshard *mockCrossShardStore) *JobService {
	logger, _ := zap.NewDevelopment()
	return NewJobService(logger, ledger, xshard, directory)
}

func setupXShardService(ledger *mockLedgerStore, xshard *mockCrossShardStore) *XShardService {
	logger, _ := zap.NewDevelopment()
	return NewXShardService(logger, xshard)
}

// === Service Integration Tests ===

func TestLedger_ProcessJob_SameShardSuccess(t *testing.T) {
	dir := &mockDirectory{shards: map[string]string{"merch-1": "shard-a", "merch-2": "shard-a"}}
	ledger := &mockLedgerStore{}
	xshard := &mockCrossShardStore{}
	svc := setupJobService(dir, ledger, xshard)

	payload := &eventsv1.JobRequestedPayload{
		JobId:      platform.NewJobID(),
		JobType:    platform.JobTypeTransfer,
		MerchantId: "merch-1",
		Data: &eventsv1.JobRequestedPayload_TransferData{
			TransferData: &eventsv1.TransferData{
				FromWallet:   "merch-1.wallet1",
				ToWallet:     "merch-2.wallet2",
				ToMerchantId: "merch-2",
				Amount:       1000,
				Currency:     "USD",
			},
		},
	}

	err := svc.ProcessJob(context.Background(), payload)
	if err != nil {
		t.Fatalf("expected nil error for same-shard transfer, got %v", err)
	}
}

func TestLedger_ProcessJob_CrossShardInitiated(t *testing.T) {
	dir := &mockDirectory{shards: map[string]string{"merch-1": "shard-a", "merch-2": "shard-b"}}
	ledger := &mockLedgerStore{}
	xshard := &mockCrossShardStore{}
	svc := setupJobService(dir, ledger, xshard)

	payload := &eventsv1.JobRequestedPayload{
		JobId:      platform.NewJobID(),
		JobType:    platform.JobTypeTransfer,
		MerchantId: "merch-1",
		Data: &eventsv1.JobRequestedPayload_TransferData{
			TransferData: &eventsv1.TransferData{
				FromWallet:   "merch-1.wallet1",
				ToWallet:     "merch-2.wallet2",
				ToMerchantId: "merch-2",
				Amount:       1000,
				Currency:     "USD",
			},
		},
	}

	err := svc.ProcessJob(context.Background(), payload)
	if err != nil {
		t.Fatalf("expected nil error for cross-shard transfer, got %v", err)
	}
}

func TestLedger_ProcessJob_TerminalErrorHandled(t *testing.T) {
	dir := &mockDirectory{shards: map[string]string{"merch-1": "shard-a", "merch-2": "shard-a"}}
	failedCalled := false
	ledger := &mockLedgerStore{
		postTransferFunc: func(ctx context.Context, shardID string, tr domain.Transfer) error {
			return domain.ErrInsufficientBalance
		},
		failTransferFunc: func(ctx context.Context, shardID string, tr domain.Transfer, reason string) error {
			failedCalled = true
			return nil
		},
	}
	xshard := &mockCrossShardStore{}
	svc := setupJobService(dir, ledger, xshard)

	payload := &eventsv1.JobRequestedPayload{
		JobId:      platform.NewJobID(),
		JobType:    platform.JobTypeTransfer,
		MerchantId: "merch-1",
		Data: &eventsv1.JobRequestedPayload_TransferData{
			TransferData: &eventsv1.TransferData{
				FromWallet:   "merch-1.wallet1",
				ToWallet:     "merch-2.wallet2",
				ToMerchantId: "merch-2",
				Amount:       1000,
				Currency:     "USD",
			},
		},
	}

	err := svc.ProcessJob(context.Background(), payload)
	if err != nil {
		t.Fatalf("expected terminal error to be handled without propagating error, got %v", err)
	}
	if !failedCalled {
		t.Fatalf("expected FailTransfer to be called for terminal error")
	}
}

func TestLedger_XShardSettledAndFailed(t *testing.T) {
	ledger := &mockLedgerStore{}
	xshard := &mockCrossShardStore{}
	svc := setupXShardService(ledger, xshard)

	settled := &eventsv1.XShardTransferSettledPayload{
		TransferId: "tr_123456",
		SrcShard:   "shard-a",
		DstShard:   "shard-b",
	}

	errSettled := svc.HandleXShardSettled(context.Background(), settled)
	if errSettled != nil {
		t.Fatalf("expected nil for HandleXShardSettled, got %v", errSettled)
	}

	failed := &eventsv1.XShardTransferFailedPayload{
		TransferId: "tr_123456",
		SrcShard:   "shard-a",
		DstShard:   "shard-b",
		Reason:     "insufficient balance",
	}

	errFailed := svc.HandleXShardFailed(context.Background(), failed)
	if errFailed != nil {
		t.Fatalf("expected nil for HandleXShardFailed, got %v", errFailed)
	}
}
