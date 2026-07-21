package app_test

import (
	"context"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/domain"
	"github.com/stretchr/testify/mock"
)

type MockLedgerStore struct{ mock.Mock }

func (m *MockLedgerStore) PostTransfer(ctx context.Context, shardID string, transfer domain.Transfer) error {
	args := m.Called(ctx, shardID, transfer)
	return args.Error(0)
}

func (m *MockLedgerStore) FailTransfer(ctx context.Context, shardID string, transfer domain.Transfer, reason string) error {
	args := m.Called(ctx, shardID, transfer, reason)
	return args.Error(0)
}

type MockCrossShardStore struct{ mock.Mock }

func (m *MockCrossShardStore) DebitToClearingAccount(ctx context.Context, srcShard, dstShard, jobID string, transfer domain.Transfer) error {
	args := m.Called(ctx, srcShard, dstShard, jobID, transfer)
	return args.Error(0)
}

func (m *MockCrossShardStore) CreditFromClearingAccount(ctx context.Context, intent *eventsv1.XShardTransferRequestedPayload) error {
	args := m.Called(ctx, intent)
	return args.Error(0)
}

func (m *MockCrossShardStore) SettleCrossShardTransfer(ctx context.Context, srcShard, transferID string) error {
	args := m.Called(ctx, srcShard, transferID)
	return args.Error(0)
}

func (m *MockCrossShardStore) ReverseCrossShardTransfer(ctx context.Context, srcShard, transferID, reason string) error {
	args := m.Called(ctx, srcShard, transferID, reason)
	return args.Error(0)
}
