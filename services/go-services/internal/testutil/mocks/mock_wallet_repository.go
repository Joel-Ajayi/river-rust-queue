package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
)

type MockWalletRepository struct{ mock.Mock }

func (m *MockWalletRepository) GetWalletStatus(ctx context.Context, shardID string, walletID string) (string, error) {
	args := m.Called(ctx, shardID, walletID)
	return args.String(0), args.Error(1)
}

func (m *MockWalletRepository) FreezeWallet(ctx context.Context, shardID string, walletID string, reason string) error {
	args := m.Called(ctx, shardID, walletID, reason)
	return args.Error(0)
}
