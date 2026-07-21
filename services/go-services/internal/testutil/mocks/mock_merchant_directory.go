package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
)

// MockMerchantDirectory implements the ShardFor interface variant
// shared by ledger-worker and fraud-worker port packages.
type MockMerchantDirectory struct{ mock.Mock }

func (m *MockMerchantDirectory) ShardFor(ctx context.Context, merchantID string) (string, error) {
	args := m.Called(ctx, merchantID)
	return args.String(0), args.Error(1)
}
