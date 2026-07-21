package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
)

type MockRedisStore struct{ mock.Mock }

func (m *MockRedisStore) UpdateVelocity(ctx context.Context, walletID string, eventID string, timestampMs int64, windowSeconds int) (int, error) {
	args := m.Called(ctx, walletID, eventID, timestampMs, windowSeconds)
	return args.Int(0), args.Error(1)
}
