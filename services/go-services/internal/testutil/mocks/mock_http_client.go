package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
)

type MockHTTPClient struct{ mock.Mock }

func (m *MockHTTPClient) Post(ctx context.Context, url string, payload []byte, signature, eventID string, attempt int) (int, error) {
	args := m.Called(ctx, url, payload, signature, eventID, attempt)
	return args.Int(0), args.Error(1)
}
