package resilience

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/testutil/mocks"
	"github.com/failsafe-go/failsafe-go/circuitbreaker"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestRedisStoreResilience_FailsFastWhenBreakerOpen(t *testing.T) {
	breaker := platform.NewRedisCircuitBreaker(
		platform.CBNameRedis,
		platform.CircuitBreakerConfig{
			MaxRequests: 1,
			Timeout:     50 * time.Millisecond,
			MaxFails:    1,
		},
		zap.NewNop(),
	)

	mockStore := new(mocks.MockRedisStore)
	mockStore.On("UpdateVelocity", mock.Anything, "wallet_1", "event_1", int64(1000), 60).Return(0, errors.New("redis down"))
	mockStore.On("UpdateVelocity", mock.Anything, "wallet_1", "event_2", int64(2000), 60).Return(0, errors.New("redis down"))

	svc := NewRedisStoreResilience(mockStore, breaker)

	_, err := svc.UpdateVelocity(context.Background(), "wallet_1", "event_1", 1000, 60)
	require.Error(t, err)

	require.Eventually(t, func() bool {
		return breaker.Breaker().State() == circuitbreaker.OpenState
	}, time.Second, 10*time.Millisecond)

	_, err = svc.UpdateVelocity(context.Background(), "wallet_1", "event_2", 2000, 60)
	require.Error(t, err)

	mockStore.AssertNumberOfCalls(t, "UpdateVelocity", 1)
}

func TestRedisStoreResilience_RecoversInHalfOpen(t *testing.T) {
	breaker := platform.NewRedisCircuitBreaker(
		platform.CBNameRedis,
		platform.CircuitBreakerConfig{
			MaxRequests: 1,
			Timeout:     50 * time.Millisecond,
			MaxFails:    1,
		},
		zap.NewNop(),
	)

	mockStore := new(mocks.MockRedisStore)
	mockStore.On("UpdateVelocity", mock.Anything, "wallet_1", "event_1", int64(1000), 60).Return(0, errors.New("redis down"))
	mockStore.On("UpdateVelocity", mock.Anything, "wallet_1", "event_3", int64(3000), 60).Return(5, nil)

	svc := NewRedisStoreResilience(mockStore, breaker)

	_, err := svc.UpdateVelocity(context.Background(), "wallet_1", "event_1", 1000, 60)
	require.Error(t, err)

	require.Eventually(t, func() bool {
		return breaker.Breaker().State() == circuitbreaker.OpenState
	}, time.Second, 10*time.Millisecond)

	// Open -> HalfOpen is lazy in failsafe-go: it happens when the next call
	// arrives after the delay has elapsed. Sleep past the delay, then the
	// trial call is admitted as half-open.
	time.Sleep(70 * time.Millisecond)

	count, err := svc.UpdateVelocity(context.Background(), "wallet_1", "event_3", 3000, 60)
	require.NoError(t, err)
	require.Equal(t, 5, count)

	mockStore.AssertNumberOfCalls(t, "UpdateVelocity", 2)
}
