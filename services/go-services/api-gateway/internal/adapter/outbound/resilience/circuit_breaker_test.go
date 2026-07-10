package resilience_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/adapter/outbound/resilience"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/port"
)

// mockJobStore is a hand-written mock for testing the JobStore Decorator
type mockJobStore struct {
	getJobFunc         func(ctx context.Context, shardID, jobID string) (domain.Job, error)
	claimAndRecordFunc func(ctx context.Context, shardID string, job domain.Job, t domain.Transfer, idempKey string) (domain.SubmitResult, error)
}

// compile time interface implementation check
var _ port.JobStore = (*mockJobStore)(nil)

func (m *mockJobStore) GetJob(ctx context.Context, shardID, jobID string) (domain.Job, error) {
	if m.getJobFunc != nil {
		return m.getJobFunc(ctx, shardID, jobID)
	}
	return domain.Job{}, nil
}

func (m *mockJobStore) ClaimAndRecord(ctx context.Context, shardID string, job domain.Job, t domain.Transfer, idempKey string) (domain.SubmitResult, error) {
	if m.claimAndRecordFunc != nil {
		return m.claimAndRecordFunc(ctx, shardID, job, t, idempKey)
	}
	return domain.SubmitResult{}, nil
}

func TestJobStoreCB_RetriesOnTransientError(t *testing.T) {
	callCount := 0
	mockStore := &mockJobStore{
		getJobFunc: func(ctx context.Context, shardID, jobID string) (domain.Job, error) {
			callCount++
			if callCount < 3 {
				// Return context.DeadlineExceeded to strictly match our taxonomy for a transient error
				return domain.Job{}, context.DeadlineExceeded
			}
			return domain.Job{ID: "success"}, nil
		},
	}

	cbStore := resilience.NewJobStoreCB(mockStore)

	// Since max retries is 3, failing twice and succeeding on 3rd should work perfectly
	job, err := cbStore.GetJob(context.Background(), "shard-a", "job-123")

	if err != nil {
		t.Fatalf("expected success after retries, got error: %v", err)
	}
	if job.ID != "success" {
		t.Fatalf("expected job ID 'success', got '%s'", job.ID)
	}
	if callCount != 3 {
		t.Fatalf("expected exactly 3 calls (2 retries + 1 success), got %d", callCount)
	}
}

func TestJobStoreCB_DoesNotRetryNonRetryableError(t *testing.T) {
	callCount := 0
	mockStore := &mockJobStore{
		getJobFunc: func(ctx context.Context, shardID, jobID string) (domain.Job, error) {
			callCount++
			// domain.ErrJobNotFound is considered non-retryable
			return domain.Job{}, domain.ErrJobNotFound
		},
	}

	cbStore := resilience.NewJobStoreCB(mockStore)

	_, err := cbStore.GetJob(context.Background(), "shard-a", "job-123")

	if !errors.Is(err, domain.ErrJobNotFound) {
		t.Fatalf("expected domain.ErrJobNotFound, got: %v", err)
	}
	if callCount != 1 {
		t.Fatalf("expected exactly 1 call (no retries for business errors), got %d", callCount)
	}
}

func TestJobStoreCB_CircuitBreakerTrips(t *testing.T) {
	mockStore := &mockJobStore{
		getJobFunc: func(ctx context.Context, shardID, jobID string) (domain.Job, error) {
			return domain.Job{}, errors.New("fatal database crash")
		},
	}

	cbStore := resilience.NewJobStoreCB(mockStore)

	// MaxFails is 3. We must fail 3 times across the circuit breaker to trip it.
	// Since "fatal database crash" is not transient, the Jitter layer fails fast without retrying.
	// So we must manually make 3 calls to trip the circuit breaker.
	for i := 0; i < 3; i++ {
		cbStore.GetJob(context.Background(), "shard-a", "job-123")
	}

	// Now the circuit breaker should be OPEN. The 4th call should return domain.ErrServiceUnavailable.
	_, err := cbStore.GetJob(context.Background(), "shard-a", "job-123")

	if !errors.Is(err, domain.ErrServiceUnavailable) {
		t.Fatalf("expected ErrServiceUnavailable (circuit breaker open), got: %v", err)
	}
}

func TestJobStoreCB_CellBasedIsolation(t *testing.T) {
	mockStore := &mockJobStore{
		getJobFunc: func(ctx context.Context, shardID, jobID string) (domain.Job, error) {
			if shardID == "shard-a" {
				return domain.Job{}, errors.New("shard-a database crash")
			}
			return domain.Job{ID: "shard-b-success"}, nil
		},
	}

	cbStore := resilience.NewJobStoreCB(mockStore)

	// Make 3 calls to shard-a to trip its circuit breaker (since it fails fast on non-transient errors)
	for i := 0; i < 3; i++ {
		cbStore.GetJob(context.Background(), "shard-a", "job-123")
	}

	// The 4th call to shard-a should return ErrServiceUnavailable
	_, errA := cbStore.GetJob(context.Background(), "shard-a", "job-123")
	if !errors.Is(errA, domain.ErrServiceUnavailable) {
		t.Fatalf("expected ErrServiceUnavailable for shard-a, got: %v", errA)
	}

	// Route a request to shard-b, it should SUCCEED and not be impacted by shard-a's failure!
	job, err := cbStore.GetJob(context.Background(), "shard-b", "job-789")
	if err != nil {
		t.Fatalf("expected success for shard-b, got error: %v", err)
	}
	if job.ID != "shard-b-success" {
		t.Fatalf("expected job ID 'shard-b-success', got '%s'", job.ID)
	}
}
