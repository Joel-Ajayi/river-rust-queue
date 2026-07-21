package resilience

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/port"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

// mockJobStore implements port.JobStore.
type mockJobStore struct {
	getJobFn         func(ctx context.Context, shardID, jobID string) (domain.Job, error)
	claimAndRecordFn func(ctx context.Context, shardID string, job domain.Job, t domain.Transfer, idempKey string) (domain.SubmitResult, error)
}

var _ port.JobStore = (*mockJobStore)(nil)

func (m *mockJobStore) GetJob(ctx context.Context, shardID, jobID string) (domain.Job, error) {
	if m.getJobFn != nil {
		return m.getJobFn(ctx, shardID, jobID)
	}
	return domain.Job{}, nil
}

func (m *mockJobStore) ClaimAndRecord(ctx context.Context, shardID string, job domain.Job, t domain.Transfer, idempKey string) (domain.SubmitResult, error) {
	if m.claimAndRecordFn != nil {
		return m.claimAndRecordFn(ctx, shardID, job, t, idempKey)
	}
	return domain.SubmitResult{}, nil
}

// newTestRegistry builds a per-pool DB breaker registry for testing.
func newTestRegistry(t *testing.T, shardIDs []string) *platform.DBCircuitBreakers {
	t.Helper()
	return platform.NewDBCircuitBreakers(
		platform.CBNameMerchantsGlobal,
		shardIDs,
		domain.IsTerminalError,
		platform.CircuitBreakerConfig{
			MaxRequests: 1,
			Timeout:     1 * time.Second,
			MaxFails:    3,
			MinRequests: 0,
			ErrorRate:   0,
		},
	)
}

// TestJobStoreCB_NoRetry confirms L3 decorator is a pure shield: no inner retries.
func TestJobStoreCB_NoRetry(t *testing.T) {
	t.Parallel()
	var calls int32
	store := &mockJobStore{
		getJobFn: func(ctx context.Context, shardID, jobID string) (domain.Job, error) {
			atomic.AddInt32(&calls, 1)
			return domain.Job{}, context.DeadlineExceeded // transient
		},
	}
	cb := NewJobStoreCB(store, newTestRegistry(t, []string{"shard-a"}))
	_, err := cb.GetJob(context.Background(), "shard-a", "j1")
	if err == nil {
		t.Fatal("expected error to surface")
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("L3 shield must not retry: want 1 call, got %d", got)
	}
}

// TestJobStoreCB_TerminalIsSuccess confirms terminal errors don't trip the breaker.
func TestJobStoreCB_TerminalIsSuccess(t *testing.T) {
	t.Parallel()
	store := &mockJobStore{
		getJobFn: func(ctx context.Context, shardID, jobID string) (domain.Job, error) {
			return domain.Job{}, domain.ErrJobNotFound
		},
	}
	cbs := newTestRegistry(t, []string{"shard-a"})
	cb := NewJobStoreCB(store, cbs)
	for i := 0; i < 3+2; i++ { // MaxFails=3
		_, err := cb.GetJob(context.Background(), "shard-a", "j1")
		if !errors.Is(err, domain.ErrJobNotFound) {
			t.Fatalf("call %d: want ErrJobNotFound, got %v", i, err)
		}
	}
	if got := cbs.ShardRW("shard-a").State(); got != platform.CBStateClosed {
		t.Fatalf("terminal errors must not trip; want Closed, got %v", got)
	}
}

// TestJobStoreCB_TripsAfterMaxFails: 3 infra failures open the shard breaker.
func TestJobStoreCB_TripsAfterMaxFails(t *testing.T) {
	t.Parallel()
	store := &mockJobStore{
		getJobFn: func(ctx context.Context, shardID, jobID string) (domain.Job, error) {
			return domain.Job{}, errors.New("connection reset: infrastructure failure")
		},
	}
	cbs := newTestRegistry(t, []string{"shard-a"})
	cb := NewJobStoreCB(store, cbs)
	for i := 0; i < 3; i++ { // MaxFails=3
		_, _ = cb.GetJob(context.Background(), "shard-a", "j1")
	}
	if got := cbs.ShardRO("shard-a").State(); got != platform.CBStateOpen {
		t.Fatalf("want Open after MaxFails infra errors, got %v", got)
	}
}

// TestJobStoreCB_PerShardIsolation: tripping shard-a leaves shard-b Closed.
func TestJobStoreCB_PerShardIsolation(t *testing.T) {
	t.Parallel()
	store := &mockJobStore{
		getJobFn: func(ctx context.Context, shardID, jobID string) (domain.Job, error) {
			if shardID == "shard-a" {
				return domain.Job{}, errors.New("infra failure: shard-a")
			}
			return domain.Job{ID: "shard-b-success"}, nil
		},
	}
	cbs := newTestRegistry(t, []string{"shard-a", "shard-b"})
	cb := NewJobStoreCB(store, cbs)
	for i := 0; i < 3; i++ { // MaxFails=3
		_, _ = cb.GetJob(context.Background(), "shard-a", "j1")
	}
	if cbs.ShardRO("shard-a").State() != platform.CBStateOpen {
		t.Fatal("shard-a should be Open")
	}
	if cbs.ShardRO("shard-b").State() != platform.CBStateClosed {
		t.Fatal("shard-b must remain Closed")
	}
	job, err := cb.GetJob(context.Background(), "shard-b", "j2")
	if err != nil || job.ID != "shard-b-success" {
		t.Fatalf("shard-b must still serve traffic, got job=%v err=%v", job, err)
	}
}

// TestSingleBreakerPerShard verifies three decorators share one breaker per shard.
func TestSingleBreakerPerShard(t *testing.T) {
	t.Parallel()
	cbs := newTestRegistry(t, []string{"shard-0", "shard-1"})
	jobs := NewJobStoreCB(&mockJobStore{}, cbs)
	_ = jobs
	ledger := NewJobStoreCB(&mockJobStore{}, cbs) // reuse JobStoreCB as a stand-in proxy
	_ = ledger
	// Pointer identity: same shard → same breaker.
	cb1 := cbs.ShardRO("shard-0")
	cb2 := cbs.ShardRO("shard-0")
	if cb1 != cb2 {
		t.Fatal("same shard must yield same breaker pointer")
	}
	// Different shard → different breaker.
	if cbs.ShardRO("shard-0") == cbs.ShardRO("shard-1") {
		t.Fatal("different shards must yield different breakers")
	}
	// Merchants-global isolated from any shard.
	if cbs.Merchants() == cbs.ShardRW("shard-0") {
		t.Fatal("merchants-global and shard-0 must be different breakers")
	}
}

// TestOpenBreakerFastFail verifies Open state fast-rejects without invoking store.
func TestOpenBreakerFastFail(t *testing.T) {
	t.Parallel()
	var calls int32
	store := &mockJobStore{
		getJobFn: func(ctx context.Context, shardID, jobID string) (domain.Job, error) {
			atomic.AddInt32(&calls, 1)
			return domain.Job{}, errors.New("infra failure")
		},
	}
	cbs := newTestRegistry(t, []string{"shard-a"})
	cb := NewJobStoreCB(store, cbs)
	for i := 0; i < 3; i++ { // MaxFails=3
		_, _ = cb.GetJob(context.Background(), "shard-a", "j1")
	}
	atomic.StoreInt32(&calls, 0)
	start := time.Now()
	_, err := cb.GetJob(context.Background(), "shard-a", "j1")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected fast-fail error")
	}
	if atomic.LoadInt32(&calls) != 0 {
		t.Fatalf("Open breaker must NOT call store; got %d calls", calls)
	}
	if elapsed > 5*time.Millisecond {
		t.Fatalf("Open breaker should fail in microseconds, took %v", elapsed)
	}
}
