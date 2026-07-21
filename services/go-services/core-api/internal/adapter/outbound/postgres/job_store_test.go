//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/adapter/outbound/postgres"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/testutil"
	"go.uber.org/zap"
)

func TestJobStore_ClaimAndRecord(t *testing.T) {
	merchantsDB, shardA, _ := testutil.SetupTestDB(t)

	log := zap.NewNop()
	cfg := &platform.Config{
		MerchantsDBURI: merchantsDB.URI,
		ShardURIs: map[string]string{
			"shard-a": shardA.URI,
		},
	}
	pools, err := platform.NewShardPools(context.Background(), cfg, log)
	if err != nil {
		t.Fatalf("failed to init pools: %v", err)
	}
	t.Cleanup(func() { pools.Close() })

	merchantID := testutil.SeedMerchantAndWallets(t, merchantsDB, shardA)

	jobStore := postgres.NewJobStore(pools)

	ctx := context.Background()
	shardID := "shard-a"

	t.Run("successful claim and record", func(t *testing.T) {
		jobID := platform.NewJobID()
		idempKey := "idem_test_1"

		transfer := domain.Transfer{
			MerchantID: merchantID,
			FromWallet: merchantID + ".01905335-9781-7000-8000-000000000001",
			ToWallet:   merchantID + ".01905335-9781-7000-8000-000000000002",
			Amount:     1000,
			Currency:   "NGN",
		}
		
		job := domain.Job{
			ID:             jobID,
			MerchantID:     merchantID,
			IdempotencyKey: idempKey,
			PayloadHash:    transfer.Hash(),
			Type:           platform.JobTypeTransfer,
			Status:         platform.JobStatusPending,
			CreatedAt:      time.Now(),
		}

		res, err := jobStore.ClaimAndRecord(ctx, shardID, job, transfer, idempKey)
		if err != nil {
			t.Fatalf("ClaimAndRecord failed: %v", err)
		}
		if res.AlreadyExisted {
			t.Errorf("expected new job creation, got AlreadyExisted=true")
		}
	})

	t.Run("idempotent replay", func(t *testing.T) {
		jobID := platform.NewJobID()
		idempKey := "idem_test_2"

		transfer := domain.Transfer{
			MerchantID: merchantID,
			FromWallet: merchantID + ".01905335-9781-7000-8000-000000000001",
			ToWallet:   merchantID + ".01905335-9781-7000-8000-000000000002",
			Amount:     1000,
			Currency:   "NGN",
		}

		job := domain.Job{
			ID:             jobID,
			MerchantID:     merchantID,
			IdempotencyKey: idempKey,
			PayloadHash:    transfer.Hash(),
			Type:           platform.JobTypeTransfer,
			Status:         platform.JobStatusPending,
			CreatedAt:      time.Now(),
		}

		// First submission
		_, err := jobStore.ClaimAndRecord(ctx, shardID, job, transfer, idempKey)
		if err != nil {
			t.Fatalf("first ClaimAndRecord failed: %v", err)
		}

		// Second submission with exact same key and body (transfer ID doesn't matter for idempotency check)
		res2, err2 := jobStore.ClaimAndRecord(ctx, shardID, job, transfer, idempKey)
		if err2 != nil {
			t.Fatalf("second ClaimAndRecord failed: %v", err2)
		}
		if !res2.AlreadyExisted {
			t.Errorf("expected idempotent replay, got AlreadyExisted=false")
		}
	})

	t.Run("idempotent mismatch", func(t *testing.T) {
		jobID := platform.NewJobID()
		idempKey := "idem_test_3"

		transfer1 := domain.Transfer{
			MerchantID: merchantID,
			FromWallet: merchantID + ".01905335-9781-7000-8000-000000000001",
			ToWallet:   merchantID + ".01905335-9781-7000-8000-000000000002",
			Amount:     1000,
			Currency:   "NGN",
		}

		job := domain.Job{
			ID:             jobID,
			MerchantID:     merchantID,
			IdempotencyKey: idempKey,
			PayloadHash:    transfer1.Hash(),
			Type:           platform.JobTypeTransfer,
			Status:         platform.JobStatusPending,
			CreatedAt:      time.Now(),
		}

		// First submission
		_, err := jobStore.ClaimAndRecord(ctx, shardID, job, transfer1, idempKey)
		if err != nil {
			t.Fatalf("first ClaimAndRecord failed: %v", err)
		}

		// Second submission with same key but different body (amount)
		transfer2 := transfer1
		transfer2.Amount = 5000
		
		job2 := job
		job2.PayloadHash = transfer2.Hash()
		
		// The job store should return ErrIdempotencyConflict
		_, err2 := jobStore.ClaimAndRecord(ctx, shardID, job2, transfer2, idempKey)
		if !errors.Is(err2, domain.ErrIdempotencyConflict) {
			t.Errorf("expected ErrIdempotencyConflict, got %v", err2)
		}
	})
}

func TestJobStore_GetJob(t *testing.T) {
	merchantsDB, shardA, _ := testutil.SetupTestDB(t)

	log := zap.NewNop()
	cfg := &platform.Config{
		MerchantsDBURI: merchantsDB.URI,
		ShardURIs: map[string]string{
			"shard-a": shardA.URI,
		},
	}
	pools, err := platform.NewShardPools(context.Background(), cfg, log)
	if err != nil {
		t.Fatalf("failed to init pools: %v", err)
	}
	t.Cleanup(func() { pools.Close() })

	merchantID := testutil.SeedMerchantAndWallets(t, merchantsDB, shardA)
	jobStore := postgres.NewJobStore(pools)
	ctx := context.Background()
	shardID := "shard-a"

	t.Run("GetJob_Success", func(t *testing.T) {
		jobID := platform.NewJobID()
		idempKey := "idem_test_getjob"
		transfer := domain.Transfer{
			MerchantID: merchantID,
			FromWallet: merchantID + ".01905335-9781-7000-8000-000000000001",
			ToWallet:   merchantID + ".01905335-9781-7000-8000-000000000002",
			Amount:     1000,
			Currency:   "NGN",
		}
		job := domain.Job{
			ID:             jobID,
			MerchantID:     merchantID,
			IdempotencyKey: idempKey,
			PayloadHash:    transfer.Hash(),
			Type:           platform.JobTypeTransfer,
			Status:         platform.JobStatusPending,
			CreatedAt:      time.Now(),
		}

		_, err := jobStore.ClaimAndRecord(ctx, shardID, job, transfer, idempKey)
		if err != nil {
			t.Fatalf("ClaimAndRecord failed: %v", err)
		}

		retrieved, err := jobStore.GetJob(ctx, shardID, jobID)
		if err != nil {
			t.Fatalf("GetJob failed: %v", err)
		}
		if retrieved.ID != jobID {
			t.Errorf("expected job ID %s, got %s", jobID, retrieved.ID)
		}
		if retrieved.MerchantID != merchantID {
			t.Errorf("expected merchant ID %s, got %s", merchantID, retrieved.MerchantID)
		}
	})

	t.Run("GetJob_NotFound", func(t *testing.T) {
		nonExistentJobID := platform.NewJobID()
		_, err := jobStore.GetJob(ctx, shardID, nonExistentJobID)
		if !errors.Is(err, domain.ErrJobNotFound) {
			t.Errorf("expected ErrJobNotFound, got %v", err)
		}
	})
}
