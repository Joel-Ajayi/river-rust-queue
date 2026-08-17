//go:build integration

package app

import (
	"context"
	"testing"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/testutil"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/adapter/outbound/postgres"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/domain"
	"go.uber.org/zap"
)

func TestLedger_Container_PostTransfer_WithRealPostgres(t *testing.T) {
	cluster := testutil.SetupTestDB(t)

	// System wallets seeded from deploy/db/seed/shard.sql
	platformMerchantID := "merchant_00000000-0000-0000-0000-000000000001"
	wallet1 := "merchant_00000000-0000-0000-0000-000000000001.00000000-0000-0000-0000-000000000000"
	wallet2 := "merchant_00000000-0000-0000-0000-000000000001.00000000-0000-0000-0000-000000000001"

	logger, _ := zap.NewDevelopment()
	ledgerStore := postgres.NewLedgerStore(cluster.ShardPools, logger)

	jobID := platform.NewJobID()

	// Seed pending job into real Postgres container
	_, err := cluster.ShardA.Pool.Exec(context.Background(), `
		INSERT INTO jobs (id, merchant_id, idempotency_key, request_hash, type, status, created_at)
		VALUES ($1, $2, 'key_ledger_001', 'hash_1', 'transfer', 'pending', NOW())
	`, jobID, platformMerchantID)
	if err != nil {
		t.Fatalf("failed to insert pending job into container postgres: %v", err)
	}

	transferID := platform.NewDeterministicTransferID(jobID)

	transfer := domain.Transfer{
		ID:         transferID,
		JobID:      jobID,
		MerchantID: platformMerchantID,
		FromWallet: wallet1,
		ToWallet:   wallet2,
		Amount:     1500,
		Currency:   "NGN",
	}

	// Execute real double-entry transfer against real Postgres container
	err = ledgerStore.PostTransfer(context.Background(), "shard-a", transfer)
	if err != nil {
		t.Fatalf("failed to post transfer against real postgres container: %v", err)
	}

	// Verify ledger entries in real Postgres container
	var entryCount int
	err = cluster.ShardA.Pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM ledger_entries WHERE transfer_id = $1`, transferID).Scan(&entryCount)
	if err != nil {
		t.Fatalf("failed to query ledger_entries in container postgres: %v", err)
	}
	if entryCount != 2 {
		t.Fatalf("expected exactly 2 ledger entries (debit and credit) in real Postgres container, got %d", entryCount)
	}
}
