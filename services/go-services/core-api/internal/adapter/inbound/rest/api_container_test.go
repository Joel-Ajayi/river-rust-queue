//go:build integration

package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/adapter/outbound/postgres"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/testutil"
	"go.uber.org/zap"
)

func TestAPI_Container_CreateTransfer_WithRealPostgres(t *testing.T) {
	cluster := testutil.SetupTestDB(t)

	// Platform Merchant and System Wallets seeded from deploy/db/seed
	platformMerchantID := "merchant_00000000-0000-0000-0000-000000000001"
	systemWalletID := "merchant_00000000-0000-0000-0000-000000000001.00000000-0000-0000-0000-000000000000"

	logger, _ := zap.NewDevelopment()
	jobStore := postgres.NewJobStore(cluster.ShardPools, logger)
	merchantRepo := postgres.NewMerchantRepository(cluster.MerchantsDB.Pool)
	walletRepo := postgres.NewWalletRepository(cluster.ShardPools)

	server := NewServer(
		logger,
		jobStore,
		merchantRepo,
		walletRepo,
		"http://localhost:8080",
		15*time.Minute,
		nil,
	)

	// Issue token for platform merchant
	principal := platform.SecurityPrincipal{
		MerchantID: platformMerchantID,
		ShardID:    "shard-a",
		Role:       "merchant",
	}
	token, err := server.tokenService.GenerateToken(principal, 15*time.Minute)
	if err != nil {
		t.Fatalf("failed to generate token for container test: %v", err)
	}

	body, _ := json.Marshal(map[string]interface{}{
		"from_wallet": systemWalletID,
		"to_wallet":   systemWalletID,
		"amount":      1000,
		"currency":    "NGN",
	})

	req := httptest.NewRequest("POST", "/v1/transfers", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Idempotency-Key", "idemp_container_001")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected HTTP 202 Accepted from real Postgres container API test, got %d. Body: %s", rec.Code, rec.Body.String())
	}

	// Verify job was written to real Postgres container
	var jobCount int
	err = cluster.ShardA.Pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM jobs WHERE merchant_id = $1 AND idempotency_key = $2`, platformMerchantID, "idemp_container_001").Scan(&jobCount)
	if err != nil {
		t.Fatalf("failed to query real postgres container jobs table: %v", err)
	}
	if jobCount != 1 {
		t.Fatalf("expected exactly 1 job row in real Postgres container, got %d", jobCount)
	}
}
