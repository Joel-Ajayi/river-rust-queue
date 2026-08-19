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

	"crypto/ed25519"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/adapter/outbound/postgres"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/app"
	"github.com/golang-jwt/jwt/v5"
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

	_, priv, _ := ed25519.GenerateKey(nil)
	cfg := &platform.Config{
		HTTPPort:       8080,
		JWTSigningKeys: map[string]ed25519.PrivateKey{"key-1": priv},
		JWTActiveKeyID: "key-1",
		Capacity: &platform.CapacityConfig{
			WorkerPoolSize:       10,
			MaxRetries:           3,
			BackoffBaseMs:        10,
			BackoffCapMs:         100,
			RetryBudgetMinTokens: 10,
			RetryBudgetMaxTokens: 100,
			RetryBudgetFraction:  0.1,
			RequestTimeoutMs:     5000,
			ServerTimeoutMs:      10000,
			ServerIdleTimeoutMs:  60000,
			JWTAccessHrs:         1,
		},
	}

	jobStore := postgres.NewJobStore(cluster.ShardPools)
	merchantRepo := postgres.NewMerchantDirectory(cluster.ShardPools)
	walletRepo := postgres.NewWalletDirectory(cluster.ShardPools)

	jobSvc := app.NewJobService(merchantRepo, jobStore)
	merchantSvc := app.NewMerchantService(merchantRepo, cluster.ShardPools.HashRing())
	walletSvc := app.NewWalletService(merchantRepo, walletRepo, walletRepo, jobStore, platform.NewJobID)
	transferSvc := app.NewTransferService(merchantRepo, walletRepo, jobStore, platform.NewJobID)
	dlqReplayer := postgres.NewDLQReplayer(cfg, cluster.ShardPools, logger)
	adminSvc := app.NewAdminService(dlqReplayer)

	server := NewServer(
		cfg,
		transferSvc,
		jobSvc,
		merchantSvc,
		walletSvc,
		adminSvc,
		func(ctx context.Context) error { return nil },
		logger,
	)

	// Issue token for platform merchant
	now := time.Now()
	claims := jwt.MapClaims{
		"sub":  platformMerchantID,
		"iss":  platform.ServiceNameCoreAPI,
		"iat":  now.Unix(),
		"exp":  now.Add(15 * time.Minute).Unix(),
		"tier": platform.MerchantTierPremium,
	}
	tokenObj := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	tokenObj.Header[platform.JWTHeaderKeyID] = "key-1"
	token, err := tokenObj.SignedString(priv)
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
	req.Header.Set("X-Merchant-ID", platformMerchantID)
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
