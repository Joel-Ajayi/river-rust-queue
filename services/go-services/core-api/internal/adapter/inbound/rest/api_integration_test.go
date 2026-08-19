package rest

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/port"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"go.uber.org/zap"
)

// === Mock Port Implementations ===

type mockTransferSubmitter struct {
	transferFunc func(ctx context.Context, t domain.Transfer, idempKey string) (domain.SubmitResult, error)
}

func (m *mockTransferSubmitter) Transfer(ctx context.Context, t domain.Transfer, idempKey string) (domain.SubmitResult, error) {
	if m.transferFunc != nil {
		return m.transferFunc(ctx, t, idempKey)
	}
	return domain.SubmitResult{Job: domain.Job{ID: platform.NewJobID(), Status: platform.JobStatusPending}}, nil
}

func (m *mockTransferSubmitter) GetBalance(ctx context.Context, merchantID, walletID string) (int64, string, error) {
	return 10000, "USD", nil
}

type mockJobReader struct {
	getJobFunc func(ctx context.Context, merchantID, jobID string) (domain.Job, error)
}

func (m *mockJobReader) GetJobStatus(ctx context.Context, merchantID, jobID string) (domain.Job, error) {
	if m.getJobFunc != nil {
		return m.getJobFunc(ctx, merchantID, jobID)
	}
	return domain.Job{ID: jobID, MerchantID: merchantID, Status: platform.JobStatusCompleted}, nil
}

type mockMerchantUseCase struct{}

func (m *mockMerchantUseCase) CreateMerchant(ctx context.Context, name, webhookURL, webhookSecret, tier string) (string, string, string, error) {
	return platform.NewMerchantID(), "rrq_live_key_abc123", "secret_123", nil
}

func (m *mockMerchantUseCase) Authenticate(ctx context.Context, apiKey string) (domain.Principal, error) {
	return domain.Principal{MerchantID: "merch_12345678-1234-7890-1234-123456789012", Tier: "tier_1", Status: platform.MerchantStatusActive}, nil
}

type mockWalletUseCase struct{}

func (m *mockWalletUseCase) CreateWallet(ctx context.Context, merchantID, currency string) (string, error) {
	return "w-123", nil
}

func (m *mockWalletUseCase) Deposit(ctx context.Context, t domain.Transfer, idempKey string) (domain.SubmitResult, error) {
	return domain.SubmitResult{Job: domain.Job{ID: platform.NewJobID(), Status: platform.JobStatusPending}}, nil
}

type mockAdminUseCase struct{}

func (m *mockAdminUseCase) ListDLQEntries(ctx context.Context, source string, status string, limit int, offset int) ([]platform.DLQEntrySummary, error) {
	return []platform.DLQEntrySummary{}, nil
}

func (m *mockAdminUseCase) ReplayDLQ(ctx context.Context, source string, limit int) (port.ReplayResult, error) {
	return port.ReplayResult{ReplayedCount: 0}, nil
}

func (m *mockAdminUseCase) ReplayDLQEntry(ctx context.Context, source string, id string) (platform.DLQEntrySummary, error) {
	return platform.DLQEntrySummary{ID: id, Status: platform.DLQStatusReplayed}, nil
}

func setupTestServer() (*Server, *http.ServeMux) {
	_, priv, _ := ed25519.GenerateKey(nil)
	cfg := &platform.Config{
		PlatformAdminKey: "test-admin-secret",
		JWTActiveKeyID:   "key-1",
		JWTSigningKeys:   map[string]ed25519.PrivateKey{"key-1": priv},
		Capacity: &platform.CapacityConfig{
			WorkerPoolSize:       10,
			MaxRetries:           3,
			BackoffBaseMs:        10,
			BackoffCapMs:         100,
			RetryBudgetMinTokens: 10,
			RetryBudgetMaxTokens: 100,
			RetryBudgetFraction:  0.1,
			RequestTimeoutMs:     2000,
			JWTAccessHrs:         24,
		},
	}

	logger, _ := zap.NewDevelopment()
	srv := NewServer(
		cfg,
		&mockTransferSubmitter{},
		&mockJobReader{},
		&mockMerchantUseCase{},
		&mockWalletUseCase{},
		&mockAdminUseCase{},
		func(ctx context.Context) error { return nil },
		logger,
	)

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)
	return srv, mux
}

// === API Integration Tests ===

func TestAPI_HealthAndReady(t *testing.T) {
	_, mux := setupTestServer()

	// Health
	req := httptest.NewRequest("GET", platform.APIHealthPath, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected /health status 200, got %d", rec.Code)
	}

	// Ready
	reqReady := httptest.NewRequest("GET", platform.APIReadyPath, nil)
	recReady := httptest.NewRecorder()
	mux.ServeHTTP(recReady, reqReady)
	if recReady.Code != http.StatusOK {
		t.Fatalf("expected /ready status 200, got %d", recReady.Code)
	}
}

func TestAPI_JWKSEndpoint(t *testing.T) {
	_, mux := setupTestServer()

	req := httptest.NewRequest("GET", "/.well-known/jwks.json", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected JWKS status 200, got %d", rec.Code)
	}

	var jwks struct {
		Keys []map[string]interface{} `json:"keys"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &jwks); err != nil {
		t.Fatalf("failed to unmarshal JWKS: %v", err)
	}
	if len(jwks.Keys) == 0 {
		t.Fatalf("expected at least 1 JWK key in response")
	}
}

func TestAPI_AuthTokenEndpoint(t *testing.T) {
	_, mux := setupTestServer()

	req := httptest.NewRequest("POST", platform.APIAuthTokenPath, nil)
	req.Header.Set("Authorization", "Bearer rrq_live_key_abc123")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected auth token status 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestAPI_CreateTransferEndpoint(t *testing.T) {
	_, mux := setupTestServer()

	merchID := platform.NewMerchantID()
	walletFrom := platform.NewWalletID(merchID)
	walletTo := platform.NewWalletID(merchID)

	// Missing Idempotency-Key
	body := bytes.NewBufferString(`{"from_wallet": "` + walletFrom + `", "to_wallet": "` + walletTo + `", "amount": 100, "currency": "USD"}`)
	reqNoIdemp := httptest.NewRequest("POST", platform.APITransfersPath, body)
	reqNoIdemp.Header.Set("Content-Type", "application/json")
	reqNoIdemp.Header.Set("X-Merchant-ID", merchID)
	recNoIdemp := httptest.NewRecorder()
	mux.ServeHTTP(recNoIdemp, reqNoIdemp)
	if recNoIdemp.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing Idempotency-Key, got %d", recNoIdemp.Code)
	}

	// Valid Transfer
	bodyValid := bytes.NewBufferString(`{"from_wallet": "` + walletFrom + `", "to_wallet": "` + walletTo + `", "amount": 100, "currency": "USD"}`)
	reqValid := httptest.NewRequest("POST", platform.APITransfersPath, bodyValid)
	reqValid.Header.Set("Content-Type", "application/json")
	reqValid.Header.Set("X-Idempotency-Key", "idemp-001")
	reqValid.Header.Set("X-Merchant-ID", merchID)
	recValid := httptest.NewRecorder()
	mux.ServeHTTP(recValid, reqValid)

	if recValid.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d: %s", recValid.Code, recValid.Body.String())
	}
}

func TestAPI_GetJobStatusEndpoint(t *testing.T) {
	_, mux := setupTestServer()

	merchID := platform.NewMerchantID()
	jobID := platform.NewJobID()

	req := httptest.NewRequest("GET", platform.APIJobPathPrefix+jobID, nil)
	req.Header.Set("X-Merchant-ID", merchID)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected job status 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestAPI_AdminDLQEndpoints(t *testing.T) {
	_, mux := setupTestServer()

	// Unauthorized (missing X-Merchant-ID)
	reqUnauth := httptest.NewRequest("GET", platform.APIAdminDLQListPath, nil)
	recUnauth := httptest.NewRecorder()
	mux.ServeHTTP(recUnauth, reqUnauth)
	if recUnauth.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got %d", recUnauth.Code)
	}

	// Authorized (platform admin identity + envoy edge header)
	reqAuth := httptest.NewRequest("GET", platform.APIAdminDLQListPath, nil)
	reqAuth.Header.Set(HeaderMerchantID, platform.PlatformMerchantID)
	reqAuth.Header.Set(HeaderEdgeOrigin, HeaderEdgeOriginValue)
	recAuth := httptest.NewRecorder()
	mux.ServeHTTP(recAuth, reqAuth)
	if recAuth.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for admin DLQ list, got %d: %s", recAuth.Code, recAuth.Body.String())
	}
}

func TestAPI_CreateMerchantEndpoint(t *testing.T) {
	_, mux := setupTestServer()

	body := bytes.NewBufferString(`{"name": "Test Merchant", "webhook_url": "https://example.com/webhook", "webhook_secret": "sec", "tier": "standard"}`)
	req := httptest.NewRequest("POST", platform.APIMerchantsPath, body)
	req.Header.Set("Content-Type", "application/json")
	
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestAPI_CreateWalletEndpoint(t *testing.T) {
	_, mux := setupTestServer()
	merchID := platform.NewMerchantID()

	body := bytes.NewBufferString(`{"currency": "USD"}`)
	req := httptest.NewRequest("POST", platform.APIWalletsPath, body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Merchant-ID", merchID)
	
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestAPI_GetBalanceEndpoint(t *testing.T) {
	_, mux := setupTestServer()
	merchID := platform.NewMerchantID()
	walletID := platform.NewWalletID(merchID)

	req := httptest.NewRequest("GET", platform.APIBalancesPath+"?wallet_id="+walletID, nil)
	req.Header.Set("X-Merchant-ID", merchID)
	
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}
}
