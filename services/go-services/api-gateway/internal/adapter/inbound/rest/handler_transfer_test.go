package rest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/adapter/inbound/rest"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/adapter/outbound/postgres"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/app"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
	apiv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/api/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/testutil"
	"github.com/golang-jwt/jwt/v5"
	"go.uber.org/zap"
)

type fakeJobStore struct {
	gotShard string
	gotJob   domain.Job
	result   domain.SubmitResult
	err      error
	jobs     map[string]domain.Job // For GetJob testing
}

func (f *fakeJobStore) ClaimAndRecord(
	_ context.Context, shardID string, job domain.Job, _ domain.Transfer, _ string,
) (domain.SubmitResult, error) {
	f.gotShard = shardID
	f.gotJob = job
	if f.err != nil {
		return domain.SubmitResult{}, f.err
	}
	return f.result, nil
}

func (f *fakeJobStore) GetJob(ctx context.Context, shardID, jobID string) (domain.Job, error) {
	if f.err != nil {
		return domain.Job{}, f.err
	}
	job, ok := f.jobs[jobID]
	if !ok {
		return domain.Job{}, domain.ErrJobNotFound
	}
	return job, nil
}

// setupEnvironment creates the real HTTP handler backed by testcontainers DBs.
func setupEnvironment(t *testing.T) (http.Handler, string, testutil.TestDB) {
	merchantsDB, shardA, shardB := testutil.SetupTestDB(t)

	log := zap.NewNop()
	cfg := &platform.Config{
		MerchantsDBURI: merchantsDB.URI,
		ShardURIs: map[string]string{
			"shard-a": shardA.URI,
			"shard-b": shardB.URI,
		},
	}
	pools, err := platform.NewShardPools(context.Background(), cfg, log)
	if err != nil {
		t.Fatalf("failed to init pools: %v", err)
	}
	t.Cleanup(func() { pools.Close() })

	// Seed a test merchant
	merchantID := "m_123"
	_, err = merchantsDB.Pool.Exec(context.Background(),
		`INSERT INTO merchants (id, name, tier, status, shard_id, api_key_hash) VALUES ($1, 'Test', 'starter', 'active', 'shard-a', 'dummy-hash')`, merchantID)
	if err != nil {
		t.Fatalf("failed to seed merchant: %v", err)
	}

	// Seed wallets for the merchant in the shard DB
	_, err = shardA.Pool.Exec(context.Background(),
		`INSERT INTO wallets (id, merchant_id, currency) VALUES
		('wal_A', $1, 'NGN'),
		('wal_B', $1, 'NGN'),
		('wal_foreign', 'm_999', 'NGN')`, merchantID)
	if err != nil {
		t.Fatalf("failed to seed wallets: %v", err)
	}

	jwtSecret := "test-secret"
	merchants := postgres.NewMerchantDirectory(pools)
	wallets := postgres.NewWalletDirectory(pools)
	jobs := postgres.NewJobStore(pools)
	// Use the app Service directly for tests (it implements both Submitter and Authenticator)
	svc := app.NewService(merchants, wallets, jobs, func() string { return "job_123" })

	// Set up the Server with our mocked/ephemeral dependencies
	server := rest.NewServer(
		svc,
		merchants,
		jwtSecret,
		func(ctx context.Context) error { return nil },
		zap.NewNop(),
	)

	// Mint a token for tests
	now := time.Now()
	claims := jwt.MapClaims{
		"sub":  "m_123",
		"iat":  now.Unix(),
		"exp":  now.Add(1 * time.Hour).Unix(),
		"tier": 1,
	}
	tokenStr, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(jwtSecret))

	return server, tokenStr, shardA
}

func TestIdempotency_ConcurrentDuplicates(t *testing.T) {
	handler, tokenStr, shardA := setupEnvironment(t)

	reqDTO := apiv1.CreateTransferRequest{
		FromWallet: "wal_A",
		ToWallet:   "wal_B",
		Amount:     1000,
		Currency:   "NGN",
		Reference:  "ref123",
	}
	body, _ := json.Marshal(&reqDTO)
	idempKey := "idem_concurrent_123"

	// Fire 50 concurrent requests
	numReqs := 50
	var wg sync.WaitGroup
	results := make([]*httptest.ResponseRecorder, numReqs)

	for i := 0; i < numReqs; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, platform.APITransfersPath, bytes.NewReader(body))
			req.Header.Set("Authorization", string(rest.HeaderValBearer)+tokenStr)
			req.Header.Set(string(rest.HeaderIdempotencyKey), idempKey)
			req.Header.Set("Content-Type", "application/json")

			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
			results[idx] = w
		}(i)
	}
	wg.Wait()

	// Assertions
	var accepted, ok, failed int
	var capturedJobID string
	for _, res := range results {
		if res.Code == http.StatusAccepted {
			accepted++
			var resp apiv1.CreateTransferResponse
			json.Unmarshal(res.Body.Bytes(), &resp)
			capturedJobID = resp.JobId
		} else if res.Code == http.StatusOK {
			ok++
		} else {
			failed++
			t.Logf("Failed request code: %d, body: %s", res.Code, res.Body.String())
		}
	}

	// 1. Exactly one should be 202 Accepted, the rest 200 OK (idempotent replay)
	if accepted != 1 {
		t.Errorf("expected exactly 1 accepted request, got %d", accepted)
	}
	if ok != numReqs-1 {
		t.Errorf("expected %d ok (replay) requests, got %d", numReqs-1, ok)
	}
	if failed > 0 {
		t.Errorf("expected 0 failed requests, got %d", failed)
	}

	// 2. Exactly one row in jobs table
	var count int
	err := shardA.Pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM jobs WHERE merchant_id = 'm_123' AND idempotency_key = $1`, idempKey).Scan(&count)
	if err != nil || count != 1 {
		t.Errorf("expected exactly 1 job in DB, got %d (err: %v)", count, err)
	}

	// 3. Exactly one row in outbox events table
	err = shardA.Pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM events WHERE aggregate_type = $2 AND aggregate_id = $1`, capturedJobID, platform.AggregateTypeJob).Scan(&count)
	if err != nil || count != 1 {
		t.Errorf("expected exactly 1 outbox event in DB, got %d (err: %v)", count, err)
	}
}

func TestIdempotency_DifferentBody(t *testing.T) {
	handler, tokenStr, _ := setupEnvironment(t)

	reqDTO1 := apiv1.CreateTransferRequest{FromWallet: "wal_A", ToWallet: "wal_B", Amount: 1000, Currency: "NGN"}
	reqDTO2 := apiv1.CreateTransferRequest{FromWallet: "wal_A", ToWallet: "wal_B", Amount: 5000, Currency: "NGN"}

	body1, _ := json.Marshal(&reqDTO1)
	body2, _ := json.Marshal(&reqDTO2)
	idempKey := "idem_diff_123"

	// First Request
	req1 := httptest.NewRequest(http.MethodPost, platform.APITransfersPath, bytes.NewReader(body1))
	req1.Header.Set("Authorization", string(rest.HeaderValBearer)+tokenStr)
	req1.Header.Set(string(rest.HeaderIdempotencyKey), idempKey)
	w1 := httptest.NewRecorder()
	handler.ServeHTTP(w1, req1)

	if w1.Code != http.StatusAccepted {
		t.Fatalf("first request failed, got %d: %s", w1.Code, w1.Body.String())
	}

	// Second Request with SAME key but DIFFERENT body
	req2 := httptest.NewRequest(http.MethodPost, platform.APITransfersPath, bytes.NewReader(body2))
	req2.Header.Set("Authorization", string(rest.HeaderValBearer)+tokenStr)
	req2.Header.Set(string(rest.HeaderIdempotencyKey), idempKey)
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, req2)

	if w2.Code != http.StatusUnprocessableEntity { // IdempotencyMismatch -> 422
		t.Fatalf("expected 422 Unprocessable Entity for different body, got %d: %s", w2.Code, w2.Body.String())
	}
}

func TestAuth_InvalidTokens(t *testing.T) {
	handler, _, _ := setupEnvironment(t)
	body, _ := json.Marshal(&apiv1.CreateTransferRequest{FromWallet: "A", ToWallet: "B", Amount: 100, Currency: "NGN"})

	tests := []struct {
		name       string
		token      string
		wantStatus int
	}{
		{"MissingToken", "", http.StatusUnauthorized},
		{"InvalidSignature", "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.invalid", http.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, platform.APITransfersPath, bytes.NewReader(body))
			if tt.token != "" {
				req.Header.Set("Authorization", tt.token)
			}
			req.Header.Set(string(rest.HeaderIdempotencyKey), "idem123")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("expected %d, got %d", tt.wantStatus, w.Code)
			}
		})
	}
}

func TestValidation_InvalidFields(t *testing.T) {
	handler, tokenStr, _ := setupEnvironment(t)

	tests := []struct {
		name    string
		req     *apiv1.CreateTransferRequest
		wantErr string
	}{
		{"MissingFields", &apiv1.CreateTransferRequest{}, "from_wallet"},
		{"NegativeAmount", &apiv1.CreateTransferRequest{FromWallet: "A", ToWallet: "B", Amount: -100, Currency: "NGN"}, "amount"},
		{"SameWallet", &apiv1.CreateTransferRequest{FromWallet: "A", ToWallet: "A", Amount: 100, Currency: "NGN"}, "to_wallet"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, _ := json.Marshal(tt.req)
			req := httptest.NewRequest(http.MethodPost, platform.APITransfersPath, bytes.NewReader(body))
			req.Header.Set("Authorization", "Bearer "+tokenStr)
			req.Header.Set(string(rest.HeaderIdempotencyKey), "idem_val_"+tt.name)

			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)

			if w.Code != http.StatusUnprocessableEntity {
				t.Errorf("expected 422, got %d: %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestAuthz_ForeignWalletRejected(t *testing.T) {
	server, tokenStr, _ := setupEnvironment(t)
	handler := server

	// Test trying to transfer from a wallet not owned by m_123
	reqDTO := apiv1.CreateTransferRequest{
		FromWallet: "wal_foreign",
		ToWallet:   "wal_B",
		Amount:     1000,
		Currency:   "NGN",
		Reference:  "ref123",
	}
	body, _ := json.Marshal(&reqDTO)
	idempKey := "idem_authz_123"

	req := httptest.NewRequest(http.MethodPost, platform.APITransfersPath, bytes.NewReader(body))
	req.Header.Set("Authorization", string(rest.HeaderValBearer)+tokenStr)
	req.Header.Set(string(rest.HeaderIdempotencyKey), idempKey)
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Since we mock the DB, 'wal_foreign' does not exist in the merchant's shard (or doesn't belong to them)
	// Our wallet directory will return ErrWalletNotOwned, mapping to 403.
	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for foreign wallet, got %d: %s", w.Code, w.Body.String())
	}
}
