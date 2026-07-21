//go:build integration

package rest_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/adapter/inbound/rest"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/adapter/outbound/postgres"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/app"

	apiv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/api/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/testutil"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
)

// setupEnvironment creates the real HTTP handler backed by testcontainers DBs.
func setupEnvironment(t *testing.T) (http.Handler, testutil.TestDB, string) {
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

	// Use the shared testutil component to provision a merchant and wallets
	merchantID := testutil.SeedMerchantAndWallets(t, merchantsDB, shardA)

	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate Ed25519 key: %v", err)
	}

	mDir := postgres.NewMerchantDirectory(pools)
	wDir := postgres.NewWalletDirectory(pools)
	jobsStore := postgres.NewJobStore(pools)
	transferSvc := app.NewTransferService(mDir, wDir, jobsStore, platform.NewJobID)
	merchantSvc := app.NewMerchantService(mDir, pools.HashRing())
	walletSvc := app.NewWalletService(mDir, wDir, wDir, jobsStore, platform.NewJobID)
	adminSvc := app.NewAdminService(postgres.NewDLQReplayer(pools, nil, log))
	jobSvc := app.NewJobService(mDir, jobsStore)

	// Set up the Server with our mocked/ephemeral dependencies
	server := rest.NewServer(
		8080,
		map[string]ed25519.PrivateKey{"default": privateKey},
		"default",
		transferSvc,
		jobSvc,
		merchantSvc,
		walletSvc,
		adminSvc,
		"admin_secret",
		func(ctx context.Context) error { return nil },
		zap.NewNop(),
	)

	return server, shardA, merchantID
}

func TestTransferHandler_Submit_IdempotencyConcurrentDuplicates(t *testing.T) {
	t.Parallel()
	handler, shardA, mID := setupEnvironment(t)

	walA := ".01905335-9781-7000-8000-000000000001"
	walB := ".01905335-9781-7000-8000-000000000002"
	reqDTO := apiv1.CreateTransferRequest{
		FromWallet: mID + walA,
		ToWallet:   mID + walB,
		Amount:     1000,
		Currency:   "NGN",
		Reference:  "ref123",
	}
	body, _ := protojson.Marshal(&reqDTO)
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
			req.Header.Set(rest.HeaderMerchantID, mID)
			req.Header.Set(string(rest.HeaderIdempotencyKey), idempKey)
			req.Header.Set(string(rest.ContentType), string(rest.ApplicationJSON))

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
			protojson.Unmarshal(res.Body.Bytes(), &resp)
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
	err := shardA.Pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM jobs WHERE merchant_id = $1 AND idempotency_key = $2`, mID, idempKey).Scan(&count)
	if err != nil || count != 1 {
		t.Errorf("expected exactly 1 job in DB, got %d (err: %v)", count, err)
	}

	// 3. Exactly one row in outbox events table
	err = shardA.Pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM events WHERE aggregate_type = $2 AND aggregate_id = $1`, capturedJobID, platform.AggregateTypeJob).Scan(&count)
	if err != nil || count != 1 {
		t.Errorf("expected exactly 1 outbox event in DB, got %d (err: %v)", count, err)
	}
}

func TestTransferHandler_Submit_IdempotencyDifferentBody(t *testing.T) {
	t.Parallel()
	handler, _, mID := setupEnvironment(t)

	walA := ".01905335-9781-7000-8000-000000000001"
	walB := ".01905335-9781-7000-8000-000000000002"
	reqDTO1 := apiv1.CreateTransferRequest{FromWallet: mID + walA, ToWallet: mID + walB, Amount: 1000, Currency: "NGN"}
	reqDTO2 := apiv1.CreateTransferRequest{FromWallet: mID + walA, ToWallet: mID + walB, Amount: 5000, Currency: "NGN"}

	body1, _ := protojson.Marshal(&reqDTO1)
	body2, _ := protojson.Marshal(&reqDTO2)
	idempKey := "idem_diff_" + mID

	// First Request
	req1 := httptest.NewRequest(http.MethodPost, platform.APITransfersPath, bytes.NewReader(body1))
	req1.Header.Set(rest.HeaderMerchantID, mID)
	req1.Header.Set(string(rest.HeaderIdempotencyKey), idempKey)
	w1 := httptest.NewRecorder()
	handler.ServeHTTP(w1, req1)

	if w1.Code != http.StatusAccepted {
		t.Fatalf("first request failed, got %d: %s", w1.Code, w1.Body.String())
	}

	// Second Request with SAME key but DIFFERENT body
	req2 := httptest.NewRequest(http.MethodPost, platform.APITransfersPath, bytes.NewReader(body2))
	req2.Header.Set(rest.HeaderMerchantID, mID)
	req2.Header.Set(string(rest.HeaderIdempotencyKey), idempKey)
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, req2)

	if w2.Code != http.StatusUnprocessableEntity { // IdempotencyMismatch -> 422
		t.Fatalf("expected 422 Unprocessable Entity for different body, got %d: %s", w2.Code, w2.Body.String())
	}
}

func TestTransferHandler_Submit_AuthInvalidTokens(t *testing.T) {
	t.Parallel()
	handler, _, mID := setupEnvironment(t)
	walA := ".01905335-9781-7000-8000-000000000001"
	walB := ".01905335-9781-7000-8000-000000000002"
	body, _ := protojson.Marshal(&apiv1.CreateTransferRequest{FromWallet: mID + walA, ToWallet: mID + walB, Amount: 100, Currency: "NGN"})

	tests := []struct {
		name       string
		username   string
		wantStatus int
	}{
		{"MissingUsername", "", http.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, platform.APITransfersPath, bytes.NewReader(body))
			if tt.username != "" {
				req.Header.Set(rest.HeaderKongConsumerCustomID, tt.username)
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

func TestTransferHandler_Submit_ValidationInvalidFields(t *testing.T) {
	t.Parallel()
	handler, _, mID := setupEnvironment(t)

	walA := ".01905335-9781-7000-8000-000000000001"
	walB := ".01905335-9781-7000-8000-000000000002"

	tests := []struct {
		name    string
		req     *apiv1.CreateTransferRequest
		wantErr string
	}{
		{"MissingFields", &apiv1.CreateTransferRequest{}, "from_wallet"},
		{"NegativeAmount", &apiv1.CreateTransferRequest{FromWallet: mID + walA, ToWallet: mID + walB, Amount: -100, Currency: "NGN"}, "amount"},
		{"SameWallet", &apiv1.CreateTransferRequest{FromWallet: mID + walA, ToWallet: mID + walA, Amount: 100, Currency: "NGN"}, "to_wallet"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, _ := protojson.Marshal(tt.req)
			req := httptest.NewRequest(http.MethodPost, platform.APITransfersPath, bytes.NewReader(body))
			req.Header.Set(rest.HeaderMerchantID, mID)
			req.Header.Set(string(rest.HeaderIdempotencyKey), "idem_val_"+tt.name)

			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)

			if w.Code != http.StatusUnprocessableEntity {
				t.Errorf("expected 422, got %d: %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestTransferHandler_Submit_AuthzForeignWalletRejected(t *testing.T) {
	t.Parallel()
	server, _, mID := setupEnvironment(t)
	handler := server

	walB := ".01905335-9781-7000-8000-000000000002"
	foreignM := "merchant_01905335-9781-7000-8000-000000000003"
	foreignWal := ".01905335-9781-7000-8000-000000000004"

	// Test trying to transfer from a wallet not owned by the merchant
	reqDTO := apiv1.CreateTransferRequest{
		FromWallet: foreignM + foreignWal,
		ToWallet:   mID + walB,
		Amount:     1000,
		Currency:   "NGN",
		Reference:  "ref123",
	}
	body, _ := protojson.Marshal(&reqDTO)
	idempKey := "idem_authz_" + mID

	req := httptest.NewRequest(http.MethodPost, platform.APITransfersPath, bytes.NewReader(body))
	req.Header.Set(rest.HeaderMerchantID, mID)
	req.Header.Set(string(rest.HeaderIdempotencyKey), idempKey)
	req.Header.Set(string(rest.ContentType), string(rest.ApplicationJSON))

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Since we mock the DB, 'm_999.wal_foreign' does not exist in the merchant's shard (or doesn't belong to them)
	// Our wallet directory will return ErrWalletNotOwned, mapping to 403 Forbidden.
	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for foreign wallet, got %d: %s", w.Code, w.Body.String())
	}
}

func TestTransferHandler_Submit_ValidationOversizedPayload(t *testing.T) {
	t.Parallel()
	handler, _, mID := setupEnvironment(t)

	// 1. Generate a 2MB JSON payload (larger than domain.MaxRequestBytes which is typically 1MB)
	largeBody := make([]byte, 2*1024*1024)
	req := httptest.NewRequest(http.MethodPost, platform.APITransfersPath, bytes.NewReader(largeBody))
	req.Header.Set(rest.HeaderKongConsumerCustomID, mID)
	req.Header.Set(string(rest.HeaderIdempotencyKey), "idem_oversized")
	req.Header.Set(string(rest.ContentType), string(rest.ApplicationJSON))

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// 2. Assert response is HTTP 413 Payload Too Large
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 Payload Too Large, got %d: %s", w.Code, w.Body.String())
	}
}
