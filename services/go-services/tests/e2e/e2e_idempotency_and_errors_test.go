//go:build integration

package e2e

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	apiv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/api/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/testutil"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestIdempotencyReplayAndConflictE2E(t *testing.T) {
	cluster := testutil.SetupTestDB(t)
	_, redisURI := testutil.StartRedis(t)
	_, brokers := testutil.StartKafka(t)

	rHost, rPort, err := net.SplitHostPort(redisURI)
	if err != nil {
		t.Fatalf("failed to parse redis URI %q: %v", redisURI, err)
	}

	echoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer echoServer.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	der, _ := x509.MarshalPKCS8PrivateKey(priv)
	pemBlock := &pem.Block{Type: "PRIVATE KEY", Bytes: der}
	pemBytes := pem.EncodeToMemory(pemBlock)
	pemStr := strings.ReplaceAll(string(pemBytes), "\n", "\\n")

	coreApiBin := buildService(t, "../../core-api")
	outboxBin := buildService(t, "../../outbox-relay")
	ledgerBin := buildService(t, "../../ledger-worker")
	fraudBin := buildService(t, "../../fraud-worker")
	webhookBin := buildService(t, "../../webhook-worker")

	commonEnv := []string{
		"LOG_LEVEL=debug",
		"KAFKA_BROKERS=" + brokers[0],
		"MERCHANTS_DB_URI=" + cluster.MerchantsDB.URI,
		"SHARD_A_URI=" + cluster.ShardA.URI,
		"SHARD_B_URI=" + cluster.ShardB.URI,
		"REDIS_HOST=" + rHost,
		"REDIS_PORT=" + rPort,
		"JWT_SIGNING_KEYS=key-1:" + pemStr,
		"JWT_ACTIVE_KEY_ID=key-1",
		"SHARD_LETTERS=AB",
	}

	prefixes := []string{"CORE_API_", "OUTBOX_RELAY_", "LEDGER_WORKER_", "FRAUD_WORKER_", "WEBHOOK_WORKER_"}
	capVars := map[string]string{
		"DB_POOL_SIZE":                      "10",
		"PG_MERCHANTS_RO_MAX_CONNS":         "10",
		"PG_SHARD_A_RO_MAX_CONNS":           "10",
		"PG_SHARD_B_RO_MAX_CONNS":           "10",
		"WORKER_POOL_SIZE":                  "10",
		"CB_INTERVAL_MS":                    "10000",
		"CB_TIMEOUT_MS":                     "1000",
		"CB_MIN_REQUESTS":                   "5",
		"CB_MAX_FAILS":                      "3",
		"SCHEDULER_POLL_INTERVAL_MS":        "1000",
		"RELAY_BUFFER_SAMPLE_INTERVAL_MS":   "1000",
		"RELAY_POOL_INTERVAL_MS":            "100",
		"RELAY_BATCH_TIMEOUT_MS":            "10",
		"BREAKER_EVICTION_INTERVAL_MS":      "10000",
		"FAST_LANE_GRACE_PERIOD_MS":         "100",
		"CONSUMER_COMMIT_FLUSH_INTERVAL_MS": "1000",
		"CONSUMER_CHANNEL_REFRESH_MS":       "1000",
		"KAFKA_SESSION_MS":                  "10000",
		"KAFKA_HEARTBEAT_MS":                "3000",
		"HTTP_TIMEOUT_MS":                   "5000",
		"VELOCITY_WINDOW_MS":                "60000",
		"VELOCITY_THRESHOLD":                "10",
		"SHUTDOWN_TIMEOUT_MS":               "5000",
		"MAX_RETRIES":                       "3",
		"BACKOFF_BASE_MS":                   "10",
		"BACKOFF_CAP_MS":                    "100",
		"REQUEST_TIMEOUT_MS":                "5000",
		"SERVER_TIMEOUT_MS":                 "5000",
		"DLQ_WRITE_TIMEOUT_MS":              "5000",
		"MAX_REQUEST_BYTES":                 "1048576",
		"WEBHOOK_MAX_CONCURRENCY":           "100",
		"FAST_LANE_BUFFER_SIZE":             "100",
		"FAST_LANE_WORKER_POOL_SIZE":        "10",
		"DELIVERY_MAX_ATTEMPTS":             "3",
		"DELIVERY_BACKOFF_BASE_MS":          "10",
		"DELIVERY_BACKOFF_CAP_MS":           "100",
		"RELAY_BATCH_MSG_COUNT":             "10",
		"RELAY_MAX_PAYLOAD_BYTES":           "1048576",
		"FETCH_BATCH_SIZE":                  "100",
		"RELAY_STAGING_KB":                  "1024",
		"PG_CONN_MAX_IDLE_TIME_MS":          "60000",
		"PG_CONN_MAX_LIFETIME_MS":           "3600000",
		"HTTP_MAX_IDLE_CONNS":               "100",
		"HTTP_MAX_IDLE_PER_HOST":            "100",
		"HTTP_IDLE_CONN_TIMEOUT_MS":         "60000",
		"HTTP_RESPONSE_HEADER_TIMEOUT_MS":   "5000",
		"HTTP_TLS_HANDSHAKE_TIMEOUT_MS":     "5000",
		"HTTP_EXPECT_CONTINUE_TIMEOUT_MS":   "1000",
		"ARGON2_MEMORY_KIB":                 "65536",
		"ARGON2_ITERATIONS":                 "3",
		"ARGON2_PARALLELISM":                "4",
		"JWT_ACCESS_HRS":                    "24",
	}

	for _, pfx := range prefixes {
		for k, v := range capVars {
			commonEnv = append(commonEnv, pfx+k+"="+v)
		}
	}
	for k, v := range capVars {
		commonEnv = append(commonEnv, k+"="+v)
	}

	_ = startService(t, outboxBin, append(commonEnv, "OUTBOX_RELAY_SHARD_ID=shard-a"))
	_ = startService(t, outboxBin, append(commonEnv, "OUTBOX_RELAY_SHARD_ID=shard-b"))
	_ = startService(t, ledgerBin, commonEnv)
	_ = startService(t, fraudBin, commonEnv)
	_ = startService(t, webhookBin, commonEnv)

	coreApiPort := 8083
	_ = startService(t, coreApiBin, append(commonEnv, fmt.Sprintf("HTTP_PORT=%d", coreApiPort)))

	time.Sleep(3 * time.Second)

	apiURL := fmt.Sprintf("http://localhost:%d", coreApiPort)

	// Create Merchant
	mReq := &apiv1.CreateMerchantRequest{
		Name:          "Idempotency Test Merchant",
		WebhookUrl:    echoServer.URL,
		WebhookSecret: "whsec_idem_test",
		Tier:          "standard",
	}
	mReqBody, _ := protojson.Marshal(mReq)
	mRespRaw, err := http.Post(apiURL+"/v1/merchants", "application/json", bytes.NewBuffer(mReqBody))
	if err != nil || mRespRaw.StatusCode != http.StatusCreated {
		t.Fatalf("failed to create merchant: err=%v code=%d", err, mRespRaw.StatusCode)
	}
	var mResp apiv1.CreateMerchantResponse
	mRespBytes, _ := io.ReadAll(mRespRaw.Body)
	protojson.Unmarshal(mRespBytes, &mResp)
	mRespRaw.Body.Close()

	// Get Token
	tReq, _ := http.NewRequest(http.MethodPost, apiURL+"/v1/auth/token", nil)
	tReq.Header.Set("Authorization", "Bearer "+mResp.ApiKey)
	tRespRaw, _ := http.DefaultClient.Do(tReq)
	var tokResp apiv1.AuthTokenResponse
	tokBytes, _ := io.ReadAll(tRespRaw.Body)
	protojson.Unmarshal(tokBytes, &tokResp)
	tRespRaw.Body.Close()
	jwt := tokResp.Token

	// Helper for creating wallets
	createWallet := func() string {
		wReq := &apiv1.CreateWalletRequest{Currency: "NGN"}
		wReqBody, _ := protojson.Marshal(wReq)
		req, _ := http.NewRequest(http.MethodPost, apiURL+"/v1/wallets", bytes.NewBuffer(wReqBody))
		req.Header.Set("Authorization", "Bearer "+jwt)
		req.Header.Set("X-Merchant-ID", mResp.MerchantId)
		req.Header.Set("X-Merchant-Tier", "standard")
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != http.StatusCreated {
			t.Fatalf("failed to create wallet: err=%v code=%d", err, resp.StatusCode)
		}
		var wResp apiv1.CreateWalletResponse
		wRespBody, _ := io.ReadAll(resp.Body)
		protojson.Unmarshal(wRespBody, &wResp)
		resp.Body.Close()
		return wResp.WalletId
	}

	wallet1 := createWallet()
	wallet2 := createWallet()

	// Step 1: Deposit funds into Wallet 1
	depPayload := map[string]interface{}{
		"from_wallet": "",
		"to_wallet":   wallet1,
		"amount":      100000,
		"currency":    "NGN",
	}
	depBody, _ := json.Marshal(depPayload)
	depReq, _ := http.NewRequest(http.MethodPost, apiURL+"/v1/transfers", bytes.NewBuffer(depBody))
	depReq.Header.Set("Authorization", "Bearer "+jwt)
	depReq.Header.Set("X-Merchant-ID", mResp.MerchantId)
	depReq.Header.Set("X-Merchant-Tier", "standard")
	depReq.Header.Set("X-Idempotency-Key", "dep_idem_init")
	depReq.Header.Set("Content-Type", "application/json")
	depResp, _ := http.DefaultClient.Do(depReq)
	var depJobResp apiv1.CreateTransferResponse
	depRespBytes, _ := io.ReadAll(depResp.Body)
	protojson.Unmarshal(depRespBytes, &depJobResp)
	depResp.Body.Close()

	// Wait for deposit to complete
	for i := 0; i < 30; i++ {
		time.Sleep(300 * time.Millisecond)
		jReq, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/v1/jobs/%s", apiURL, depJobResp.JobId), nil)
		jReq.Header.Set("Authorization", "Bearer "+jwt)
		jReq.Header.Set("X-Merchant-ID", mResp.MerchantId)
		jReq.Header.Set("X-Merchant-Tier", "standard")
		jResp, _ := http.DefaultClient.Do(jReq)
		var jData apiv1.GetJobResponse
		jBytes, _ := io.ReadAll(jResp.Body)
		protojson.Unmarshal(jBytes, &jData)
		jResp.Body.Close()
		if jData.Status == "completed" {
			break
		}
	}

	// Step 2: First transfer submission with key "idem_unique_key_001"
	idemKey := "idem_unique_key_001"
	txPayload1 := map[string]interface{}{
		"from_wallet": wallet1,
		"to_wallet":   wallet2,
		"amount":      25000,
		"currency":    "NGN",
	}
	txBody1, _ := json.Marshal(txPayload1)
	txReq1, _ := http.NewRequest(http.MethodPost, apiURL+"/v1/transfers", bytes.NewBuffer(txBody1))
	txReq1.Header.Set("Authorization", "Bearer "+jwt)
	txReq1.Header.Set("X-Merchant-ID", mResp.MerchantId)
	txReq1.Header.Set("X-Merchant-Tier", "standard")
	txReq1.Header.Set("X-Idempotency-Key", idemKey)
	txReq1.Header.Set("Content-Type", "application/json")
	txResp1, err := http.DefaultClient.Do(txReq1)
	if err != nil || txResp1.StatusCode != http.StatusAccepted {
		t.Fatalf("first transfer failed: err=%v status=%d", err, txResp1.StatusCode)
	}
	var txJobResp1 apiv1.CreateTransferResponse
	txRespBytes1, _ := io.ReadAll(txResp1.Body)
	protojson.Unmarshal(txRespBytes1, &txJobResp1)
	txResp1.Body.Close()

	// Wait for transfer to complete
	for i := 0; i < 30; i++ {
		time.Sleep(300 * time.Millisecond)
		jReq, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/v1/jobs/%s", apiURL, txJobResp1.JobId), nil)
		jReq.Header.Set("Authorization", "Bearer "+jwt)
		jReq.Header.Set("X-Merchant-ID", mResp.MerchantId)
		jReq.Header.Set("X-Merchant-Tier", "standard")
		jResp, _ := http.DefaultClient.Do(jReq)
		var jData apiv1.GetJobResponse
		jBytes, _ := io.ReadAll(jResp.Body)
		protojson.Unmarshal(jBytes, &jData)
		jResp.Body.Close()
		if jData.Status == "completed" {
			break
		}
	}

	// Step 3: Exact same transfer submission with key "idem_unique_key_001" (Idempotent replay)
	txReq2, _ := http.NewRequest(http.MethodPost, apiURL+"/v1/transfers", bytes.NewBuffer(txBody1))
	txReq2.Header.Set("Authorization", "Bearer "+jwt)
	txReq2.Header.Set("X-Merchant-ID", mResp.MerchantId)
	txReq2.Header.Set("X-Merchant-Tier", "standard")
	txReq2.Header.Set("X-Idempotency-Key", idemKey)
	txReq2.Header.Set("Content-Type", "application/json")
	txResp2, err := http.DefaultClient.Do(txReq2)
	if err != nil || (txResp2.StatusCode != http.StatusOK && txResp2.StatusCode != http.StatusAccepted) {
		t.Fatalf("idempotent replay failed: err=%v status=%d", err, txResp2.StatusCode)
	}
	var txJobResp2 apiv1.CreateTransferResponse
	txRespBytes2, _ := io.ReadAll(txResp2.Body)
	protojson.Unmarshal(txRespBytes2, &txJobResp2)
	txResp2.Body.Close()

	if txJobResp2.JobId != txJobResp1.JobId {
		t.Fatalf("expected identical job ID on replay: original=%s replay=%s", txJobResp1.JobId, txJobResp2.JobId)
	}
	t.Logf("Idempotency replay verified successfully! Job ID: %s", txJobResp2.JobId)

	// Step 4: Conflicting transfer with DIFFERENT payload but SAME idempotency key -> MUST return 409 Conflict
	txPayloadConflict := map[string]interface{}{
		"from_wallet": wallet1,
		"to_wallet":   wallet2,
		"amount":      99999, // DIFFERENT AMOUNT
		"currency":    "NGN",
	}
	txBodyConflict, _ := json.Marshal(txPayloadConflict)
	txReqConflict, _ := http.NewRequest(http.MethodPost, apiURL+"/v1/transfers", bytes.NewBuffer(txBodyConflict))
	txReqConflict.Header.Set("Authorization", "Bearer "+jwt)
	txReqConflict.Header.Set("X-Merchant-ID", mResp.MerchantId)
	txReqConflict.Header.Set("X-Merchant-Tier", "standard")
	txReqConflict.Header.Set("X-Idempotency-Key", idemKey) // SAME KEY
	txReqConflict.Header.Set("Content-Type", "application/json")
	txRespConflict, err := http.DefaultClient.Do(txReqConflict)
	if err != nil || txRespConflict.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 Unprocessable Entity on idempotency mismatch, got status=%d err=%v", txRespConflict.StatusCode, err)
	}
	var errResp apiv1.ErrorResponse
	errRespBytes, _ := io.ReadAll(txRespConflict.Body)
	protojson.Unmarshal(errRespBytes, &errResp)
	txRespConflict.Body.Close()
	if errResp.Error != "IDEMPOTENCY_KEY_REUSED_WITH_DIFFERENT_BODY" {
		t.Fatalf("expected error code IDEMPOTENCY_KEY_REUSED_WITH_DIFFERENT_BODY, got %s", errResp.Error)
	}
	t.Log("Idempotency mismatch correctly rejected with 422 IDEMPOTENCY_KEY_REUSED_WITH_DIFFERENT_BODY!")

	// Step 5: Verify final balance -> Only 1 transfer of 25,000 was executed
	bReq1, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/v1/balances?wallet_id=%s", apiURL, wallet1), nil)
	bReq1.Header.Set("Authorization", "Bearer "+jwt)
	bReq1.Header.Set("X-Merchant-ID", mResp.MerchantId)
	bReq1.Header.Set("X-Merchant-Tier", "standard")
	bResp1, _ := http.DefaultClient.Do(bReq1)
	var bData1 apiv1.GetBalanceResponse
	bBytes1, _ := io.ReadAll(bResp1.Body)
	protojson.Unmarshal(bBytes1, &bData1)
	bResp1.Body.Close()
	if bData1.Balance != 75000 {
		t.Fatalf("expected wallet1 balance 75000 (100000 - 25000), got %d", bData1.Balance)
	}

	bReq2, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/v1/balances?wallet_id=%s", apiURL, wallet2), nil)
	bReq2.Header.Set("Authorization", "Bearer "+jwt)
	bReq2.Header.Set("X-Merchant-ID", mResp.MerchantId)
	bReq2.Header.Set("X-Merchant-Tier", "standard")
	bResp2, _ := http.DefaultClient.Do(bReq2)
	var bData2 apiv1.GetBalanceResponse
	bBytes2, _ := io.ReadAll(bResp2.Body)
	protojson.Unmarshal(bBytes2, &bData2)
	bResp2.Body.Close()
	if bData2.Balance != 25000 {
		t.Fatalf("expected wallet2 balance 25000, got %d", bData2.Balance)
	}

	// Step 6: Verify 0 DLQ entries
	var dlqCount int64
	_ = cluster.MerchantsDB.Pool.QueryRow(context.Background(), "SELECT count(*) FROM dlq_entries").Scan(&dlqCount)
	if dlqCount != 0 {
		t.Fatalf("expected 0 DLQ entries, got %d", dlqCount)
	}

	t.Log("Idempotency replay and conflict test passed 100%!")
}

func TestInsufficientBalanceDeclineE2E(t *testing.T) {
	cluster := testutil.SetupTestDB(t)
	_, redisURI := testutil.StartRedis(t)
	_, brokers := testutil.StartKafka(t)

	rHost, rPort, _ := net.SplitHostPort(redisURI)

	echoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer echoServer.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	der, _ := x509.MarshalPKCS8PrivateKey(priv)
	pemBlock := &pem.Block{Type: "PRIVATE KEY", Bytes: der}
	pemBytes := pem.EncodeToMemory(pemBlock)
	pemStr := strings.ReplaceAll(string(pemBytes), "\n", "\\n")

	coreApiBin := buildService(t, "../../core-api")
	outboxBin := buildService(t, "../../outbox-relay")
	ledgerBin := buildService(t, "../../ledger-worker")
	fraudBin := buildService(t, "../../fraud-worker")
	webhookBin := buildService(t, "../../webhook-worker")

	commonEnv := []string{
		"LOG_LEVEL=debug",
		"KAFKA_BROKERS=" + brokers[0],
		"MERCHANTS_DB_URI=" + cluster.MerchantsDB.URI,
		"SHARD_A_URI=" + cluster.ShardA.URI,
		"SHARD_B_URI=" + cluster.ShardB.URI,
		"REDIS_HOST=" + rHost,
		"REDIS_PORT=" + rPort,
		"JWT_SIGNING_KEYS=key-1:" + pemStr,
		"JWT_ACTIVE_KEY_ID=key-1",
		"SHARD_LETTERS=AB",
	}

	prefixes := []string{"CORE_API_", "OUTBOX_RELAY_", "LEDGER_WORKER_", "FRAUD_WORKER_", "WEBHOOK_WORKER_"}
	capVars := map[string]string{
		"DB_POOL_SIZE":                      "10",
		"PG_MERCHANTS_RO_MAX_CONNS":         "10",
		"PG_SHARD_A_RO_MAX_CONNS":           "10",
		"PG_SHARD_B_RO_MAX_CONNS":           "10",
		"WORKER_POOL_SIZE":                  "10",
		"CB_INTERVAL_MS":                    "10000",
		"CB_TIMEOUT_MS":                     "1000",
		"CB_MIN_REQUESTS":                   "5",
		"CB_MAX_FAILS":                      "3",
		"SCHEDULER_POLL_INTERVAL_MS":        "1000",
		"RELAY_BUFFER_SAMPLE_INTERVAL_MS":   "1000",
		"RELAY_POOL_INTERVAL_MS":            "100",
		"RELAY_BATCH_TIMEOUT_MS":            "10",
		"BREAKER_EVICTION_INTERVAL_MS":      "10000",
		"FAST_LANE_GRACE_PERIOD_MS":         "100",
		"CONSUMER_COMMIT_FLUSH_INTERVAL_MS": "1000",
		"CONSUMER_CHANNEL_REFRESH_MS":       "1000",
		"KAFKA_SESSION_MS":                  "10000",
		"KAFKA_HEARTBEAT_MS":                "3000",
		"HTTP_TIMEOUT_MS":                   "5000",
		"VELOCITY_WINDOW_MS":                "60000",
		"VELOCITY_THRESHOLD":                "10",
		"SHUTDOWN_TIMEOUT_MS":               "5000",
		"MAX_RETRIES":                       "3",
		"BACKOFF_BASE_MS":                   "10",
		"BACKOFF_CAP_MS":                    "100",
		"REQUEST_TIMEOUT_MS":                "5000",
		"SERVER_TIMEOUT_MS":                 "5000",
		"DLQ_WRITE_TIMEOUT_MS":              "5000",
		"MAX_REQUEST_BYTES":                 "1048576",
		"WEBHOOK_MAX_CONCURRENCY":           "100",
		"FAST_LANE_BUFFER_SIZE":             "100",
		"FAST_LANE_WORKER_POOL_SIZE":        "10",
		"DELIVERY_MAX_ATTEMPTS":             "3",
		"DELIVERY_BACKOFF_BASE_MS":          "10",
		"DELIVERY_BACKOFF_CAP_MS":           "100",
		"RELAY_BATCH_MSG_COUNT":             "10",
		"RELAY_MAX_PAYLOAD_BYTES":           "1048576",
		"FETCH_BATCH_SIZE":                  "100",
		"RELAY_STAGING_KB":                  "1024",
		"PG_CONN_MAX_IDLE_TIME_MS":          "60000",
		"PG_CONN_MAX_LIFETIME_MS":           "3600000",
		"HTTP_MAX_IDLE_CONNS":               "100",
		"HTTP_MAX_IDLE_PER_HOST":            "100",
		"HTTP_IDLE_CONN_TIMEOUT_MS":         "60000",
		"HTTP_RESPONSE_HEADER_TIMEOUT_MS":   "5000",
		"HTTP_TLS_HANDSHAKE_TIMEOUT_MS":     "5000",
		"HTTP_EXPECT_CONTINUE_TIMEOUT_MS":   "1000",
		"ARGON2_MEMORY_KIB":                 "65536",
		"ARGON2_ITERATIONS":                 "3",
		"ARGON2_PARALLELISM":                "4",
		"JWT_ACCESS_HRS":                    "24",
	}

	for _, pfx := range prefixes {
		for k, v := range capVars {
			commonEnv = append(commonEnv, pfx+k+"="+v)
		}
	}
	for k, v := range capVars {
		commonEnv = append(commonEnv, k+"="+v)
	}

	_ = startService(t, outboxBin, append(commonEnv, "OUTBOX_RELAY_SHARD_ID=shard-a"))
	_ = startService(t, outboxBin, append(commonEnv, "OUTBOX_RELAY_SHARD_ID=shard-b"))
	_ = startService(t, ledgerBin, commonEnv)
	_ = startService(t, fraudBin, commonEnv)
	_ = startService(t, webhookBin, commonEnv)

	coreApiPort := 8084
	_ = startService(t, coreApiBin, append(commonEnv, fmt.Sprintf("HTTP_PORT=%d", coreApiPort)))

	time.Sleep(3 * time.Second)

	apiURL := fmt.Sprintf("http://localhost:%d", coreApiPort)

	// Create Merchant
	mReq := &apiv1.CreateMerchantRequest{
		Name:          "Decline Test Merchant",
		WebhookUrl:    echoServer.URL,
		WebhookSecret: "whsec_decline_test",
		Tier:          "standard",
	}
	mReqBody, _ := protojson.Marshal(mReq)
	mRespRaw, _ := http.Post(apiURL+"/v1/merchants", "application/json", bytes.NewBuffer(mReqBody))
	var mResp apiv1.CreateMerchantResponse
	mRespBytes, _ := io.ReadAll(mRespRaw.Body)
	protojson.Unmarshal(mRespBytes, &mResp)
	mRespRaw.Body.Close()

	// Get Token
	tReq, _ := http.NewRequest(http.MethodPost, apiURL+"/v1/auth/token", nil)
	tReq.Header.Set("Authorization", "Bearer "+mResp.ApiKey)
	tRespRaw, _ := http.DefaultClient.Do(tReq)
	var tokResp apiv1.AuthTokenResponse
	tokBytes, _ := io.ReadAll(tRespRaw.Body)
	protojson.Unmarshal(tokBytes, &tokResp)
	tRespRaw.Body.Close()
	jwt := tokResp.Token

	// Create Wallets
	createWallet := func() string {
		wReq := &apiv1.CreateWalletRequest{Currency: "NGN"}
		wReqBody, _ := protojson.Marshal(wReq)
		req, _ := http.NewRequest(http.MethodPost, apiURL+"/v1/wallets", bytes.NewBuffer(wReqBody))
		req.Header.Set("Authorization", "Bearer "+jwt)
		req.Header.Set("X-Merchant-ID", mResp.MerchantId)
		req.Header.Set("X-Merchant-Tier", "standard")
		req.Header.Set("Content-Type", "application/json")
		resp, _ := http.DefaultClient.Do(req)
		var wResp apiv1.CreateWalletResponse
		wRespBody, _ := io.ReadAll(resp.Body)
		protojson.Unmarshal(wRespBody, &wResp)
		resp.Body.Close()
		return wResp.WalletId
	}

	wallet1 := createWallet()
	wallet2 := createWallet()

	// Deposit small initial amount (1,000 NGN)
	depPayload := map[string]interface{}{
		"from_wallet": "",
		"to_wallet":   wallet1,
		"amount":      1000,
		"currency":    "NGN",
	}
	depBody, _ := json.Marshal(depPayload)
	depReq, _ := http.NewRequest(http.MethodPost, apiURL+"/v1/transfers", bytes.NewBuffer(depBody))
	depReq.Header.Set("Authorization", "Bearer "+jwt)
	depReq.Header.Set("X-Merchant-ID", mResp.MerchantId)
	depReq.Header.Set("X-Merchant-Tier", "standard")
	depReq.Header.Set("X-Idempotency-Key", "dep_decline_init")
	depReq.Header.Set("Content-Type", "application/json")
	depResp, _ := http.DefaultClient.Do(depReq)
	var depJobResp apiv1.CreateTransferResponse
	depRespBytes, _ := io.ReadAll(depResp.Body)
	protojson.Unmarshal(depRespBytes, &depJobResp)
	depResp.Body.Close()

	for i := 0; i < 30; i++ {
		time.Sleep(300 * time.Millisecond)
		jReq, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/v1/jobs/%s", apiURL, depJobResp.JobId), nil)
		jReq.Header.Set("Authorization", "Bearer "+jwt)
		jReq.Header.Set("X-Merchant-ID", mResp.MerchantId)
		jReq.Header.Set("X-Merchant-Tier", "standard")
		jResp, _ := http.DefaultClient.Do(jReq)
		var jData apiv1.GetJobResponse
		jBytes, _ := io.ReadAll(jResp.Body)
		protojson.Unmarshal(jBytes, &jData)
		jResp.Body.Close()
		if jData.Status == "completed" {
			break
		}
	}

	// Attempt transfer of 50,000 NGN (which exceeds balance of 1,000 NGN)
	txPayload := map[string]interface{}{
		"from_wallet": wallet1,
		"to_wallet":   wallet2,
		"amount":      50000,
		"currency":    "NGN",
	}
	txBody, _ := json.Marshal(txPayload)
	txReq, _ := http.NewRequest(http.MethodPost, apiURL+"/v1/transfers", bytes.NewBuffer(txBody))
	txReq.Header.Set("Authorization", "Bearer "+jwt)
	txReq.Header.Set("X-Merchant-ID", mResp.MerchantId)
	txReq.Header.Set("X-Merchant-Tier", "standard")
	txReq.Header.Set("X-Idempotency-Key", "tx_overdraft_attempt")
	txReq.Header.Set("Content-Type", "application/json")
	txResp, err := http.DefaultClient.Do(txReq)
	if err != nil || txResp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected transfer accepted for async processing: err=%v status=%d", err, txResp.StatusCode)
	}
	var txJobResp apiv1.CreateTransferResponse
	txRespBytes, _ := io.ReadAll(txResp.Body)
	protojson.Unmarshal(txRespBytes, &txJobResp)
	txResp.Body.Close()

	// Poll job until status becomes "failed"
	var finalJobStatus string
	for i := 0; i < 30; i++ {
		time.Sleep(300 * time.Millisecond)
		jReq, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/v1/jobs/%s", apiURL, txJobResp.JobId), nil)
		jReq.Header.Set("Authorization", "Bearer "+jwt)
		jReq.Header.Set("X-Merchant-ID", mResp.MerchantId)
		jReq.Header.Set("X-Merchant-Tier", "standard")
		jResp, jErr := http.DefaultClient.Do(jReq)
		if jErr == nil && jResp.StatusCode == http.StatusOK {
			var jData apiv1.GetJobResponse
			jBytes, _ := io.ReadAll(jResp.Body)
			protojson.Unmarshal(jBytes, &jData)
			jResp.Body.Close()
			finalJobStatus = jData.Status
			if finalJobStatus == "failed" {
				t.Logf("Job correctly marked failed: %s (reason=%v)", jData.JobId, jData.Failure)
				break
			}
		}
	}

	if finalJobStatus != "failed" {
		t.Fatalf("expected job status 'failed' due to overdraft, got '%s'", finalJobStatus)
	}

	// Verify balance of Wallet 1 is untouched (still 1,000 NGN)
	bReq, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/v1/balances?wallet_id=%s", apiURL, wallet1), nil)
	bReq.Header.Set("Authorization", "Bearer "+jwt)
	bReq.Header.Set("X-Merchant-ID", mResp.MerchantId)
	bReq.Header.Set("X-Merchant-Tier", "standard")
	bResp, _ := http.DefaultClient.Do(bReq)
	var bData apiv1.GetBalanceResponse
	bBytes, _ := io.ReadAll(bResp.Body)
	protojson.Unmarshal(bBytes, &bData)
	bResp.Body.Close()
	if bData.Balance != 1000 {
		t.Fatalf("expected wallet1 balance to remain 1000, got %d", bData.Balance)
	}

	// Verify 0 DLQ entries (clean business failure, not a poison pill or crash)
	var dlqCount int64
	_ = cluster.MerchantsDB.Pool.QueryRow(context.Background(), "SELECT count(*) FROM dlq_entries").Scan(&dlqCount)
	if dlqCount != 0 {
		t.Fatalf("expected 0 DLQ entries on overdraft decline, got %d", dlqCount)
	}

	t.Log("Insufficient balance decline handled cleanly end-to-end!")
}

