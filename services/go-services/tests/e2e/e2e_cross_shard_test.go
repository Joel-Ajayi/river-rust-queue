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

func TestCrossShardTransferE2E(t *testing.T) {
	// 1. Start Containers
	cluster := testutil.SetupTestDB(t)
	redisClient, redisURI := testutil.StartRedis(t)
	_, brokers := testutil.StartKafka(t)

	rHost, rPort, err := net.SplitHostPort(redisURI)
	if err != nil {
		t.Fatalf("failed to parse redis URI %q: %v", redisURI, err)
	}

	webhookReceived := make(chan []byte, 10)
	echoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") == "application/json" {
			body, _ := io.ReadAll(r.Body)
			select {
			case webhookReceived <- body:
			default:
			}
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer echoServer.Close()

	// 2. Prepare Environment Variables
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

	coreApiPort := 8082
	_ = startService(t, coreApiBin, append(commonEnv, fmt.Sprintf("HTTP_PORT=%d", coreApiPort)))

	time.Sleep(3 * time.Second) // Wait for services to boot

	apiURL := fmt.Sprintf("http://localhost:%d", coreApiPort)

	// Helper to create a merchant and ensure we get one on shard-a and one on shard-b
	type testMerchant struct {
		id      string
		apiKey  string
		shardID string
		jwt     string
	}

	createMerchant := func(name string) testMerchant {
		mReq := &apiv1.CreateMerchantRequest{
			Name:          name,
			WebhookUrl:    echoServer.URL,
			WebhookSecret: "whsec_cross_shard_test",
			Tier:          "standard",
		}
		mReqBody, _ := protojson.Marshal(mReq)
		resp, err := http.Post(apiURL+"/v1/merchants", "application/json", bytes.NewBuffer(mReqBody))
		if err != nil || resp.StatusCode != http.StatusCreated {
			t.Fatalf("failed to create merchant %s: err=%v code=%d", name, err, resp.StatusCode)
		}
		var mResp apiv1.CreateMerchantResponse
		mRespBody, _ := io.ReadAll(resp.Body)
		protojson.Unmarshal(mRespBody, &mResp)
		resp.Body.Close()

		// Get JWT Token
		tReq, _ := http.NewRequest(http.MethodPost, apiURL+"/v1/auth/token", nil)
		tReq.Header.Set("Authorization", "Bearer "+mResp.ApiKey)
		tResp, err := http.DefaultClient.Do(tReq)
		if err != nil || tResp.StatusCode != http.StatusOK {
			t.Fatalf("failed to get token for %s: err=%v code=%d", name, err, tResp.StatusCode)
		}
		var tokResp apiv1.AuthTokenResponse
		tokBody, _ := io.ReadAll(tResp.Body)
		protojson.Unmarshal(tokBody, &tokResp)
		tResp.Body.Close()

		return testMerchant{
			id:      mResp.MerchantId,
			apiKey:  mResp.ApiKey,
			shardID: mResp.ShardId,
			jwt:     tokResp.Token,
		}
	}

	// Create merchants until we have one on shard-a and one on shard-b
	var merchantA, merchantB testMerchant
	for i := 0; i < 20; i++ {
		m := createMerchant(fmt.Sprintf("Cross Shard Merchant %d", i))
		if m.shardID == "shard-a" && merchantA.id == "" {
			merchantA = m
		} else if m.shardID == "shard-b" && merchantB.id == "" {
			merchantB = m
		}
		if merchantA.id != "" && merchantB.id != "" {
			break
		}
	}

	if merchantA.id == "" || merchantB.id == "" {
		t.Fatalf("failed to provision merchants across both shards: shardA=%v shardB=%v", merchantA.id, merchantB.id)
	}

	t.Logf("Cross-shard setup: MerchantA=%s (%s), MerchantB=%s (%s)",
		merchantA.id, merchantA.shardID, merchantB.id, merchantB.shardID)

	// Helper to create a wallet
	createWallet := func(m testMerchant) string {
		wReq := &apiv1.CreateWalletRequest{
			Currency: "NGN",
		}
		wReqBody, _ := protojson.Marshal(wReq)
		req, _ := http.NewRequest(http.MethodPost, apiURL+"/v1/wallets", bytes.NewBuffer(wReqBody))
		req.Header.Set("Authorization", "Bearer "+m.jwt)
		req.Header.Set("X-Merchant-ID", m.id)
		req.Header.Set("X-Merchant-Tier", "standard")
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != http.StatusCreated {
			t.Fatalf("failed to create wallet for merchant %s: err=%v code=%d", m.id, err, resp.StatusCode)
		}
		var wResp apiv1.CreateWalletResponse
		wRespBody, _ := io.ReadAll(resp.Body)
		protojson.Unmarshal(wRespBody, &wResp)
		resp.Body.Close()
		return wResp.WalletId
	}

	walletA := createWallet(merchantA)
	walletB := createWallet(merchantB)

	t.Logf("Created Wallets: WalletA=%s (on %s), WalletB=%s (on %s)",
		walletA, merchantA.shardID, walletB, merchantB.shardID)

	// Step 1: Pre-fund Wallet A via API Deposit
	depositAmount := int64(200000) // 200,000 NGN
	depPayload := map[string]interface{}{
		"from_wallet": "",
		"to_wallet":   walletA,
		"amount":      depositAmount,
		"currency":    "NGN",
	}
	depBody, _ := json.Marshal(depPayload)
	depReq, _ := http.NewRequest(http.MethodPost, apiURL+"/v1/transfers", bytes.NewBuffer(depBody))
	depReq.Header.Set("Authorization", "Bearer "+merchantA.jwt)
	depReq.Header.Set("X-Merchant-ID", merchantA.id)
	depReq.Header.Set("X-Merchant-Tier", "standard")
	depReq.Header.Set("X-Idempotency-Key", "xshard_dep_1")
	depReq.Header.Set("Content-Type", "application/json")
	depResp, err := http.DefaultClient.Do(depReq)
	if err != nil || depResp.StatusCode != http.StatusAccepted {
		t.Fatalf("failed to deposit to walletA: err=%v code=%d", err, depResp.StatusCode)
	}
	var depJobResp apiv1.CreateTransferResponse
	depRespBody, _ := io.ReadAll(depResp.Body)
	protojson.Unmarshal(depRespBody, &depJobResp)
	depResp.Body.Close()

	// Poll Deposit Job Status via API
	var depStatus string
	for i := 0; i < 30; i++ {
		time.Sleep(500 * time.Millisecond)
		jReq, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/v1/jobs/%s", apiURL, depJobResp.JobId), nil)
		jReq.Header.Set("Authorization", "Bearer "+merchantA.jwt)
		jReq.Header.Set("X-Merchant-ID", merchantA.id)
		jReq.Header.Set("X-Merchant-Tier", "standard")
		jResp, jErr := http.DefaultClient.Do(jReq)
		if jErr == nil && jResp.StatusCode == http.StatusOK {
			var jData apiv1.GetJobResponse
			jDataBody, _ := io.ReadAll(jResp.Body)
			protojson.Unmarshal(jDataBody, &jData)
			jResp.Body.Close()
			depStatus = jData.Status
			if depStatus == "completed" {
				break
			}
		}
	}
	if depStatus != "completed" {
		t.Fatalf("expected deposit completed via API, got status: %s", depStatus)
	}

	// Verify Wallet A Initial Balance via API
	bReqA, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/v1/balances?wallet_id=%s", apiURL, walletA), nil)
	bReqA.Header.Set("Authorization", "Bearer "+merchantA.jwt)
	bReqA.Header.Set("X-Merchant-ID", merchantA.id)
	bReqA.Header.Set("X-Merchant-Tier", "standard")
	bRespA, err := http.DefaultClient.Do(bReqA)
	if err != nil || bRespA.StatusCode != http.StatusOK {
		t.Fatalf("failed to get balance for walletA: err=%v code=%d", err, bRespA.StatusCode)
	}
	var bDataA apiv1.GetBalanceResponse
	bDataBodyA, _ := io.ReadAll(bRespA.Body)
	protojson.Unmarshal(bDataBodyA, &bDataA)
	bRespA.Body.Close()
	if bDataA.Balance != depositAmount {
		t.Fatalf("expected walletA initial balance to be %d, got %d", depositAmount, bDataA.Balance)
	}

	// Step 2: Execute Cross-Shard Transfer from Wallet A (shard-a) to Wallet B (shard-b)
	transferAmount := int64(75000) // 75,000 NGN
	txPayload := map[string]interface{}{
		"from_wallet": walletA,
		"to_wallet":   walletB,
		"amount":      transferAmount,
		"currency":    "NGN",
	}
	txBody, _ := json.Marshal(txPayload)
	txReq, _ := http.NewRequest(http.MethodPost, apiURL+"/v1/transfers", bytes.NewBuffer(txBody))
	txReq.Header.Set("Authorization", "Bearer "+merchantA.jwt)
	txReq.Header.Set("X-Merchant-ID", merchantA.id)
	txReq.Header.Set("X-Merchant-Tier", "standard")
	txReq.Header.Set("X-Idempotency-Key", "xshard_transfer_1")
	txReq.Header.Set("Content-Type", "application/json")
	txResp, err := http.DefaultClient.Do(txReq)
	if err != nil || txResp.StatusCode != http.StatusAccepted {
		t.Fatalf("failed to execute cross-shard transfer: err=%v code=%d", err, txResp.StatusCode)
	}
	var txJobResp apiv1.CreateTransferResponse
	txRespBody, _ := io.ReadAll(txResp.Body)
	protojson.Unmarshal(txRespBody, &txJobResp)
	txResp.Body.Close()

	// Step 3: Poll Cross-Shard Transfer Job Status via API
	var txStatus string
	for i := 0; i < 40; i++ {
		time.Sleep(500 * time.Millisecond)
		jReq, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/v1/jobs/%s", apiURL, txJobResp.JobId), nil)
		jReq.Header.Set("Authorization", "Bearer "+merchantA.jwt)
		jReq.Header.Set("X-Merchant-ID", merchantA.id)
		jReq.Header.Set("X-Merchant-Tier", "standard")
		jResp, jErr := http.DefaultClient.Do(jReq)
		if jErr == nil && jResp.StatusCode == http.StatusOK {
			var jData apiv1.GetJobResponse
			jDataBody, _ := io.ReadAll(jResp.Body)
			protojson.Unmarshal(jDataBody, &jData)
			jResp.Body.Close()
			txStatus = jData.Status
			if txStatus == "completed" {
				break
			}
		}
	}
	if txStatus != "completed" {
		t.Fatalf("expected cross-shard transfer completed via API, got status: %s", txStatus)
	}

	t.Log("Cross-shard transfer job successfully completed!")

	// Step 4: Verify Final Balances via API on both shards
	// Wallet A (shard-a) should have: 200,000 - 75,000 = 125,000 NGN
	bReqAEnd, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/v1/balances?wallet_id=%s", apiURL, walletA), nil)
	bReqAEnd.Header.Set("Authorization", "Bearer "+merchantA.jwt)
	bReqAEnd.Header.Set("X-Merchant-ID", merchantA.id)
	bReqAEnd.Header.Set("X-Merchant-Tier", "standard")
	bRespAEnd, _ := http.DefaultClient.Do(bReqAEnd)
	var bDataAEnd apiv1.GetBalanceResponse
	bDataBodyAEnd, _ := io.ReadAll(bRespAEnd.Body)
	protojson.Unmarshal(bDataBodyAEnd, &bDataAEnd)
	bRespAEnd.Body.Close()
	expectedA := depositAmount - transferAmount
	if bDataAEnd.Balance != expectedA {
		t.Fatalf("expected walletA final balance %d, got %d", expectedA, bDataAEnd.Balance)
	}

	// Wallet B (shard-b) should have: 0 + 75,000 = 75,000 NGN
	bReqBEnd, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/v1/balances?wallet_id=%s", apiURL, walletB), nil)
	bReqBEnd.Header.Set("Authorization", "Bearer "+merchantB.jwt)
	bReqBEnd.Header.Set("X-Merchant-ID", merchantB.id)
	bReqBEnd.Header.Set("X-Merchant-Tier", "standard")
	bRespBEnd, _ := http.DefaultClient.Do(bReqBEnd)
	var bDataBEnd apiv1.GetBalanceResponse
	bDataBodyBEnd, _ := io.ReadAll(bRespBEnd.Body)
	protojson.Unmarshal(bDataBodyBEnd, &bDataBEnd)
	bRespBEnd.Body.Close()
	if bDataBEnd.Balance != transferAmount {
		t.Fatalf("expected walletB final balance %d, got %d", transferAmount, bDataBEnd.Balance)
	}

	t.Logf("Balances verified: WalletA=%d, WalletB=%d", bDataAEnd.Balance, bDataBEnd.Balance)

	// Step 5: Verify Clearing Ledger Entries on both shards directly
	var clearingCountA, clearingCountB int64
	_ = cluster.ShardA.Pool.QueryRow(context.Background(),
		"SELECT count(*) FROM ledger_entries WHERE wallet_id LIKE 'merchant_00000000-0000-0000-0000-000000000001.%'").Scan(&clearingCountA)
	_ = cluster.ShardB.Pool.QueryRow(context.Background(),
		"SELECT count(*) FROM ledger_entries WHERE wallet_id LIKE 'merchant_00000000-0000-0000-0000-000000000001.%'").Scan(&clearingCountB)

	if clearingCountA == 0 || clearingCountB == 0 {
		t.Fatalf("expected system clearing entries on both shards, got shardA=%d shardB=%d", clearingCountA, clearingCountB)
	}
	t.Logf("Clearing ledger entries verified across shards: shardA=%d entries, shardB=%d entries", clearingCountA, clearingCountB)

	// Step 6: Verify Outbox Events completely published (0 pending)
	var pendingOutboxA, pendingOutboxB int64
	_ = cluster.ShardA.Pool.QueryRow(context.Background(),
		"SELECT count(*) FROM events WHERE published_at IS NULL AND publish_topic IS NOT NULL").Scan(&pendingOutboxA)
	_ = cluster.ShardB.Pool.QueryRow(context.Background(),
		"SELECT count(*) FROM events WHERE published_at IS NULL AND publish_topic IS NOT NULL").Scan(&pendingOutboxB)

	if pendingOutboxA != 0 || pendingOutboxB != 0 {
		t.Fatalf("expected 0 pending outbox events, got shardA=%d shardB=%d", pendingOutboxA, pendingOutboxB)
	}

	// Step 7: Verify Redis Velocity
	velocityCount, err := redisClient.ZCard(context.Background(), "velocity:wallet:"+walletA).Result()
	if err != nil || velocityCount < 1 {
		t.Fatalf("expected fraud worker velocity in Redis for walletA, count=%d err=%v", velocityCount, err)
	}

	// Step 8: Verify Zero Poison Pills in DLQ
	var dlqCount int64
	_ = cluster.MerchantsDB.Pool.QueryRow(context.Background(), "SELECT count(*) FROM dlq_entries").Scan(&dlqCount)
	if dlqCount != 0 {
		var errMsg, source string
		_ = cluster.MerchantsDB.Pool.QueryRow(context.Background(), "SELECT source, error_message FROM dlq_entries LIMIT 1").Scan(&source, &errMsg)
		t.Fatalf("Cross-shard DLQ failure: %d entries in DLQ! Source: %s, Error: %s", dlqCount, source, errMsg)
	}

	t.Log("Cross-shard E2E test passed with 0 DLQ entries and 100% balance integrity!")
}
