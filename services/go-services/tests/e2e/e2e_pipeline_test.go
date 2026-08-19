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
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	apiv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/api/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/testutil"
	"google.golang.org/protobuf/encoding/protojson"
)

// buildService compiles a go service into a temporary binary
func buildService(t *testing.T, sourceDir string) string {
	t.Helper()
	tempDir := t.TempDir()
	binPath := filepath.Join(tempDir, "service-bin")

	cmd := exec.Command("go", "build", "-o", binPath, "./cmd")
	cmd.Dir = sourceDir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to build %s: %v\nOutput: %s", sourceDir, err, out)
	}
	return binPath
}

// startService runs a compiled binary in the background with the given environment variables
func startService(t *testing.T, binPath string, env []string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(binPath)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start %s: %v", binPath, err)
	}

	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})

	return cmd
}

func TestFullPipelineE2E(t *testing.T) {
	// 1. Start Containers
	cluster := testutil.SetupTestDB(t)
	redisClient, _ := testutil.StartRedis(t)
	_, brokers := testutil.StartKafka(t)

	webhookReceived := make(chan bool, 1)
	echoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") == "application/json" {
			select {
			case webhookReceived <- true:
			default:
			}
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer echoServer.Close()

	// Merchant will be created via API after services boot

	// 2. Prepare Environment Variables for Services
	_, priv, _ := ed25519.GenerateKey(nil)
	der, _ := x509.MarshalPKCS8PrivateKey(priv)
	pemBlock := &pem.Block{Type: "PRIVATE KEY", Bytes: der}
	pemBytes := pem.EncodeToMemory(pemBlock)
	pemStr := strings.ReplaceAll(string(pemBytes), "\n", "\\n")

	t.Log("Building services...")
	coreApiBin := buildService(t, "../../core-api")
	outboxBin := buildService(t, "../../outbox-relay")
	ledgerBin := buildService(t, "../../ledger-worker")
	fraudBin := buildService(t, "../../fraud-worker")
	webhookBin := buildService(t, "../../webhook-worker")

	t.Log("Services built successfully. Starting them...")
	
	commonEnv := []string{
		"LOG_LEVEL=debug",
		"KAFKA_BROKERS=" + brokers[0],
		"MERCHANTS_DB_URI=" + cluster.MerchantsDB.URI,
		"SHARD_A_URI=" + cluster.ShardA.URI,
		"SHARD_B_URI=" + cluster.ShardB.URI,
		"REDIS_DATA_HOST=" + strings.Split(redisClient.Options().Addr, ":")[0],
		"REDIS_DATA_PORT=" + strings.Split(redisClient.Options().Addr, ":")[1],
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
		"VELOCITY_THRESHOLD":                "5",
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
	// Add globals (no prefix)
	for k, v := range capVars {
		commonEnv = append(commonEnv, k+"="+v)
	}

	_ = startService(t, outboxBin, append(commonEnv, "OUTBOX_RELAY_SHARD_ID=shard-a"))
	_ = startService(t, outboxBin, append(commonEnv, "OUTBOX_RELAY_SHARD_ID=shard-b"))
	_ = startService(t, ledgerBin, commonEnv)
	_ = startService(t, fraudBin, commonEnv)
	_ = startService(t, webhookBin, commonEnv)

	// For core-api, we need an HTTP port.
	coreApiPort := 8081
	_ = startService(t, coreApiBin, append(commonEnv, fmt.Sprintf("HTTP_PORT=%d", coreApiPort)))

	time.Sleep(3 * time.Second) // Wait for services to boot

	apiURL := fmt.Sprintf("http://localhost:%d", coreApiPort)

	// API: Create a Standard Merchant
	mReq := &apiv1.CreateMerchantRequest{
		Name:          "E2E Standard Merchant",
		WebhookUrl:    echoServer.URL,
		WebhookSecret: "whsec_e2e_123",
		Tier:          "standard",
	}
	mReqBody, _ := protojson.Marshal(mReq)
	resp, err := http.Post(apiURL+"/v1/merchants", "application/json", bytes.NewBuffer(mReqBody))
	if err != nil {
		t.Fatalf("failed to create merchant via API: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("failed to create merchant via API: expected 201, got %d", resp.StatusCode)
	}
	var mResp apiv1.CreateMerchantResponse
	mRespBody, _ := io.ReadAll(resp.Body)
	protojson.Unmarshal(mRespBody, &mResp)
	resp.Body.Close()
	merchantID := mResp.MerchantId
	apiKey := mResp.ApiKey
	assignedShard := mResp.ShardId

	t.Logf("Created Merchant %s assigned to %s", merchantID, assignedShard)

	// API: Get JWT Token
	tReq, _ := http.NewRequest(http.MethodPost, apiURL+"/v1/auth/token", nil)
	tReq.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err = http.DefaultClient.Do(tReq)
	if err != nil {
		t.Fatalf("failed to get token via API: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to get token via API: expected 200, got %d", resp.StatusCode)
	}
	var tResp apiv1.AuthTokenResponse
	tRespBody, _ := io.ReadAll(resp.Body)
	protojson.Unmarshal(tRespBody, &tResp)
	resp.Body.Close()
	token := tResp.Token

	createWallet := func() string {
		wReq := &apiv1.CreateWalletRequest{
			Currency: "USD",
		}
		wReqBody, _ := protojson.Marshal(wReq)
		req, _ := http.NewRequest(http.MethodPost, apiURL+"/v1/wallets", bytes.NewBuffer(wReqBody))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("X-Merchant-ID", merchantID)
		req.Header.Set("X-Merchant-Tier", "standard")
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != http.StatusCreated {
			t.Fatalf("failed to create wallet via API: err=%v code=%d", err, resp.StatusCode)
		}
		var wResp apiv1.CreateWalletResponse
		wRespBody, _ := io.ReadAll(resp.Body)
		protojson.Unmarshal(wRespBody, &wResp)
		resp.Body.Close()
		return wResp.WalletId
	}
	
	wallet1 := createWallet()
	wallet2 := createWallet()

	shardPool := cluster.ShardA.Pool
	if assignedShard == "shard-b" {
		shardPool = cluster.ShardB.Pool
	}

	_, err = shardPool.Exec(context.Background(), "UPDATE wallet_balance_cache SET balance = 100000 WHERE wallet_id = $1", wallet1)
	if err != nil {
		t.Fatalf("failed to fund wallet: %v", err)
	}

	payload := map[string]interface{}{
		"idempotency_key": "e2e_idem_1",
		"from_wallet":     wallet1,
		"to_wallet":       wallet2,
		"amount":          5000,
		"currency":        "USD",
	}
	reqBody, _ := json.Marshal(payload)

	req, _ := http.NewRequest(http.MethodPost, apiURL+"/v1/transfers", bytes.NewBuffer(reqBody))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Merchant-ID", merchantID)
	req.Header.Set("X-Merchant-Tier", "standard")
	req.Header.Set("X-Idempotency-Key", "e2e_idem_1")
	req.Header.Set("Content-Type", "application/json")

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to execute POST /api/v1/transfers: %v", err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d", resp.StatusCode)
	}
	
	select {
	case <-webhookReceived:
		t.Log("Webhook successfully received! Pipeline complete.")
	case <-time.After(15 * time.Second):
		t.Fatalf("timed out waiting for webhook echo to receive the event")
	}

	var balance int64
	err = shardPool.QueryRow(context.Background(), "SELECT balance FROM wallet_balance_cache WHERE wallet_id = $1", wallet1).Scan(&balance)
	if err != nil {
		t.Fatalf("failed to query wallet1: %v", err)
	}
	if balance != 95000 {
		t.Fatalf("expected wallet1 balance to be 95000, got %d", balance)
	}

	err = shardPool.QueryRow(context.Background(), "SELECT balance FROM wallet_balance_cache WHERE wallet_id = $1", wallet2).Scan(&balance)
	if err != nil {
		t.Fatalf("failed to query wallet2: %v", err)
	}
	if balance != 5000 {
		t.Fatalf("expected wallet2 balance to be 5000, got %d", balance)
	}

	var status string
	err = shardPool.QueryRow(context.Background(), "SELECT status FROM jobs WHERE idempotency_key = 'e2e_idem_1'").Scan(&status)
	if err != nil {
		t.Fatalf("failed to query job: %v", err)
	}
	if status != "completed" {
		t.Fatalf("expected job status completed, got %s", status)
	}
}
