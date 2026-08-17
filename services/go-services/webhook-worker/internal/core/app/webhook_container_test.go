//go:build integration

package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/testutil"
	httpadapter "github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/adapter/outbound/http"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/adapter/outbound/postgres"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/domain"
	"go.uber.org/zap"
)

func TestWebhook_Container_HandleMessage_WithRealPostgresAndEchoEndpoint(t *testing.T) {
	cluster := testutil.SetupTestDB(t)

	// 1. Spin up a test HTTP server representing the Merchant Webhook Endpoint (like webhook-echo)
	receivedWebhook := false
	echoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") == "application/json" {
			receivedWebhook = true
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		}
	}))
	defer echoServer.Close()

	// 2. Seed a test merchant into real Postgres container with echoServer.URL as its webhook_url
	merchantID := platform.NewMerchantID()
	_, err := cluster.MerchantsDB.Pool.Exec(context.Background(), `
		INSERT INTO merchants (id, name, api_key_hash, tier, status, shard_id, webhook_url, webhook_secret)
		VALUES ($1, 'Webhook Test Merchant', 'hash', 'starter', 'active', 'shard-a', $2, 'whsec_test123')
	`, merchantID, echoServer.URL)
	if err != nil {
		t.Fatalf("failed to seed merchant with webhook_url in container postgres: %v", err)
	}

	logger, _ := zap.NewDevelopment()
	repo := postgres.NewRepository(cluster.ShardPools, logger, platform.RetryConfig{MaxRetries: 3, InitialIntervalMs: 10, MaxIntervalMs: 100, BackoffFactor: 2.0})

	cfg := &platform.Config{
		Capacity: platform.CapacityConfig{
			HTTPTimeoutMs:               5000,
			HTTPMaxIdleConns:            10,
			HTTPMaxIdleConnsPerHost:     5,
			HTTPIdleConnTimeoutMs:       10000,
			HTTPResponseHeaderTimeoutMs: 5000,
			HTTPTLSHandshakeTimeoutMs:   5000,
			HTTPExpectContinueTimeoutMs: 1000,
		},
	}
	httpClient := httpadapter.NewWebhookClient(cfg)

	serviceCfg := WebhookConfig{
		MaxDeliveryAttempts:   3,
		BaseRetryDelaySec:     1,
		CapRetryDelaySec:      10,
		SchedulerPollInterval: 1 * time.Second,
		SchedulerBatchSize:    10,
		FastLaneGracePeriod:   100 * time.Millisecond,
		FastLaneBufferSize:    100,
		MaxConcurrency:        10,
	}

	svc := NewWebhookService(repo, httpClient, logger, serviceCfg)

	eventID := platform.NewEventID()
	payload := []byte(`{"event_id":"` + eventID + `","type":"transfer.completed"}`)

	// Process message against real Postgres container + live Echo HTTP endpoint
	err = svc.HandleMessage(context.Background(), merchantID, "notify", merchantID, payload)
	if err != nil {
		t.Fatalf("failed to handle webhook message: %v", err)
	}

	if !receivedWebhook {
		t.Fatalf("expected merchant echo endpoint to receive HTTP POST webhook")
	}

	// Verify webhook delivery was recorded in real Postgres container
	deliveryID := platform.NewDeterministicDeliveryID(eventID, merchantID)
	var status string
	err = cluster.ShardA.Pool.QueryRow(context.Background(),
		`SELECT status FROM webhook_deliveries WHERE id = $1`, deliveryID).Scan(&status)
	if err != nil {
		t.Fatalf("failed to query webhook_deliveries in container postgres: %v", err)
	}
	if status != string(domain.DeliveryStatusSuccess) {
		t.Fatalf("expected webhook delivery status 'success' in real Postgres container, got %s", status)
	}
}
