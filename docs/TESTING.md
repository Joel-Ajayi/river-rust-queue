# Testing Strategy & Architecture Specification

This document establishes the testing architecture, testing pyramid, execution commands, and verification strategy for the River Rust Queue (RRQ) payment processing core.

---

## 1. Testing Philosophy & Pyramid

To ensure financial correctness, low latency, and zero value drift under saturation, RRQ employs a multi-tiered testing strategy:

```
                       /\
                      /  \     End-to-End (E2E) & Chaos Tests
                     / E2E\    (Live K8s Cluster via Kind & Skaffold)
                    /------\
                   /        \  Storage Integration Tests
                  /  Storage \ (Real PostgreSQL & Strimzi Kafka)
                 /------------\
                /  Component & \ API & Service Integration Tests (80% of Suite)
               / API Integration\ (In-Memory Port Mocks — Zero Infra Required)
              /------------------\
```

---

## 2. Test Execution Modes (Choosing How to Run)

Developers and CI pipelines can choose between **two explicit execution modes**:

### Mode 1: Fast In-Memory Mode (Default — 0.01s, Zero Docker Required)
* **Goal**: Validate 100% of HTTP handler routing, authentication, idempotency rules, saga state transitions, exponential retry backoffs, and error classification without network overhead.
* **Speed**: Executes all microservice test suites across `core-api`, `ledger-worker`, `webhook-worker`, `fraud-worker`, and `outbox-relay` in **under 0.02 seconds**.
* **Zero External Dependencies**: Does not require running PostgreSQL, Kafka, or Redis containers locally.
* **Command**:
  ```bash
  cd services/go-services
  go test ./...
  ```

### Mode 2: Storage & Container Mode (`//go:build integration`)
* **Goal**: Validate real database migrations ([deploy/db/migrations](../deploy/db/migrations)), PostgreSQL constraints (`ON CONFLICT`, foreign keys), seed data ([deploy/db/seed](../deploy/db/seed)), and SQL queries against real Docker containers.
* **Persistence & Reuse**: Storage containers start **once per test run** (`sync.Once`) and stay persistent across test invocations for maximum execution speed.
* **Container Test Files**:
  * [api_container_test.go](../services/go-services/core-api/internal/adapter/inbound/rest/api_container_test.go) (PostgreSQL 16 REST Ingress)
  * [ledger_container_test.go](../services/go-services/ledger-worker/internal/core/app/ledger_container_test.go) (PostgreSQL 16 Double-Entry & Sagas)
  * [webhook_container_test.go](../services/go-services/webhook-worker/internal/core/app/webhook_container_test.go) (PostgreSQL 16 & Echo HTTP Endpoint)
  * [fraud_container_test.go](../services/go-services/fraud-worker/internal/core/app/fraud_container_test.go) (Redis 7 Velocity Checks)
  * [testutil_sanity_test.go](../services/go-services/internal/testutil/testutil_sanity_test.go) (Cluster Setup & Seeds)
* **Command to Run**:
  ```bash
  make test-containers   # or: go test -v -tags=integration ./...
  ```
* **Command to Kill Persistent Containers**:
  ```bash
  make test-clean        # or: docker rm -f $(docker ps -q --filter label=org.testcontainers=true)
  ```

---

## 3. Test Suite Breakdown by Service

### A. `core-api` (API Gateway & HTTP Ingress)
* **Fast Suite**: [api_integration_test.go](../services/go-services/core-api/internal/adapter/inbound/rest/api_integration_test.go)
* **Container Suite**: [api_container_test.go](../services/go-services/core-api/internal/adapter/inbound/rest/api_container_test.go)
* **Coverage**:
  * `TestAPI_HealthAndReady`: Validates `/health` and `/ready` probes (HTTP 200 OK).
  * `TestAPI_JWKSEndpoint`: Validates RFC 7517/8037 Ed25519 JWKS public key set serving (`/.well-known/jwks.json`).
  * `TestAPI_AuthTokenEndpoint`: Validates Bearer API key authentication & JWT issuance (`POST /v1/auth/token`).
  * `TestAPI_CreateTransferEndpoint`: Validates transfer submission (`POST /v1/transfers`), enforcing required `X-Idempotency-Key` headers (HTTP 400 vs 202 Accepted).
  * `TestAPI_GetJobStatusEndpoint`: Validates job status queries (`GET /v1/jobs/{id}`).
  * `TestAPI_AdminDLQEndpoints`: Validates operator DLQ listing (`GET /v1/admin/dlq`) and Envoy edge origin authentication (`X-RRQ-Edge` header).

### B. `ledger-worker` (Transaction Processing & Dual-Phase Saga)
* **Fast Suite**: [service_integration_test.go](../services/go-services/ledger-worker/internal/core/app/service_integration_test.go)
* **Container Suite**: [ledger_container_test.go](../services/go-services/ledger-worker/internal/core/app/ledger_container_test.go)
* **Coverage**:
  * `TestLedger_ProcessJob_SameShardSuccess`: Intra-shard double-entry debit/credit posting (`PostTransfer`).
  * `TestLedger_ProcessJob_CrossShardInitiated`: Cross-Shard saga Phase 1 debit & clearing account posting (`DebitToClearingAccount`).
  * `TestLedger_ProcessJob_TerminalErrorHandled`: Graceful business decline recording (`FailTransfer`).
  * `TestLedger_XShardSettledAndFailed`: Phase 2 saga commitment (`HandleXShardSettled`) and compensation rollback (`HandleXShardFailed`).

### C. `webhook-worker` (HMAC Delivery & DLQ Routing)
* **Fast Suite**: [service_integration_test.go](../services/go-services/webhook-worker/internal/core/app/service_integration_test.go)
* **Container Suite**: [webhook_container_test.go](../services/go-services/webhook-worker/internal/core/app/webhook_container_test.go)
* **Coverage**:
  * `TestWebhook_HandleMessage_Success`: Valid webhook message ingestion & fast-lane worker dispatch.
  * `TestWebhook_HandleMessage_InvalidJSON_RoutesToGlobalDLQ`: Poison pill unmarshal error protection & global DLQ routing (`RouteToGlobalDLQ`).
  * `TestWebhook_HandleMessage_InactiveMerchant_RoutesToGlobalDLQ`: Inactive merchant delivery routing to global DLQ.

### D. `fraud-worker` (Velocity Checking & Wallet Freezing)
* **Fast Suite**: [service_integration_test.go](../services/go-services/fraud-worker/internal/core/app/service_integration_test.go)
* **Container Suite**: [fraud_container_test.go](../services/go-services/fraud-worker/internal/core/app/fraud_container_test.go)
* **Coverage**:
  * `TestFraud_ProcessJob_UnderThreshold`: Velocity checking under configured rate threshold.
  * `TestFraud_ProcessJob_OverThresholdFreezesWallet`: Velocity limit violation triggering automated wallet status transition to `frozen` in the shard database.

### E. `outbox-relay` (Transactional Outbox & Event Publishing)
* **Fast Suite**: [service_integration_test.go](../services/go-services/outbox-relay/internal/core/app/service_integration_test.go)
* **Coverage**:
  * `TestOutboxRelay_ProcessEvents_Success`: Batch validation & Kafka topic event publishing (`PublishBatch`).
  * `TestOutboxRelay_ProcessEvents_InvalidSchema_RoutesToDLQ`: Unroutable/poison pill event payload filtering and global DLQ routing (`RouteToDLQ`).

---

## 4. Verification & Idempotency Testing

Every test suite verifies key safety invariants:
1. **Redelivery Safety**: Kafka message redeliveries carrying duplicate `job_id` or `event_id` keys return `nil` or update existing records without writing duplicate ledger entries or events.
2. **Isolation & Concurrency**: Tests verify that concurrent requests with identical idempotency keys execute at-most-once.
