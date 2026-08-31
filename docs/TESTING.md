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
  * [outbox_container_test.go](../services/go-services/outbox-relay/internal/core/app/outbox_container_test.go) (PostgreSQL 16 & Kafka Outbox Relay)
  * [testutil_sanity_test.go](../services/go-services/internal/testutil/testutil_sanity_test.go) (Cluster Setup & Seeds)
* **Command to Run**:
  ```bash
  make test-containers   # or: go test -v -tags=integration ./...
  ```
* **Command to Kill Persistent Containers**:
  ```bash
  make test-clean        # or: docker rm -f $(docker ps -q --filter label=org.testcontainers=true)
  ```

### Mode 3: Full End-to-End Pipeline Mode
* **Goal**: Validate the entire data flow asynchronously across all microservices, ensuring Kafka topics, outbox events, background workers, and webhooks all integrate flawlessly.
* **Architecture**: Compiles and launches all worker binaries simultaneously, connects them to real PostgreSQL, Kafka, and Redis containers, sets up an HTTP echo server for webhook delivery, and pushes real HTTP requests through the gateway.
* **Container Test File**:
  * [e2e_pipeline_test.go](../services/go-services/tests/e2e/e2e_pipeline_test.go)
* **Command to Run**:
  ```bash
  cd services/go-services
  go test -v -tags=integration -run=^TestFullPipelineE2E$ ./tests/e2e/...
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

## 5. Live Cluster Performance & Load Benchmark Suite

The live Kubernetes environment is validated using the enterprise **k6 load testing suite** located in `rrq-gitops/tools/load-tests/`.

### Benchmark Scenarios

| Scenario | Target Rate / Profile | Objectives Tested | Command |
| :--- | :--- | :--- | :--- |
| **`smoke.ts`** | Constant $5\text{ RPS}$ ($30\text{s}$) | DB write counts per transaction under zero contention | `./run.sh smoke dev` |
| **`full_workload.ts`** | $100 \rightarrow 1,000\text{ RPS}$ | Steady-state multi-endpoint latency, $C_a^2$, $C_s^2$ | `./run.sh full_workload dev` |
| **`stress.ts`** | $300 \rightarrow 3,000\text{ RPS}$ | Peak outbox drain, consumer backpressure, HPA scaling | `./run.sh stress dev` |
| **`spike.ts`** | $500 \rightarrow 3,000\text{ RPS}$ in $30\text{s}$ | Channel buffer depth, circuit breaker trip & recovery | `./run.sh spike dev` |
| **`breakpoint.ts`** | Ramp to saturation | Point of collapse and DB isolation thresholds | `./run.sh breakpoint dev` |
| **`soak.ts`** | Constant $500\text{ RPS}$ ($1\text{h}-24\text{h}$) | Long-term memory leaks, GC pauses, index bloat | `./run.sh soak dev` |

### Key Measured Benchmarks

* **Peak Throughput**: $3,000\text{ RPS}$ sustained across ingress and event pipelines.
* **Outbox Drain Rate**: $\approx 1,000\text{ events/sec}$ continuous publisher throughput with $< 8\%$ Kafka buffer fill.
* **Intra-Shard Transfer Latency**: $38.7\text{ ms}$ (P50) / $45.0\text{ ms}$ (P95) under $1,000\text{ RPS}$ nominal load.
* **Cross-Shard Clearing Saga**: Double-phase saga completion in $< 65\text{ ms}$ with $100\%$ zero balance drift.
* **Circuit Breaker Trip & Recovery**: Tripped at $10\%$ error threshold during extreme bursts, shedding load in $< 0.01\text{ ms}$ via HTTP 503, and auto-recovered in $10\text{s}$.
* **DLQ Batch Recovery**: $100\%$ message replay success rate across $43$ poisoned/timeout entries via `replay-dlq.mts`.
