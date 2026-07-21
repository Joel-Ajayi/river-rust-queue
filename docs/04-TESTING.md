# 04: Testing Strategy

RRQ is a **high-throughput closed-loop ledger** targeted at **2,000–5,000 TPS sustained** and **10,000+ TPS peak**. The testing strategy is designed to match that — it is _not_ a side-project test suite.

---

## Test Pyramid

```
          ╱╲
         ╱  ╲          Contract + Chaos (5%)
        ╱    ╲
       ╱──────╲
      ╱        ╲       Integration + E2E (20%)
     ╱          ╲
    ╱────────────╲
   ╱              ╲    Unit tests (50%)
  ╱                ╲
 ╱──────────────────╲
╱                    ╲  Non-Functional: k6 (25%)
```

The pyramid is wider in the middle than classic because a distributed payment system's failure modes live at integration and load boundaries.

| Category           | Type        | Tool                          | % Effort | Critical For                         |
| ------------------ | ----------- | ----------------------------- | -------- | ------------------------------------ |
| **Functional**     | Unit        | Go `testing` + testify        | 50%      | Domain logic correctness, invariants |
| **Functional**     | Integration | Go `testing` + testcontainers | 20%      | Repository, Kafka, shard routing     |
| **Non-Functional** | Load/Stress | k6                            | 25%      | Throughput, latency, resilience      |
| **Functional**     | Contract    | `buf`                         | 3%       | Proto schema compatibility           |
| **Functional**     | E2E/Chaos   | k6 + kubectl                  | 2%       | Invariant enforcement under failure  |

---

## Production Throughput Targets

| Metric                         | Target       | How Measured                       |
| ------------------------------ | ------------ | ---------------------------------- |
| **Sustained TPS**              | 2,000+       | API 202 acceptance rate over 30min |
| **Peak TPS**                   | 10,000+      | Ramp-to-peak scenario              |
| **API acceptance p95 latency** | <200ms       | k6 `http_req_duration`             |
| **API acceptance p99 latency** | <500ms       | k6 `http_req_duration`             |
| **End-to-end settlement p95**  | <2s          | Job completion → status check      |
| **End-to-end settlement p99**  | <5s          | Job completion → status check      |
| **Error rate**                 | <0.1%        | k6 `http_req_failed`               |
| **Idempotency correctness**    | 0 duplicates | Reconciliation cross-check         |
| **Conservation of value**      | 0 drift      | Nightly reconciliation             |

---

## Functional Testing

### Unit Tests (50%)

Every Go package under `services/go-services/` has corresponding tests in `*_test.go` files using the `xxx_test` external test package convention for black-box testing. Service interfaces (`port.*`) are mocked using `testify/mock`.

#### Conventions

- Test function names follow `Test<Type>_<Method>_<Scenario>`, e.g. `TestLedgerWorker_ProcessTransfer_ConservationOfValue`.
- All external dependencies (database, Kafka, Redis, HTTP) are mocked at the port interface boundary.
- Shared mock structs for interfaces that use only primitive types live in `internal/testutil/mocks/`. Per-service mock files handle interfaces that depend on service-specific domain types.
- Every invariant from `docs/01-INVARIANTS.md` must have at least one test proving it holds and one proving it fails correctly.

#### Critical Paths to Test

| Domain                                   | Priority | Failure Mode                                    |
| ---------------------------------------- | -------- | ----------------------------------------------- |
| **Ledger Worker** — SERIALIZABLE txns    | P0       | Double-spend, partial completion (invariant #1) |
| **Ledger Worker** — Balance checks       | P0       | Negative balances (invariant #2)                |
| **Core API** — Idempotency key           | P0       | Duplicate processing (invariant #3)             |
| **Core API** — Transfer validation       | P1       | Invalid state transitions                       |
| **Fraud Worker** — Velocity rules        | P1       | Missing freeze on threshold trip                |
| **Recon Worker** — Balance comparison    | P1       | Silent drift (invariant #6)                     |
| **Webhook Worker** — Retry/backoff       | P1       | Lost notifications (invariant #5)               |
| **Outbox Relay** — At-least-once         | P0       | Lost events                                     |
| **Circuit Breakers** — State transitions | P1       | Unhealthy downstream propagation                |
| **Platform** — ID generation, errors     | P2       | Collisions, masked errors                       |

### Integration Tests (20%)

Integration tests start real infrastructure via `testcontainers-go` and are gated with the `//go:build integration` build tag.

#### Infrastructure helpers

Shared test infrastructure lives in `internal/testutil/`:

- Database setup (`pg_testutil.go`) — starts Postgres containers for merchants, shard A, and shard B
- Kafka setup (`kafka.go`) — starts a Kafka container and returns broker addresses
- Seeder (`seeder.go`) — seeds merchant records and wallets with unique ULIDs
- Fixtures (`fixtures/`) — reusable JSON payloads loaded via the `Load` helper
- Builders (`builders.go`) — builder pattern for constructing test data

#### What is integration tested

- **Shard routing** — requests to wrong shard return 404, not wrong data
- **Repository adapters** — Postgres wallet, job, event stores under real queries
- **Cross-shard transfers** — two-phase flow, partial failure recovery
- **Outbox relay** — DB → Kafka publish flow with offset tracking and DLQ
- **Kafka consumer** — message processing, offset commits, rebalance handling
- **HTTP handlers** — idempotency, auth, validation against real infrastructure

#### Data isolation

Each test uses unique merchant IDs generated via `platform.NewMerchantID()` to avoid cross-test contamination. The preferred cleanup pattern is transaction rollback: begin a transaction, run the test, and roll back at cleanup — avoiding container restart overhead. For cross-shard tests, a cleanup shard is used to verify no leaked transactions.

### Contract Tests (3%)

Protobuf schemas in `api/proto/` are validated with `buf` for lint and breaking change detection on every PR. The `buf lint` and `buf breaking` checks run in CI alongside the unit and integration test suites.

---

## Non-Functional Testing (k6)

All performance testing uses [k6](https://k6.io). Scenarios are designed to simulate real payment traffic patterns against the production-grade target of **2,000+ sustained TPS**.

### Wallet Pool

The k6 test pool uses **100,000 wallets** across **100 merchants** to avoid hotspotting and simulate realistic traffic distribution.

### Scenarios

The scenarios are divided into five main testing categories:

#### 1. Performance Testing

Verifies response times, throughput, and system resource limits under varying load conditions.

- **Load Testing (`load-sustained.js`)**: Evaluates normal operating parameters with 1,000 VUs over 30 minutes at a target rate of 2,000+ TPS. Thresholds: `p95<200ms`, `errors<0.1%`.
- **Stress Testing (`stress-bulk-payout.js`)**: Assesses behaviour at/beyond capacity limits (50 VUs, 15m, 25,000 payout legs). Thresholds: `p95<2s`, `errors<1%`.
- **Spike Testing (`spike-surge.js`)**: Tests response to sudden, extreme load changes (0 to 2,000 VUs in 5s). Thresholds: `p99<1s`, `errors<1%`.
- **Soak Testing (`soak-endurance.js`)**: Confirms endurance and checks for memory/connection leaks over 4 hours with 500 VUs. Thresholds: `no degradation`.

#### 2. Security Testing

Verifies that the edge gateway and API handlers restrict unauthorized access and handle malicious payloads under load.

- **Edge Protection (`edge-protection.js`)**: Floods the gateway with requests lacking credentials, using invalid signatures, or carrying giant bodies to verify edge isolation. Thresholds: `100% blocked/unauthorized`.

#### 3. Compatibility Testing

Validates API contract schema structure and types against specification schema definitions.

- **Contract Compliance (`contract-compliance.js`)**: Validates that all REST response payloads conform exactly to Protobuf specifications. Thresholds: `errors<0.001%`.

#### 4. Reliability / Fault Tolerant Testing

Verifies system behaviour, self-healing, and fault tolerance during errors, limit violations, or component failures.

- **Circuit Breaker / Fallback (`circuit-breaker.js`)**: Verifies that downstream database failures trigger circuit breakers, preserving API response times.
- **Fraud Limit Restrictions (`fraud-throughput.js`)**: Validates that velocity limit checks trigger correct 429/403 status codes and protect system databases under high rate.
- **Data Integrity / Reconciliation (`reconciliation-integrity.js`)**: Proves ledger correctness and double-entry consistency under concurrent load.

#### 5. Scalability Testing

Evaluates how the system scales as load increases and verifies dynamic multi-shard database routing.

- **Ramp to Peak (`ramp-to-peak.js`)**: Ramps up VUs (0 to 5,000) over 20 minutes to profile scaling limits at 10,000 TPS. Thresholds: `p95<500ms`, `errors<0.5%`.
- **Cross-Shard Throughput (`cross-shard-throughput.js`)**: Verifies two-phase commit saga coordinator throughput and network balance across database shards.

### Running k6

```bash
# Run a specific scenario (default: load-sustained)
make bench SCENARIO=load-sustained

# Run all scenarios sequentially (nightly)
make bench-all

# Run against a specific environment
BASE_URL=https://staging.rrq.io make bench SCENARIO=load-sustained

# Quick smoke test (1 minute, for CI)
make bench-smoke
```

### CI Integration

| Trigger         | What Runs                                 | Duration | Purpose                    |
| --------------- | ----------------------------------------- | -------- | -------------------------- |
| **Every PR**    | `make k6-smoke` + `make test-unit`        | ~2min    | Catch regressions fast     |
| **Every PR**    | `make test-integration` (Docker required) | ~5min    | Infrastructure correctness |
| **Nightly**     | `make k6-all`                             | ~6hr     | Full performance profile   |
| **Pre-release** | `make k6-all` + 4hr soak + chaos          | ~10hr    | Release qualification      |

### Chaos Testing (Pre-release)

In addition to k6 scenarios, pre-release qualification includes:

1. **Pod kill during transfer** — kill a ledger-worker pod mid-SERIALIZABLE transaction, verify no value loss
2. **Kafka broker failure** — kill a Strimzi broker, verify producer/consumer recovery
3. **Postgres failover** — trigger CloudNativePG switchover, verify no duplicate processing
4. **Network partition** — block traffic between outbox-relay and Kafka, verify replay after recovery

---

## Observability-Backed Verification

k6 tests **are not just load generators**. They are real users driving real traffic through the full deployed platform (Kong → Gateway → Postgres → Outbox Relay → Kafka → Workers). Every request produces real Postgres rows, Kafka messages, OTel traces, Prometheus metrics, and canonical log lines.

After the load generation phase ends, `k6/verify.sh` runs automated checks against the observability stack to validate the system _actually_ behaved correctly — not just that HTTP status codes looked right.

### What gets verified

#### 1. Business Metric Correctness (Prometheus)

Every k6 scenario generates known transaction volumes. After the test, `verify.sh` queries Prometheus to confirm that `rrq_business_*` metrics match expectations:

| Check                   | Query                                            | Pass Condition | Critical For                         |
| ----------------------- | ------------------------------------------------ | -------------- | ------------------------------------ |
| **GTV flowing**         | `sum(rate(rrq_business_gtv_total[5m]))`          | > 0            | Conservation of value (invariant #1) |
| **Transfers counting**  | `sum(rate(rrq_business_transfers_total[5m]))`    | > 0            | Pipeline health                      |
| **TSR above threshold** | `(transfers_non_failed / total_transfers) * 100` | >= 95%         | Invariant #3 — at-most-once          |
| **Circuit breakers**    | `count(rrq_circuit_breaker_state == 2)`          | 0 open         | No systemic failure                  |
| **DLQ rate**            | `rate(rrq_dlq_ingestion_rate[5m])`               | < 1/min        | No poison pills                      |
| **Outbox lag**          | `rrq_outbox_lag_seconds`                         | < 10s          | Outbox relay healthy                 |

#### 2. Distributed Trace Completeness (Jaeger)

Every transfer should produce a complete trace spanning:

```
HTTP POST /v1/transfers (core-api)
  → DB INSERT jobs + outbox event (Postgres)
  → Kafka produce job.requested (outbox-relay)
  → Kafka consume job.requested (ledger-worker)
  → DB SERIALIZABLE txn (Postgres)
  → Kafka produce transfer.completed (outbox-relay)
  → Kafka consume transfer.completed (webhook-worker)
  → HTTP POST webhook to merchant (webhook-worker)
```

`verify.sh` queries Jaeger's API for traces within the test window and confirms:

- At least one trace exists for the `core-api` service
- At least 50% of traces have >= 3 spans (HTTP → Kafka → DB boundary crossing)
- No orphaned consumer spans (every consumer span has a matching producer span)

#### 3. Alert Silence (Alertmanager)

The 28 Prometheus alert rules define what must **never** happen during normal operation. `verify.sh` queries Alertmanager for any firing alerts whose `startsAt` falls within the test window:

| Must NOT fire          | Rule                                          | Severity |
| ---------------------- | --------------------------------------------- | -------- |
| Circuit breaker open   | `LedgerWorkerCBOpen`, `WebhookWorkerCBOpen`   | Critical |
| DLQ spike              | `LedgerWorkerDLQSpike`, `FraudWorkerDLQSpike` | Critical |
| HTTP 5xx > 1%          | `APIGatewayHTTP5xxRateHigh`                   | Critical |
| TSR < 95%              | `BusinessTSRDrop`                             | Critical |
| Saga failure > 10%     | `SagaFailureRateHigh`                         | Critical |
| Outbox relay stuck     | `OutboxRelayStuck`                            | Warning  |
| GTV flatlined          | `BusinessGTVAnomaly`                          | Warning  |
| Consumer backoff > 50% | `ConsumerBackoffDominant`                     | Warning  |

Any fired alert during a performance test is an automatic failure.

### Verification lifecycle

Every k6 run follows this sequence:

```
k6 run (load generation)
  ↓
START_TS captured before first request
  ↓
k6 run completes, END_TS captured
  ↓
verify.sh runs against:
  ├── Prometheus   — rrq_business_* metric validation
  ├── Jaeger       — trace completeness check
  ├── Alertmanager — no-firing-alerts check
  └── (pre-release) Postgres — invariant SQL queries
  ↓
Verification report written to k6/reports/<scenario>-verify.json
```

### Verification output

Summary reports are written to `k6/reports/<scenario>-verify.json` with all check results, enabling CI to parse and fail the pipeline if any verification check fails. See `k6/verify.sh` for the full implementation.

---

## Naming and organization

| Convention            | Rule                                                    |
| --------------------- | ------------------------------------------------------- |
| Unit test file        | `*_test.go` in the same package as code under test      |
| Integration test file | `*_test.go` with `//go:build integration` tag           |
| Test package          | `xxx_test` (external) for black-box testing             |
| Mock file             | `mock_*.go` or `mock_ports_test.go` in the same package |
| Test function         | `Test<Type>_<Method>_<Scenario>`                        |
| k6 scenario file      | `k6/scenarios/<letter>-<hyphenated-name>.js`            |
| k6 helper             | `lib/helpers.js` — shared HTTP request builders         |

---

## Key implementation locations

| Area                          | File/Directory                                                              |
| ----------------------------- | --------------------------------------------------------------------------- |
| Test Makefile targets         | `services/go-services/Makefile`                                             |
| k6 Makefile targets           | `Makefile` (root)                                                           |
| Go CI workflow                | `.github/workflows/test.yml`                                                |
| k6 CI workflow                | `.github/workflows/performance.yml`                                         |
| Shared test infrastructure    | `services/go-services/internal/testutil/`                                   |
| Test seeder                   | `services/go-services/internal/testutil/seeder.go`                          |
| Platform unit tests           | `services/go-services/internal/platform/`                                   |
| Per-service unit tests        | Each service's `internal/core/app/`                                         |
| Per-service integration tests | Various `*_test.go` files with `integration` build tag                      |
| k6 scenarios                  | `k6/scenarios/performance/`, `k6/scenarios/reliability/`, etc.              |
| k6 shared lib                 | `k6/lib/`                                                                   |
| k6 reports                    | `k6/reports/`                                                               |
| Fraud service tests           | `services/go-services/fraud-worker/internal/core/app/fraud_service_test.go` |
