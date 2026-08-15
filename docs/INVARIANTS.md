# System Invariants & Correctness Guarantees

This document establishes the nine core engineering invariants (`I1`–`I9`) guaranteed by the River Rust Queue (RRQ) payment processing core. Each invariant is a testable statement enforced by specific database constraints, transaction isolation levels, and application mechanisms, and verified by automated unit, integration, and chaos test suites.

---

## Invariant Matrix

| ID | Invariant | Primary Enforcement Mechanism | Storage / Engine Layer |
|---|---|---|---|
| **I1** | Conservation of Value | Atomic double-entry transaction + `UNIQUE(transfer_id, leg)` | PostgreSQL (`ledger_entries`) |
| **I2** | No Negative Balance | `SELECT ... FOR UPDATE` row locks + pre-debit check | PostgreSQL (`wallets` & `ledger_entries`) |
| **I3** | At-Most-Once Execution | `UNIQUE(merchant_id, idempotency_key)` | PostgreSQL (`jobs`) |
| **I4** | Per-Wallet Entry Ordering | `BIGSERIAL` entry IDs + serialized wallet transactions | PostgreSQL (`ledger_entries`) |
| **I5** | Per-Merchant Webhook Ordering | Kafka topic partitioning by `merchant_id` | Kafka (`notify` topic) |
| **I6** | Immutable History | Append-only table rules + correction posting pattern | PostgreSQL (`ledger_entries` & `events`) |
| **I7** | Job Termination | Bounded execution timeouts & token bucket retry budgets | Go Microservices & Kafka Consumers |
| **I8** | Recoverable DLQ Entries | Transactional `dlq_entries` store with replay capability | PostgreSQL (`dlq_entries`) & Admin API |
| **I9** | Tenant Isolation | Gateway JWT `sub` validation & wallet ownership checks | API Gateway |

---

## Detailed Specifications

### I1 — Conservation of Value

> **Statement**: Every transfer posts exactly one `debit` and one `credit` leg of equal magnitude to two distinct wallets within a single transaction. For every `transfer_id`, there exist exactly two rows in `ledger_entries`: `(transfer_id, 'debit')` and `(transfer_id, 'credit')`, where `|debit.amount| = credit.amount`.

* **Enforcement Mechanism**:
  1. **Intra-Shard Transfers**: Both legs are inserted inside a single `SERIALIZABLE` PostgreSQL transaction. If either leg fails or the worker crashes, the entire transaction rolls back.
  2. **Cross-Shard Sagas**: The 2-phase clearing protocol debits the source wallet and credits a source-shard clearing account in Phase 1, followed by debiting the destination clearing account and crediting the destination wallet in Phase 2.
  3. **Redelivery Safety**: `UNIQUE (transfer_id, leg)` constraint on `ledger_entries` prevents duplicate leg postings on Kafka message redeliveries.
* **Verification**: Integration tests simulate worker crashes during leg insertion to confirm zero value drift.

---

### I2 — No Negative Balance on Active Wallets

> **Statement**: For every active wallet $W$, its derived balance ($\sum \text{amount}$) is non-negative ($\ge 0$) at every point in the entry sequence.

* **Enforcement Mechanism**:
  1. Source and target wallet rows are fetched with `SELECT ... FOR UPDATE` (ordered deterministically by `id` to prevent deadlocks).
  2. The current balance is derived under the lock, and the debit attempt is checked: if `balance + debit_amount < 0`, the transaction aborts immediately with `ErrInsufficientFunds`.
* **Verification**: Concurrent transfer tests execute parallel debit attempts against a wallet with limited funds to confirm that exactly one succeeds and the balance never drops below zero.

---

### I3 — At-Most-Once Execution per Idempotency Key

> **Statement**: For each `(merchant_id, idempotency_key)` accepted by the API Gateway, exactly one `jobs` row exists, and at most one set of ledger postings derives from it.

* **Enforcement Mechanism**:
  1. PostgreSQL constraint: `UNIQUE (merchant_id, idempotency_key)` on the `jobs` table.
  2. The API Gateway issues `INSERT INTO jobs ... ON CONFLICT (merchant_id, idempotency_key) DO NOTHING`. Retried requests with the same key return the existing job state without re-enqueueing execution.
* **Verification**: Stress tests submit 100 concurrent requests with identical idempotency keys, confirming only 1 job is created.

---

### I4 — Per-Wallet Entry Ordering

> **Statement**: For any wallet $W$, its `ledger_entries` are totally ordered by `id` (`BIGSERIAL`), and that order reflects causal transaction sequence.

* **Enforcement Mechanism**:
  1. `BIGSERIAL` primary keys assign monotonically increasing IDs at commit time on the shard primary.
  2. Row-level `FOR UPDATE` locks serialize concurrent transfers affecting the same wallet.
* **Verification**: Automated audit scripts verify that replaying entries in `id` order reconstructs valid non-negative balance histories.

---

### I5 — Per-Merchant Webhook Ordering

> **Statement**: Webhook notifications for a given merchant are attempted in the exact order their source events occurred in the transactional outbox.

* **Enforcement Mechanism**:
  1. Outbox relay publishes events to Kafka's `notify` topic using `merchant_id` as the partition key.
  2. Kafka guarantees single-consumer partition assignment per consumer group, ensuring webhooks for a given merchant are processed in strict sequence.
* **Verification**: Integration tests publish outbox event sequences across multiple merchants and verify in-order delivery at merchant endpoints.

---

### I6 — Immutable History

> **Statement**: Once a row exists in `ledger_entries` or `events`, it is never updated or deleted by application code.

* **Enforcement Mechanism**:
  1. PostgreSQL role permissions restrict the application user from running `UPDATE` or `DELETE` on `ledger_entries` and `events` (the outbox relay is only permitted to update `published_at` on `events`).
  2. Financial adjustments or corrections are executed by appending new debit/credit entries rather than modifying historical rows.
* **Verification**: Database privilege audits and unit tests verify that attempted mutation queries fail at the database level.

---

### I7 — Job Termination

> **Statement**: Every accepted job reaches a terminal state (`completed`, `failed`, or `dlq_entries`) within bounded time, or is observably stuck via system metrics.

* **Enforcement Mechanism**:
  1. Per-message process deadlines (`REQUEST_TIMEOUT_MS`) limit in-flight processing.
  2. Token Bucket Retry Budgets fail fast to the Dead Letter Queue (DLQ) when downstream dependencies are distressed, preventing infinite retry loops.
* **Verification**: Chaos tests inject simulated database outages and network drops to verify that all queued jobs terminate gracefully.

---

### I8 — Recoverable DLQ Entries

> **Statement**: Every job or webhook that exhausts its retry budget or encounters a unrecoverable error is persisted to `dlq_entries` with full context, payload, error classification, and tracing metadata.

* **Enforcement Mechanism**:
  1. Failing consumers insert a record into `dlq_entries` before committing the Kafka message offset.
  2. The Admin API provides endpoints (`POST /v1/admin/dlq/{id}/replay`) to safely re-inject DLQ payloads.
* **Verification**: Automated DLQ tests verify payload completeness and test replay execution against recovered endpoints.

---

### I9 — Tenant Isolation

> **Statement**: No merchant can observe or mutate another tenant's wallets, transfers, or jobs.

* **Enforcement Mechanism**:
  1. The API Gateway extracts the authenticated `merchant_id` from the JWT `sub` claim.
  2. Before inserting a job, the gateway verifies that `from_wallet.merchant_id == jwt.sub`. Requests targeting unowned wallets are rejected with `403 Forbidden`.
* **Verification**: Multi-tenant security tests submit cross-tenant wallet transfers and assert `403` rejection.
