# 02: Architecture

The system topology, service responsibilities, data flow, and the sharding model that distinguishes intra-shard transfers from cross-shard sagas.

---

## System topology

RRQ is a set of stateless services in front of sharded Postgres, Kafka, and Redis. Every stateless component runs at least two replicas; there is no single point of failure. The data backends are self-hosted in-cluster so the development environment mirrors production exactly.

| Service         | Role                                                                    | Replicas        |
| --------------- | ----------------------------------------------------------------------- | --------------- |
| API Gateway     | Synchronous HTTP frontend — auth, validation, durable idempotency claim | ≥2, HPA         |
| Outbox Relay    | Per-shard bridge from Postgres events table to Kafka topics             | ≥2, KEDA on lag |
| Ledger Worker   | The money mover — posts transfers as serializable transactions          | ≥2, HPA on lag  |
| Webhook Worker  | Signed notifications to merchants, partitioned by merchant_id           | ≥2, KEDA on lag |
| Fraud Worker    | Detective velocity checks — order-insensitive, shared Redis counters    | ≥2              |
| Reconciliation  | Nightly batch — re-derives balances, checks conservation                | CronJob         |

### Data backends

- **Postgres** — the ledger, sharded by merchant. Holds postings (`ledger_entries`), jobs, transfers, the events outbox, webhook deliveries, and the DLQ. Every correctness guarantee is enforced here by transactions, row locks, and unique constraints. Each shard is a CloudNativePG primary + standby with synchronous replication.
- **Kafka** — the broker, fed only by the outbox relay (events table → Kafka). Two topics: `jobs` (consumed by Ledger and Fraud workers) and `notify` (partitioned by `merchant_id` for webhook ordering).
- **Redis** — ephemeral and non-correctness-critical: fraud velocity counters and webhook circuit-breaker state. No invariant depends on Redis.

### Request flow

**Synchronous path** (HTTP → durable acceptance):

1. Kong terminates TLS, validates JWT, enforces per-merchant rate limits.
2. API Gateway verifies wallet ownership, validates the request body.
3. One Postgres transaction inserts the `jobs` row and a `job.requested` outbox event. The idempotency key (`UNIQUE(merchant_id, idempotency_key)`) provides at-most-once guarantees.
4. Returns `202 Accepted` with `job_id`. The merchant never waits for execution.

**Async path** (Postgres → Kafka → worker → Postgres):

1. The outbox relay polls `events` for unpublished rows (ordered by `events.id`) and produces each to the correct Kafka topic.
2. Ledger Worker consumes `jobs`, resolves the merchant's home shard, posts both legs in one serializable transaction (`SELECT … FOR UPDATE`, balance check, debit + credit legs, outbox event). Idempotent via `UNIQUE(transfer_id, leg)`.
3. Webhook Worker consumes `notify` (partitioned by `merchant_id`), delivers signed HTTPS notifications with backoff, jitter, a per-merchant circuit breaker, and a DLQ on terminal failure.

**The outbox pattern** is central: facts are never written to the DB and then separately published to Kafka. The outbox event is written in the same transaction as the state change, so a crash between the two is impossible. The outbox relay publishes from the events table in id order — a fact and its notification are equally durable.

---

## Ledger sharding

The ledger is partitioned into N independent shards, each a strongly-consistent Postgres holding a disjoint set of merchants. The shard key is **merchant_id** (via consistent hashing over a ring of virtual nodes), so every wallet a merchant owns — customer wallets, the merchant's own wallet, and a per-shard clearing wallet — co-locates on one shard.

### Intra-shard transfer (the common path)

A transfer whose source and destination wallets belong to merchants on the same shard. This is the majority of transfers and remains a single serializable transaction:

1. Route the job to the merchant's home shard.
2. One serializable transaction: `SELECT … FOR UPDATE` both wallet rows (ordered by id), check the source balance, post debit and credit legs, update `jobs`, insert the outbox event.
3. `UNIQUE(transfer_id, leg)` makes a redelivered job a no-op.

Nothing about sharding touches this path. The shard is just a smaller Postgres than before, and per-wallet row locking is the entire concurrency story within it.

### Cross-shard transfer (the clearing protocol)

A transfer whose source and destination wallets live on different shards. A single transaction cannot span both shards, so RRQ uses a two-phase clearing protocol with compensation — a saga used only where a transaction cannot reach.

**Phase 1 — debit on source shard (one local transaction):**

- Debit source wallet, credit the source shard's clearing wallet.
- Insert a `cross_shard_transfer` row with state `pending`.
- Emit `xshard.transfer.requested` via the outbox.

**Phase 2 — credit on destination shard (one local transaction):**

- The outbox relay delivers the intent to the destination shard via Kafka. Idempotent on `transfer_id`.
- Debit the destination shard's clearing wallet, credit the destination wallet.
- Emit `xshard.transfer.confirmed` via the outbox.

**Settlement:** on receiving `confirmed`, the source shard marks the transfer `completed`.

**Compensation:** if the destination cannot post (closed or frozen wallet), it emits `xshard.transfer.rejected`. The source shard reverses in one local transaction — credit source wallet, debit clearing wallet — and marks the transfer `reversed`. No lock is ever held across the network.

---

## Service interactions

Each service implements port interfaces for its dependencies (Postgres, Kafka, Redis) and wraps them with observability decorators for metrics, tracing, and resilience.

### Key implementation locations

| Area                      | File/Directory                                                           | Language |
| ------------------------- | ------------------------------------------------------------------------ | -------- |
| API Gateway               | `services/go-services/core-api/cmd/`                                     | Go       |
| Ledger Worker             | `services/go-services/ledger-worker/cmd/`                                | Go       |
| Webhook Worker            | `services/go-services/webhook-worker/cmd/`                               | Go       |
| Fraud Worker              | `services/go-services/fraud-worker/cmd/`                                 | Go       |
| Reconciliation            | `services/go-services/recon-worker/cmd/`                                 | Go       |
| Outbox Relay              | `services/go-services/outbox-relay/cmd/`                                 | Go       |
| Shard routing             | `services/go-services/internal/platform/`                                | Go       |
| Cross-shard logic         | `services/go-services/ledger-worker/internal/adapter/outbound/postgres/` | Go       |
| Kafka producers/consumers | `services/go-services/internal/platform/kafka.go`                        | Go       |
| Rust prototype (WIP)      | `services/rust-services/`                                                | Rust     |

### Image Naming Convention

Container images use a `-go` suffix to differentiate from future Rust implementations:

- `rrq/core-api-go`, `rrq/outbox-relay-go`, etc.
- When Rust versions are ready: `rrq/core-api-rust`, `rrq/outbox-relay-rust`
