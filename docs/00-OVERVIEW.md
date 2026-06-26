# 00: Overview

The whole system in one read — what RRQ is, the problem it solves, and how the pieces fit. If you read one document, read this.

## What RRQ is

RRQ is the correctness-critical **core** of a payment system: merchants tell it "move 5,000 NGN from wallet A to wallet B" and it executes that durably, without losing money or paying twice. It is a **closed-loop ledger** — value enters when an operator funds a wallet and only ever moves *between wallets inside the system*; there is no bank or card off-ramp.

It deliberately leaves out everything that isn't the hard correctness core: no custody of real funds, no card-network or bank integration, no KYC/AML, no FX. Those are years of regulatory and integration work, and none of them is where money is silently lost to a race or a crash. RRQ is the engine underneath, built to be right under failure.

## Why it's hard

On one machine, a transfer is a database transaction — three lines, ACID, done. RRQ is hard because the world is hostile: workers crash mid-operation, networks drop responses, merchants retry, brokers redeliver. The root cause of almost all of it is the **unknown outcome**: when a network call gets no response, you cannot tell whether it succeeded, failed, or never arrived. You can't engineer that away; you design so it doesn't matter.

A naive processor — read balance, debit A, credit B, return `200` — breaks in five concrete ways, each a real incident:

| # | Failure | What goes wrong |
| --- | --- | --- |
| A | Crash between debit and credit | Money left A, never reached B — gone, unreconstructable |
| B | Two transfers on A run concurrently | Both read the old balance; one debit is lost (lost update) |
| C | Merchant retries a lost response | The transfer runs twice; the customer is paid double |
| D | An off-by-one ships | Ledger drifts silently until the month-end close |
| E | A downstream endpoint hangs | Workers pile up on doomed retries and cascade |

These aren't avoidable — in a busy system they happen constantly. The design question is how to structure things so each is *automatically* prevented or recovered. That structure is RRQ.

## The ideas that answer them

Each failure maps to a mechanism. This table is the rosetta stone — every component is just one of these made concrete:

| Failure | Mechanism |
| --- | --- |
| **A. Partial completion** | **Atomic double-entry.** The debit and credit are two `ledger_entries` rows in **one serializable transaction** — they commit together or not at all, so a half-done transfer can't exist. A crash rolls back; the job is redelivered and re-run. |
| **B. Concurrent operations** | **In-transaction row lock.** `SELECT … FOR UPDATE` on both wallet rows (ordered by id), held for the whole check-and-post. No distributed lease, no TTL. |
| **C. Duplicate requests** | **Durable idempotency key.** `UNIQUE (merchant_id, idempotency_key)` inserted with `ON CONFLICT DO NOTHING`. First request wins; retries get the cached result. At most once per key, in Postgres — not a cache. |
| **D. Silent integrity bugs** | **Immutable ledger + reconciliation.** Balances are *derived* by summing append-only postings; a nightly job re-derives and checks them. Any drift is an alert, never a silent fix. |
| **E. External unavailability** | **Circuit breakers, backoff with jitter, DLQs.** Stop calling a dead endpoint, spread retries, and park terminally-failed work where a human can see it. |

Two more ideas tie these together:

- **At-least-once delivery + idempotent handlers.** A broker can't give exactly-once (impossible in general). Kafka delivers at least once; a `UNIQUE` constraint turns any duplicate into a no-op. The combination is *effectively* exactly-once — the only correct answer.
- **The transactional outbox.** Never "write to the DB, then publish to Kafka" — a crash between the two loses the message. The message is written to the `events` table *in the same transaction* as the state change, and a relay publishes it afterward. A fact and its notification are equally durable.

And one people get wrong: a Kafka **consumer group gives you delivery, not order** — two replicas can process two messages for one key at once. Where order matters (a merchant's webhooks) RRQ pins the key to a Kafka partition so exactly one worker owns it. Where it only *looks* like it matters (fraud velocity) the computation is order-insensitive and needs no ordering at all.

## The closed loop, and the one place a saga appears

The decisive scoping choice is the closed loop: because RRQ never settles to an external bank, a transfer moves value between two wallets that can share a transaction — so it is **one transaction**, not a multi-step saga with compensating undos. That deletes a whole category of machinery (sagas, distributed locks, in-flight recovery state). The rule is simply: **if a transaction can cover it, use a transaction.**

A saga returns in exactly one place. The ledger is **sharded by merchant** for write scale, so a transfer between two *different* merchants crosses a shard boundary a single transaction can't span. Those cross-shard transfers use a clearing-account two-phase protocol with compensation — a saga used only where a transaction genuinely cannot reach, and nowhere else. The common transfer (a customer paying its own merchant, same shard) stays one transaction.

## The merchant's view

Merchants use a small HTTP API; two endpoints carry essentially all traffic:

```
POST /v1/transfers   ── move value between two wallets
POST /v1/payouts     ── many transfers as one batch (bulk payout)
```

A `POST /v1/transfers` (carrying an `Idempotency-Key` header) returns almost immediately:

```http
HTTP/1.1 202 Accepted
{ "job_id": "job_01HQX4...", "status": "pending" }
```

It's `202`, not `200`: the transfer hasn't happened yet — it's been *durably accepted*. The response is fast because the gateway's job is to persist work, not to wait for it. The merchant learns the outcome from a signed **webhook** (`transfer.completed` / `transfer.failed`) under a second later, or by polling `GET /v1/jobs/<id>`. That is the whole surface; everything else exists behind it to make it correct under failure.

## Architecture at a glance

```mermaid
graph TD
  merchant["Merchant System"] -->|HTTPS request| kong["Kong, edge gateway<br/>TLS · JWT precheck · rate limit"]
  kong -->|routes /v1| gateway["API Gateway<br/>(auth, validation, idempotency)"]
  gateway -->|"INSERT jobs + job.requested (one txn)"| db[("PostgreSQL — sharded by merchant<br/>source of truth · append-only postings")]
  relay["Outbox Relay<br/>(publishes events table → Kafka)"] -->|read unpublished| db
  relay -->|job.requested| jobsTopic["Kafka topic: jobs<br/>group: ledger-workers<br/>group: fraud-workers"]
  relay -->|transfer.completed/failed| notifyTopic["Kafka topic: notify<br/>partitioned by merchant_id"]
  jobsTopic --> ledgerWorker["Ledger Worker<br/>(one serializable txn per transfer)"]
  jobsTopic --> fraudWorker["Fraud Worker<br/>(velocity, detective)"]
  ledgerWorker -->|"post legs + transfer.completed (one txn)"| db
  fraudWorker -->|freeze wallet| db
  notifyTopic --> webhookWorker["Webhook Worker<br/>(per-merchant ordering, retry, breaker)"]
  webhookWorker -->|HTTPS| merchantEndpoint["Merchant Endpoint"]

  reconciliation["Reconciliation<br/>(nightly batch)"] -.->|reads & compares| db
  adminDashboard["Admin Dashboard"] -.->|lag, breaker, DLQ replay, freeze| db
```

Kong sits at the edge (TLS, coarse JWT check, rate limit). The custom **API Gateway** does the part no off-the-shelf gateway can: the durable idempotency claim and the hand-off into the ledger. Two paths:

- **Synchronous (the request path):** Merchant → Kong → Gateway → one Postgres transaction (insert the `jobs` row + the `job.requested` outbox event) → `202`. That commit is the durability boundary. Nothing else is in the path.
- **Asynchronous (everything else):** the relay publishes the outbox to Kafka; the Ledger Worker posts each transfer in one transaction; webhooks go out. None of it blocks the merchant's call.

**Every box is a fleet of ≥2 instances** — there is no "the ledger worker," only the ledger workers, and the same for every backend (primary + standby, automatic failover). You add throughput by adding replicas (and shards); you survive the loss of any single pod, node, or primary because a peer takes over in seconds. Where replicas could race, the database serializes them (a row lock, a unique constraint, a partition assignment) — never anything in process memory.

## The happy path, once

```mermaid
sequenceDiagram
    autonumber
    participant M as Merchant
    participant API as API Gateway
    participant DB as Postgres (merchant's shard)
    participant RL as Outbox Relay
    participant K as Kafka
    participant LW as Ledger Worker
    participant WW as Webhook Worker

    M->>API: POST /v1/transfers (Idempotency-Key)
    API->>DB: BEGIN; INSERT jobs ON CONFLICT DO NOTHING; INSERT job.requested (outbox); COMMIT
    API-->>M: 202 Accepted (job_id)

    RL->>DB: read unpublished events
    RL->>K: publish job.requested → jobs topic

    LW->>K: consume job.requested
    LW->>DB: BEGIN SERIALIZABLE
    Note over LW,DB: SELECT wallets FOR UPDATE (ordered)<br/>check balance · INSERT debit + credit legs<br/>UPDATE jobs completed · INSERT transfer.completed (outbox)
    LW->>DB: COMMIT
    LW->>K: commit offset

    RL->>K: publish transfer.completed → notify topic
    WW->>K: consume (assigned partition)
    WW->>M: POST signed webhook
    M-->>WW: 200 OK
    WW->>DB: record delivery
```

Notice: the API responds *before* any posting happens (the commit at step 2 is the durability boundary — once the `jobs` row exists, the system owns the work); the posting is **one atomic step** (both legs, the job's status, and the outbox notification commit together, or roll back with nothing to repair); the webhook is its own durability domain and retries independently.

A **failure** (insufficient balance, frozen wallet, currency mismatch) is not a crash to recover — it's a normal terminal outcome, still one transaction: no `ledger_entries` are written, so no money moved and there is nothing to undo (conservation holds trivially). A **retry** conflicts on the idempotency key and returns the original `job_id`, so the transfer happens at most once.

## The services, one line each

- **API Gateway** — auth (JWT), wallet-ownership check, validation; inserts the `jobs` row + `job.requested` outbox event in one transaction, returns `202`. The only synchronous component.
- **Outbox Relay** — drains the `events` table to the right Kafka topic in id order. The single bridge from Postgres to Kafka.
- **Ledger Worker** — the money mover: posts each transfer as one serializable transaction (lock both wallets, check balance, write both legs, finish the job, enqueue the notification). Crash-safe by construction, idempotent via `UNIQUE(transfer_id, leg)`.
- **Webhook Worker** — signed notifications over the Kafka `notify` topic, partitioned by `merchant_id` so per-merchant order holds across replicas; backoff with jitter, a per-merchant breaker, DLQ on terminal failure.
- **Fraud Worker** — detective control: watches for velocity anomalies and freezes suspect wallets; the count is order-insensitive, so it load-balances freely. Doesn't gate transfers.
- **Reconciliation** — nightly batch: re-derives balances from the postings and checks conservation and the cache; any drift is an alert, never a silent fix.
- **Admin Dashboard** — operator surface: DLQ replay, consumer lag, breaker state, freeze/unfreeze. Not in the request path.

## The data backends

- **Postgres** — the ledger, **sharded by merchant** (each shard HA: primary + standby, synchronous replication). Holds the postings (`ledger_entries`, the source of truth), `jobs`/`transfers`, the `events` outbox, webhook deliveries, and the DLQ. **Every correctness guarantee is enforced here**, by transactions, row locks, and unique constraints.
- **Kafka** — the broker, fed only by the outbox relay. Topic `jobs` (consumed by the Ledger and Fraud workers) and topic `notify` (partitioned by `merchant_id` for webhook ordering).
- **Redis** — ephemeral, non-correctness-critical: fraud velocity counters and webhook breaker state. **No invariant depends on Redis.**

## What "correct" means

"Correct" means a specific list of invariants holds at all times — testable statements, not slogans: conservation of value, no negative balances, at-most-once execution per idempotency key, per-wallet posting order, per-merchant webhook order, immutable history, and tenant isolation. Each is spelled out with the mechanism that enforces it, and tested adversarially.

## Out of scope (deliberately)

Not PayPal: no custody, settlement to banks, card networks, KYC/AML, FX, disputes, or fraud ML — each a project of its own. Not multi-region, not active-active, not globally ordered. The ledger *is* sharded for write scale, but staying single-region is a deliberate boundary, not an oversight. The ~1,000 TPS benchmark figure is what a small cluster proves, not a design ceiling.

## Where to read next

- The testable correctness statements → [`01-INVARIANTS.md`](01-INVARIANTS.md)
