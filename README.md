# RRQ — A Payment Processing Core

RRQ moves value between wallets and stays correct while doing it: through worker
crashes, network partitions, and duplicate retries. It is the part of a payment
platform that silently loses money when it's built wrong — built here as a
**closed-loop, double-entry ledger on one logical Postgres**, where every
transfer is a **single serializable transaction**, messaging rides a
**transactional outbox into Kafka**, and a nightly **reconciliation** job proves
the books balance.

It is built in **Go first**, with a **Rust** port as a controlled language
study: same infrastructure, same invariants, same test suite, so the only
variable is the language.

> **Status — design complete, implementation not started.**
> The full system is specified in [`docs/`](docs/): six services plus an outbox
> relay, nine named correctness invariants, an explicit failure-mode analysis,
> and a simulation harness that exercises the whole pipeline without real
> merchants. The repository tree (`proto/`, `migrations/`, `v-go/`, `k8s/`, …)
> is laid out but the schemas, migrations, manifests, and CI are not written
> yet. See [`STATUS.md`](STATUS.md) for the exact, per-component state — it does
> not round up.

---

## Why it exists

Every payment system has a story: a retry path that double-charges, a worker
that debits one wallet and dies before crediting the other, a reconciliation gap
that surfaces weeks later. These are not exotic — they are the *default*
behavior of a distributed system built without specific countermeasures.

RRQ is built *with* the countermeasures, and nothing else. The decisive design
choice is scoping it as a **closed-loop ledger on one logical Postgres**: because
both wallets in any transfer live in the same database, a transfer is *one
transaction*, which deletes a whole category of machinery (sagas, compensations,
distributed leases, in-flight recovery state) rather than building elaborate
mechanisms to survive it. Every component earns its place by handling a named
failure mode:

| Failure mode | Mechanism |
| --- | --- |
| Partial completion mid-operation | **One serializable transaction** — both debit and credit legs commit together or not at all |
| Duplicate retries | **Durable idempotency key** — Postgres `UNIQUE (merchant_id, idempotency_key)`, no cache to lose it |
| Concurrent access to a wallet | **In-transaction row lock** — `SELECT … FOR UPDATE`, released at commit; no distributed lease |
| Silent integrity drift | **Event-sourced ledger** + nightly **reconciliation** |
| Unhealthy downstreams | **Circuit breakers**, jittered backoff, **DLQ** |

The full problem statement and the failure-to-mechanism mapping are in
[`docs/01-PROBLEM.md`](docs/01-PROBLEM.md).

---

## What it guarantees

Nine invariants, each stated precisely enough to be tested and adversarially
validated:

1. **Conservation of value** — every transfer is exactly one debit and one credit
   of equal magnitude, written atomically.
2. **No negative balances** on active wallets.
3. **At-most-once execution per idempotency key** — retry a million times, the
   operation happens once.
4. **Per-wallet entry ordering** — a wallet's history is reconstructable by replay.
5. **Per-merchant webhook ordering** — notifications arrive in the order events occurred.
6. **Immutable history** — postings and events are never mutated; corrections are new rows.
7. **Job termination** — every job reaches a terminal state in bounded time, or
   is observably stuck.
8. **Recoverable DLQ** — messages that exhaust retries are persisted with full
   context, never dropped.
9. **Tenant isolation** — cross-tenant access is rejected at the gateway before
   any work is enqueued.

How each is enforced and validated: [`docs/02-INVARIANTS.md`](docs/02-INVARIANTS.md).

---

## Architecture

```mermaid
graph TD
    merchant["Merchant System"]
    kong["Kong — edge gateway<br/>TLS · coarse JWT check · per-merchant rate limiting"]
    gateway["API Gateway<br/>JWT auth · wallet-ownership authz · validation<br/>INSERT jobs + job.requested outbox (one txn) → 202 Accepted"]
    postgres["PostgreSQL — one logical ledger, append-only source of truth<br/>ledger_entries (the money) · jobs · transfers<br/>events (fact log + outbox) · webhook_deliveries · dlq_entries · wallets"]
    relay["Outbox Relay<br/>publishes unpublished events → Kafka, in id order"]
    kafka["Kafka<br/>topic jobs (groups: ledger-workers, fraud-workers)<br/>topic notify (partitioned by merchant_id)"]
    ledgerWorker["Ledger Worker<br/>one SERIALIZABLE txn per transfer:<br/>lock wallets (FOR UPDATE) · check balance · post debit+credit<br/>UNIQUE(transfer_id, leg) ⇒ redelivery is a no-op · no saga"]
    fraudWorker["Fraud Worker — detective, non-blocking · ≥2 replicas<br/>velocity in shared atomic Redis (order-insensitive) · auto-freeze"]
    webhookWorker["Webhook Worker<br/>per-merchant FIFO via Kafka partitions · HMAC payloads<br/>jittered backoff · per-merchant breaker · DLQ at 10 attempts"]
    redisState["Redis — ephemeral, non-correctness-critical<br/>velocity sorted sets · circuit-breaker state"]
    merchantEndpoint["Merchant Endpoint"]
    reconciliation["Reconciliation — nightly CronJob<br/>replay ledger_entries vs. cached balances · global SUM = 0<br/>reconciliation.alert on any divergence"]
    adminDashboard["Admin Dashboard<br/>DLQ replay · wallet freeze · breaker reset · consumer lag · stuck jobs"]

    merchant -->|"HTTPS POST + Idempotency-Key"| kong
    kong -->|"rate-limit OK · forward"| gateway
    gateway -->|"INSERT jobs ON CONFLICT + outbox (one txn)"| postgres

    relay -->|"read unpublished events"| postgres
    relay -->|"produce"| kafka

    kafka -->|"consume job.requested (ledger-workers)"| ledgerWorker
    kafka -->|"consume job.requested (fraud-workers)"| fraudWorker
    kafka -->|"consume transfer.* (one consumer per partition)"| webhookWorker

    ledgerWorker -->|"post legs + transfer.completed outbox (one txn)"| postgres
    fraudWorker -->|"velocity sorted set (Lua)"| redisState
    fraudWorker -->|"freeze wallet · INSERT events"| postgres
    webhookWorker -->|"GET/SET breaker:{merchant}"| redisState
    webhookWorker -->|"INSERT webhook_deliveries · dlq_entries"| postgres
    webhookWorker -->|"HTTPS POST X-RRQ-Signature · X-RRQ-Event-Id"| merchantEndpoint

    postgres -->|"replay ledger (read-only)"| reconciliation
    reconciliation -->|"INSERT reconciliation.alert"| postgres
    adminDashboard -->|"SELECT · audit events"| postgres
    adminDashboard -->|"breaker state"| redisState
```

Six services plus the outbox relay, three stateful backends, Kong at the edge.
Kong owns generic edge work (TLS, a coarse JWT check, rate limiting); the custom
gateway owns the part no off-the-shelf component does — the durable idempotency
claim and the durable hand-off into the ledger. **The single durable write on
the request path is one Postgres transaction** (the `jobs` row plus the
`job.requested` outbox event); everything past it is asynchronous and
crash-recoverable. **Every correctness guarantee is enforced in Postgres**, by
transactions, row locks, and unique constraints — Redis holds only ephemeral
fraud/breaker state and no invariant depends on it.

Every box runs as **≥2 live instances** with automatic failover: RRQ is
horizontally scalable and highly available, not a single-process design. The
stateless tier scales out by adding replicas; the authoritative ledger is one
logical Postgres (primary + standby) scaled vertically — deliberately *not*
sharded, because sharding would force money movements across a transaction
boundary and reintroduce the very machinery this design removes. The full
scale-out and HA model is
[`docs/03-SCALING-AND-AVAILABILITY.md`](docs/03-SCALING-AND-AVAILABILITY.md).

Full system in one read, with success/failure/retry sequence diagrams:
[`docs/00-OVERVIEW.md`](docs/00-OVERVIEW.md). Per-service designs live in
[`docs/services/`](docs/services/).

In production there is no real merchant on either side. The simulated outside
world that drives traffic in and receives webhooks — including a synthetic
end-user population — is `merchant-sim`, specified in
[`docs/services/17-SIMULATION-HARNESS.md`](docs/services/17-SIMULATION-HARNESS.md).

---

## Two implementations, Go first

Building the same system twice is the method, not an indulgence: it turns claims
about each language into demonstrations. The sequence is deliberate — **Go ships
first**, driven to a deployed, tested, demonstrable state. Rust follows as a
comparison study with the working Go system as its reference. Building both
before either runs is the surest way to ship neither.

- **Go** is the reference: chi for routing, the posting path as one
  `SERIALIZABLE` transaction over `pgx`, `FOR UPDATE SKIP LOCKED` for the outbox
  relay and webhook retry claiming, `sony/gobreaker` for circuit breaking.
- **Rust** explores what the type system buys for correctness-critical code:
  money and wallet ids as distinct newtypes (a debit and a credit can't be
  swapped by accident), the double-entry posting expressed so an unbalanced
  transfer won't compile, a Tower-layer circuit breaker, and deterministic
  failure-injection testing with **turmoil**.

Both target identical infrastructure, uphold identical invariants, and pass an
identical end-to-end suite. Because `merchant-sim` talks to RRQ only over HTTP,
the same scenarios run unchanged against either binary — itself part of the
comparison. The benchmark that actually separates the runtimes is the
**reconciliation batch** (CPU-bound, parallelizable); HTTP throughput saturates
the network long before the language matters.

---

## Repository layout

| Path | Purpose |
| --- | --- |
| [`docs/`](docs/) | System design: overview, problem, the nine invariants, per-service docs |
| [`proto/`](proto/) | Protobuf event and gRPC contracts *(placeholder)* |
| [`migrations/`](migrations/) | PostgreSQL schema — tables, indexes, constraints that uphold the invariants *(placeholder)* |
| [`v-go/`](v-go/) | Go reference implementation — six services + outbox relay + shared package *(placeholder)* |
| [`v-rust/`](v-rust/) | Rust comparison study *(Cargo workspace scaffolded)* |
| [`tools/merchant-sim/`](tools/merchant-sim/) | Simulated merchant: traffic driver, webhook receiver, end-user population, scenario engine *(placeholder)* |
| [`k8s/`](k8s/) | Kubernetes manifests — the deployment target *(placeholder)* |
| [`scripts/`](scripts/) | k6 benchmarks, seed and Prometheus config *(placeholder)* |
| [`benchmarks/`](benchmarks/) | Results, populated when the suite runs |
| [`Makefile`](Makefile) | Developer entry point — `make help` lists targets |
| [`STATUS.md`](STATUS.md) | Honest, per-component project state |

---

## Quick start

> Targets are wired to the design but not yet functional — the services don't
> exist. `make help` lists everything; today most targets report what they will
> do once their component is built.

```bash
make dev       # local kind cluster (dev overlay)
make migrate   # apply schema migrations
make build     # build the Go services
make test      # Go test suite with -race, including the scenario suite
make sim       # run merchant-sim in steady mode against the local stack
```

Local observability consoles (Jaeger `:16686`, Prometheus `:9090`, Grafana
`:3000`) come up with the dev stack. With `merchant-sim` running in steady mode,
the Admin Dashboard shows live merchants, moving balances, posting transfers, and
arriving webhooks — the stack behaves like a running system, not an idle one.

---

## Non-goals

Scope discipline matters more than ambition.

- **Not a complete payment platform** — no card networks, bank rails, KYC/AML, FX
  pricing, PCI-DSS, or multi-region. RRQ is the correctness-critical *core*.
- **One logical ledger, not a sharded one** — RRQ is horizontally scalable and
  highly available *within* a region (≥2 of every component, automatic failover),
  but it keeps a single authoritative Postgres ledger so every transfer stays one
  transaction. Reads and the stateless tier scale out; the ledger scales
  vertically. Sharding the write ledger, spanning regions, and active-active are
  out of scope — each would reintroduce cross-boundary money movement. The
  ~1,000 transfers/sec figure is the size the benchmarks prove on a small
  cluster, not a ceiling.
- **Not a research artifact** — every pattern is drawn from existing practice
  (double-entry bookkeeping, event sourcing, the transactional outbox; the same
  single-atomic-posting model used by ledgers like TigerBeetle and Formance). The
  contribution is the rigor with which they're composed and demonstrated.

---

## License

[MIT](LICENSE).
