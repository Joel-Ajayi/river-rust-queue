# RRQ — A Payment Processing Core

A correctness-critical payment engine that moves value between wallets while
surviving worker crashes, network partitions, and duplicate retries — built in
parallel implementations in **Go** and **Rust** to compare what each language
gives you when you take distributed-systems correctness seriously.

> **Project status: design complete, implementation in progress.**
> A full system design exists in [`docs/`](docs/), with eight named
> correctness invariants and an explicit failure-mode analysis. The code
> scaffolding (proto schemas, SQL migrations, Docker development environment,
> CI) is in place. Service implementations are not yet written.
> See [`STATUS.md`](STATUS.md) for the precise state.

---

## The problem this exists to solve

In 2012, Knight Capital Group lost $440 million in 45 minutes because a
deployment left old code on one of eight servers, and that one server
interpreted incoming orders using stale logic. By the time the team understood
what was happening, the firm was insolvent.

The Knight Capital incident is vivid but not unusual. Every payment company
has its own version: a retry path that double-charges customers, a
reconciliation gap that takes weeks to surface, a worker that crashes after
debiting a wallet but before crediting the recipient, leaving money in
nobody's hands. These are not exotic failure modes. They are the _expected_
behavior of distributed systems built without specific countermeasures.

RRQ is the kind of system you build _with_ the countermeasures. Every
component exists because removing it would unhandle a specific named failure
mode. The patterns are not decoration: orchestrated sagas handle partial
completion, idempotency keys handle duplicate retries, distributed locks
handle concurrent access, event sourcing handles silent integrity drift,
circuit breakers handle external dependency failures, and a reconciliation
job verifies that the ledger and the event log have not diverged.

For the full problem statement and the failure-modes-to-mechanisms mapping,
read [`docs/01-PROBLEM.md`](docs/01-PROBLEM.md).

---

## What RRQ guarantees

Eight invariants, stated precisely enough to be tested:

1. **Conservation of value.** Every debit is paired with either a corresponding
   credit or a corresponding reversal. No floating debits or credits.
2. **No negative balances on active wallets.**
3. **At-most-once execution per idempotency key.** A merchant can retry a
   request a million times with the same key; the underlying operation
   happens once.
4. **Per-wallet event ordering.** Events for a wallet are causally ordered;
   the wallet's history can always be reconstructed by replay.
5. **Per-merchant webhook ordering.** A merchant sees their notifications in
   the order things happened.
6. **Immutable history.** Events are never updated or deleted. Corrections
   are new events.
7. **Saga termination.** Every saga reaches a terminal state in bounded time,
   or is observable as stuck by operational tooling.
8. **DLQ entries are recoverable.** Messages exhausting automatic retry are
   persisted with full context for operator replay, never silently dropped.

For the testable form of each invariant, including how they're enforced and
how they're validated, see [`docs/02-INVARIANTS.md`](docs/02-INVARIANTS.md).

---

## Architecture

```mermaid
graph TD
    merchant["Merchant System"]
    gateway["API Gateway<br/>JWT auth · SETNX idempotency (NX EX 86400) · structure validation<br/>returns 202 Accepted after XADD succeeds · never touches Postgres in hot path"]
    redisState["Redis — shared state<br/>idempotency cache (SETNX / GET / DEL)<br/>Redlock per wallet (SET NX PX 5000)<br/>velocity sorted sets · circuit breaker state"]
    jobStream["Job Stream — Redis Streams<br/>stream:jobs<br/>consumer-group: saga-workers<br/>consumer-group: fraud-workers"]
    notifyStream["Notify Stream — Redis Streams<br/>stream:notify-{0..15}<br/>sharded by hash(merchant_id) mod 16<br/>consumer-group: webhook-workers"]
    sagaWorker["Saga Worker<br/>Validate → AcquireLock → Debit → Credit → Complete<br/>Redlock on wallet pairs (sorted IDs) · XAUTOCLAIM crash recovery<br/>idempotent steps: UNIQUE(saga_id, step_name) · compensation saga on failure"]
    fraudWorker["Fraud Worker — detective control (non-blocking)<br/>reads JobRequested events · does not gate transfers<br/>two-level dispatch: outer consumer + lazy per-wallet goroutines/tasks<br/>Lua: ZADD · ZREMRANGEBYSCORE · ZCARD (sliding-window velocity)<br/>auto-freeze wallet when threshold exceeded"]
    webhookWorker["Webhook Worker<br/>per-merchant FIFO ordering (one consumer per shard)<br/>HMAC-SHA256 signed payloads · exponential backoff with full jitter<br/>circuit breaker per merchant · DLQ on retry exhaustion (10 attempts)"]
    postgres["PostgreSQL — append-only source of truth<br/>events · ledger_entries · saga_state<br/>wallets · webhook_deliveries · dlq_entries · merchants"]
    merchantEndpoint["Merchant Endpoint"]
    reconciliation["Reconciliation — nightly CronJob<br/>replays event log per wallet (streaming cursor, O(1) memory)<br/>derived balance vs. ledger_entries SUM<br/>emits ReconciliationAlert on any divergence"]
    adminCLI["Admin CLI (rrq)<br/>dlq inspect / replay · saga abort · wallet freeze / unfreeze<br/>circuit reset · stream lag · reconcile trigger · event search"]

    merchant -->|"HTTPS POST /v1/transfers + Idempotency-Key"| gateway
    gateway -->|"SETNX idemp:{merchant}:{key} NX EX 86400"| redisState
    gateway -->|"XADD stream:jobs * (JobRequested event)"| jobStream
    gateway -.->|"merchant status check (60s Redis cache · Postgres fallback)"| postgres

    jobStream -->|"XREADGROUP saga-workers<br/>XAUTOCLAIM on startup (reclaim crashed-worker messages)"| sagaWorker
    jobStream -->|"XREADGROUP fraud-workers"| fraudWorker

    sagaWorker -->|"SET NX lock:wallet:W PX 5000 (Redlock) · DEL on complete/compensate"| redisState
    sagaWorker -->|"INSERT events · ledger_entries · saga_state (per-step tx)<br/>SELECT saga_state FOR UPDATE (prevent concurrent execution)"| postgres
    sagaWorker -->|"XADD stream:notify-N (N = hash(merchant_id) mod 16)"| notifyStream

    fraudWorker -->|"Lua: ZADD · ZREMRANGEBYSCORE · ZCARD<br/>velocity:wallet:{id} sorted set"| redisState
    fraudWorker -->|"UPDATE wallets SET status=frozen WHERE status=active<br/>INSERT events (wallet.frozen)"| postgres

    notifyStream -->|"XREADGROUP webhook-workers (one consumer per shard)"| webhookWorker
    webhookWorker -->|"GET / SET breaker:{merchant_id}"| redisState
    webhookWorker -->|"INSERT webhook_deliveries · events · dlq_entries<br/>SELECT FOR UPDATE SKIP LOCKED (retry scheduler)"| postgres
    webhookWorker -->|"HTTPS POST X-RRQ-Signature: sha256=...<br/>X-RRQ-Event-Id for merchant dedup · 10s timeout"| merchantEndpoint

    postgres -->|"SELECT events · ledger_entries (streaming cursor, read-only)"| reconciliation
    reconciliation -->|"INSERT ReconciliationCompleted · ReconciliationAlert"| postgres

    adminCLI -->|"SELECT · UPDATE · INSERT operator.action audit events"| postgres
    adminCLI -->|"XINFO stream lag · GET circuit breaker state"| redisState
```

Six services, three stateful backends. Every arrow is intentional; every
component handles at least one named failure mode. For the full system in one
read — including the sequence diagrams for the success, failure, and retry
paths — see [`docs/00-OVERVIEW.md`](docs/00-OVERVIEW.md).

---

## Why two implementations

Building the same system in Go and Rust is not gratuitous. It's the
methodology by which the project's claims about each language are
_demonstrated_, not asserted.

The Go implementation is the reference. It uses the patterns Go-shop
engineering teams will recognize: chi for routing, an interface-based saga
step machine with runtime-enforced state transitions, `sync.RWMutex` and
`map[K]chan T` for per-key dispatch, `sony/gobreaker` for circuit breaking.

The Rust implementation explores what Rust's type system buys you for
correctness-critical code. The Saga state machine is encoded with the
**type-state pattern**: `Saga<Debited>` is a distinct type from
`Saga<Credited>`, and calling `credit()` on a `Saga<Init>` is a compile error,
not a runtime panic. The circuit breaker is a Tower middleware layer composed
into the HTTP client stack. Deterministic distributed-systems testing uses
**turmoil**, which simulates network failures, host crashes, and message
drops inside a single-threaded test runner — a category of test Go does not
have a direct equivalent for.

Both implementations target identical infrastructure, uphold identical
invariants, and pass an identical integration test suite. Where the languages
genuinely differ — saga state encoding, concurrency patterns, ecosystem of
testing tools — the differences are documented as observations, not
preferences. The benchmark suite measures them honestly: same hardware, same
warm-up, median of three runs, no GC tuning, no cherry-picking.

The Go-vs-Rust comparison is most informative for the **reconciliation
batch**, which is CPU-bound and parallelizable — the place where the two
runtimes actually differ. HTTP throughput benchmarks tend to saturate the
network long before the runtime matters; reconciliation does not.

---

## What's in the repo

| Path                                       | Purpose                                                                                                     |
| ------------------------------------------ | ----------------------------------------------------------------------------------------------------------- |
| [`docs/`](docs/)                           | Full system design — overview, problem, invariants, service docs, deep-dives, deferred features, appendices |
| [`proto/`](proto/)                         | Protobuf schemas: every event type, every internal gRPC contract                                            |
| [`migrations/`](migrations/)               | PostgreSQL schema: seven tables with the indexes and constraints that uphold the invariants                 |
| [`v-go/`](v-go/)                           | Go reference implementation (six services, shared package)                                                  |
| [`v-rust/`](v-rust/)                       | Rust comparison implementation (six services, shared crate)                                                 |
| [`k8s/`](k8s/)                             | Kubernetes manifests (documentation; not deployed in v1)                                                    |
| [`scripts/`](scripts/)                     | k6 benchmark scripts, seed scripts, Prometheus config                                                       |
| [`benchmarks/`](benchmarks/)               | Benchmark results (populated when the suite runs)                                                           |
| [`docker-compose.yml`](docker-compose.yml) | Local infrastructure: Postgres, Redis, Jaeger, Prometheus, Grafana                                          |
| [`Makefile`](Makefile)                     | The developer entry point (`make help` lists targets)                                                       |
| [`STATUS.md`](STATUS.md)                   | Honest, up-to-date project state                                                                            |

---

## Quick start, Yet Pending

Bring up the local infrastructure and run migrations:

```bash
make dev       # Start Postgres, Redis, Jaeger, Prometheus, Grafana
make migrate   # Apply schema migrations to local Postgres
```

Once services are implemented, individual builds and tests are:

```bash
make build     # Build both Go and Rust implementations
make test      # Run both test suites with -race / cargo test
make lint      # Vet, clippy, gofmt, rustfmt, buf lint
```

For the local infrastructure consoles:

- Jaeger UI: <http://localhost:16686>
- Prometheus: <http://localhost:9090>
- Grafana: <http://localhost:3000>

---

## Reading order

If you have 15 minutes: read [`docs/00-OVERVIEW.md`](docs/00-OVERVIEW.md). That's the whole system.

If you have an hour: read `00-OVERVIEW.md`, then `01-PROBLEM.md`, then `02-INVARIANTS.md`. After that you understand what RRQ is, why it exists, and what it promises.

If you're reviewing the project for a role and want to assess engineering depth: skim the three foundation docs, then pick a service doc from [`docs/services/`](docs/services/) (coming in Pass 2) and a deep-dive from [`docs/deep-dives/`](docs/deep-dives/) (coming in Pass 3) and read those in full. That's roughly 90 minutes and tells you what you need to know.

---

## Non-goals

Being explicit about scope matters more than being aspirational about it.

**RRQ is not a complete payment platform.** No card network integration, no
bank rails, no KYC/AML, no FX pricing, no PCI-DSS compliance, no multi-region
replication. RRQ is the correctness-critical _core_ of a payment platform —
the part that, if implemented wrong, silently loses money.

**RRQ is not optimized for global scale.** v1 targets 1,000 transfers/second
on a single machine. Real payment companies handle tens of thousands; the
techniques (database sharding, partitioned Redis, multi-region) are
well-understood and not the focus.

**RRQ is not a research artifact.** Every pattern in here is a working
engineer's tool drawn from existing literature — sagas (Garcia-Molina &
Salem, 1987), idempotency at Stripe scale, Redlock (Antirez, 2014),
event sourcing (Fowler, Vernon). The contribution, if any, is the rigor with
which they're composed and demonstrated.

---

## Why I'm building this

I'm a computer engineering student going deep on distributed systems because
the kind of engineering I want to do — payment infrastructure, ledger systems,
the boring correctness-critical guts of money movement — is judged on
exactly this kind of work. The project is small enough for one person to
build correctly and rigorous enough to demonstrate the craft.

The design docs are public, the implementation will be public, the
benchmarks will report whatever numbers come out, and the failure modes are
demonstrated with tests, not assertions.

— [Ayotunde Ajayi](https://github.com/Joel-Ajayi) · [LinkedIn](https://linkedin.com/in/yotstack)

---

## License

[MIT](LICENSE).
