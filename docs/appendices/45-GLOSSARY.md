# 45 — Glossary

> **What this is.** Reference for terminology used throughout the docs. Look up any term you encounter; find the canonical definition here.

---

## A

**ACK (Acknowledgment).** A consumer's signal to Redis Streams that a message has been processed and should be removed from the pending list. In RRQ, `XACK` is called after a saga step (or webhook delivery) commits to its source of truth.

**Aggregate.** In event sourcing, the entity an event is about. RRQ's aggregates are: wallet, saga, merchant, webhook. Each event has an `aggregate_type` and `aggregate_id` identifying which specific entity it describes.

**Append-only.** A storage discipline where rows are inserted but never updated or deleted. The events table is append-only. Enforced at the database permission level (the application's role lacks UPDATE/DELETE).

**At-least-once delivery.** A message-delivery guarantee where a message is delivered to its consumer one or more times. Combined with idempotent handlers, gives effective exactly-once semantics. RRQ's webhook delivery is at-least-once.

**At-most-once execution.** A processing guarantee where an operation happens zero or one times. Combined with retries, gives the property merchants care about: "my retry doesn't double-charge." RRQ's idempotency keys provide at-most-once execution per `(merchant_id, idempotency_key)`.

**Audit event.** An event recording an operator action. Type prefix `operator.*`. Includes the operator's identity, the command run, and the entity affected. Makes operator interventions visible in the same event log as system events.

---

## B

**Backoff.** A retry strategy where the delay between retries increases over time. RRQ uses exponential backoff with full jitter for webhook retries: `delay = random(0, base * 2^attempt)`.

**Bcrypt.** The hashing algorithm used to store API key hashes in the merchants table. Slow by design (10-12 work factor); resistant to brute force. The raw API key is never stored.

**BulkPayoutSaga.** A parent saga that spawns N independent sub-Transfer sagas, one per recipient. Each sub-saga is autonomous; the parent tracks completion counts.

---

## C

**Cache stampede.** A failure mode where many concurrent requests miss the cache at the same moment and all hit the underlying store, briefly overwhelming it. Mitigated in RRQ by randomized TTLs and (where applicable) single-flight patterns.

**Causation ID.** The event_id of the immediate predecessor in a chain of events. Lets reconstruction follow the causal graph of "this happened because of that." Distinct from correlation ID (which groups all events from a logical operation, not just immediate predecessors).

**Causal ordering.** Ordering that respects cause-effect relationships, without requiring strict wall-clock ordering. Per-wallet event ordering in RRQ is causal: a wallet's events are applied in the order they were committed, which reflects the causal sequence of changes to that wallet.

**CDC (Change Data Capture).** A pattern for propagating database changes to other systems by tailing the WAL. RRQ doesn't use CDC because the event store already serves this purpose — events are the canonical change stream.

**Circuit breaker.** A resilience pattern that fast-fails after consecutive failures, reducing wasted resources on a broken dependency. State machine: Closed → Open (on N failures) → Half-Open (after cooldown) → Closed (on success) or Open (on failure). RRQ uses per-merchant circuit breakers on webhook delivery.

**Compensating transaction.** In a saga, the step that semantically undoes a previously-completed step. Required to be idempotent. For RRQ's Debit step, the compensation is `compensation_credit` (write a credit entry that restores the source wallet).

**Compensation idempotency.** The property that running a compensation twice has the same effect as running it once. Enforced in RRQ by the `UNIQUE(saga_id, step_name)` constraint on ledger_entries; a duplicate compensation insert fails the constraint and the step recognizes it as already done.

**Configmap.** A Kubernetes object holding non-secret configuration (database hostnames, log levels, feature flags). Mounted into pods as environment variables.

**Consumer group.** A Redis Streams concept allowing multiple consumers to share a stream. Each message in the stream is delivered to exactly one consumer in the group. RRQ uses consumer groups (`saga-workers`, `fraud-workers`, `webhook-workers`) to scale workers horizontally.

**Correlation ID.** An identifier shared by all events from a single logical operation (typically a saga). In RRQ, the saga_id is the correlation ID. Lets queries find "all events for this saga."

**CQRS.** Command Query Responsibility Segregation. The pattern of separating write-side (commands) and read-side (queries) models. RRQ implements a soft CQRS: writes go through events; reads hit projections.

**CronJob.** A Kubernetes resource that runs a Job on a schedule. RRQ uses one for the nightly reconciliation run.

**Customer wallet.** A wallet owned by a merchant on behalf of an end-user. The merchant initiates transfers for the user; the user does not directly interact with RRQ.

---

## D

**Dashboard (Admin Dashboard).** The web UI used by operators to inspect and manage the system. Replaces the original CLI. Also serves as the demo surface for the project. Authenticated; not publicly accessible.

**DLQ (Dead Letter Queue).** A persistent destination for messages that exhausted automatic retry. In RRQ, the `dlq_entries` table. Surfaces work that needs human judgment.

**Detective control.** A control that observes operations after they happen and flags anomalies. Contrasts with preventative control (which gates operations before they happen). RRQ's fraud worker is detective.

**Distributed lock.** A coordination primitive shared across machines, used to ensure exclusive access to a resource. RRQ uses Redlock (a specific algorithm using Redis) to lock wallets during saga execution.

**Double-entry bookkeeping.** An accounting principle where every transaction is recorded as two entries (a debit and a credit) that sum to zero. RRQ's ledger follows this principle: every transfer produces a paired debit and credit; reconciliation verifies the sum.

**Drift.** The phenomenon where derived data diverges from its source. RRQ's reconciliation detects drift between the event log and the materialized ledger.

---

## E

**Eventual consistency.** A consistency model where reads may temporarily reflect stale data, but eventually catch up. RRQ's wallet_balance_cache is eventually consistent (lag typically < 1 second). Reads from the cache may be stale; reads from `ledger_entries` are always current.

**Event sourcing.** A pattern where state changes are recorded as immutable events, and the current state is derived by replaying events. RRQ uses event sourcing: events are the source of truth, ledger and other tables are projections.

**Event store.** The append-only database that holds events. In RRQ, the `events` table.

**Expand and contract.** A schema migration pattern: first expand (add the new structure alongside the old), then deploy code that writes to both / reads from new, then contract (remove the old structure). Allows zero-downtime migrations.

---

## F

**Fan-out.** A pattern where one input produces many outputs. RRQ's bulk payout uses fan-out: one parent saga spawns N sub-transfers.

**Fencing token.** A monotonically increasing number issued at lock acquisition. Used to detect stale lock holders (their token is older than the latest issued). RRQ doesn't implement fencing tokens in v1; storage-layer idempotency (via UNIQUE constraints) provides equivalent protection for its specific operations.

**Full jitter.** A backoff strategy where the delay is uniformly random within an exponentially-growing window. Specifically: `delay = random(0, base * 2^attempt)`. Maximally decorrelates retries; prevents thundering herd.

**Funding model.** How money enters and exits the system. See `44-FUNDING-MODEL.md`. v1 uses operator seeding (dev only); v2 will use real bank/card integrations.

---

## G

**Graceful shutdown.** A shutdown pattern where the process finishes in-flight work before exiting. In K8s, signaled by SIGTERM with a `terminationGracePeriodSeconds` budget. RRQ workers use a preStop hook to begin draining; the consumer loop checks a stop flag between message claims.

---

## H

**HMAC.** Hash-based Message Authentication Code. A cryptographic primitive for verifying that a message hasn't been tampered with, given a shared secret. RRQ signs webhook payloads with HMAC-SHA256; merchants verify the signature.

**HPA (Horizontal Pod Autoscaler).** A Kubernetes resource that scales pod count up or down based on a metric. RRQ's saga worker scales on Redis Stream consumer lag.

---

## I

**Idempotency.** The property that performing an operation multiple times has the same effect as performing it once. RRQ has idempotency at three layers: API gateway (via idempotency keys), saga steps (via UNIQUE constraints), and merchant webhook handlers (via event_id deduplication on the merchant's side).

**Idempotency key.** A merchant-supplied unique identifier for a request, used by the API gateway to deduplicate retries. Typically a UUIDv4. Cached for 24 hours.

**Init container.** A Kubernetes container that runs to completion before the main container starts. RRQ uses init containers to wait for database migrations to complete before application pods start.

**Invariant.** A property that the system always maintains. RRQ has 8 invariants, defined in `02-INVARIANTS.md` (I1 through I8).

---

## J

**JCS (JSON Canonicalization Scheme).** RFC 8785; a deterministic JSON serialization (sorted keys, no whitespace, etc.) used for canonical hashing. RRQ uses JCS for the idempotency body hash and webhook signature.

**JWT (JSON Web Token).** A self-contained authentication token signed by the platform. Issued by exchanging an API key. Carries claims (merchant_id, expiration, tier) and is short-lived (1 hour). Verified by signature only; no per-request DB lookup needed.

---

## K

**Knight Capital.** A trading firm that lost $440M in 45 minutes in 2012 due to a deployment error. Referenced in `01-PROBLEM.md` as the canonical "this is why correctness matters" cautionary tale.

---

## L

**Ledger.** The record of all wallet balance changes, with each entry attributed to a specific saga step. In RRQ, the `ledger_entries` table. Functions as a projection from the event store; the canonical balance derivation is `SUM(amount) WHERE wallet_id = ?`.

**Ledger entry.** One row in `ledger_entries`. Signed amount (negative=debit, positive=credit). Idempotent via `UNIQUE(saga_id, step_name)`.

**Linkerd.** A service mesh providing mTLS, observability, and traffic management for Kubernetes. Designed for use in RRQ's v2 deployment; not deployed in v1.

**Liveness probe.** A Kubernetes health check that tests whether a pod should be killed and restarted. Distinct from readiness probe.

**Long-lived transaction (LLT).** A logical operation that takes longer than is practical to hold a database transaction. The original Sagas paper (1987) introduced sagas as the solution.

---

## M

**Merchant.** A business customer of the RRQ platform. Has an API key, webhook URL, signing secret, and owns wallets. Not the same as an end-user.

**Migration.** A script that changes the database schema. RRQ migrations are numbered SQL files in `/migrations/`. Run via a Kubernetes Job before application deploys.

**mTLS (Mutual TLS).** TLS where both client and server present certificates, authenticating each other. Designed for v2 deployment; not in v1.

---

## N

**Notify stream.** The Redis Streams stream(s) where the Saga Worker writes webhook events. Partitioned into 16 shards (`stream:notify-0` through `stream:notify-15`) for per-merchant ordering with parallelism.

---

## O

**OpenTelemetry (OTel).** A vendor-neutral standard for instrumenting application code to emit telemetry (traces, metrics, logs). RRQ's services use OTel SDKs to emit data to the OTel Collector, which routes to Jaeger / Prometheus / Loki.

**Operational wallet.** A wallet owned by the merchant for their own operational funds (revenue, float). Distinct from customer wallets (which they manage on behalf of users).

**Operator.** A human operating the system. Uses the dashboard, makes interventions, responds to alerts. Actions are auditable via `operator.action` events.

**Orchestration (saga orchestration).** A saga pattern where a central coordinator owns the control flow, calling each step in sequence. RRQ uses orchestration. Contrasts with choreography (where each service reacts to events without a central coordinator).

**Outbox pattern.** A pattern for atomically updating the database and emitting an event by writing both to the same database transaction, then having a separate process publish the event from a "outbox" table. Not used in RRQ explicitly because the events table serves the same purpose (events are written transactionally with the ledger, and consumers read from the stream populated from events).

---

## P

**Partial index.** A Postgres index that only includes rows matching a WHERE clause. RRQ uses partial indexes for `webhook_deliveries(status='pending')` and similar — the index is much smaller than a full index because most rows don't match.

**Partitioning (stream partitioning).** Dividing a stream into multiple sub-streams (shards) for parallelism while preserving ordering within each shard. RRQ partitions the notify stream by `hash(merchant_id) mod 16`.

**Pending list.** In Redis Streams, the set of messages a consumer has claimed but not yet ACKed. After a configurable idle time, messages can be reclaimed by other consumers via `XAUTOCLAIM`.

**Per-merchant ordering.** The guarantee that webhooks for a given merchant are delivered (attempted) in the order their source events occurred. Enforced by stream partitioning.

**Per-saga atomicity.** The guarantee that a saga's steps are not interleaved with another saga touching the same resources. Enforced by Redlock distributed locks on wallets.

**Per-wallet ordering.** The guarantee that events for a wallet are processed in their causal order. Enforced by the Fraud Worker's two-level dispatch (lazy per-wallet tasks).

**Persistent Volume (PV).** A Kubernetes resource representing durable storage. Postgres and Redis use PVs to survive pod restarts.

**Preventative control.** A control that gates an operation before it happens, rejecting any that fail. Contrasts with detective control.

**Projection.** A derived view of event log data, materialized for query performance. The `ledger_entries` and `wallet_balance_cache` tables are projections.

**Protobuf (Protocol Buffers).** A binary serialization format with schemas defined in `.proto` files. RRQ uses Protobuf for event payloads and the internal admin gRPC API.

---

## Q

**Quorum.** A majority of participants in a distributed system. Required by Redlock: N/2 + 1 of N Redis instances must agree on a lock acquisition.

---

## R

**Readiness probe.** A Kubernetes health check that determines whether a pod should receive traffic. Distinct from liveness probe.

**Reconciliation.** The nightly batch that verifies the ledger agrees with the event log. Replays events to derive balances; compares to materialized ledger; emits alerts on discrepancy.

**Redlock.** A distributed lock algorithm for Redis, designed to be robust to single-node failure with multiple Redis instances. v1 deploys with one instance (degraded safety); v2 with multiple.

**Replay.** Computing derived state by iterating events in order. Reconciliation does replay; balance derivation does replay; audit queries do replay.

**RPO (Recovery Point Objective).** The maximum amount of data loss acceptable in a disaster recovery scenario. RRQ targets effectively zero RPO for committed transactions (via continuous WAL archiving).

**RTO (Recovery Time Objective).** The maximum acceptable downtime in a disaster recovery scenario. RRQ targets 1 hour for full restore.

---

## S

**Saga.** A multi-step operation where each step has a compensation. If any step fails, prior compensations run in reverse. Provides "semantic atomicity" — either the whole thing succeeds, or it ends up equivalent to having never started.

**Saga state.** A row in `saga_state` tracking where a saga is in its lifecycle. Updated transactionally with each step's work. Source of truth for crash recovery.

**Sagas paper.** The 1987 paper by Garcia-Molina and Salem that introduced the saga pattern. Foundation for everything RRQ's saga worker does.

**SETNX.** A Redis command: set a key only if it doesn't exist. Atomic. RRQ uses it for idempotency claims at the API gateway.

**Single-writer principle.** The discipline that each piece of state has at most one concurrent writer at any moment. RRQ enforces this for wallets (via Redlock) and per-wallet tasks (via in-process dispatch).

**Stampede protection.** Defenses against cache stampedes: randomized TTLs, single-flight requests.

**StatefulSet.** A Kubernetes workload for stateful applications. Provides stable pod names and persistent volumes. Used for Postgres and Redis in RRQ's K8s deployment.

**Stream (Redis Stream).** An append-only log data structure in Redis, with consumer-group semantics. RRQ uses streams for `stream:jobs` and partitioned `stream:notify-*`.

**Synchronous service.** A service that responds to requests inline. The API Gateway is RRQ's only synchronous service; everything else is asynchronous.

---

## T

**Two-level dispatch.** The pattern where an outer consumer reads any event from a stream and routes it to an in-process per-key task. Used by the Fraud Worker for per-wallet ordering with cross-wallet parallelism.

**TTL (Time To Live).** A duration after which a key automatically expires. RRQ uses TTLs on idempotency keys (24h), merchant metadata cache (60s), and Redlocks (5s).

**Turmoil.** A Rust crate for deterministic distributed-systems testing. Simulates network failures, host crashes, message drops in a single-threaded test runner. Used by RRQ's Rust chaos tests.

---

## U

**ULID.** Universally Lexicographically Identifier. A 26-character string identifier that sorts by time. Globally unique without coordination. RRQ uses ULIDs for entity IDs (merchant, wallet, saga, event, etc.).

**Unique constraint.** A database constraint enforcing that no two rows have the same value for a column (or column combination). RRQ uses `UNIQUE(saga_id, step_name)` on ledger_entries as an idempotency anchor.

**Unknown outcome problem.** The failure mode where a request was sent but its outcome is unknown (the response was lost). The merchant can't distinguish "didn't happen" from "happened but I don't know." Solved by idempotency keys.

---

## V

**Validate step.** The first step of every saga. Checks preconditions: wallets exist, are in valid status, have sufficient balance, etc. Rejects sagas that would violate invariants.

**Velocity.** The rate of events per unit time. Velocity-based fraud detection: too many transfers for one wallet in too short a window triggers a freeze.

---

## W

**WAL (Write-Ahead Log).** Postgres's transaction log. Continuous WAL archiving is the mechanism for point-in-time recovery.

**Webhook.** An HTTP POST from RRQ to a merchant's configured URL, notifying them of events. Signed with HMAC-SHA256. Subject to retry, breaker, DLQ.

**Wallet type.** Distinction between `merchant_operational`, `customer`, `escrow`, `platform`. Affects who can modify the wallet and under what circumstances.

---

## X

**XACK.** Redis Streams command to acknowledge processing of a message.

**XADD.** Redis Streams command to append a message to a stream.

**XAUTOCLAIM.** Redis Streams command to reclaim messages that have been claimed but not ACKed by a consumer that's no longer responsive. Used for crash recovery.

**XREADGROUP.** Redis Streams command for a consumer to read messages from a stream as part of a consumer group.

---

*Pass 5 addition. Consolidates terminology used throughout the docs.*
