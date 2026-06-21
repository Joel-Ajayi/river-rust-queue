# 02: Invariants

> **What this is.** The precise, testable statements about what RRQ guarantees. Not slogans. Statements that can be falsified by a test, and that _are_ falsified by a test in the test suite.
>
> **Reading time.** ~10 minutes.
>
> **Audience.** Anyone reasoning about correctness — author, reviewer, future-you debugging an incident. Each invariant has a name (`I1`, `I2`, …) used elsewhere in the docs to reference it.

---

## Invariant I1: Conservation of value

> Every transfer posts exactly one `debit` leg and one `credit` leg, of equal magnitude, to two distinct wallets, in a single transaction. For every `transfer_id` there is exactly one `(transfer_id, 'debit')` and one `(transfer_id, 'credit')` row in `ledger_entries`, with `|debit.amount| = credit.amount`.

In English: money is never created or destroyed. Every movement out of one wallet is exactly a movement into another, recorded as two halves of one indivisible posting.

**Why it matters.** This is the financial soundness invariant. A violation means the system created or destroyed value — in a real system, a regulatory incident. The classic way to violate it is a worker that debits, then crashes before crediting, leaving money in limbo.

**How it's enforced.** Both legs are written in **one serializable Postgres transaction** ([→ `services/11-LEDGER-WORKER.md`](services/11-LEDGER-WORKER.md)). They commit together or not at all, so the limbo state cannot exist — not even for a microsecond, because no other transaction can observe a half-applied posting. The `UNIQUE (transfer_id, leg)` constraint guarantees a redelivered job cannot add a third leg. Reconciliation ([→ `services/14-RECONCILIATION.md`](services/14-RECONCILIATION.md)) re-verifies the pairing nightly by replaying `ledger_entries`.

**How it's tested.**
- Unit test: for every `transfer_id`, assert exactly one debit + one credit of equal magnitude.
- Chaos test: submit 1,000 transfers while randomly killing the Ledger Worker; after the queue drains, assert I1 holds for every transfer (a killed transaction simply rolled back — there is no partial posting to find).

---

## Invariant I2: No negative balance on active wallets

> For every wallet `W` with `status = active`, the derived balance of `W` (sum of `ledger_entries.amount` for `W`) is greater than or equal to zero at every point in the entry sequence.

In English: the system does not let an active wallet go negative. Frozen or closed wallets are exempt because they may have been frozen *because* of a discovered discrepancy.

**Why it matters.** A negative active wallet means the system extended credit it didn't have — letting a customer spend money they don't own.

**How it's enforced.** The posting transaction takes a `SELECT … FOR UPDATE` row lock on the source wallet, computes its current balance from `ledger_entries` *under that lock*, and aborts the transfer if the debit would take it below zero. Because the check and the debit are in the same locked transaction, no concurrent transfer can slip a debit in between. This is the entire concurrency-control story — an in-transaction row lock, not a distributed lease.

**How it's tested.**
- Concurrency test: wallet balance 100; fire two concurrent transfers of 60. Assert exactly one succeeds, one fails with `INSUFFICIENT_BALANCE`, and the balance never goes negative. This is the core test for the row-lock guarantee.
- Property test: random sequence of transfers with random amounts; assert no active wallet's running balance is ever negative.

---

## Invariant I3: At-most-once execution per idempotency key

> For each `(merchant_id, idempotency_key)` the API has accepted, exactly one `jobs` row exists, and at most one set of postings derives from it.

In English: a merchant can retry the same operation a million times with the same key; the underlying transfer happens at most once.

**Why it matters.** The merchant's expected behavior is "if I retry, nothing bad happens." RRQ guarantees that thinking about double-charges is unnecessary.

**How it's enforced.** Durably, in Postgres. The gateway does `INSERT INTO jobs … ON CONFLICT (merchant_id, idempotency_key) DO NOTHING`. The first request inserts the row and proceeds; every retry conflicts, reads back the existing `job_id`, and returns the same response. The `UNIQUE (merchant_id, idempotency_key)` constraint is the single source of truth — there is no Redis cache that could lose the claim on a node failure.

**How it's tested.**
- Concurrency test: fire 100 simultaneous requests with the same key; assert exactly one `jobs` row and one set of postings.
- Sequential retry test: fire 10 sequential requests with the same key; assert all 10 receive the same `job_id`.
- Different-payload test: same key, different body; assert the second receives `422` (detected via `request_hash`).

---

## Invariant I4: Per-wallet entry ordering

> For any wallet `W`, its `ledger_entries` are totally ordered by `id`, and that order is causal: an entry never depends on the effects of an entry with a higher `id`.

In English: a wallet's history is a clean, replayable sequence. Reading its entries in `id` order and applying them reconstructs its balance at every point.

**Why it matters.** Reconciliation, audit, and balance derivation all depend on a single, well-defined order per wallet.

**How it's enforced.**
1. `ledger_entries.id` is a single `BIGSERIAL` sequence on one logical primary, so any two entries written serially get strictly increasing ids.
2. Concurrent transfers touching the same wallet are serialized by the `FOR UPDATE` row lock (I2), so their entries can't interleave incoherently.
3. A transfer's two legs are written in one transaction, so a credit can never be ordered before the debit it pairs with.

**Holds across replicas.** Many Ledger Worker replicas can run at once; ordering still holds because the ordering authority (the sequence and the row lock) lives in the shared database, not in any worker's memory. This is why the worker scales horizontally without weakening I4. See [`03-SCALING-AND-AVAILABILITY.md`](03-SCALING-AND-AVAILABILITY.md).

**How it's tested.**
- Replay test: for every wallet, fetch entries in `id` order, apply them to a fresh balance, assert no leg is invalid given prior state (no debit below zero on an active wallet; no credit before its paired debit).

**Note.** This is *per wallet*. Two different wallets' entries may interleave arbitrarily; RRQ makes no global-ordering promise, because that would serialize the whole system on one point.

---

## Invariant I5: Per-merchant webhook ordering

> For any merchant `M`, webhook deliveries are *attempted* in the order their source events occurred.

In English: a merchant sees their webhooks in the order things happened. `transfer.completed(job_42)` is attempted before `transfer.completed(job_43)` if `job_42` finished first.

**Why it matters.** Merchants build state machines on webhooks. Unpredictable order forces them to be defensive or breaks them.

**How it's enforced.** The outbox relay publishes `transfer.completed`/`transfer.failed` events to the Kafka `notify` topic, keyed by `merchant_id`, in `events.id` order. All of `M`'s events therefore land on one partition, and Kafka's consumer-group protocol assigns each partition to **exactly one live worker at a time**. One partition per merchant + one consumer per partition ⇒ `M`'s deliveries are attempted serially, across any number of replicas. The mechanism and its failover are specified in [`03-SCALING-AND-AVAILABILITY.md`](03-SCALING-AND-AVAILABILITY.md) and [`services/12-WEBHOOK-WORKER.md`](services/12-WEBHOOK-WORKER.md).

**Caveats.**
- "Attempts in order" is not "succeeds in order." If `E1` is failing and retrying while `E2` is queued, `E2` waits. Ordering is preserved at the *attempt* level; head-of-line blocking is bounded by the retry policy.
- Different merchants make no ordering promise relative to each other.

**How it's tested.**
- Integration test: emit 100 events for each of 10 merchants in a known sequence; assert each merchant's endpoint receives them in that sequence.
- Adversarial test: one merchant's endpoint is slow (3 s/request); assert its order is preserved and other merchants are unaffected.
- Failover test: kill the worker owning `M`'s partition mid-stream; assert Kafka reassigns it and the sequence resumes with no reorder.

---

## Invariant I6: Immutable history

> Once a row exists in `ledger_entries` or `events`, application code never updates or deletes it. (The relay's single allowed write — stamping `events.published_at` — is an append-only bookkeeping flag, not a rewrite of a fact.)

In English: financial postings and business facts are permanent. A wrong fact is corrected by recording a new fact, never by editing the old one.

**Why it matters.** The whole correctness story rests on these logs being trustworthy evidence. If they can be mutated, they stop being evidence and become just more mutable state that might lie.

**How it's enforced.**
1. The app Postgres role has `INSERT, SELECT` on `ledger_entries` and `events`, but not `UPDATE`/`DELETE`. A bug that tries to rewrite history fails at the database. (`events.published_at` is `UPDATE`-able only by the separate relay role.)
2. Code review rejects any `UPDATE`/`DELETE` against these tables.
3. Corrections are new rows: an operator fixing a discrepancy posts an adjustment transfer and records an `operator.action` event; the original entries remain.

**How it's tested.**
- Schema test: assert the app role lacks `UPDATE`/`DELETE` on `ledger_entries` and `events`.
- Integration test: attempt an `UPDATE events` from app code; assert it fails.

**Why this is harder than it sounds.** Some "obvious" features secretly want to mutate history — "anonymize PII after 90 days," "delete a customer on GDPR request." RRQ's answer: those operate on derived projections, not on the immutable logs. What you show users and what's in the log are different things.

---

## Invariant I7: Job termination

> Every accepted job reaches a terminal state (`completed`, `failed`, or is routed to the DLQ) within bounded time, or is observable as "stuck" via operational tooling.

In English: no job runs forever invisibly. Either it finishes, or it's clearly stuck and an operator can find it.

**Why it matters.** A silently in-flight job is invisible: not done, not failed, not surfaced. In an incident you can't tell whether a transfer is "still working" or "lost." This invariant says there is no such ambiguous state.

**How it's enforced.** A transfer is one transaction, so the worker's own crash is not a recovery problem — Postgres rolls the transaction back and Kafka redelivers the job; the `UNIQUE (transfer_id, leg)` guard makes the reprocess safe. The genuine failure modes are bounded:
- A *terminal* error (insufficient balance, frozen/closed wallet, bad currency) commits a `failed` job + `transfer.failed` event immediately — no retry.
- A *retryable* error (transient DB unavailability) retries with bounded backoff. After the budget is exhausted, the job is a **poison message**: it's written to `dlq_entries` and the Kafka offset is committed, so it leaves the live path and waits for a human (I8).

**How it's tested.**
- Chaos test: kill the worker mid-post; assert the job is redelivered and reaches a terminal state, with no double posting.
- Poison-message test: force a job to fail every attempt; assert it lands in `dlq_entries` after the retry budget, with the cause recorded.

---

## Invariant I8: DLQ entries are recoverable, not lost

> Every job or webhook that exhausts its retry budget is persisted to `dlq_entries` with the original payload, the failure history, and enough context to replay it. No message is silently dropped.

In English: when the system gives up on a message, it gives up *visibly*. The DLQ is where things wait for human judgment, not where they go to die.

**Why it matters.** Silent drop is the worst failure mode in a payment system — money "just disappears" with no trace. A loud failure (DLQ row, alert, page) is fixable; a silent one surfaces in next quarter's earnings call.

**How it's enforced.**
- Ledger path: a job that exhausts its retry budget is written to `dlq_entries` (source `ledger`) with the `job.requested` payload, *then* the Kafka offset is committed — the message is now owned by the DLQ table, not the stream.
- Webhook path: same shape — DLQ row first, then ACK.
- The Admin Dashboard's replay re-emits the original payload and marks the row `replayed`. Replayed work re-derives the same deterministic `tf_` id, so a replay that races with a late original cannot double-post.

**How it's tested.**
- Failure-injection: 100 webhooks to an always-500 endpoint; after max retries assert 100 `dlq_entries` rows and no uncommitted offsets.
- Replay: replay a DLQ entry against a now-healthy path; assert it executes and the row flips to `replayed`.

---

## Invariant I9: Tenant isolation

> No merchant can observe or affect another merchant's data. Every wallet-mutating request is rejected unless the source wallet is owned by the authenticated merchant (`from_wallet.merchant_id = jwt.sub`), and every read returns only rows owned by the authenticated merchant.

In English: a merchant is sealed inside its own data. It cannot transfer *from* a wallet it doesn't own, cannot read another merchant's wallets, jobs, ledger, or webhook history, and cannot even confirm another merchant's resources exist.

**Why it matters.** Cross-tenant access is the highest-severity failure in a multi-merchant payment system — one merchant draining or reading another's money. It is the one correctness property whose violation is also a security incident.

**How it's enforced.**
1. The gateway authorizes every wallet-mutating request against the JWT's `sub`. A `POST /v1/transfers` whose `from_wallet` is not owned by `sub` is rejected with `403 WALLET_NOT_OWNED` before a `jobs` row is written. Ownership is immutable, so the gateway checks it from a short-TTL cache; the *mutable* wallet state (frozen status, balance) is re-read fresh inside the posting transaction.
2. Every read endpoint is scoped to `sub`. A merchant querying another's job/wallet/ledger gets `404` (not `403`), so the response doesn't even leak existence.
3. Ownership is a column, not a convention: `wallets.merchant_id` is the authority.

**How it's tested.**
- Authorization test: merchant A submits a transfer whose `from_wallet` belongs to B; assert `403 WALLET_NOT_OWNED` and no `jobs` row.
- Read-isolation test: A requests B's `job_id`, `wallet_id`, ledger window; assert `404` for each.
- Property test: a population of merchants issuing random operations; assert no entry or response ever attributes one merchant's wallet activity to another.

**Subtlety.** Isolation is enforced at the gateway; downstream workers trust that a `jobs` row was authorized when written. That trust is sound because the gateway is the only writer of `jobs`. Any future path that can create a job (an internal admin tool, a backfill) must re-establish the same ownership check, or this invariant silently weakens from "enforced" to "assumed."

---

## What's not an invariant (and why)

Being explicit about non-promises avoids implicit-promise drift.

**Not an invariant: read-your-writes for dashboards.** A merchant submits at T=0 and queries the dashboard at T=0.5 s; the dashboard may not reflect it yet (projection lag). Intentional — poll `GET /v1/jobs/{id}` for strong consistency; dashboards trade it for query performance.

**Not an invariant: linearizability across wallets/merchants.** Two transfers on different wallets may be ordered in `ledger_entries` differently than wall-clock time. Per-wallet (I4) and per-merchant (I5) ordering are the actual promises.

**Not an invariant: low latency under load.** RRQ sustains its demonstrated throughput (~1,000 TPS on a small cluster, scaling out by adding replicas), but no latency SLO is defined. Under load, latency degrades via queue lag, not request failures; a request fails only if the system cannot *durably accept* it.

**Not an invariant: zero-downtime upgrades.** Rolling deploys are designed for, and Postgres runs a hot standby with automatic failover (CloudNativePG), but a deploy can briefly fail in-flight requests at a precise moment. Deploys happen in low-traffic windows; preStop drain hooks get close to zero-downtime; RRQ does not claim *true* zero-downtime.

**Not an invariant: fairness among merchants.** A merchant submitting 10× the load gets 10× the worker share. Fairness is handled only as far as Kong's edge rate limiting; the workers don't arbitrate it.

These are scope decisions, not bugs — each documented so reviewers know exactly what RRQ promises and what it doesn't.

---

## Where to read next

- The system shape that upholds these invariants → [`00-OVERVIEW.md`](00-OVERVIEW.md)
- The *why* behind each invariant, in narrative form → [`01-PROBLEM.md`](01-PROBLEM.md)
- A specific service implementation → `services/`

---

_Pass 1 of the architecture series. Last updated pre-implementation._
