# 02 — Invariants

> **What this is.** The precise, testable statements about what RRQ guarantees. Not slogans. Statements that can be falsified by a test, and that *are* falsified by a test in the test suite.
>
> **Reading time.** ~10 minutes.
>
> **Audience.** Anyone reasoning about correctness — author, reviewer, future-you debugging an incident. Each invariant has a name (`I1`, `I2`, ...) used elsewhere in the docs to reference it.

---

## Why a separate doc for this

A common failure mode in system design documents is to describe *what the system does* without ever saying *what must always be true*. Without invariants, "is this correct?" has no answer. With invariants, every code change has a checklist: which invariants does this affect, are they still upheld, where's the test that proves it.

The invariants below are deliberately small in number. Eight of them. Each is a single English sentence. Each is checked by a specific test or class of tests. If one is violated, RRQ has a bug, and the violation is the precise specification of what's broken.

A reviewer who reads only this document plus `00-OVERVIEW.md` knows what RRQ promises and how to verify it. That's the whole point.

---

## Invariant I1: Conservation of value

> For every `DebitApplied(wallet=W, amount=X, saga_id=S)` event, exactly one of the following also exists in the event log:
>   (a) `CreditApplied(wallet=W', amount=X, saga_id=S)` for some `W' ≠ W`, or
>   (b) `DebitReversed(wallet=W, amount=X, saga_id=S)`.

In English: every debit is paired with either a corresponding credit (the saga succeeded) or a corresponding reversal (the saga compensated). There are no floating debits — money never enters or leaves the system without a matching entry.

**Why it matters.** This is the financial soundness invariant. A violation means the system has either created or destroyed value; in a real system, this is a regulatory incident.

**How it's enforced.** The Saga Worker writes `DebitApplied` and `CreditApplied` (or `DebitApplied` and `DebitReversed`) within the same logical saga, and persists `saga_state` so that a crash between the two is recoverable. Reconciliation (§14 service doc) verifies this invariant nightly by replaying the event log and asserting the pairing for every saga_id.

**How it's tested.**
- Unit test: replay engine asserts every `DebitApplied` has a matching credit or reversal in the same saga_id.
- Integration test: submit 1,000 transfers under chaos (random worker kills); after all sagas terminate, run the replay engine and assert I1.

**Known recovery scenarios.** Worker crash after `DebitApplied` but before `CreditApplied`: a replacement worker reads `saga_state`, sees state=`Debited`, attempts `Credit`. If `Credit` succeeds → I1 holds via (a). If `Credit` is unrecoverable → compensation runs, writes `DebitReversed` → I1 holds via (b).

---

## Invariant I2: No negative balance on active wallets

> For every wallet `W` with `status = active` at any point in time, the derived balance of `W` (sum of ledger amounts for `W`) is greater than or equal to zero.

In English: the system does not let wallets go negative. Frozen or closed wallets are exempt because they may have been frozen *because* of a discovered discrepancy that put them temporarily negative.

**Why it matters.** A wallet going negative means the system extended credit it didn't have — the equivalent of letting a customer withdraw money they don't own. Real payment systems strictly forbid this on most wallet types.

**How it's enforced.** The Saga Worker's `Validate` step computes the derived balance of the source wallet and rejects the transfer if it would go negative. The check is performed *under the wallet's Redlock*, so no other concurrent saga can debit between the check and the actual `DebitApplied`. ([→ `23-LOCKING.md`](deep-dives/23-LOCKING.md) explains why the lock is necessary for this check.)

**How it's tested.**
- Integration test: submit two concurrent transfers from a wallet with balance 100, each for 60. Without the lock, one of them might bypass the check. With the lock, exactly one succeeds and the other fails with `InsufficientBalance`. Assert balance > 0 throughout.
- Property test: random sequence of transfers with random amounts; assert balance never goes negative on any active wallet at any point in the event stream.

**Subtlety.** "The derived balance" must include all in-flight committed events, not just events from terminal sagas. If saga S1 has written `DebitApplied(W, 50)` but not yet completed, that 50 is *spent* from W's perspective even though the saga isn't done. The balance check uses the event store directly, not the eventually-consistent projection.

---

## Invariant I3: At-most-once execution per idempotency key

> For each `(merchant_id, idempotency_key)` pair where the API has accepted a request, exactly one `JobRequested` event exists in the job stream, and exactly one chain of saga events derives from it.

In English: a merchant can retry the same operation a million times with the same idempotency key; the underlying transfer happens at most once.

**Why it matters.** This is the duplicate-protection invariant. The merchant's expected behavior is "if I retry, nothing bad happens" — they don't want to think about whether their retry might double-charge their customer. RRQ guarantees that thinking is unnecessary.

**How it's enforced.** The API Gateway's idempotency middleware uses an atomic `SET NX` on the key; only the first request with a given key wins the race. Subsequent requests either see "in progress" (return 409) or get the cached response. ([→ `20-IDEMPOTENCY.md`](deep-dives/20-IDEMPOTENCY.md) for the full mechanism, including the request-body-hash check that prevents key reuse with different payloads.)

**How it's tested.**
- Concurrency test: fire 100 simultaneous requests with the same idempotency key; assert exactly one `JobRequested` event was written and exactly one saga ran.
- Sequential retry test: fire 10 sequential requests with the same key; assert all 10 receive the same job_id in the response.
- Different-payload test: fire two requests with the same key but different bodies; assert the second receives 422.

**Edge case to be aware of.** The idempotency cache has a TTL (24 hours by default). After the TTL expires, the same key behaves as a new request. This is documented in the merchant-facing API. If a merchant's retry strategy spans more than 24 hours, they need to use a different key for each retry, which is unusual but possible.

---

## Invariant I4: Per-wallet event ordering

> For any wallet `W`, events affecting `W` appear in the event store in causal order. If event `E1` causally precedes event `E2` (i.e., `E2`'s preconditions depend on `E1`'s effects), then `E1.id < E2.id` in the event store.

In English: events for a given wallet are linearly ordered. A `CreditApplied` cannot appear before the `DebitApplied` it pairs with. A `WalletFrozen` cannot precede the `FraudSuspected` event that triggered it. The history of any wallet is reconstructible by reading events in event-id order and applying them sequentially.

**Why it matters.** Reconciliation, audit, and any state-machine logic that depends on order need this. If events for a wallet were unordered, you couldn't reliably replay them to derive the wallet's history.

**How it's enforced.**
1. Causally-related events are written by the same saga, sequentially. The saga's step machine writes `DebitApplied` before `CreditApplied` and waits for the first INSERT to commit before issuing the second.
2. Each saga holds the wallet's Redlock during its mutating section, preventing other sagas from interleaving writes to the same wallet.
3. The event store assigns IDs from a single monotonic sequence (Postgres `bigserial`), so any two events written serially get strictly increasing IDs.

**How it's tested.**
- Replay test: for every wallet in the test corpus, fetch all events in event-id order, apply them to a fresh state, and assert no operation is invalid given prior state (e.g., no Credit before Debit; no second Freeze when already frozen).

**Note.** This invariant is *per wallet*, not global. Two different wallets' events can be interleaved arbitrarily; RRQ doesn't promise any global ordering, because doing so would serialize the entire system on a single sequence point. Per-wallet ordering is what's needed for correctness, and it's what RRQ provides.

---

## Invariant I5: Per-merchant webhook ordering

> For any merchant `M`, webhook deliveries for events affecting `M` are attempted in the order those events were emitted. If event `E1` was emitted before event `E2`, the system attempts to deliver `E1`'s webhook before `E2`'s.

In English: a merchant sees their webhooks in the order things happened. They will see `transfer.completed(job_42)` before `transfer.completed(job_43)` if `job_42`'s saga finished first.

**Why it matters.** Merchants build state machines on top of webhooks. If the order is unpredictable, those state machines either become very defensive (treating every webhook as potentially-out-of-order) or break.

**How it's enforced.** The notify stream is partitioned into N shards by `hash(merchant_id) mod N`. Within a shard, the consumer group guarantees that one consumer at a time processes the messages — Redis Streams' consumer-group semantics give us this for free within a shard. All of merchant M's events land on the same shard, so they're delivered serially. ([→ `22-ORDERING.md`](deep-dives/22-ORDERING.md))

**Caveats.**
- "Attempts to deliver in order" is not "successfully delivers in order." If `E1`'s delivery is failing and getting retried while `E2`'s delivery is queued, `E2` waits for `E1`'s next attempt or DLQ-routing. Per-merchant ordering is preserved at the *attempt* level. This is the right tradeoff because head-of-line blocking is bounded by the retry policy.
- Different merchants make no ordering promise. Merchant M's events and merchant M''s events are unordered relative to each other.

**How it's tested.**
- Integration test: emit 100 events for each of 10 merchants in a known sequence; assert each merchant's webhook endpoint receives them in the same sequence (mock endpoint records arrival order).
- Adversarial test: same but with one merchant's endpoint slow (3-second delay per request); assert that merchant's order is preserved despite the slowdown, and that other merchants are not affected.

---

## Invariant I6: Immutable event history

> Once a row exists in the `events` table, it is never updated and never deleted by any application code, ever, for any reason.

In English: events are facts. Facts don't change. If a fact is wrong, you record a new fact correcting it. You don't go back and modify history.

**Why it matters.** The entire correctness story rests on the event log being trustworthy. If events can be modified or deleted, then the log is no longer evidence of what actually happened — it's just another piece of mutable state that might lie. Reconciliation, audit, and replay all become unreliable.

**How it's enforced.**
1. The Postgres role used by application services has `INSERT` and `SELECT` permission on the `events` table, but not `UPDATE` or `DELETE`. A bug that tries to update would fail at the database with a permission error, not silently corrupt history. (DBAs operating with elevated roles can still do anything; this guards against application bugs, not malicious operators.)
2. Code review enforces the rule: any PR that adds an `UPDATE events ...` or `DELETE FROM events ...` is rejected. The architecture forbids it.
3. Corrections are new event types: `WalletAdjustment` for manual operator corrections after a reconciliation alert, etc. The new event records what was wrong and the new authoritative state, but the original event remains.

**How it's tested.**
- Schema test: the migration that creates the role asserts the role lacks `UPDATE` and `DELETE` on `events`.
- Integration test: attempt to `UPDATE events` from application code; assert it fails.

**Why this is harder than it sounds.** Some "obvious" features secretly want to mutate events. "Anonymize PII after 90 days" wants to update events. "Delete a customer's data on GDPR request" wants to delete events. RRQ's answer: those features need a separate system that operates on derived projections, not on the event store. The event store is a sealed log; what you show users and what's in the log are different things.

---

## Invariant I7: Saga termination

> Every saga that has been started either reaches a terminal state (`Completed`, `Failed`, or `DeadLettered`) within finite time, or is observable as "stuck" via the operational tooling.

In English: no saga runs forever invisibly. Either it finishes, or it's clearly stuck and an operator can find it.

**Why it matters.** A saga that is silently in-flight is invisible to the system: it's not done, not failed, not surfaced for operator attention. In an incident, you can't tell whether a transfer is "still working" or "lost." This invariant says: there is no such ambiguous state.

**How it's enforced.**
- Every saga has a `deadline_at` column on `saga_state`. A saga that exceeds its deadline is logged at WARN, surfaced via the admin CLI's `stuck-sagas` command, and emits a Prometheus metric.
- Crashed-worker recovery: `XPENDING` and `XAUTOCLAIM` reassign messages whose worker died, so a saga doesn't sit "in-flight" indefinitely just because the worker holding it crashed.
- Saga steps that hit retryable errors retry with exponential backoff, but with a bounded total retry duration (typically 5 minutes per step). After that, the step is treated as terminal-failure for the saga.
- Saga steps that hit terminal errors (validation, permission denied) move the saga immediately to `Failed`, no retry.

**How it's tested.**
- Chaos test: kill the saga worker mid-saga; assert the replacement picks up and the saga reaches a terminal state within the deadline.
- Stuck-saga test: introduce a permanent error in a saga step (mock the dependency to always fail); assert the saga transitions to `DeadLettered` after retry exhaustion, with the cause recorded.

**Subtle point.** "Within finite time" doesn't mean "within a fixed time budget." A saga can take seconds (Single Transfer) or hours (a hypothetical Chargeback dispute). Each saga type has its own deadline. The invariant is that *some* deadline exists for every saga and that exceeding it is observable.

---

## Invariant I8: DLQ entries are recoverable, not lost

> Every message that exhausts its retry budget in the saga path or webhook path is persisted to the `dlq_entries` table with the original payload, the failure history, and enough context to replay it. No message is silently dropped.

In English: when the system gives up on a message, it gives up *visibly*. The DLQ is not a place where things go to die; it's a place where things go to wait for human judgment.

**Why it matters.** Silent message drop is the worst possible failure mode in a payment system: it's the failure mode where money "just disappears" with no trace. A loud failure (DLQ entry, alert, operator paged) is something an engineer can fix. A silent failure is something that surfaces in next quarter's earnings call.

**How it's enforced.**
- Saga path: when a saga exceeds its retry budget on a step, the orchestrator writes a row to `dlq_entries` containing the saga_id, the failed step, the error, and the original `JobRequested` payload. Then it ACKs the source stream message (the message is now "owned" by the DLQ table, not the stream).
- Webhook path: when a webhook delivery exceeds its retry budget, the same — DLQ row first, then ACK.
- The admin CLI's `dlq replay <id>` command re-emits the original payload to the appropriate stream and marks the DLQ row `replayed`. The replayed work has a fresh idempotency key suffix to distinguish it from the original.

**How it's tested.**
- Failure-injection test: send 100 webhooks to an endpoint that always returns 500. After max retries, assert 100 rows exist in `dlq_entries` and the source stream has nothing pending.
- Replay test: replay a DLQ entry; assert the work executes (against a now-healthy endpoint) and the DLQ row's status flips to `replayed`.

---

## What's not an invariant (and why)

It is worth being explicit about what RRQ does *not* guarantee, to avoid implicit-promise drift.

**Not an invariant: read-your-writes for dashboards.** A merchant submits a transfer at T=0 and queries the dashboard at T=0.5 seconds. The dashboard may not reflect the transfer yet (projection lag). This is intentional. The merchant can poll `GET /v1/jobs/{id}` for strong consistency; dashboards trade strong consistency for query performance and availability.

**Not an invariant: linearizability across merchants.** Two transfers initiated "simultaneously" by different merchants may be ordered differently in the event log than by wall-clock time. The system makes no global ordering promise. Per-wallet and per-merchant ordering are the actual promises (I4, I5).

**Not an invariant: low latency under load.** RRQ is designed to handle 1,000 TPS sustained on a single machine, but no SLO is defined. Latency degrades gracefully under load (via queue lag, not request failures); request failures only happen if the system is unable to *durably accept* work, not because internal queues are full.

**Not an invariant: zero-downtime upgrades.** Rolling deploys are designed for, but a deploy can briefly fail in-flight requests if it coincides with a precise moment in saga lifecycles. Operationally, deploys happen during low-traffic windows and the API gateway's health checks ensure new pods are healthy before old ones are killed. v1 doesn't claim true zero-downtime; v2 with proper preStop drain hooks does.

**Not an invariant: fairness among merchants.** A merchant submitting 10x the load gets 10x the share of worker capacity. v1 has no per-merchant rate limiting. A real production deployment would add this, partly for fairness and partly to prevent one merchant's traffic from starving others.

These non-invariants are not bugs; they are scope decisions. Each is documented so reviewers know exactly what RRQ promises and exactly what it doesn't.

---

## How invariants flow through the rest of the docs

When the service docs (`services/*`) and deep-dives (`deep-dives/*`) describe a mechanism, they reference invariants by ID. For example:

> "The Saga Worker acquires a Redlock on both wallet IDs before the Debit step. This is what makes I2 (no negative balance) and I4 (per-wallet ordering) hold under concurrent transfers." — from `services/11-SAGA-WORKER.md`

> "The atomic SETNX on the idempotency key is what makes I3 (at-most-once execution) hold even under concurrent retries." — from `deep-dives/20-IDEMPOTENCY.md`

This cross-referencing is deliberate. It means anyone tracing a correctness question can follow it from the invariant statement here, to the mechanism that enforces it, to the test that validates it, and back. The whole doc set forms a directed graph rooted at this file.

---

## Where to read next

- The system shape that upholds these invariants → [`00-OVERVIEW.md`](00-OVERVIEW.md)
- The *why* behind each invariant, in narrative form → [`01-PROBLEM.md`](01-PROBLEM.md)
- A specific service implementation → `services/`
- A specific mechanism in depth → `deep-dives/`

---

*Pass 1 of the architecture series. Last updated pre-implementation.*
