# 01: The Problem

> **What this is.** The problem RRQ exists to solve, in detail, with the specific failure modes that make it hard. Read this if the *why* of the architecture is not obvious from `00-OVERVIEW.md`.
>
> **Reading time.** ~12 minutes.
>
> **Audience.** Engineers who haven't worked on payment systems before, or who have but want the failure modes named explicitly. If you've shipped a Stripe-scale ledger to production, you can skip this, there's nothing here that will surprise you.

---

## A story you've probably heard

In 2012, Knight Capital Group, a high-frequency trading firm, lost $440 million in 45 minutes. The cause was a deployment that left old code on one of eight servers. When the new feature went live, that one server interpreted incoming orders using the old logic. For 45 minutes it executed millions of trades that nobody had asked for, in a market that doesn't roll back. By the time the team understood what was happening, the firm was insolvent. They were acquired weeks later.

The Knight Capital incident is the canonical reference because it's vivid, but it's not unusual. Every payment company has its own version. A retry path that double-charges customers. A reconciliation gap that takes weeks to surface. A rounding error that compounds across millions of transactions. A worker that crashes after debiting a wallet but before crediting the recipient, leaving money in nobody's hands.

These are not exotic failure modes. They are the *expected* behavior of distributed systems built without specific countermeasures. The question is not whether they will happen, they will, but whether your system is structured so that when they happen, you find out and recover, or whether the bug compounds in the dark.

RRQ is the second kind of system. The next sections explain why that's hard to build, and what specifically RRQ does about it.

---

## The fundamental disanalogy

If you've written backend code before, you have an intuition for what "running an operation" means. You call a function, it runs to completion, you get a return value. If it fails, you get an exception. If it crashes, your process dies and you know it died. Three outcomes, all observable.

Networks are different. When you send a request to another machine and the response doesn't come back, you have *four* outcomes:

1. The request reached the other machine and succeeded. You'll never know.
2. The request reached the other machine and failed. You'll never know.
3. The request never reached the other machine. You'll never know.
4. The request reached the other machine, succeeded, the response was sent, and the response was lost in transit. You'll never know.

You cannot distinguish these cases. From your side, all four look identical: you sent something, you waited, nothing came back. The other machine *might* have done what you asked. It might not have. You cannot find out from where you stand.

This is the **unknown outcome**, and it is the source of approximately every difficult problem in distributed systems. Every pattern in the next sections, idempotency, sagas, event sourcing, dead letter queues, exists because of this single fact. Once you internalize that the unknown-outcome case is permanent, not something you can engineer away, the rest of distributed systems starts to make sense.

The naive response is to retry. If you didn't hear back, send the request again. This is correct! It's also dangerous, because the request might have succeeded the first time, and now you're asking for it to be done a second time. If the operation is "send 1,000 NGN to recipient X," the merchant just sent 2,000 NGN.

The fix isn't "don't retry" (then you lose work to lost responses). The fix is to make retries *safe*, to design every operation so that doing it twice produces the same result as doing it once. This is **idempotency**, and it's the first major idea RRQ is built around.

---

## A concrete failure cascade

Let me walk through a realistic scenario in painful detail, because the concrete version is the only one that lands. Imagine a small payment processor without RRQ's protections.

A merchant calls the API:

```
POST /transfer  { from: A, to: B, amount: 5000 }
```

The handler does, in order:

```
1. SELECT balance FROM wallets WHERE id = A   →  10000
2. UPDATE wallets SET balance = 5000 WHERE id = A
3. UPDATE wallets SET balance = balance + 5000 WHERE id = B
4. INSERT INTO transactions (...)
5. return 200 OK
```

Looks reasonable. It's wrong in five different ways, and each one corresponds to an incident a real payment company has had.

**Failure 1: the worker dies between steps 2 and 3.** The Kubernetes scheduler decided to evict this pod, or the OOM killer fired, or the host hardware faulted. Wallet A has been debited. Wallet B has not been credited. There is no record of what was supposed to happen. The 5,000 NGN is *gone*, not in A, not in B, not anywhere reconstructible. When the merchant complains a week later, no one can explain where the money went.

**Failure 2: another transfer ran concurrently.** Two merchants both initiated transfers from wallet A. Both handlers ran step 1 simultaneously and read balance = 10,000. Both ran step 2, setting balance = 5,000. Wallet A has been debited *once* but the system thinks it was debited *twice*, or, depending on which UPDATE landed last, *not at all*. Either way, the database state and the world state disagree. This is a **lost update**, and it's the canonical concurrency bug.

**Failure 3: the merchant retried.** The response from step 5 was lost in transit. The merchant retried. Both transfers ran. The merchant's customer received 10,000 NGN instead of 5,000.

**Failure 4: an integrity bug shipped.** A subtle off-by-one in the handler caused step 3 to credit `amount + 1` instead of `amount`. Over a week, ledger drift accumulates to thousands of currency units. Nobody notices until the monthly accounting close, by which time the cause is buried in commits.

**Failure 5: an external dependency died.** Step 3 actually involves an HTTP call to an upstream banking partner. The call returns 504 Gateway Timeout. The handler doesn't know whether the credit succeeded on the partner's side. It returns 500 to the merchant. The merchant retries. Now the partner *might* have processed two credits, or one, or zero, and the merchant's customer either has nothing, the right amount, or double.

These five failures don't happen in isolation. In a busy system they happen *constantly*, often simultaneously. A naive payment processor accumulates corruption silently and the bill comes due at reconciliation time, which is too late.

The interesting question is not "how do we avoid these failures", you cannot, they are intrinsic to operating a distributed system. The interesting question is "how do we structure the system so that each one is automatically detected and recovered, leaving the world in a consistent state."

That structuring is what RRQ is.

---

## The five failure modes, named and classified

The failures above map to five distinct problem categories. Each has a corresponding pattern in RRQ. This mapping is the rosetta stone, once you have it, the architecture stops being a list of components and becomes a list of solutions to specific problems.

### A. Partial completion of a multi-step operation

**The problem.** Operation has steps S1, S2, S3. S1 and S2 succeed. S3 fails or the worker dies. The world is now in a state that wasn't supposed to exist.

**Why a database transaction doesn't fix it.** Database transactions cover a single connection's writes within a single database. The moment your operation crosses processes, services, or databases, or involves an external HTTP call, the transaction boundary doesn't reach the work that needs atomicity. You cannot wrap "debit wallet in our DB, then call partner bank" in a database transaction. The `BEGIN ... COMMIT` is over before the bank call even starts.

**RRQ's answer: sagas.** A saga is a sequence of steps where each step has a corresponding compensation. If S3 fails, the system runs the compensations for S2 and S1 in reverse, returning the world to a consistent state. The state of which steps have been done is **persisted** to a durable store (Postgres) before each step transitions. If the worker crashes, a replacement reads the persisted state and resumes from exactly where the dead worker stopped. ([Deep dive: `21-SAGAS.md`](deep-dives/21-SAGAS.md))

Sagas are not magic. They have costs:

- **Compensations must exist.** Some operations have no real undo (you can't un-send a notification). The system has to be designed so that genuinely-irreversible operations are last in the chain or quarantined behind a confirmation step.
- **Compensations must be idempotent.** If the saga crashes during compensation, the compensation will be retried. Running it twice must produce the same result as running it once.
- **Reading saga state is mandatory before every step.** The replacement worker doesn't know whether the previous worker did step 2 or not. It reads. This is overhead but it's the price of crash-safety.

### B. Concurrent operations on shared state

**The problem.** Two operations, running on different workers (or different threads), both need to modify the same wallet. Without coordination, one of their writes is lost.

**Why "use a database row lock" doesn't fix it.** Database row locks live for the duration of a database transaction. As argued in (A), a saga's atomic section is *longer* than a database transaction. By the time you'd want to release the row lock (after S3 completes), the transaction that holds it has long since committed and released the lock automatically.

**RRQ's answer: distributed locks via Redlock.** Before the first wallet-mutating step in a saga, acquire a lock on each wallet involved. The lock lives in Redis with a TTL longer than the saga's expected duration. Hold it through the entire wallet-touching section of the saga. Release it after the saga commits to a terminal state. Any other saga that wants to touch one of the same wallets blocks on the lock until released. ([Deep dive: `23-LOCKING.md`](deep-dives/23-LOCKING.md))

Costs:

- **Lock TTLs are tricky.** Too short: the saga outlives its lock and another saga starts working on the same wallet. Too long: a crashed worker holds a lock that nobody else can take, blocking that wallet for the TTL duration. RRQ's answer is "long enough that healthy sagas always finish in time" plus a watchdog that extends the lease for genuinely long-running sagas.
- **Single-Redis-node Redlock is not safe in production.** A real production deployment requires a majority of independent Redis nodes. RRQ runs a single node and documents this clearly; the algorithm is unchanged when you scale up.

### C. Duplicate requests from retries

**The problem.** Merchant didn't hear back, so they retried. Without idempotency, the underlying operation runs twice.

**RRQ's answer: idempotency keys.** Every state-changing API request carries an `Idempotency-Key` header that the merchant generates. The system uses this key as a distributed mutex: the first request with key K wins, all subsequent requests with key K either see "in progress" or get the cached response from the first execution. The underlying operation runs **at most once per key**, regardless of how many retries arrive. ([Deep dive: `20-IDEMPOTENCY.md`](deep-dives/20-IDEMPOTENCY.md))

Costs:

- **Cache lifetime is finite.** RRQ caches idempotency results for 24 hours. After that, retrying with the same key looks like a brand-new request. This is documented in the merchant API.
- **Concurrent duplicates need extra care.** If the merchant fires two simultaneous requests with the same key, you can't just check "does the key exist?", at the moment of check, neither does, but you can't admit both. This requires an *atomic* SETNX, not a check-then-set.

### D. Silent integrity bugs

**The problem.** A bug in the handler causes the database state and the "true" state (what the operation was supposed to do) to drift. Nobody knows because the state looks superficially fine, balances are non-negative, transactions exist, queries return reasonable numbers. The drift compounds until reconciliation, which might be weeks away.

**RRQ's answer: event sourcing + scheduled reconciliation.** The source of truth is not a mutable wallet balance. It is the append-only log of every event that happened. Wallet balances are *derived* from the event log by summing ledger entries. A scheduled job, nightly, replays the events and computes derived balances from scratch, then compares them to the materialized ledger. **Any disagreement is an alert**, not a silent correction. The mismatch indicates a bug, either in the saga (event written but ledger entry not), or in the projection (ledger entry exists but doesn't match the events). Either way, a human needs to look. ([Deep dive: `25-EVENT-STORE.md`](deep-dives/25-EVENT-STORE.md))

Costs:

- **Storage is larger.** Every state change is an event; the events are kept forever. RRQ accepts this; storage is cheap and audit value is high.
- **Computing balances is more expensive than reading a column.** RRQ mitigates with a `wallet_balance_cache` projection table refreshed asynchronously. The cache is a read optimization, not a source of truth, reconciliation always uses derived balances.
- **Reconciliation is genuinely CPU-intensive at scale.** A million events takes seconds to replay. The benchmarks measure this explicitly; it's the headline Go-vs-Rust comparison.

### E. External dependency unavailability

**The problem.** A downstream service (merchant webhook endpoint, FX rate provider, partner bank API) becomes unresponsive. Without protection, your workers waste resources on doomed retries and may even cascade-fail the rest of the system.

**RRQ's answer: circuit breakers, exponential backoff with jitter, and DLQs.** The circuit breaker stops attempting calls to a known-failing endpoint, fast-failing requests for a cooldown period before tentatively retrying. Backoff with jitter spreads out retries so a recovering endpoint isn't crushed by a thundering herd. The DLQ is the terminal destination for messages that have exhausted all retries, a place where a human can decide what to do, rather than an infinite retry loop or a silent drop. ([Deep dive: `24-RESILIENCE.md`](deep-dives/24-RESILIENCE.md))

Costs:

- **Circuit breakers can mask real problems.** If the breaker is too aggressive, transient blips trip it and customers see fast-failures unnecessarily. If too lenient, it doesn't protect the system. Tuning is empirical.
- **Long backoff means long mean-time-to-recover.** If the merchant endpoint comes back online during the breaker's cooldown, RRQ won't notice for the cooldown duration. Acceptable tradeoff, documented.
- **DLQs accumulate stale work.** If nobody monitors and drains the DLQ, it grows without bound. RRQ's Admin Dashboard is the operational answer; a healthy production system also alerts on DLQ size.

---

## The hidden hard parts: things people get wrong

The five failure modes above are well-known. The patterns that solve them are well-known. What's *not* well-known, what trips up engineers building a first distributed system, is the second-order subtleties. Here are the ones that matter for RRQ.

### Idempotency keys must be scoped per-merchant

Naive implementation: `idemp:{key}`. Two merchants happen to both use the UUID `123e4567-...` for unrelated requests. The second merchant's request is rejected because "the key already exists."

Correct: `idemp:{merchant_id}:{key}`. UUIDs aren't actually unique across merchants, they're unique *within* a merchant's request flow. Scope the key accordingly.

### Compensations must be idempotent

A naive compensation just runs the inverse: "credit the wallet back the amount we debited." If the saga crashed during compensation and the orchestrator runs it again, you've credited the wallet *twice*, and the source wallet is now richer than it started.

Correct: ledger entries have a `UNIQUE (saga_id, step_name)` constraint. The compensation tries to insert `(saga_42, compensation_credit, X)`. If it already exists from a previous attempt, the insert fails, the compensation no-ops, and the saga moves to the next step. The deduplication is in the database, not the code.

### "At-least-once delivery + idempotent handlers" is the only correct combination

You cannot get exactly-once delivery from a message broker. It is mathematically impossible in the general case (Two Generals' Problem applies). Any vendor or library that claims "exactly-once" either means "at-least-once with vendor-side deduplication that works most of the time" or "you have to make handlers idempotent yourself."

RRQ's answer: assume at-least-once. Make every handler idempotent. The combination achieves *effective* exactly-once. This is not a workaround; it is the canonical solution. There is no other one.

### Per-key ordering is harder than it looks

Suppose the fraud worker needs to process events for wallet W in order. Naive solution: a single consumer, processing serially. This works but doesn't scale, if you have 10,000 active wallets, you've serialized everything globally.

Better: a consumer group with multiple consumers. But Redis Streams' consumer group balances messages across consumers without regard to wallet_id, so two consumers might process two events for wallet W concurrently, violating the per-W ordering.

Correct: a two-level dispatch. The outer consumer pulls messages from the stream and routes them to per-wallet in-process queues. Each per-wallet queue is drained by a dedicated task. Different wallets run in parallel; same wallet runs serially. This is one of the most subtle pieces of RRQ; it gets a deep-dive of its own. ([→ `22-ORDERING.md`](deep-dives/22-ORDERING.md))

### Webhook ordering requires partitioning, not a global consumer

Same shape as fraud worker but with merchants instead of wallets. A merchant expects their webhooks in order: "transfer.completed" before "webhook.failed_to_deliver" before whatever the next event is. Solution: partition the notify stream into N shards (e.g., 16), with merchant_id mod 16 selecting the shard. Each shard is consumed by exactly one consumer at a time. Different merchants land on different shards and run in parallel; same merchant always lands on the same shard and runs serially.

### Timeouts don't tell you what happened

A request times out at 5 seconds. The downstream might have processed it (and is still trying to respond), might have crashed, might never have received it. Timeouts are observation gaps, not failure signals. You can't use a timeout to conclude "the operation didn't happen." You can only conclude "I don't know if it happened." Combined with idempotency, that's enough to make safe decisions; without idempotency, it's a recipe for double-charges.

### A clock you trust is a clock that lies to you

Wall clocks on distributed nodes drift. They go backward (NTP corrections). Two events with timestamps `t1` and `t2` where `t1 < t2` may have actually happened in the opposite order, or simultaneously. RRQ never uses wall-clock timestamps for ordering. Event ordering uses **monotonic event IDs** assigned by the event store at insert time. Wall-clock timestamps are recorded for debugging and human-readable reports, not for system logic.

### "Fail fast" applied wrongly is "fail loudly and lose work"

A common mistake: a worker hits a transient error and immediately writes the saga to "failed." But the error was transient, a network blip, a momentary database overload, and a retry would succeed. RRQ distinguishes:

- **Retryable errors:** network timeouts, 5xx from upstream, lock contention. Retry with backoff. Don't fail the saga.
- **Terminal errors:** 4xx from upstream that won't change (invalid wallet ID, insufficient balance, sanctioned destination). Fail the saga immediately.

The classifier matters. Misclassifying a retryable error as terminal causes spurious failures. Misclassifying a terminal error as retryable wastes resources on doomed work.

---

## What this leaves us with

The architecture documented in `00-OVERVIEW.md` is not a set of design choices an engineer made because they liked the patterns. Each component exists because removing it would unhandle one of the failure modes above. Specifically:

| Failure mode                          | RRQ component(s) that handles it                                       |
| ------------------------------------- | ---------------------------------------------------------------------- |
| A. Partial completion                 | Saga Worker (orchestrator, state persistence, compensations)            |
| B. Concurrent operations              | Redlock on wallet IDs, sorted to prevent deadlock                       |
| C. Duplicate requests                 | API Gateway idempotency middleware, atomic SETNX                        |
| D. Silent integrity bugs              | Event store + Reconciliation                                            |
| E. External dependency unavailability | Circuit breaker, exponential backoff with jitter, DLQ, Admin Dashboard replay |

There is a second column not yet drawn: which failure mode does *each component* exist for. Symmetry: every component should map to at least one failure mode, and every failure mode should be covered by at least one component. If a component doesn't map to a failure mode, it's probably not necessary. If a failure mode isn't covered, the system has a known correctness gap.

This map is the structure of the system. Everything in the rest of the docs is the elaboration.

---

## What this design is not trying to solve

It is worth being explicit about scope, because every payment-system architecture document on the internet is implicitly compared against PayPal's. RRQ is not trying to be PayPal.

**Not solved:** real custody of funds, real settlement to bank accounts, real card-network integration, real KYC/AML compliance, real multi-region failover, real fraud ML, real customer dispute resolution, real currency conversion with hedging, real chargeback handling against card networks. Each of these is a project of its own. RRQ is the *engine* underneath any system that does these things, the part that, given an instruction "move X from A to B," actually moves X from A to B with the correctness properties documented.

**Not solved (operational):** running a 24/7 on-call rotation, regulatory reporting, audit logs in the GDPR/SOX sense (RRQ's event log is internal), PCI-DSS compliance, anything involving paper checks. Out of scope.

**Not solved (performance):** RRQ targets 1,000 TPS on a single machine. A real payment processor at scale handles tens of thousands. Scaling RRQ to that regime requires sharding the database, partitioning Redis, and running many replicas, well-understood techniques that aren't novel and aren't part of RRQ's scope.

The point of saying all this is not modesty. It's that **a system with a clearly-stated scope is a stronger artifact than a system that pretends to do everything**. A reviewer who reads "RRQ is the correctness-critical core of a payment processor; here's exactly what it does and exactly what it doesn't" knows what they're looking at. A reviewer who reads "RRQ is a complete payment platform" and then finds that half the platform is missing concludes the author doesn't know what they don't know. The first framing is senior. The second is junior.

---

## Where to read next

- The testable invariants version of the above → [`02-INVARIANTS.md`](02-INVARIANTS.md)
- The system overview → [`00-OVERVIEW.md`](00-OVERVIEW.md)
- The mechanism behind a specific failure mode handler → corresponding deep-dive

---

*Pass 1 of the architecture series. Last updated pre-implementation.*
