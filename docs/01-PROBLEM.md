# 01: The Problem

> **What this is.** The problem RRQ exists to solve, in detail, with the specific failure modes that make it hard. Read this if the _why_ of the architecture is not obvious from `00-OVERVIEW.md`.
>
> **Reading time.** ~12 minutes.
>
> **Audience.** Engineers who haven't worked on payment systems before, or who have but want the failure modes named explicitly.

---

## The fundamental disanalogy

If you've written backend code before, you have an intuition for what "running an operation" means. You call a function, it runs to completion, you get a return value. If it fails, you get an exception. If it crashes, your process dies and you know it died. Three outcomes, all observable.

Networks are different. When you send a request to another machine and the response doesn't come back, you have _four_ outcomes:

1. The request reached the other machine and succeeded. You'll never know.
2. The request reached the other machine and failed. You'll never know.
3. The request never reached the other machine. You'll never know.
4. The request reached the machine, succeeded, the response was sent, and the response was lost in transit. You'll never know.

You cannot distinguish these. From your side, all four look identical: you sent something, you waited, nothing came back. This is the **unknown outcome**, and it is the source of approximately every difficult problem in distributed systems. Once you internalize that the unknown-outcome case is permanent — not something you can engineer away — the rest starts to make sense.

The naive response is to retry. If you didn't hear back, send it again. This is correct — and dangerous, because the request might have succeeded the first time, and now you're asking for it twice. If the operation is "send 1,000 NGN to recipient X," the merchant just sent 2,000.

The fix isn't "don't retry" (then you lose work to lost responses). The fix is to make retries _safe_ — design every operation so that doing it twice produces the same result as doing it once. That's **idempotency**, the first idea RRQ is built around.

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

Looks reasonable. It's wrong in five different ways, and each corresponds to an incident a real payment company has had.

**Failure 1: the worker dies between steps 2 and 3.** The pod was evicted, the OOM killer fired, the host faulted. Wallet A has been debited; B has not been credited. The 5,000 NGN is _gone_ — not in A, not in B, not reconstructible. When the merchant complains a week later, no one can explain where the money went.

**Failure 2: another transfer ran concurrently.** Two transfers from A both ran step 1 and read 10,000. Both ran step 2, setting 5,000. A was debited _once_ but the system thinks it happened twice (or, depending on which UPDATE landed last, not at all). This is a **lost update** — the canonical concurrency bug.

**Failure 3: the merchant retried.** The response from step 5 was lost. The merchant retried. Both transfers ran. Their customer received 10,000 instead of 5,000.

**Failure 4: an integrity bug shipped.** A subtle off-by-one made step 3 credit `amount + 1`. Over a week, ledger drift accumulates. Nobody notices until the monthly close, by which time the cause is buried in commits.

**Failure 5: an external dependency died.** In a fuller system step 3 is an HTTP call to a partner bank. It returns 504. The handler doesn't know whether the credit landed. It returns 500. The merchant retries. The partner _might_ now have processed two credits, or one, or zero.

These don't happen in isolation. In a busy system they happen _constantly_, often simultaneously. A naive processor accumulates corruption silently, and the bill comes due at reconciliation, too late. The interesting question is not "how do we avoid these" — you can't — but "how do we structure the system so each one is automatically prevented or recovered." That structuring is what RRQ is.

---

## The five failure modes, named and classified

The failures map to five problem categories. Each has a corresponding answer in RRQ. This mapping is the rosetta stone: once you have it, the architecture stops being a list of components and becomes a list of solutions to specific problems.

### A. Partial completion of a multi-step operation

**The problem.** An operation has parts that must all happen or none. A worker applies some, then dies. The world is now in a state that wasn't supposed to exist (Failure 1).

**Why the naive handler fails.** Its two `UPDATE`s aren't wrapped in a transaction, and it mutates balances in place, so a crash between them leaves a permanent half-state with no record of intent.

**RRQ's answer: one serializable transaction over an append-only ledger.** Because RRQ is a closed-loop ledger, both wallets live in the same database, so the debit and the credit are written as two `ledger_entries` rows **in a single transaction**. They commit together or not at all. A crash mid-transaction rolls the whole thing back — there is no half-state to find, and the job is simply redelivered and re-run. This is the decisive consequence of the closed-loop scope: the operation never crosses a boundary a transaction can't cover, so a transaction is all you need. (A **saga** — split the operation into steps with compensating undos — is the tool you'd reach for only if the legs _couldn't_ share a transaction, e.g. across databases or an external bank call. RRQ scopes those out precisely so it never has to.)

### B. Concurrent operations on shared state

**The problem.** Two transfers, on different workers, both need to modify the same wallet. Without coordination, one write is lost (Failure 2).

**Why "just use a row lock" is exactly right here.** In the naive handler the read and the write straddle no transaction, so the row lock doesn't span them. In RRQ, the balance check and the debit are in the _same_ transaction, so a `SELECT … FOR UPDATE` on the wallet row holds for the whole critical section.

**RRQ's answer: an in-transaction row lock.** The posting transaction locks both wallet rows (`FOR UPDATE`, ordered by id to avoid deadlock), reads the source balance under the lock, and posts. Any other transfer touching either wallet waits. The lock is released automatically at `COMMIT` — there is no distributed lease, no TTL to tune, no watchdog, and no "lock expired mid-operation" edge case, because there is no multi-step window for it to guard.

### C. Duplicate requests from retries

**The problem.** The merchant didn't hear back, so they retried. Without idempotency the operation runs twice (Failure 3).

**RRQ's answer: a durable idempotency key in Postgres.** Every state-changing request carries a merchant-supplied `Idempotency-Key`. The gateway records it as a row with `UNIQUE (merchant_id, idempotency_key)` and inserts via `ON CONFLICT DO NOTHING`: the first request wins, every retry conflicts and gets the cached response. The operation runs **at most once per key**, and the claim is durable — it survives a node failure, because it's a committed database row, not a cache entry.

Cost: the key is meaningful for a documented window (default 24 hours of retained job rows); a retry strategy spanning longer must use a fresh key. Documented in the merchant API.

### D. Silent integrity bugs

**The problem.** A handler bug makes the stored state drift from the "true" state. Nobody notices because the state looks fine — balances non-negative, rows present, queries reasonable. The drift compounds until reconciliation, weeks away (Failure 4).

**RRQ's answer: an immutable ledger + scheduled reconciliation.** The source of truth is not a mutable balance; it is the append-only log of double-entry postings. Balances are _derived_ by summing postings. A nightly job re-derives every balance from scratch and checks it against the balance cache and the conservation invariant. **Any disagreement is an alert, never a silent correction** — divergence means a bug, and a human must look.

Cost: storage grows (postings are kept forever — cheap, high audit value); deriving balances is more expensive than reading a column (mitigated by the `wallet_balance_cache` projection, which reconciliation never trusts as ground truth).

### E. External dependency unavailability

**The problem.** A downstream service — most concretely, a merchant's webhook endpoint — becomes unresponsive. Without protection, workers waste resources on doomed retries and may cascade-fail (Failure 5).

**RRQ's answer: circuit breakers, exponential backoff with jitter, and DLQs.** The breaker stops calling a known-failing endpoint, fast-failing for a cooldown before a tentative retry. Backoff with jitter spreads retries so a recovering endpoint isn't crushed. The DLQ is the terminal home for messages that exhaust retries — a place a human decides what to do, rather than an infinite loop or a silent drop.

Cost: breakers can mask real problems if mistuned; long backoff lengthens mean-time-to-recover; DLQs accumulate stale work if nobody drains them (the Admin Dashboard is the operational answer).

---

## The hidden hard parts: things people get wrong

The five failure modes are well-known. What trips up engineers building a first distributed system is the second-order subtleties.

### Idempotency keys must be scoped per-merchant

Naive: `idempotency_key` is globally unique. Two merchants both use the UUID `123e4567-…` for unrelated requests; the second is wrongly rejected. Correct: the unique key is `(merchant_id, idempotency_key)`. UUIDs are unique _within_ a merchant's flow, not across merchants.

### Idempotency must be durable, not cached

It is tempting to make the idempotency check a fast Redis `SETNX`. But a cache can lose a recently-written key on a node failure, and then a retry in that window double-executes. For money, the claim must be as durable as the money itself — so it lives as a committed row in the same Postgres that holds the ledger, with the database's `UNIQUE` constraint doing the enforcement. There is no second, faster mechanism in front of it to disagree with it.

### Posting must be idempotent under redelivery

The broker delivers at-least-once, so the Ledger Worker will sometimes see the same job twice. The defense is a database constraint, not careful code: `UNIQUE (transfer_id, leg)` on the postings. A redelivered job re-attempts the same two inserts; the constraint no-ops them. The deduplication lives in the database, where it can't be bypassed.

### "At-least-once delivery + idempotent handlers" is the only correct combination

You cannot get exactly-once delivery from a message broker — it's impossible in the general case (the Two Generals' Problem). Any product claiming "exactly-once" means "at-least-once with vendor-side dedup that usually works" or "make your handlers idempotent." RRQ assumes at-least-once and makes every handler idempotent. The combination is _effective_ exactly-once. There is no other correct answer.

### Per-key ordering across a fleet needs more than a consumer group

Some places need events for one key processed in order — a merchant's webhooks, so their state machine doesn't see `completed` before `requested`. The naive fix, "use a consumer group, it'll order them," is wrong once you run more than one replica (which RRQ always does): a consumer group load-balances _consecutive_ messages across consumers, so two events for one key get processed concurrently, in either order. A group gives you _delivery_, not _order_.

The real requirement is **single-writer-per-key, enforced outside any one process**, and RRQ needs it in exactly one place, met one way:

- **Per-merchant webhooks (ordering required).** Publish to a Kafka `notify` topic partitioned by `merchant_id`. Kafka assigns each partition to exactly one live worker, so per-merchant order survives across replicas; on a worker death Kafka rebalances. The broker enforces it, not application code.

And two places where it _looks_ required but isn't:

- **Per-wallet posting order** is enforced by the database itself: a wallet's `ledger_entries` get a monotonic `id`, and concurrent transfers on one wallet are serialized by the row lock. There is no separate ordering mechanism to build.
- **Per-wallet fraud velocity** needs no ordering at all: the window is a shared Redis structure mutated atomically, so concurrent events for one wallet produce the same count in any order. The lesson: before building per-key ordering, check whether you actually need it.

### Timeouts don't tell you what happened

A request times out at 5 seconds. The downstream might have processed it, crashed, or never received it. Timeouts are observation gaps, not failure signals. You can only conclude "I don't know if it happened" — which, combined with idempotency, is enough to make safe decisions.

### A clock you trust is a clock that lies to you

Wall clocks drift and jump backward (NTP corrections). Two events with `t1 < t2` may have happened in the opposite order. RRQ never uses wall-clock time for ordering — it uses the monotonic `ledger_entries.id` / `events.id` assigned by Postgres at insert. Wall-clock timestamps are recorded for debugging, not for logic.

### "Fail fast" applied wrongly is "fail loudly and lose work"

A worker hits a transient error and immediately marks the job failed — but the error was a network blip, and a retry would have succeeded. RRQ distinguishes **retryable** errors (timeouts, transient DB unavailability, serialization conflicts → retry with backoff) from **terminal** errors (insufficient balance, frozen wallet, bad currency → fail immediately). Misclassifying either way is costly, so the classifier is explicit and tested.

---

## What this leaves us with

The architecture in `00-OVERVIEW.md` is not a set of patterns chosen because they're fashionable. Each component exists because removing it would unhandle one of the failure modes above:

| Failure mode                          | RRQ mechanism that handles it                                        |
| ------------------------------------- | -------------------------------------------------------------------- |
| A. Partial completion                 | One serializable transaction over the append-only ledger             |
| B. Concurrent operations              | In-transaction `FOR UPDATE` row lock, wallets ordered by id          |
| C. Duplicate requests                 | Durable `UNIQUE (merchant_id, idempotency_key)` in Postgres          |
| D. Silent integrity bugs              | Immutable postings + nightly reconciliation                          |
| E. External dependency unavailability | Circuit breaker, backoff with jitter, DLQ, Admin Dashboard replay    |

Every component maps to at least one failure mode; every failure mode is covered by at least one mechanism. If a component mapped to nothing, it wouldn't be necessary; if a failure mode were uncovered, the system would have a known correctness gap. This map is the structure of the system.

---

## What this design is not trying to solve

Every payment-architecture document on the internet is implicitly compared against PayPal's. RRQ is not trying to be PayPal.

**Not solved:** real custody of funds, settlement to bank accounts, card-network integration, KYC/AML, multi-region failover, fraud ML, dispute resolution, currency conversion with hedging, chargebacks against card networks. Each is a project of its own. RRQ is the _engine_ underneath any system that does these — the part that, given "move X from A to B," actually moves it with the documented correctness properties.

**Not solved (operational):** 24/7 on-call, regulatory reporting, GDPR/SOX-grade external audit, PCI-DSS. Out of scope.

**Not solved (scale-out of the write ledger):** RRQ keeps **one logical Postgres ledger** so that every transfer is one transaction — that is a deliberate correctness choice, not an oversight. It scales reads (standbys + projections) and the stateless tier (replicas) horizontally, and it is highly available (primary + standby failover). What it does _not_ do is shard the authoritative ledger across machines or span regions — doing so would force money movements to cross a transaction boundary and reintroduce the exact multi-step, compensating machinery this design exists to avoid. The ~1,000 TPS figure is what the benchmarks prove on a small cluster, not a hard ceiling — see [`03-SCALING-AND-AVAILABILITY.md`](03-SCALING-AND-AVAILABILITY.md).

---

## Where to read next

- The testable-invariants version of the above → [`02-INVARIANTS.md`](02-INVARIANTS.md)
- The system overview → [`00-OVERVIEW.md`](00-OVERVIEW.md)
- The mechanism behind a specific failure-mode handler → the corresponding deep-dive

---

_Pass 1 of the architecture series. Last updated pre-implementation._
