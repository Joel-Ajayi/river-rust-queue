# 22: Ordering

> **What this is.** The deep dive on ordering guarantees in RRQ. The system has three distinct ordering problems, per-wallet, per-merchant, per-saga, and each is solved by a different mechanism. This doc explains why one-size-fits-all doesn't work and what each mechanism gives up to gain its specific guarantee.
>
> **Reading time.** ~20 minutes.
>
> **Prerequisites.** Service docs for [Saga Worker](../services/11-SAGA-WORKER.md), [Webhook Worker](../services/12-WEBHOOK-WORKER.md), and [Fraud Worker](../services/13-FRAUD-WORKER.md).

---

## Why ordering is hard

Ordering in distributed systems is hard because the obvious property, "events happen in some order, just preserve it", is wrong from the start. There is no single "order" in which events happen. Two events on different machines have no inherent temporal relationship; their clocks disagree, their messages arrive at different speeds, and the very notion of "simultaneity" breaks down at network scale.

What distributed systems can offer is **ordering relative to some scope**. RRQ defines three scopes that matter:

- **Per saga** (atomic): within one logical operation, steps must occur in their designed sequence.
- **Per merchant** (sequential): notifications to one merchant must arrive in the order events occurred from their perspective.
- **Per wallet** (causal): events affecting one wallet must be processed in their causal order, especially for stateful detection.

These are *not* the same problem. A solution for one isn't automatically a solution for another. Trying to solve all three with one mechanism, a single global serial consumer, say, would either fail to scale or fail to preserve the orderings that actually matter.

This doc walks through each scope, the mechanism that solves it, and the alternatives that don't.

---

## Per-saga atomicity: Redlock

### The problem

A single Transfer saga touches two wallets. Within the saga, the steps must run in order: Validate → Lock → Debit → Credit → Complete. If another saga runs concurrently on the same wallets while the first is mid-transition, the wallets' states could change between the first saga's steps, violating its assumptions.

Concrete example: Saga A is debiting wallet W1 and crediting wallet W2. After A's Validate confirms W1.balance = 100 is sufficient for amount = 50, but *before* A's Debit, Saga B sneaks in and debits W1 by 60. Saga A's Debit then runs against a wallet that no longer has the assumed balance. Either the debit succeeds (going negative, violating I2) or the database catches it (raising an error during what should be the happy path).

The same problem in different form: two sagas A and B both debit W1 simultaneously. Both read balance = 100, both decide they can subtract 60. The first to commit lands successfully. The second tries to commit and either succeeds (negative balance) or fails (but A's commit already wrote `DebitApplied`, leaving an asymmetry between events and ledger). Either outcome is wrong.

The mechanism for preventing this is a **distributed lock** on the wallet IDs. While Saga A holds the lock on W1, no other saga can mutate W1's state. Saga B, attempting to debit W1, blocks on the lock acquisition until A finishes.

### Redlock specifics

The lock mechanism is Redlock, an algorithm published by Salvatore Sanfilippo (Redis's creator) in 2014. The algorithm:

1. Client generates a unique token (e.g., `saga_id`).
2. Client attempts to acquire the lock by writing the token to N independent Redis nodes, with a TTL: `SET lock:<resource> <token> NX PX <ttl_ms>`.
3. If a majority of nodes (N/2 + 1) confirm the write within an acceptable time, the lock is held.
4. If not, the client releases whatever partial locks it acquired and retries with backoff.
5. To release the lock, the client runs a Lua script on each node that deletes the key *only if* the value matches the token. This prevents accidentally releasing a lock acquired by someone else (after our TTL expired, another client's token replaced ours).

The "majority of N independent nodes" is what makes the algorithm tolerant of single-node failure. With N=5 nodes, the algorithm tolerates 2 nodes failing without losing the lock's safety property.

**Why not a single-node SETNX?** With one Redis node, if the node dies and another takes over (via failover), the lock state can be lost or inconsistent, two clients could each believe they hold the lock. Single-node SETNX is sufficient for non-critical advisory locking but not for protecting financial state.

**RRQ deploys on a single Redis node.** This is a documented gap: production Redlock requires 3+ independent nodes. The algorithm is implemented correctly so it scales to multi-node trivially when needed; only the deployment changes. The trade-off: operational simplicity over production-grade lock safety.

### Why locks specifically, not transactions

Could we use a database transaction instead? In principle, `BEGIN; SELECT FOR UPDATE; UPDATE; COMMIT` would lock the rows for the duration of the transaction.

The problem: the saga spans many database transactions. Its scope is longer than any single transaction's. The Validate step uses one connection; the Debit step uses another; the Credit step uses another. Holding a row lock across all of them would mean holding a database transaction open across all of them, which means a single connection per saga, which doesn't compose with connection pooling.

You could hack around this with Postgres advisory locks (which can span transactions on a single session), but you'd still need to keep the session open across the whole saga. And advisory locks are local to one Postgres instance, so if you ever scale to multiple Postgres replicas or shards, the advisory lock doesn't extend across them. Redlock does, by design.

The deeper answer: **a saga isn't a database operation, it's a process.** Locks that span the saga's scope have to live outside the database. Redis is the natural choice for that, it's the place where "ephemeral coordination state" already lives in the system.

### The deadlock problem and lexicographic sorting

If two sagas need locks on the same two wallets, in different orders, you can deadlock:

- Saga A: holds lock on W1, tries to acquire lock on W2.
- Saga B: holds lock on W2, tries to acquire lock on W1.

Neither can proceed. Classic deadlock.

The fix: **lock IDs in a consistent order across all sagas.** If both sagas always lock W1 first, then W2, they can't deadlock, one of them gets to W1 first, the other waits, but neither holds something the first needs.

The order is lexicographic on the wallet IDs (strings sorted as bytes). For Saga A with wallets `[W2, W1]` and Saga B with wallets `[W1, W2]`, both compute the sorted order `[W1, W2]` and lock W1 first. Deadlock impossible.

This is one of those tiny details that's invisible when it works and catastrophic when it doesn't. The first time anyone observes a deadlocked saga in production, they look at the lock acquisition code and discover the bug. Adding it correctly from the start saves an incident.

### What if the lock TTL expires mid-saga?

Realistic scenario: saga is running normally, but the wallet-mutating section takes longer than expected, maybe Postgres had a brief slowdown. The lock TTL expires. Another saga acquires the lock. Now two sagas are operating on the same wallet concurrently, exactly the situation locking was supposed to prevent.

Two defenses:

1. **The TTL is set comfortably above the longest expected duration.** RRQ uses 5 seconds; the mutating section is normally < 500ms. The expected case is the lock has plenty of headroom.
2. **The compensating-credit / debit pattern is idempotent.** Even if two sagas did concurrent work, the `UNIQUE(saga_id, step_name)` constraint on `ledger_entries` prevents double-writes. The worst-case effect of TTL expiry is two sagas both *trying* to advance, and both eventually realize "I no longer hold the lock" via fencing tokens (see below).

**Fencing tokens** are a stronger solution still. Each lock acquisition increments a monotonic counter (the fencing token). When the saga writes to the ledger, it includes the token. The database can refuse writes from stale tokens. RRQ doesn't implement fencing tokens, the TTL-plus-idempotency combo is sufficient at our scale; they're a known enhancement if it ever isn't. The trade-off: fencing tokens add per-write overhead; the TTL approach is simpler and works for most workloads.

The detailed treatment of locking, including the formal proof sketch and Martin Kleppmann's critique of the Redlock algorithm, lives in [`23-LOCKING.md`](23-LOCKING.md).

---

## Per-merchant ordering: stream partitioning

### The problem

A merchant builds a state machine on top of webhooks: they receive `transfer.requested`, then `transfer.completed` (or `transfer.failed`), then maybe `webhook.delivered`. If these arrive out of order, `transfer.completed` before `transfer.requested`, the merchant's state machine breaks. Either they handle "completed for an unknown transfer" defensively (and they often don't), or their database ends up in an inconsistent state.

The system has to guarantee that for a given merchant, webhooks are *attempted* in the order their source events occurred. Cross-merchant order doesn't matter; each merchant only sees their own webhooks anyway.

### Why a global ordering doesn't work

The naive solution: one consumer, one stream, one merchant at a time. Process events in stream order, deliver to each merchant in turn.

This trivially gives ordering. It also gives terrible throughput. One slow merchant blocks every other merchant. A merchant with a 5-second-response endpoint blocks the entire system for 5 seconds per webhook.

You need parallelism across merchants. But naive parallelism, a consumer group with N consumers all reading from one stream, destroys ordering. Two consumers can each grab a message for merchant M, and process them concurrently, in either order.

### The partitioning approach

The fix: partition the stream by merchant_id. Compute `shard = hash(merchant_id) mod N` (N is typically 16) and write to `stream:notify-<shard>`. Each shard is consumed by exactly one consumer at a time within the `webhook-workers` consumer group, Redis Streams enforces this for normal stream consumption (one message goes to one consumer in the group).

Result: all of merchant M's events go to the same shard, consumed by one consumer, in arrival order. Different merchants land on (potentially) different shards and run in parallel.

```
                    Saga Worker
                         │
                         │ XADD stream:notify-{hash(merchant_id) mod 16}
                         ▼
       ┌─────────────────────────────────────────────┐
       │   stream:notify-0   stream:notify-1  ...    │
       │   stream:notify-7   stream:notify-15        │
       └────┬──────────────┬──────────┬──────────────┘
            │              │          │
            ▼              ▼          ▼
       ┌─────────┐    ┌─────────┐    ┌─────────┐
       │ Worker  │    │ Worker  │    │ Worker  │
       │ Group:  │    │ Group:  │    │ Group:  │
       │ webhook │    │ webhook │    │ webhook │
       └─────────┘    └─────────┘    └─────────┘
```

The number of shards is fixed at startup (configuration). 16 is a reasonable default for the throughput levels RRQ targets, rule of thumb is 4× the number of worker replicas, so 4 workers handle 16 shards = 4 shards each.

### Why this works

The key property is that for a given merchant M, *all* of M's events have the same shard. The shard is determined by `hash(merchant_id) mod 16`, and the hash is deterministic. There's no way for M's events to split across shards.

Within a single shard, Redis Streams guarantees serial processing within a consumer group: each message in the shard goes to exactly one consumer, the consumer processes it (acks it), and only then can the next message be claimed. No parallelism within a shard.

So M's events are processed serially. Order preserved.

### The cost

**Load imbalance.** Sixteen shards distribute *number of merchants* evenly across shards (via hash uniformity), but they don't distribute *load* evenly. A merchant with 1000 events/sec and a merchant with 1 event/day each contribute one slot to their shard. The slot is the same; the load isn't. A shard with a hot merchant becomes the bottleneck; other shards are idle.

For RRQ's scale, this is fine. We have a small number of merchants generating most traffic, and we'll have 4 workers with 4 shards each. The worst case is "one merchant generates all the traffic on one shard", and that's still a single slow consumer, which is the baseline we started from.

For very large scale, you'd address this with more shards (smaller granularity) or shard-rebalancing strategies. RRQ doesn't need either at its target scale.

**Resharding is hard.** If you ever decide to change the shard count (16 → 32), you change which merchant hashes to which shard. Mid-flight, some events are in the old shard scheme and some in the new. You either drain the stream completely before changing (downtime) or use a more complex rebalancing scheme. RRQ doesn't reshard; the count is fixed.

### Why not by merchant directly?

Why partition by hash rather than by literally `stream:notify:<merchant_id>`?

Two reasons. First, stream count: with one stream per merchant, you'd have thousands of streams. Redis handles this technically, but operational tooling (monitoring, lag tracking, consumer assignment) gets unwieldy. Second, consumer assignment: with N streams, you need N consumer assignments, and if your worker count doesn't divide evenly into N, you have load imbalance you didn't plan for.

Hash-partitioning gives you a fixed, small set of streams (16) regardless of merchant count. Easier to operate.

---

## Per-wallet ordering: in-process two-level dispatch

### The problem

The fraud worker needs to process events for wallet W in causal order. A velocity computation over a sliding window is stateful, out-of-order events would give wrong counts.

This is shaped like the webhook problem (per-key ordering with parallelism), but with one crucial difference: **wallet cardinality is much higher than merchant cardinality**, and load skew is much worse.

A merchant has typically a handful to dozens of wallets, all serving their transactions. A merchant's primary funding wallet might generate 1000 events/sec while most of their wallets generate one event per week. With merchants there are maybe thousands of active accounts at a given moment; with wallets there are tens of thousands to millions.

If you partition by wallet_id the same way you partition by merchant_id, the hot wallets all hash to single shards and bottleneck those shards. Adding more shards doesn't help, the hot wallet still hashes to one. You'd need *adaptive* partitioning (rebalance shards based on load), which is significantly more complex than fixed partitioning.

### The dispatch approach

The alternative: don't partition at the stream level. Instead, every fraud worker reads any event, then routes the event to an in-process queue dedicated to that wallet.

```
                    Job stream (any consumer reads any event)
                         │
                         ▼
       ┌──────────────────────────────────────────────┐
       │              Fraud Worker process            │
       │                                              │
       │   ┌──────────┐                               │
       │   │  Outer   │  Reads event, looks at        │
       │   │ Consumer │  wallet_id, dispatches        │
       │   └────┬─────┘                               │
       │        │                                     │
       │        ▼                                     │
       │   ┌────────────┐                             │
       │   │ Dispatcher │  map[WalletID]chan Event    │
       │   └────┬───────┘                             │
       │        │                                     │
       │        ▼                                     │
       │   ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐        │
       │   │ W1   │ │ W2   │ │ W3   │ │ W4   │  ...   │
       │   │ task │ │ task │ │ task │ │ task │        │
       │   └──────┘ └──────┘ └──────┘ └──────┘        │
       │   (one task per active wallet)               │
       │                                              │
       └──────────────────────────────────────────────┘
```

The properties:

- **Per-wallet ordering preserved.** Each wallet has one task draining its in-process channel. Within the task, processing is strictly serial. So events for W1 are processed in their dispatch order.
- **Parallelism across wallets.** Different wallets have different tasks running concurrently. The scheduling is the language runtime's (Go's scheduler, Tokio's executor), both are highly tuned for this kind of "many small tasks" workload.
- **No static partitioning.** Tasks are spawned lazily when a wallet's first event arrives. No pre-allocation; no configuration of "max wallets."
- **Load follows demand.** Hot wallets have busy tasks; cold wallets have idle tasks. Idle tasks exit after a timeout (5 minutes) to reclaim resources.

### Why not just partition?

Stream partitioning works for merchants because (a) merchant cardinality is bounded (thousands, not millions), (b) load distribution is somewhat even across merchants, and (c) the cost of one shard bottlenecking is acceptable (one slow merchant blocks events for ~1/16 of the system).

None of those hold for wallets:

- Cardinality is high (millions).
- Load skew is severe (hot wallets dominate).
- A bottlenecked shard would be a much bigger fraction of the system.

The two-level dispatch handles this by *moving the partitioning from the stream layer to the in-process layer*. The stream is read by any consumer (load-balanced across the consumer group); per-wallet ordering is enforced by the dispatcher's tasks. This is the **"actor per key" pattern** common in financial engines, game servers, and event-sourced systems.

### Why this works

The correctness argument:

1. The outer consumer reads events from the stream in arrival order.
2. For each event, the dispatcher routes it to the wallet's channel.
3. The channel is FIFO (Go's channels are; Tokio's `mpsc::channel` is).
4. The wallet's task drains the channel serially.

Per-wallet ordering preserved through the chain: arrival order → channel order → processing order.

Cross-wallet parallelism comes from different wallets having different tasks; the language runtime schedules them concurrently on multiple cores.

### The cost

**The ACK question.** When does the outer consumer ACK the stream message? After dispatching to the channel (before the wallet task processes), or after the wallet task confirms processing?

If we ACK before processing: a worker crash between ACK and processing loses the event. The next worker won't see it (it's already ACKed); only reconciliation can catch the gap.

If we ACK after processing: the outer consumer has to wait for the wallet task to confirm. This serializes the outer consumer to the slowest wallet task, killing throughput. Plus the coordination is non-trivial, you need a back-channel from task to dispatcher.

RRQ takes the first option (ACK before processing) and accepts the small loss window for fraud (a detective control, where missing a few events doesn't break anything fundamentally). This is documented in [`../services/13-FRAUD-WORKER.md`](../services/13-FRAUD-WORKER.md) and the system's STATUS.

For a stronger guarantee (say, in a hypothetical future critical-fraud variant), you'd choose the second option and accept the throughput cost.

**Task lifecycle management.** Tasks are spawned per wallet; they consume memory (goroutine stack ~2KB, channel buffer ~13KB). For millions of total wallets, you'd run out of memory if every wallet ever seen had a permanent task. The mitigation is the idle timeout, tasks exit after 5 minutes of inactivity, freeing memory. Active task count is bounded by "currently busy wallets," which is much smaller.

**Coordination overhead within a worker.** The dispatcher's `map[WalletID]chan Event` is accessed on every event. RRQ uses `sync.RWMutex` with a fast-path read-lock and slow-path write-lock-with-double-check (in Go) or `DashMap` (lock-free concurrent map, in Rust). At scale, this contention is measurable but not blocking.

### Comparing to actor frameworks

Some readers will recognize this as the standard pattern in actor systems (Akka, Erlang/OTP, Orleans). Actor systems treat the "per-actor mailbox + serial processing" as the unit of computation; RRQ does the same without the framework.

The choice not to use an actor framework: RRQ doesn't need actor *supervision*, *location transparency*, or *distributed actor placement*, features that justify the complexity of a framework. We need the *single-writer-per-key* pattern, which is a small dedicated implementation, not a framework. Smaller dependency surface, less to learn, easier to debug.

If we were building a much larger system with many such patterns, the calculus would shift. For RRQ's scope, in-language primitives win.

---

## Comparing the three approaches

The three ordering problems and their solutions:

| Problem | Mechanism | Granularity | Cost |
| --- | --- | --- | --- |
| Per saga atomicity | Distributed lock (Redlock) | Per resource (wallet) | Lock acquisition latency; lock TTL tuning |
| Per merchant ordering | Stream partitioning | Per shard (hash of merchant_id) | Load imbalance across shards; resharding hard |
| Per wallet ordering | In-process dispatch | Per wallet, lazy | Crash-before-process loses events; task overhead |

Each is the right tool for its problem. None of them generalizes well to the others:

- **Locks across all of a merchant's webhook deliveries?** Wasteful, the deliveries are independent operations; only the order matters, not exclusion.
- **Partitioning per saga?** Doesn't make sense, sagas are short-lived; you can't statically partition them.
- **In-process dispatch for wallet locking?** Doesn't work across workers, if Saga A on Worker 1 and Saga B on Worker 2 both touch wallet W, an in-process map on either worker doesn't see the other.

The lesson: **specific ordering guarantees come from specific mechanisms.** The naive impulse to find one elegant primitive that solves all of them produces a system that solves none well. RRQ's three mechanisms are the right answer because they each solve their own problem at the right level of abstraction.

---

## What about global ordering?

RRQ deliberately doesn't guarantee any global ordering. Across merchants, across wallets, across sagas, events are not strictly ordered. Two unrelated transfers may appear in the event log in either order, regardless of wall-clock time.

This is *intentional*. Global ordering would require a single sequencing point that every operation passes through, serializing the entire system on one resource. The cost, throughput cap and single point of failure, far exceeds the value, because no correctness property in RRQ depends on global ordering.

The properties that need ordering get it (per-saga, per-merchant, per-wallet); everything else is free to happen in any order. This is the difference between "ordered enough to be correct" and "ordered for the sake of ordering."

Some systems do need global ordering, financial markets with strict price-time priority, for instance. Those systems pay the throughput cost. RRQ doesn't, and shouldn't.

---

## Monotonic event IDs

A small but important note: within the event store, events have a monotonic `id` (BIGSERIAL). This gives a *global* sequence number for all events, just by virtue of being assigned at INSERT.

This is *not* a global ordering guarantee in the strict sense, two events with adjacent IDs might have been "concurrent" in any reasonable definition (their saga executions overlapped in real time). But the ID does give a useful property: **within the event store, replay is deterministic by ID order**. Reconciliation reads events by ID; it gets a consistent picture each time.

The monotonic ID is also what makes per-wallet ordering verifiable. Reconciliation queries `WHERE aggregate_id = W ORDER BY id ASC` and gets W's events in the order they were committed. As long as the writer respected per-wallet locking, this order is also the causal order. If the order is wrong (e.g., a credit appears before its paired debit), that's a bug, and reconciliation surfaces it.

The IDs are *not* timestamps. They're sequence numbers. Wall-clock time is recorded separately as `occurred_at` for debugging and human-readable reports, but never used for ordering.

---

## The vector-clock alternative

In academic distributed systems literature, a fancier approach to ordering uses **vector clocks** or **CRDTs** (conflict-free replicated data types). These let multiple writers operate on the same logical entity simultaneously, with mathematical guarantees about how their writes merge.

RRQ doesn't use any of this. The single-writer-per-key pattern (Redlock for sagas, dispatch tasks for fraud, partitions for webhooks) sidesteps the need for vector clocks entirely. As long as one writer at a time is mutating a given key, you don't need fancy merge logic.

The price: you can't write to the same key in parallel. For RRQ's workload, this is fine, wallets are typically modified one transfer at a time, not in concurrent batches. For workloads where parallel writes to the same key are essential (high-write-rate counters, distributed inventory), vector clocks become relevant.

It's worth knowing they exist; it's also worth knowing that for systems shaped like RRQ, you don't need them.

---

## Test patterns for ordering

Three categories of tests in the test suite verify the ordering guarantees:

**Per-saga atomicity tests.**
- Two concurrent sagas on the same wallet pair: assert serial execution, no negative balance.
- Sagas with sorted lock acquisition: assert no deadlock with adversarial pairings.

**Per-merchant ordering tests.**
- Emit 100 events for merchant M in known sequence: assert merchant's endpoint receives them in same sequence.
- Multiple merchants with one slow merchant: assert slow merchant doesn't block others; fast merchants stay ordered.

**Per-wallet ordering tests.**
- Emit 1000 events for wallet W in known sequence: assert per-wallet task processes in same sequence (instrument the task with a test recorder).
- High parallelism across many wallets: assert each wallet's sequence preserved while different wallets process concurrently.

The tests are simple to write because each ordering guarantee has a clean test fixture: emit ordered events, observe the consumer, compare sequences. Tests that verify a stronger property (linearizability, global ordering) are much harder; RRQ avoids them because the system doesn't claim those properties.

---

## A real-world reminder: ordering surprises in production

Even with all three mechanisms correct, ordering bugs can hide in places that aren't covered. A few examples worth being alert to:

**Logging order ≠ event order.** Workers logging to stdout will interleave their lines; the order in the log file doesn't reflect the order of events in the system. This catches people during debugging: "the log says A happened before B" doesn't mean A's event was committed before B's.

**Cross-system ordering.** If the saga writes to Postgres *and* writes to Redis (notify stream), the two writes are not atomically ordered. A reader looking at "the latest Postgres state" and "the latest Redis state" might see them at different points relative to each other. This is fine because each side has its own consistent ordering; cross-side ordering isn't claimed.

**Reconciliation against a moving target.** If reconciliation runs while sagas are completing, it might see an event without its paired ledger entry (transaction not committed yet on the ledger side). The safety margin (60-second window cutoff) addresses this. Without it, the reconciliation would produce false positives every night.

**The merchant's clock.** Merchants timestamp things on their end. If they timestamp a request at 14:23 and submit it to RRQ at 14:25, RRQ's `occurred_at` is 14:25 but the merchant thinks the request "happened" at 14:23. This is fine, the timestamps are for logging, but it can confuse cross-system debugging.

---

## Where to read next

- The locking algorithm in depth → [`23-LOCKING.md`](23-LOCKING.md)
- The fraud worker that uses two-level dispatch → [`../services/13-FRAUD-WORKER.md`](../services/13-FRAUD-WORKER.md)
- The webhook worker that uses stream partitioning → [`../services/12-WEBHOOK-WORKER.md`](../services/12-WEBHOOK-WORKER.md)

---

*Pass 3 of the architecture series. Last updated pre-implementation.*
