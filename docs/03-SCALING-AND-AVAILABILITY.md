# 03: Scaling & Availability

> **What this is.** The canonical account of how RRQ scales out and stays up. RRQ is a **highly available, horizontally scalable** system with **one strongly-consistent authoritative ledger**. Every other doc defers here for "how does this scale / how does it survive failure."
>
> **Reading time.** ~15 minutes.
>
> **Audience.** Anyone evaluating whether RRQ is a toy or a system that could actually run. If a service doc and this doc ever disagree about replicas, ownership, or the ledger's consistency, **this doc wins** and the other is a bug.

---

## The thesis, stated once

RRQ runs as a fleet, not a process — but its authoritative ledger is **one logical, strongly-consistent Postgres**. Three properties hold everywhere in the design, and the rest of this document is their consequences:

1. **No single point of failure.** Every component runs **≥ 2 live instances**. There is no "the ledger worker," only _the ledger workers_; the same for the edge, the relay, the dashboard, the batch jobs, _and_ the stateful backends (each runs primary + standby with automatic failover). Losing any one pod, node, or backend primary degrades capacity or pauses briefly, never breaks availability.
2. **Horizontal scale-out for the stateless tier.** The gateway, the relay, and the workers hold no durable state of their own. Throughput grows by adding replicas; an autoscaler does it off queue lag. They scale the way stateless tiers always do — the work is in a queue, more consumers drain it faster.
3. **One authoritative ledger, kept strongly consistent.** All money lives in a single logical Postgres so that **every transfer is one serializable transaction** ([→ `services/11-LEDGER-WORKER.md`](services/11-LEDGER-WORKER.md)). This is the deliberate design choice that gives RRQ exactly one money-movement flow. Its write throughput scales _vertically_ (one primary, plus HA standby); its reads scale _horizontally_ (standbys + projections).

What RRQ is **not**: sharded across machines for ledger writes, multi-region, active-active, or globally ordered. Those are real engineering projects with their own failure models and are deliberately out of scope. "Highly available" here means "survives the loss of any pod, node, or backend primary within a single region," not "survives the loss of a region."

> **A note on "highly available" vs "fallback."** Running two of something — two ledger-worker pods, a Postgres standby ready to be promoted — is *the same implementation, duplicated*. That is HA, and it is present everywhere. It is **not** a "fallback": a fallback is a *different* second mechanism that takes over when the first fails. RRQ has no fallbacks. It has one way to do each job, run on enough replicas to survive failure.

---

## Why the ledger is not sharded (and what that costs)

This is the load-bearing decision, so it gets its own section.

You could shard the ledger by merchant or wallet to scale writes horizontally. The problem: a transfer moves money between two wallets, and if those wallets land on **different shards**, you can no longer post both legs in one transaction — you'd be forced into a **second, different flow** (a cross-shard saga with compensations) for exactly the transfers that span shards. That second flow is strictly weaker (it has an in-flight, debited-but-not-credited window) and it's the precise machinery RRQ is designed to avoid. Sharding the write ledger trades RRQ's single, atomic money-movement flow for two flows whose behavior depends on where the data happens to live.

So RRQ keeps **one logical ledger** and scales its writes vertically. The honest cost: **write throughput is bounded by one Postgres primary.** That bound is high — a well-tuned primary on modern hardware posts thousands of small transactions per second, comfortably past the ~1,000 TPS the benchmarks demonstrate — and it is raised the boring way (faster disks, more cores, batching, connection pooling), not by changing the architecture. If a deployment genuinely outgrew a single primary, sharding the ledger would be a *new project* with its own (saga-based) correctness model — explicitly out of scope here, and called out so no reviewer mistakes the ceiling for an accident.

> **About the throughput number.** Elsewhere you'll see ~1,000 transfers/sec as a working figure. That is the _demonstrated_ target on a small cluster — the number the benchmark suite drives to — not the limit of the design. You raise it by adding stateless replicas until the primary is the bottleneck, then by scaling the primary vertically.

---

## Two axes of scale

| Axis | What scales | How | Bounded by |
| --- | --- | --- | --- |
| **Stateless tier** | API Gateway, Outbox Relay, Ledger / Webhook / Fraud workers, Admin Dashboard | Add replicas (HPA on consumer lag) | The ledger primary |
| **Read path** | Dashboard / operator queries | Postgres standbys + projections (CQRS) | Replication lag (seconds) |
| **Write ledger** | Postings | Vertical (one primary) + HA standby | One primary's write throughput |

The interesting engineering is making "more stateless replicas" _safe_ for the orderings RRQ promises — which is the next section.

---

## Single-writer-per-key, across a fleet

Most of RRQ's correctness rests on one idea: **for any key with ordered or exclusive semantics, exactly one writer acts on it at a time** — and "at a time" must hold _across replicas_, not just within a process. A Kafka/Redis consumer group does **not** give you this for free: a group load-balances consecutive messages across consumers, so two replicas can process two messages for the same key concurrently. A group gives you _delivery_, not _order_.

RRQ needs cross-replica single-writer semantics in exactly one place, and gets the other two for free from the database:

| Scope | Key | Mechanism that survives ≥2 replicas | Why this one |
| --- | --- | --- | --- |
| **Per-wallet posting** (ordering + exclusion) | wallet id | **In-transaction `SELECT … FOR UPDATE`** on the wallet row, inside the one posting transaction | The lock lives in the database with the data; concurrent transfers on a wallet serialize on it, and the monotonic `ledger_entries.id` gives the order. No process-level coordination exists to get wrong. |
| **Per-merchant webhooks** (ordering, I5) | merchant id → partition | **Kafka partitions** — each `notify` partition is consumed by exactly one live worker | Kafka consumer groups provide exclusive partition ownership out of the box, and it survives a worker death via rebalance. |
| **Per-wallet fraud velocity** | wallet id | **None — ordering is not required.** Velocity state is a shared atomic Redis structure | Counting is order-insensitive; per-wallet ordering across millions of wallets would cost more than it buys. |

The Ledger Worker is the **exemplar**: it scales to any number of replicas with zero ordering machinery of its own, because all of its coordination lives _inside the database_ — the row lock and the unique constraints. Two ledger workers cannot corrupt a wallet because the lock is in Postgres, not in either process's memory. Every other worker is measured against that standard.

### Kafka partitions: how per-merchant webhook order survives replicas

The outbox relay produces `transfer.completed`/`transfer.failed` events to a Kafka topic `notify` with `N` partitions (default `16`), using `merchant_id` as the message key. So **all of a merchant's events land on one partition, in `events.id` order**. Kafka's consumer-group protocol assigns each partition to exactly one live webhook worker, so a merchant's deliveries are attempted serially across any number of replicas. On a worker death, Kafka rebalances the partition to a peer; the only ordering-relevant window is the brief rebalance pause, during which the partition is paused, never reordered.

Changing `N` (16 → 32) changes which merchant hashes to which partition, so RRQ fixes `N` at deploy time. That's the genuine cost of the partitioning approach, and the only resharding edge in the system — there is no ledger resharding to worry about, because the ledger isn't sharded.

---

## Per-tier scale-out and availability

Replica counts are the _production floor_; the [deployment doc](deep-dives/28-DEPLOYMENT-AND-OPERATIONS.md) carries exact manifests and dev-overlay sizes.

| Tier | Min replicas | Stateless? | Scales by | Survives a pod death by |
| --- | --- | --- | --- | --- |
| Kong (edge) | 2 | yes | replicas behind the LB | LB drops the dead pod; peers serve |
| API Gateway | 3 | yes | replicas (HPA) | LB reroutes; nothing in-flight is durable past the Postgres commit |
| Outbox Relay | 2 | yes | partitioned claim (`FOR UPDATE SKIP LOCKED`) | a peer keeps draining the outbox; publishing is at-least-once, consumers idempotent |
| Ledger Worker | 2 | yes | replicas (HPA on Kafka `jobs` lag) | Kafka rebalances partitions to peers; a rolled-back transaction is simply redelivered |
| Webhook Worker | 2 | yes | replicas (HPA on Kafka `notify` lag) | Kafka rebalances partitions; retry state is durable in Postgres |
| Fraud Worker | 2 | yes | replicas (plain consumer group) | plain rebalance; velocity state is in shared Redis |
| Reconciliation | n/a (batch) | yes | parallel per-wallet-range jobs | re-run is idempotent; leader-elected (Postgres advisory lock) to avoid overlap |
| Admin Dashboard | 2 | yes | replicas | LB reroutes; holds no session state worth preserving |
| Postgres | 2 (primary+standby) | n/a | vertical (primary) | CloudNativePG promotes the standby in seconds |
| Redis | 2 + Sentinel | n/a | replica failover | Sentinel promotes a replica |

Three of these deserve a note:

**Outbox Relay is HA, not a singleton.** Multiple relay replicas claim disjoint batches of unpublished `events` rows with `FOR UPDATE SKIP LOCKED`, so two replicas never publish the same row. Publishing is at-least-once (a relay can crash after Kafka accepts but before stamping `published_at`); the Ledger and Webhook workers are idempotent, so a re-published message is harmless.

**Fraud Worker is ≥2, plain.** Velocity counting needs no per-wallet ordering (the window is a shared, atomically-updated Redis structure), so fraud workers sit in an ordinary Kafka consumer group and load-balance the `jobs` topic freely.

**Reconciliation is HA batch work.** It's idempotent, so the availability story is "≥2 candidates, leader-elected via a Postgres advisory lock, the winner runs, a loser takes over if it dies." It is never a single pod whose death stops the function.

---

## What happens when things die

The availability story is concrete. Walking the failure cases:

- **A worker pod dies.** Kubernetes reschedules it; peers carry the load. Kafka reassigns the dead worker's partitions to survivors. A ledger transaction that was in flight was rolled back by Postgres, so its job is just redelivered and re-run (idempotent via `UNIQUE(transfer_id, leg)`). No work is lost; latency briefly rises.
- **A node dies.** Same as above for every pod on it, in parallel. Because each Deployment runs ≥2 replicas spread across nodes (anti-affinity), no service goes to zero.
- **The Postgres primary dies.** CloudNativePG promotes the standby; the `postgres-rw` Service follows the new primary. Workers reconnect and retry; an in-flight transaction that may or may not have committed is safe to re-attempt because the postings are idempotent. RPO is effectively zero for committed transactions (synchronous replication / continuous WAL); the gap is a few seconds of promotion.
- **A Redis node dies.** Sentinel promotes a replica. Velocity counters and breaker memory may lose a moment of un-fsynced state — tolerable, because **no invariant depends on Redis**. Idempotency and the ledger are in Postgres.
- **The whole region dies.** Out of scope. RRQ is single-region. Stated plainly so no reviewer mistakes the HA story for a DR story.

The shape to notice: every failure resolves to "a peer or a standby takes over within seconds, and the work is idempotent so retry is safe." That is the entire availability design, and it is the same pattern at every tier.

---

## What this buys, and what it costs

**Buys:** the system can lose any single component and keep running; it absorbs more load by adding replicas; and its money-movement path is a single, atomic, strongly-consistent flow that a reviewer can reason about completely. "What happens when this pod dies?" gets a specific answer at every tier.

**Costs:** more moving parts than a single-process design (Kafka rebalancing, leader election, failover controllers), one resharding edge (the Kafka partition count), and a write-throughput ceiling at one Postgres primary. These are paid deliberately: the first two are the price of HA, and the third is the price of keeping exactly one correct flow for money — which, for a correctness-critical ledger, is the right thing to buy.
