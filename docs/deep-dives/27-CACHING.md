# 27: Caching

> **What this is.** The deep dive on caching in RRQ. Smaller than the other deep-dives because RRQ deliberately leans light on caching, but a worthwhile read because *the absence of heavy caching is itself a design choice* worth understanding and defending.
>
> **Reading time.** ~12 minutes.
>
> **Prerequisites.** None specific.

---

## Why RRQ doesn't lean on caching

Most systems designed around "cache aggressively" share a common shape: read-heavy workloads where the same data is requested many times relative to how often it changes. Social media feeds, product catalogs, search results, user profiles, all classic cache targets.

RRQ is structurally different. The dominant workload is *writes*: each transfer is a new saga, processing fresh, never repeating. There's no equivalent of "10,000 reads per write" that justifies aggressive caching. The reads RRQ does have are either:

- Tiny and infrequent (a merchant occasionally polls `GET /v1/jobs/<id>`).
- Inherently cheap (Postgres indexed lookups by ID).
- Already optimized via projections (the `ledger_entries` table is itself a denormalization of events).

So the design discipline is: **cache only where caching solves a real performance or correctness problem**, not where caching is fashionable.

This document covers the three caches that do exist, the access patterns that intentionally don't have caches, and what would change at higher scale.

---

## The three caches in RRQ

### 1. The idempotency cache

**What it is.** Redis keys of the form `idemp:{merchant_id}:{idempotency_key}` storing either `"processing:{body_hash}"` or `"{body_hash}:{cached_response_json}"`. 24-hour TTL.

**What it's caching.** Not really data, it's caching the *result* of having processed a particular request. The cache is the deduplication record.

**Why it's a cache and not the primary store.** Because durability isn't critical. If Redis loses the idempotency cache (crash before AOF fsync, restart, etc.), the worst outcome is: a duplicate request arrives during the loss window and gets processed as new. The downstream saga has its *own* idempotency (the `UNIQUE(saga_id, step_name)` constraint on `ledger_entries`), so a "second" execution caused by cache loss doesn't cause double-debits, the saga retries the steps, hits the constraint, and treats them as already done.

The cache is the first line of defense (catches retries at the API boundary); the database constraint is the second line (catches anything the cache missed). Two-layer defense means the cache can be in Redis with relaxed durability.

**Invalidation.** TTL-based. After 24 hours, the entry expires. Manual invalidation isn't needed; the `SET ... NX EX 86400` pattern handles expiry naturally.

**Consistency model.** Within a single request lifecycle: strongly consistent (the SETNX is atomic). Across the broader 24-hour window: keys may be deleted slightly before their TTL if Redis evicts under memory pressure (RRQ disables eviction on this keyspace via Redis `noeviction` policy on production).

Covered in depth in [`20-IDEMPOTENCY.md`](20-IDEMPOTENCY.md). The framing there is "deduplication store"; the framing here is "cache." Both views are valid; they describe the same thing.

---

### 2. The merchant metadata cache

**What it is.** Redis keys of the form `merchant:{merchant_id}` storing a JSON snapshot of merchant config (URL, signing secret, status). 60-second TTL.

**What it's caching.** The result of `SELECT * FROM merchants WHERE id = ?`. Each API request needs the merchant's status; each webhook delivery needs the URL and signing secret. Without caching, every request hits Postgres for this data, which is fine at low throughput but adds latency and database load at scale.

**Why a cache here.** Merchant data changes rarely (a merchant updates their webhook URL maybe once a month) but is read constantly (every request, every webhook delivery). The cache trades a small staleness window for significant load reduction.

**Read-through pattern.** When the cache is empty:

```
fn get_merchant(id):
    cached = REDIS.GET("merchant:" + id)
    if cached:
        return parse(cached)
    
    fresh = POSTGRES.SELECT * FROM merchants WHERE id = $id
    REDIS.SET("merchant:" + id, json(fresh), EX=60)
    return fresh
```

The first request after a cache miss pays the Postgres latency. Subsequent requests hit the cache. On TTL expiry, the next request pays the Postgres latency again.

**The staleness tradeoff.** If a merchant updates their webhook URL at 14:00:00, requests for the next ~60 seconds may still use the cached old URL. The merchant has explicitly agreed to a "settings take up to one minute to propagate" SLA, documented in the admin API.

This is *eventual consistency for a configuration value*, which is acceptable for the use case. A merchant who needs immediate propagation can have the cache invalidated from the Admin Dashboard (or via its equivalent admin API endpoint).

**Invalidation.** Two mechanisms:

1. **TTL.** Default. 60 seconds is the upper bound on staleness.
2. **Explicit invalidation** when merchant settings change via the admin API. The handler that updates `merchants` also deletes the cache key. This is best-effort; if the DEL fails, the TTL is the fallback.

The combination ensures the staleness window is bounded by 60 seconds in the worst case (a failed invalidation) and zero in the typical case (successful invalidation).

**Failure modes.**
- **Cache stale after invalidation failure.** Requests during the next-up-to-60s use old config. Acceptable.
- **Cache evicted unexpectedly.** Next request rebuilds from Postgres. Acceptable.
- **Cache returns wrong data due to corruption.** Would be a bug; not realistic in production with Redis. Defense: cache key includes the merchant_id; corruption would have to specifically target this exact key.

This cache is described in scattered service docs but doesn't have a unified home, that's part of what this deep-dive consolidates.

---

### 3. The wallet balance projection

**What it is.** The `wallet_balance_cache` table in Postgres (notably *not* in Redis, see why below), storing the current derived balance of each wallet, refreshed asynchronously from `ledger_entries`.

**What it's caching.** The result of `SELECT COALESCE(SUM(amount), 0) FROM ledger_entries WHERE wallet_id = ?`. For a wallet with 100,000 lifetime ledger entries, this query takes some milliseconds even with the right index. For a dashboard listing 50 wallet balances, that's seconds of latency.

The cache is a flat row per wallet:

```sql
CREATE TABLE wallet_balance_cache (
    wallet_id      TEXT PRIMARY KEY REFERENCES wallets(id),
    balance        BIGINT NOT NULL,
    last_event_id  BIGINT NOT NULL,  -- the events.id of the most recent event reflected
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

A dashboard reads `SELECT balance FROM wallet_balance_cache WHERE wallet_id = $1`, one row lookup, microseconds.

**Why Postgres and not Redis?** Three reasons:

1. **Consistency.** The cache is updated as part of the same transaction that updates `ledger_entries`, *if* we choose synchronous update (we don't, see below). Even with async updates, having the cache in the same database means transactional queries can join it with other tables.

2. **Durability.** If we lose Redis, the cache rebuilds quickly from ledger summation. If we lose the *Postgres* cache, we have a bigger problem (Postgres is the source of truth). The risk profile is just different.

3. **Operational simplicity.** One database to manage; the cache is a regular table with regular tooling.

**Why async refresh, not synchronous?** If the cache were updated in the same transaction as the ledger entry, every saga step would have one more write. Synchronous updates are correct but slow. Async is a deliberate staleness trade for throughput.

The refresh strategy: a background worker (part of the recon-worker process, runs continuously not just nightly) tails the events stream, applies updates to `wallet_balance_cache`. The worker tracks `last_event_id` per wallet so it knows where to resume.

**The staleness model.** A wallet's `wallet_balance_cache` is at most ~1 second behind reality under normal load. Under burst load, the lag can grow to seconds. This is bounded by the projection worker's throughput.

**Where this cache is allowed to be stale.**
- Dashboard reads (merchant viewing their wallet balances). Acceptable lag: a few seconds.
- Operator queries via the Admin Dashboard. Same.

**Where this cache is NOT used.**
- Saga validation. The Validate step reads `ledger_entries` directly because we need exact, current balance for the overdraft check. (Upholds I2.)
- Reconciliation. The whole point of reconciliation is to verify the derived balance against the cache; using the cache to verify the cache is meaningless.

The principle: **the cache serves reads that can tolerate seconds of staleness; correctness-critical paths read from the source.**

---

## What is NOT cached, and why

Worth being explicit about access patterns that *don't* have caches, because every absent cache is also a design decision.

**Saga state.** Saga state is read once per step transition (under `SELECT FOR UPDATE` for concurrency control). Caching it would be wrong, it's mutated frequently and we need strong consistency. Caching here would introduce races.

**Event log queries.** Events are read at scale during reconciliation (a million events per nightly run) but not at request-time. The reads are batch-oriented; caching individual rows wouldn't help. The proper optimizations are indexes (which we have) and streaming queries (we read row-by-row, not into memory).

**Wallet records.** A wallet's *mutable* metadata (currency, status, balance inputs) is read on every saga's Validate step. We don't cache that. Reasons: (a) reads are point lookups on the primary key, extremely fast in Postgres; (b) status changes are infrequent but matter, caching introduces staleness in a value that affects correctness (a frozen wallet's freeze status must be respected immediately). The exception is **ownership**: a wallet's owning merchant never changes, so the API Gateway *can* cache `wallet → merchant_id` and check `from_wallet` ownership at the edge (upholding I9) without the staleness risk that rules out caching status or balance.

The status change case is interesting. Why don't we cache wallet status? Because:
- A frozen wallet must reject new transfers immediately. If the cache says "active" for up to 60 seconds, transfers from a frozen wallet sneak through during that window. This is a correctness gap, not just a staleness one.
- The merchant cache *can* have staleness because merchant status changes are merchant-initiated (the merchant submitted the change; they accept the propagation window).
- Wallet freezes are *system-initiated* (fraud worker auto-freezes). The system must enforce them immediately. No cache window.

So: same kind of data (a status field), different caching decisions, based on who's making the change and what the correctness impact of staleness would be.

**Circuit breaker state.** This is in Redis but it's not really caching, it's coordination. The breaker's state has to be reliable; we treat the Redis key as the source of truth for breaker state, with TTLs for natural expiry. No "rebuild from elsewhere" fallback.

**Stream consumer lag.** Computed on demand by the Admin Dashboard. Not cached. Reads are infrequent (operator-initiated); the computation is fast.

---

## When to add more caches at higher scale

The current design works for RRQ's target (1,000 TPS, ~1M transfers/day). At significantly higher scale, additional caching becomes worth considering. The decision tree:

**If the bottleneck is database read throughput**:
- Cache wallet records (with strict invalidation on status changes).
- Cache the merchant API tier/permissions info (rarely changes).
- Use read replicas for non-critical reads.

**If the bottleneck is database write throughput**:
- Caching doesn't help here. Sharding does.

**If the bottleneck is network latency to Postgres**:
- Aggressive caching in the application layer (e.g., a per-replica in-process LRU for merchant data).
- Connection pooling tuning.

**If the bottleneck is CPU on saga workers**:
- Caching doesn't help directly. Horizontal scaling of workers does.

The pattern: caching solves *specific* bottlenecks. Adding caches without identified bottlenecks adds complexity without value.

---

## The "stampeding herd" problem

A specific failure mode worth flagging: when a popular cache key expires and many requests simultaneously miss the cache, they all hit the database at once. The database is briefly slammed. This is the **cache stampede**.

For RRQ's caches:

**Idempotency cache.** Each key is per-(merchant, idempotency_key). The probability of multiple requests hitting the same key at the same moment as expiry is essentially zero, they'd all be the same merchant's retries of the same operation, which would be rare and bounded.

**Merchant metadata cache.** Could in principle have a stampede if many requests for the same merchant arrive at exactly the cache-expiry moment. Mitigations:
- **Randomized TTL.** Instead of exactly 60 seconds, use 60 ± 5 seconds randomized per write. Different keys expire at different moments; stampedes spread out.
- **Single-flight pattern.** When the cache is missed, only one concurrent request actually queries Postgres; others wait for the result.

RRQ uses randomized TTL but not single-flight. Single-flight is a fine optimization but it has implementation cost (a per-key mutex map in the application); RRQ leans on randomization, which is simpler.

**Wallet balance projection.** No stampede risk, the cache is updated by the projection worker, not invalidated on read.

---

## Cache testing

Test patterns for the caches above:

**Idempotency cache.**
- Concurrent SETNX races (covered in `20-IDEMPOTENCY.md` tests).
- TTL behavior, set a key, advance Redis time, assert it's gone.
- Body-hash mismatch, caching does the right thing with key reuse.

**Merchant metadata cache.**
- Cache miss → Postgres read → cache populated.
- Cache hit → no Postgres read (instrumented Postgres mock counts queries).
- Cache invalidation on merchant update.
- TTL expiry → next request rebuilds from Postgres.

**Wallet balance projection.**
- Insert ledger entries → projection worker updates cache.
- Read cache while projection is mid-write, no torn reads (Postgres MVCC handles this).
- Cache disagrees with ledger sum (manually corrupt) → reconciliation detects.

The tests are simple because the caches themselves are simple. Where caching gets complex (multi-level caches, write-through, cache replication), more sophisticated testing is needed. RRQ avoids that complexity.

---

## A note on what "cache" means

The word is overloaded. In RRQ:

- **Idempotency cache**: kind of a cache, but really a deduplication store with cache-like properties (TTL, can be rebuilt).
- **Merchant metadata cache**: a classic read-through cache.
- **Wallet balance projection**: a denormalization with write-behind semantics, but not really a "cache" in the strict sense, the source data (events) isn't accessed for these reads at all.

The strict definition of "cache", a fast store holding the result of an expensive computation, expected to be invalidated when the underlying data changes, fits only the merchant metadata cache cleanly. The others stretch the definition. But the colloquial use of "cache" to mean "any read optimization layer" captures all three, and that's how this doc uses the word.

---

## The summary

What RRQ caches:

1. **Idempotency results** in Redis (24h TTL). For deduplication.
2. **Merchant metadata** in Redis (60s TTL). For request-path performance.
3. **Wallet balances** in Postgres (refreshed continuously). For dashboard performance.

What RRQ does NOT cache:

- Saga state.
- Event log queries.
- Wallet records (status matters too much for staleness).
- Stream lag, breaker state, operational reads.

The design discipline: **caching adds complexity. Each cache must justify itself with a specific performance or correctness problem solved.** "Just in case" caches are a code smell, they introduce staleness without proportionate benefit.

At RRQ's scale, these three caches are the right set. At higher scale, the analysis would expand; this doc would grow accordingly.

---

## Where to read next

- The idempotency cache in depth → [`20-IDEMPOTENCY.md`](20-IDEMPOTENCY.md)
- The event store from which the wallet balance projection derives → [`25-EVENT-STORE.md`](25-EVENT-STORE.md)
- The API Gateway and Webhook Worker that use the merchant metadata cache → [`../services/10-API-GATEWAY.md`](../services/10-API-GATEWAY.md), [`../services/12-WEBHOOK-WORKER.md`](../services/12-WEBHOOK-WORKER.md)

---

*Pass 3 of the architecture series. Last updated pre-implementation.*
