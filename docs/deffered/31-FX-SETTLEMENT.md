# 31 — FX Settlement

> **What this is.** A design sketch for FX (foreign exchange) settlement, supporting transfers between wallets in different currencies. Designed for v2; not built in v1.
>
> **Reading time.** ~10 minutes.
>
> **Status.** Designed, not built. See [`../../STATUS.md`](../../STATUS.md).

---

## The problem

A merchant in Nigeria has an NGN wallet. A merchant in Kenya wants to receive funds in KES. A transfer between them requires converting NGN to KES, which means a currency exchange happens at some moment, at some rate, with some fee structure.

v1 of RRQ only supports same-currency transfers. The Validate step rejects transfers where source and destination currencies don't match. This works because all of v1's intended use cases are within a single currency. As soon as cross-currency transfers are needed, the system needs FX.

FX is genuinely complex in production payment systems. RRQ's v2 scope is narrower: support FX transfers using a published-rate external provider, with appropriate caching and circuit-breaker protection. Not real-time market-making, not hedging, not arbitrage detection. Just: convert at a rate, charge a transparent fee, complete the transfer.

---

## The architecture

An FX Transfer is a Transfer with an extra step in the saga. The new step sits between `Validate` and `AcquireLock`:

```
Init → Validate → FetchRate → AcquireLock → Debit → Credit → Complete → Notify
```

**FetchRate.** Query an external FX rate provider for the conversion rate between source currency and destination currency. Cache the rate briefly (15-second TTL) and use it for the saga.

The rest of the saga is the same as Transfer, with the rate applied at the Debit/Credit boundary:

- Debit removes `amount_source` from the source wallet.
- Credit adds `amount_dest = amount_source * rate * (1 - fee)` to the destination wallet.

Both ledger entries reference the same `saga_id`. The conversion factor (the rate at the moment of the saga) is stored in the saga's `state_data` for audit. Two separate "I1: conservation of value" checks now apply, one per currency.

---

## The new failure mode: rate provider unavailability

The defining new failure mode: the external FX rate provider is unavailable. Two responses:

**Wait for it.** Naive. If the provider is down for 5 minutes, every FX transfer is blocked for 5 minutes. Threads are tied up in failed HTTP requests. The system's resources are wasted on doomed retries.

**Fail-fast and decline new FX transfers.** Better. After a few failures from the rate provider, stop trying for a cooldown period. New FX transfers are rejected with a clear error: "FX rate provider currently unavailable; retry in N seconds."

The fail-fast pattern is the **circuit breaker** from `24-RESILIENCE.md`. The breaker is keyed *per provider*, not per merchant. If we used a single provider, there's one breaker. If we use multiple providers (some systems do — fallback hierarchies of providers), one breaker per provider.

```
fn fetch_rate(from_currency, to_currency):
    cached = REDIS.GET("fx:rate:" + from + ":" + to)
    if cached and not stale(cached):
        return parse(cached)
    
    breaker = breakers.get("fx-provider-primary")
    
    if breaker.is_open():
        raise FxProviderUnavailable()
    
    try:
        rate = httpsCall(FX_PROVIDER_URL, {from, to})
        REDIS.SET("fx:rate:" + from + ":" + to, json(rate), EX=15)
        breaker.record_success()
        return rate
    except:
        breaker.record_failure()
        raise
```

The 15-second cache is critical for performance. Without it, every FX saga makes an external HTTP call. With it, most sagas hit the cache (a hot pair like NGN→KES might serve hundreds of transfers from one cached rate). The 15-second freshness is acceptable for most use cases; the merchant agrees in their API contract.

---

## Why this is deferred

Three reasons:

**1. The breaker pattern needs to be production-tested first.** v1 has the circuit breaker pattern on webhook delivery (per merchant). Adding another breaker (per FX provider) is straightforward once the pattern is validated in production. v2 builds on v1's experience.

**2. Operational complexity.** FX rates are price-sensitive. Showing a merchant a rate, having them initiate a transfer, then settling at a different rate (because the cache was stale) is a customer-experience problem. The v2 design includes rate locking — when a merchant requests an FX transfer, the rate is captured at request time and that rate is used for settlement, even if the market moved by saga-execution time. This adds an API surface for "get current rate" / "execute transfer at this rate" that needs separate design.

**3. Provider integration.** Real FX rate providers have their own auth, rate limits, callback schemes, and SLAs. Integrating with a specific provider isn't a generic problem — it's a per-provider implementation. v2 picks a specific provider (likely OXR or similar) and builds against their API.

For v1, FX is out of scope and the Validate step rejects cross-currency transfers with a clear error.

---

## Data model additions

```sql
-- FX rate snapshots (for audit; not the cache).
CREATE TABLE fx_rate_snapshots (
    id            BIGSERIAL PRIMARY KEY,
    from_currency TEXT NOT NULL,
    to_currency   TEXT NOT NULL,
    rate          NUMERIC(20, 10) NOT NULL,    -- high precision
    provider      TEXT NOT NULL,
    fetched_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX fx_rate_snapshots_pair_idx
    ON fx_rate_snapshots (from_currency, to_currency, fetched_at DESC);
```

Every rate fetched from the provider is logged for audit. If a merchant disputes the rate used for their transfer, the audit log shows what was fetched, when, from which provider. The merchant's transfer references a specific snapshot ID via the saga's `state_data`.

The cache (in Redis) is the operational layer; the audit log (in Postgres) is the auditable layer. Different durability needs, different stores.

---

## Event types

```protobuf
message FxRateFetched {
    string from_currency = 1;
    string to_currency = 2;
    string rate = 3;             // string to preserve precision
    string provider = 4;
}

message FxTransferCompleted {
    string job_id = 1;
    string from_wallet = 2;
    string to_wallet = 3;
    int64 amount_source = 4;
    int64 amount_dest = 5;
    string rate_used = 6;
    int64 fee = 7;               // in destination currency
}
```

The events record the rate used and the resulting amount. Reconciliation verifies that `amount_source * rate * (1 - fee_rate)` equals `amount_dest` within rounding tolerance.

---

## Reconciliation extension

The reconciliation job, currently checking conservation of value, extends to handle FX transfers:

- A same-currency transfer: `debit_source + credit_destination = 0`. (Already checked by I1.)
- A cross-currency transfer: `credit_destination = -debit_source * rate * (1 - fee_rate)` within precision tolerance.

The reconciler reads the saga's `state_data` to find the rate used, then verifies the math. Discrepancies (rounding errors, provider errors, bugs in the FX math) surface as alerts.

---

## What an interviewer asks

**"How do you handle the FX rate changing between when the merchant initiated and when the saga executed?"** Answer: rate locking. The rate is captured at request time and stored in the saga's state_data. The saga executes against the captured rate, not the current rate. If the merchant wants to retry with a current rate, they make a new request.

**"What if the FX provider is unavailable when a merchant submits a transfer?"** Answer: the circuit breaker fast-fails. The API returns 503 with `FX_PROVIDER_UNAVAILABLE`. The merchant retries when the provider recovers. Some systems would queue the transfer; we reject because we can't lock a rate without a rate.

**"Why not run multiple providers in parallel?"** Answer: in v2, we plan to. The architecture supports it — one breaker per provider, a small fallback chain (try primary; on failure, try secondary; on failure, fail-fast). v2 starts with one provider for simplicity; multi-provider is v3.

**"What's the precision concern with FX rates?"** Answer: floating point would introduce drift; we use fixed-point decimals (Postgres `NUMERIC`) with high precision (10 decimal places). The math is integer arithmetic in the saga; the rate is multiplied as a fraction. Audit logs preserve the exact rate used.

---

## Where to read next

- The saga that this extends → [`../services/11-SAGA-WORKER.md`](../services/11-SAGA-WORKER.md)
- The resilience patterns this builds on → [`../deep-dives/24-RESILIENCE.md`](../deep-dives/24-RESILIENCE.md)
- The reconciliation that verifies it → [`../services/14-RECONCILIATION.md`](../services/14-RECONCILIATION.md)

---

*Pass 4 of the architecture series. Deferred feature; not implemented in v1.*
