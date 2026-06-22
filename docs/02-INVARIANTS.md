# 02: Invariants

The precise, testable statements about what RRQ guarantees — not slogans, but statements a test can falsify, and that the suite *does* try to falsify. Each has a name (`I1`…`I9`) used across the other docs.

## I1 — Conservation of value

> Every transfer posts exactly one `debit` and one `credit` leg of equal magnitude, to two distinct wallets, in a single transaction. For every `transfer_id` there is exactly one `(transfer_id,'debit')` and one `(transfer_id,'credit')` row in `ledger_entries`, with `|debit.amount| = credit.amount`.

Money is never created or destroyed. **Enforced** by writing both legs in one serializable transaction ([→ `services/11-LEDGER-WORKER.md`](services/11-LEDGER-WORKER.md)) — they commit together or not at all, so the debit-then-crash limbo can't exist — plus `UNIQUE(transfer_id, leg)` against a redelivered third leg; reconciliation re-verifies the pairing nightly. **Tested** by chaos: 1,000 transfers while killing the worker, then assert one debit + one credit of equal magnitude per `transfer_id`.

## I2 — No negative balance on active wallets

> For every `active` wallet `W`, its derived balance (sum of `ledger_entries.amount` for `W`) is ≥ 0 at every point in the entry sequence.

The system never lets an active wallet spend money it doesn't have. (Frozen/closed wallets are exempt — they may have been frozen *because* of a discrepancy.) **Enforced** by the posting transaction's `SELECT … FOR UPDATE` on the source wallet: it computes the balance under the lock and aborts if the debit would go negative, so no concurrent transfer can slip in between. **Tested**: balance 100, two concurrent transfers of 60 → exactly one succeeds, balance never negative.

## I3 — At-most-once execution per idempotency key

> For each `(merchant_id, idempotency_key)` the API accepted, exactly one `jobs` row exists, and at most one set of postings derives from it.

A merchant can retry forever; the transfer happens at most once. **Enforced** durably in Postgres: `INSERT … ON CONFLICT (merchant_id, idempotency_key) DO NOTHING` — first request wins, retries read back the same `job_id`. No cache to lose the claim. **Tested**: 100 simultaneous same-key requests → one `jobs` row; same key + different body → `422`.

## I4 — Per-wallet entry ordering

> For any wallet `W`, its `ledger_entries` are totally ordered by `id`, and that order is causal.

A wallet's history is a clean, replayable sequence. **Enforced** by the `BIGSERIAL` `id` on the wallet's shard primary, the `FOR UPDATE` lock serializing concurrent transfers on the wallet, and both legs sharing one transaction. It holds across worker replicas because the ordering authority lives in the shard, not in worker memory. Per *wallet* only — RRQ promises no cross-wallet or cross-shard global order. **Tested**: replay each wallet's entries in `id` order onto a fresh balance; assert no invalid leg.

## I5 — Per-merchant webhook ordering

> For any merchant `M`, webhook deliveries are *attempted* in the order their source events occurred.

Merchants build state machines on webhooks, so order matters. **Enforced** by publishing notify events keyed by `merchant_id` to Kafka in `events.id` order: all of `M`'s events land on one partition, and Kafka assigns each partition to exactly one live worker ([→ `services/12-WEBHOOK-WORKER.md`](services/12-WEBHOOK-WORKER.md)). "Attempted in order," not "succeeds in order" — a failing event head-of-line-blocks its own partition, bounded by the retry policy; different merchants are independent. **Tested**: 100 events × 10 merchants in known order → each endpoint receives its sequence; a slow merchant neither reorders nor blocks others.

## I6 — Immutable history

> Once a row exists in `ledger_entries` or `events`, application code never updates or deletes it. (The relay stamping `events.published_at` is an append-only flag, not a rewrite.)

Postings and facts are permanent; a wrong fact is corrected by a new fact. **Enforced** by the app Postgres role lacking `UPDATE`/`DELETE` on these tables (the relay role may only set `published_at`), by code review, and by corrections being new rows (an adjustment transfer + `operator.action` event). **Tested**: assert the app role can't `UPDATE`/`DELETE`; an `UPDATE events` from app code fails. (Features that "want" to mutate history — GDPR delete, PII anonymization — operate on derived projections, never the logs.)

## I7 — Job termination

> Every accepted job reaches a terminal state (`completed`, `failed`, or DLQ) within bounded time, or is observable as "stuck" via tooling.

No job runs forever invisibly. **Enforced** because a transfer is one transaction (a crash rolls back and Kafka redelivers, safe via `UNIQUE(transfer_id, leg)`): terminal errors commit `failed` immediately; retryable errors back off, then after the budget become a poison message written to `dlq_entries` with the offset committed (I8). **Tested**: kill mid-post → the job still terminates with no double posting; force perpetual failure → it lands in the DLQ after the budget.

## I8 — DLQ entries are recoverable, not lost

> Every job or webhook that exhausts its retry budget is persisted to `dlq_entries` with the payload, failure history, and replay context. No message is silently dropped.

Silent drop is the worst failure in a payment system; the DLQ makes giving-up *visible*. **Enforced** by writing the `dlq_entries` row *before* committing the Kafka offset (ledger and webhook paths alike), so the message is owned by the table, not the stream; Dashboard replay re-emits the payload and re-derives the same deterministic id, so a replay racing a late original can't double-post. **Tested**: 100 webhooks to an always-500 endpoint → 100 DLQ rows, no uncommitted offsets; replay against a healthy path executes and flips the row to `replayed`.

## I9 — Tenant isolation

> No merchant can observe or affect another's data. A wallet-mutating request is rejected unless the source wallet is owned by the authenticated merchant (`from_wallet.merchant_id = jwt.sub`); reads return only the caller's rows.

Cross-tenant access is the highest-severity failure — one merchant draining or reading another's money — and a security incident. **Enforced** at the gateway: a transfer whose `from_wallet` isn't owned by `sub` gets `403 WALLET_NOT_OWNED` before any `jobs` row; reads scoped to `sub` return `404` for others' resources (so existence doesn't even leak); `wallets.merchant_id` is the authority. **Tested**: A transfers from B's wallet → `403`, no job; A reads B's job/wallet/ledger → `404` each. **Subtlety**: isolation is enforced only at the gateway (the sole `jobs` writer); any future job-creating path must re-check ownership or this silently weakens.

## What's deliberately *not* an invariant

Stated so reviewers know the exact boundary of the promises:

- **Read-your-writes on dashboards** — projections lag; poll `GET /v1/jobs/{id}` for a strongly-consistent answer.
- **Cross-wallet / cross-merchant linearizability** — only per-wallet (I4) and per-merchant (I5) order are promised.
- **A latency SLO** — under load, latency degrades via queue lag, not failure; a request fails only if it can't be *durably accepted*.
- **True zero-downtime deploys** — rolling deploys + HA failover get close, but a deploy can briefly fail in-flight requests.
- **Fairness among merchants** — handled only by Kong edge rate limiting; the workers don't arbitrate it.

## Where to read next

- The system shape that upholds these → [`00-OVERVIEW.md`](00-OVERVIEW.md)
- A specific implementation → `services/`
