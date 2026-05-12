# 40 — Data Model

> **What this is.** Reference for the Postgres schema. Every table, every column, every index, with the reasoning.
>
> **Format.** Look-up reference. Read the table you need; skim the rest.

---

## Conventions

**IDs are ULIDs as TEXT.** ULIDs are 26-character strings, lexicographically sortable by time. They sort like timestamps but are globally unique without coordination. Stored as TEXT in Postgres (not bytea, not UUID) for readability and easy interaction with command-line tools.

ID prefixes by entity type for human-readability:
- `m_` — merchant
- `wal_` — wallet
- `job_` — job
- `sg_` — saga
- `ev_` — event
- `wd_` — webhook delivery
- `dlq_` — DLQ entry
- `rec_` — reconciliation run
- `disp_` — dispute (v2)

**Timestamps are `TIMESTAMPTZ`.** Always UTC; the timezone-aware variant prevents bugs from local-time interpretation.

**Money is `BIGINT`, in the smallest currency unit.** Never floating point. For NGN, the unit is kobo (1 NGN = 100 kobo). For USD, cents. For currencies with no subunit, the unit is the currency itself. Application code converts for display.

**Boolean status values use TEXT with CHECK constraints, not boolean.** A `status` column with values `'active' | 'frozen' | 'closed'` is more extensible than two boolean columns (`is_active`, `is_frozen`); adding a fourth state is one CHECK update, not a schema migration.

---

## merchants

The merchant table. Customers of the platform.

```sql
CREATE TABLE merchants (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    api_key_hash    TEXT NOT NULL UNIQUE,
    webhook_url     TEXT,
    webhook_secret  TEXT,
    status          TEXT NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'frozen', 'closed')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX merchants_status_idx ON merchants (status);
```

| Column | Why it exists |
| --- | --- |
| `id` | Primary key. ULID with `m_` prefix. |
| `name` | Display name for ops UIs. Not used by the system. |
| `api_key_hash` | Bcrypt hash of the merchant's API key. The raw key is never stored. JWT auth uses the merchant's identity from the JWT claim; the API key is for initial JWT issuance. |
| `webhook_url` | Destination for webhook notifications. May be NULL if the merchant doesn't use webhooks. |
| `webhook_secret` | HMAC-SHA256 signing secret. Random per merchant; rotated via admin API. |
| `status` | Merchant lifecycle. `active`: normal. `frozen`: suspended; API rejects new requests. `closed`: permanent. |
| `created_at` | Audit/ops. |

The `merchants_status_idx` supports filtering "all active merchants" for ops queries.

---

## wallets

The wallet table. Notable absence: no `balance` column.

```sql
CREATE TABLE wallets (
    id             TEXT PRIMARY KEY,
    merchant_id    TEXT NOT NULL REFERENCES merchants(id),
    currency       TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'active'
                   CHECK (status IN ('active', 'frozen', 'closed')),
    frozen_reason  TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX wallets_merchant_idx ON wallets (merchant_id);
CREATE INDEX wallets_status_idx ON wallets (status);
```

| Column | Why it exists |
| --- | --- |
| `id` | ULID with `wal_` prefix. |
| `merchant_id` | Owner. |
| `currency` | ISO 4217 code, e.g., `NGN`, `USD`, `KES`. |
| `status` | Wallet lifecycle. Frozen wallets reject new debits. |
| `frozen_reason` | Set when `status = 'frozen'`. Explains why. |
| `created_at` | Audit. |

The balance is derived from `ledger_entries`. See [`../deep-dives/25-EVENT-STORE.md`](../deep-dives/25-EVENT-STORE.md) for why.

---

## events

The event store. Append-only, source of truth.

```sql
CREATE TABLE events (
    id              BIGSERIAL PRIMARY KEY,
    event_id        TEXT NOT NULL UNIQUE,
    event_type      TEXT NOT NULL,
    aggregate_type  TEXT NOT NULL,
    aggregate_id    TEXT NOT NULL,
    correlation_id  TEXT,
    causation_id    TEXT,
    payload         JSONB NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX events_aggregate_idx
    ON events (aggregate_id, id);
CREATE INDEX events_type_time_idx
    ON events (event_type, occurred_at);
CREATE INDEX events_correlation_idx
    ON events (correlation_id) WHERE correlation_id IS NOT NULL;
```

| Column | Why it exists |
| --- | --- |
| `id` | Monotonic. The ordering key. Used by reconciliation and per-aggregate replay. |
| `event_id` | ULID. The identity key. Referenced by ledger entries and webhook payloads. Unique constraint enforces idempotency. |
| `event_type` | E.g., `transfer.completed`, `ledger.debit_applied`. Lookup key for projections and consumers. |
| `aggregate_type` | `wallet` \| `saga` \| `merchant` \| `webhook`. Coarse-grained partitioning of the event space. |
| `aggregate_id` | The specific entity this event is about. Combined with `aggregate_type`, identifies which entity. |
| `correlation_id` | Usually the saga_id. Groups events from a single logical operation. |
| `causation_id` | The immediate predecessor event. Forms the causal graph within a correlation. |
| `payload` | Event-specific data. Schema varies by `event_type`. JSONB for indexing flexibility. |
| `occurred_at` | When the event semantically happened. Application-set. |
| `created_at` | When the row was committed. Database-set. Useful for ops latency analysis. |

Production permissions: app role has `INSERT, SELECT`; no `UPDATE` or `DELETE`. Enforced via a separate role-grant migration.

The indexes:

| Index | Used by |
| --- | --- |
| `events_aggregate_idx` | Per-aggregate replay. Reconciliation, audit, balance derivation. |
| `events_type_time_idx` | Time-window queries by event type. Reconciliation, reporting. |
| `events_correlation_idx` | Saga history lookup. Admin CLI. |

Partial index on `correlation_id` because many events don't have a correlation (system-initiated events like reconciliation runs).

---

## ledger_entries

The materialized projection from which wallet balances are derived. UNIQUE constraint is the idempotency anchor for saga steps.

```sql
CREATE TABLE ledger_entries (
    id             BIGSERIAL PRIMARY KEY,
    wallet_id      TEXT NOT NULL REFERENCES wallets(id),
    amount         BIGINT NOT NULL,
    balance_after  BIGINT NOT NULL,
    saga_id        TEXT NOT NULL,
    step_name      TEXT NOT NULL,
    event_id       TEXT NOT NULL REFERENCES events(event_id),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (saga_id, step_name)
);

CREATE INDEX ledger_entries_wallet_idx ON ledger_entries (wallet_id, id);
CREATE INDEX ledger_entries_created_idx ON ledger_entries (created_at);
```

| Column | Why it exists |
| --- | --- |
| `id` | Primary key. Monotonic. |
| `wallet_id` | The wallet being mutated. |
| `amount` | Signed: negative = debit, positive = credit. |
| `balance_after` | Snapshot of wallet balance after this entry was applied. Convenient for queries; verified by reconciliation. |
| `saga_id` | The saga that produced this entry. |
| `step_name` | The saga step. With saga_id, forms the idempotency key. |
| `event_id` | Cross-reference to the event that records this ledger movement. |
| `created_at` | Audit. |

The `UNIQUE (saga_id, step_name)` constraint is what makes saga retries safe. A retry of the Debit step for saga sg_42 attempts to insert `(sg_42, debit, ...)`. If it already exists from a previous attempt, the insert fails with duplicate-key, and the step recognizes "this work was already done" and proceeds.

`ledger_entries_wallet_idx` supports balance computation: `SELECT SUM(amount) FROM ledger_entries WHERE wallet_id = ?`. Index range scan rather than table scan.

---

## saga_state

Tracks each saga's current state. Read on every step transition; updated transactionally with the step's work.

```sql
CREATE TABLE saga_state (
    saga_id              TEXT PRIMARY KEY,
    job_id               TEXT NOT NULL,
    saga_type            TEXT NOT NULL,
    current_state        TEXT NOT NULL,
    last_completed_step  TEXT,
    state_data           JSONB NOT NULL DEFAULT '{}',
    started_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deadline_at          TIMESTAMPTZ NOT NULL,
    terminated_at        TIMESTAMPTZ,
    failure_reason       TEXT,
    failure_detail       TEXT
);

CREATE INDEX saga_state_active_idx
    ON saga_state (current_state, deadline_at)
    WHERE terminated_at IS NULL;

CREATE INDEX saga_state_stuck_idx
    ON saga_state (deadline_at)
    WHERE terminated_at IS NULL;

CREATE INDEX saga_state_job_idx ON saga_state (job_id);
```

| Column | Why it exists |
| --- | --- |
| `saga_id` | ULID with `sg_` prefix. Also the events' `correlation_id`. |
| `job_id` | The triggering job (one-to-one with sagas in v1). |
| `saga_type` | `transfer` \| `bulk_payout` \| (v2: `chargeback`, `fx_transfer`). |
| `current_state` | The state machine's current state. `Init`, `Valid`, `Locked`, `Debited`, `Credited`, `Compensating`, `Completed`, `Failed`, `DeadLettered`. |
| `last_completed_step` | The most recent step that committed. Used by resume logic to determine the next step. |
| `state_data` | Step outputs threaded through the saga. Lock token, wallet metadata, etc. JSONB. |
| `started_at`, `updated_at` | Saga lifecycle timestamps. |
| `deadline_at` | When the saga is considered stuck. Used by the stuck-saga query. |
| `terminated_at` | Set when the saga reaches a terminal state. NULL while in progress. |
| `failure_reason`, `failure_detail` | Populated when the saga fails. Operator-facing context. |

The `saga_state_active_idx` and `saga_state_stuck_idx` are partial indexes (only non-terminated rows) because terminal sagas are the vast majority; the operational interest is in active sagas. Postgres skips the terminal rows entirely in these indexes.

---

## webhook_deliveries

Tracks every outbound notification attempt to a merchant.

```sql
CREATE TABLE webhook_deliveries (
    id              TEXT PRIMARY KEY,
    merchant_id     TEXT NOT NULL REFERENCES merchants(id),
    source_event_id TEXT NOT NULL REFERENCES events(event_id),
    url             TEXT NOT NULL,
    payload         JSONB NOT NULL,
    signature       TEXT NOT NULL,
    attempt_count   INT NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMPTZ,
    next_retry_at   TIMESTAMPTZ,
    last_error      TEXT,
    last_status     INT,
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'delivered', 'dlq')),
    delivered_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX webhook_deliveries_pending_idx
    ON webhook_deliveries (status, next_retry_at)
    WHERE status = 'pending';

CREATE INDEX webhook_deliveries_merchant_idx
    ON webhook_deliveries (merchant_id, created_at);
```

| Column | Why it exists |
| --- | --- |
| `id` | ULID with `wd_` prefix. |
| `merchant_id` | Owner of the destination endpoint. |
| `source_event_id` | The event that triggered this delivery. |
| `url` | Snapshot at emit time. Merchants can change URLs; we use the snapshot. |
| `payload` | The canonical-JSON body that was signed and sent. |
| `signature` | The HMAC-SHA256 over the payload. Stored so we can replay with the same signature. |
| `attempt_count` | Number of attempts so far. Capped at `max_retries` (default 10). |
| `last_attempt_at` | When the most recent attempt was made. |
| `next_retry_at` | When the next attempt is due. NULL for terminal deliveries. |
| `last_error` | Error message from the most recent failure. |
| `last_status` | HTTP status from the most recent attempt. |
| `status` | `pending`: not yet terminal. `delivered`: 2xx received. `dlq`: max retries exhausted. |
| `delivered_at` | Set when status transitions to delivered. |
| `created_at` | Audit. |

The partial index on `pending` is the retry scheduler's hot query. With most deliveries in `delivered` state (i.e., not pending), the index touches only the small set we care about.

---

## dlq_entries

Terminal failures awaiting human attention.

```sql
CREATE TABLE dlq_entries (
    id                TEXT PRIMARY KEY,
    source            TEXT NOT NULL
                      CHECK (source IN ('saga', 'webhook')),
    original_payload  JSONB NOT NULL,
    error_message     TEXT NOT NULL,
    attempt_count     INT NOT NULL,
    first_failed_at   TIMESTAMPTZ NOT NULL,
    last_failed_at    TIMESTAMPTZ NOT NULL,
    status            TEXT NOT NULL DEFAULT 'open'
                      CHECK (status IN ('open', 'replayed', 'resolved')),
    replayed_at       TIMESTAMPTZ,
    replayed_job_id   TEXT,
    resolved_at       TIMESTAMPTZ,
    resolved_by       TEXT,
    resolution_note   TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX dlq_entries_open_idx
    ON dlq_entries (created_at DESC) WHERE status = 'open';

CREATE INDEX dlq_entries_source_idx ON dlq_entries (source, status);
```

| Column | Why it exists |
| --- | --- |
| `id` | ULID with `dlq_` prefix. |
| `source` | `saga` (compensation failed) or `webhook` (delivery exhausted retries). |
| `original_payload` | Enough context to replay. For sagas, the JobRequested payload. For webhooks, the payload + URL + merchant ID. |
| `error_message` | Final failure error. |
| `attempt_count` | How many attempts before giving up. |
| `first_failed_at`, `last_failed_at` | Time bounds for diagnostic purposes. |
| `status` | `open`: needs review. `replayed`: re-enqueued (with operator action). `resolved`: closed without replay. |
| `replayed_at`, `replayed_job_id` | Populated when an operator replays. |
| `resolved_at`, `resolved_by`, `resolution_note` | Populated when an operator resolves without replay. |
| `created_at` | When the entry was created. |

The partial index supports the dominant operator query: "open entries, newest first."

---

## wallet_balance_cache (read projection)

Denormalized balance per wallet. Refreshed asynchronously from `ledger_entries`. *Not* the source of truth — reconciliation verifies this against the derived sum.

```sql
CREATE TABLE wallet_balance_cache (
    wallet_id      TEXT PRIMARY KEY REFERENCES wallets(id),
    balance        BIGINT NOT NULL,
    last_event_id  BIGINT NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

| Column | Why it exists |
| --- | --- |
| `wallet_id` | The wallet. PK. |
| `balance` | Cached balance. Updated incrementally by the projector. |
| `last_event_id` | The events.id of the most recent event reflected. Used by the projector to resume. |
| `updated_at` | When the projection was last updated. |

The projector tails events and updates this table. Lag is typically < 1 second; bounded by the projector's throughput. Used for dashboard reads where staleness is acceptable.

---

## Cross-reference

| Component | Tables it reads | Tables it writes |
| --- | --- | --- |
| API Gateway | `merchants` (cached), `wallets` (rare) | none (writes events via stream, not directly) |
| Saga Worker | `saga_state`, `wallets`, `ledger_entries` | `saga_state`, `ledger_entries`, `events` |
| Webhook Worker | `merchants` (cached), `webhook_deliveries` | `webhook_deliveries`, `events`, `dlq_entries` |
| Fraud Worker | `wallets`, `events` (rebuild) | `wallets`, `events` |
| Reconciliation | `events`, `ledger_entries`, `wallets` | `events` (alert entries only) |
| Admin CLI | all tables (reads) | `events` (audit entries), specific writes per command |

---

## Migration order

Migrations run in numbered order:

1. `001_create_merchants.sql`
2. `002_create_wallets.sql`
3. `003_create_events.sql`
4. `004_create_ledger_entries.sql` (depends on wallets, events)
5. `005_create_saga_state.sql`
6. `006_create_webhook_deliveries.sql` (depends on merchants, events)
7. `007_create_dlq_entries.sql`

Future migrations add the projections, the v2 features (disputes, fx_rate_snapshots), and the role-based permissions for production.

---

*Pass 4 of the architecture series. Last updated pre-implementation.*
