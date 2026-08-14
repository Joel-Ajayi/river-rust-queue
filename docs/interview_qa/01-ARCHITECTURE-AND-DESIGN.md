# Technical Interview Q&A: Core System Architecture & Ledger Design

This document contains deep-dive interview questions and rigorous architectural explanations covering RRQ's closed-loop ledger design, transaction boundaries, database sharding, cross-shard clearing sagas, and idempotency guarantees.

---

## Q1: Why build RRQ as a closed-loop ledger instead of integrating directly with external banking or card networks?

### Answer:
In payment systems engineering, **external leg integrations** (card networks, bank APIs, clearing houses) and **correctness-critical core ledgers** have fundamentally different failure characteristics:

1. **Failure Domain Isolation**: External bank leg integrations require managing regulatory compliance (KYC/AML), PCI-DSS tokenization, ISO 8583 message parsing, and external network timeouts. These integrations are prone to transient external outages, network drops, and non-deterministic third-party responses.
2. **Deterministic Correctness Core**: Money is lost silently when distributed state changes are split across network boundaries without strict transactional boundaries. Scoping RRQ as a **closed-loop ledger** means value moves exclusively between internal merchant wallets inside the system. 
3. **Single Transaction Boundary**: Because both the source and target wallets share internal database storage, intra-shard transfers execute in **one serializable PostgreSQL transaction**. This completely eliminates distributed sagas, compensating undo operations, and distributed locks for over 85% of system traffic.

---

## Q2: How does RRQ enforce "Conservation of Value" (Invariant I1) under high concurrency and node crashes?

### Answer:
Conservation of value guarantees that every transfer posts exactly one debit leg and one credit leg of equal magnitude ($| \text{debit} | = \text{credit}$), and money is never created or destroyed.

1. **Single Transaction Commit**: The debit leg (`-amount`) and credit leg (`+amount`) are inserted into `ledger_entries` within the **same PostgreSQL transaction**. If the worker process dies mid-operation or the database connection drops, PostgreSQL rolls back the entire transaction. A "half-posted" transfer cannot exist on disk.
2. **Constraint Enforcement**: A `UNIQUE (transfer_id, leg)` constraint on the `ledger_entries` table prevents message redeliveries (from Kafka or retries) from inserting duplicate legs.
3. **Audit Verification**: The `recon-worker` executes a nightly batch job that re-derives balances by summing all append-only `ledger_entries` rows ($\sum \text{amount}$) per wallet and asserts that the net global sum matches opening balances.

---

## Q3: How do intra-shard transfers differ from cross-shard transfers in RRQ?

### Answer:
The system balances write scalability and correctness by separating intra-shard transfers from cross-shard transfers based on database sharding:

```
┌─────────────────────────────────────────────────────────────────────────────┐
|                              SHARD ROUTING RING                             |
|                           (merchant_id hash ring)                           |
└──────────────────────┬──────────────────────────────┬───────────────────────┘
                       │                              │
                       v                              v
            ┌──────────────────────┐      ┌──────────────────────┐
            │       SHARD A        │      │       SHARD B        │
            │  (Merchant 101, 102) │      │  (Merchant 201, 202) │
            └──────────────────────┘      └──────────────────────┘
```

| Dimension | Intra-Shard Transfer | Cross-Shard Transfer |
|---|---|---|
| **Condition** | `from_wallet` and `to_wallet` belong to merchants on the **same DB shard**. | `from_wallet` and `to_wallet` belong to merchants on **different DB shards**. |
| **Transaction Model** | Single `SERIALIZABLE` local transaction. | 2-Phase Clearing Account Saga. |
| **Latency Profile** | Low latency (<20ms). Single DB commit. | Asynchronous 2-phase commit (<500ms). |
| **Locking** | `SELECT ... FOR UPDATE` on both wallet rows. | Phase 1 local lock on source shard; Phase 2 local lock on dest shard. No cross-network lock. |
| **Compensation Path** | None needed (local rollback on error). | Reversal transaction on source shard if Phase 2 fails. |

---

## Q4: How does the 2-Phase Clearing Account Saga handle cross-shard transfer failures?

### Answer:
When a transfer crosses database shards, a single ACID transaction cannot span both databases without 2PC network locks (which introduce severe latency and lock starvation). RRQ uses an asynchronous 2-phase clearing protocol with an intermediate clearing account:

1. **Phase 1 (Source Shard)**:
   - Debit source wallet ($-\text{amount}$).
   - Credit source shard's system clearing account ($+\text{amount}$).
   - Insert `cross_shard_transfer` record with status `pending`.
   - Emit `xshard.transfer.requested` to Kafka outbox.
2. **Phase 2 (Destination Shard)**:
   - Consumer reads `xshard.transfer.requested`.
   - Debit destination shard clearing account ($-\text{amount}$).
   - Credit target wallet ($+\text{amount}$).
   - Emit `xshard.transfer.confirmed`.
3. **Compensation (If Target Wallet Closed/Frozen)**:
   - Destination shard emits `xshard.transfer.rejected`.
   - Source shard consumer catches rejection and executes a compensation transaction: credit source wallet ($+\text{amount}$), debit clearing account ($-\text{amount}$), and mark transfer status as `reversed`.
   - **Key Principle**: No database lock is ever held across the network.

---

## Q5: How does RRQ guarantee "At-Most-Once" execution per Idempotency Key (Invariant I3)?

### Answer:
Idempotency in distributed payment systems must be **durable** and **database-enforced**, not stored in volatile caches like Redis (which can lose keys during failovers):

1. **Database Constraint**: The `jobs` table enforces `UNIQUE (merchant_id, idempotency_key)`.
2. **Atomic Ingress Claim**: The API Gateway issues:
   ```sql
   INSERT INTO jobs (id, merchant_id, idempotency_key, request_hash, status)
   VALUES ($1, $2, $3, $4, 'pending')
   ON CONFLICT (merchant_id, idempotency_key) DO NOTHING;
   ```
3. **Conflict Handling**:
   - If 100 identical requests hit the gateway concurrently, exactly one row is inserted.
   - The remaining 99 requests trigger a conflict lookup, fetch the existing `job_id` and status, and return `HTTP 202 Accepted` with the original `job_id`.
   - If the request body differs for an existing key, the gateway rejects it with `HTTP 422 Unprocessable Entity`.

---

## Q6: Why use the Transactional Outbox pattern instead of publishing directly to Kafka after database writes?

### Answer:
Directly publishing to Kafka after a database commit introduces a classic distributed systems bug known as the **dual-write failure mode**:

```
[ Dual-Write Failure ]
1. DB Transaction Commits   -->  SUCCESS
2. App Process Crashes      -->  CRASH! (Kafka publish never executes)
Result: Database state changed, but downstream workers never notified (Message Loss).
```

**The RRQ Solution**:
1. Application writes business state changes (`jobs`, `ledger_entries`) AND inserts an event record into the `events` outbox table within the **same local database transaction**.
2. The event and the state change are equally durable on disk.
3. The `outbox-relay` background worker polls unpublished events (`WHERE published_at IS NULL`), produces them to Kafka, and stamps `published_at` upon broker acknowledgment.
4. If the relay dies, it resumes from the last unpublished `events.id`. Kafka redeliveries are made idempotent downstream via `UNIQUE (transfer_id, leg)`.
