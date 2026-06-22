# 11: Ledger Worker

The Ledger Worker is the service that actually moves money. The API Gateway accepts work; the Ledger Worker executes it. It is the spine of the system: every other service either feeds it (Gateway) or consumes its output (Webhook, Fraud, Reconciliation).

## What it does

The Ledger Worker consumes `job.requested` events from the Kafka `jobs` topic and posts the money movement they describe. For a transfer, "post the movement" means one thing: write the **debit leg and the credit leg in a single serializable Postgres transaction**, so the two halves of the double entry commit together or not at all.

That single sentence is the whole design. The hard parts of a money mover — "what if it crashes after the debit but before the credit?", "what if two transfers hit the same wallet at once?", "what if the same job is delivered twice?" — are not solved by elaborate recovery machinery here. They're solved by *not creating the problem*: a transaction has no half-applied state to recover, a row lock serializes concurrent transfers, and a uniqueness constraint makes redelivery a no-op.

The rest of this document is that claim, made precise.

---

## Why one transaction, and not a saga

This is the most important design decision in the system, so it's worth stating plainly. A **saga** — debit in one step, credit in a later step, with a *compensation* to undo the debit if the credit fails — is the tool you're forced to use when the two legs **cannot** share a transaction (different databases, or a call to an external bank). It is strictly *weaker*: it has an in-flight, debited-but-not-credited window, and "undo" is a second movement rather than a clean "never happened."

For an **intra-shard** transfer — both wallets on one merchant's shard, which is the common case — a single transaction *is* available, so the professional rule applies: **if you can use a transaction, you must.** This worker posts in one transaction, with no compensation and no distributed lock. The one place a saga genuinely returns is a **cross-shard** transfer, whose two wallets live on different shards a transaction can't span; that path uses the clearing protocol in [`../deep-dives/29-LEDGER-SHARDING.md`](../deep-dives/29-LEDGER-SHARDING.md), and nowhere else. RRQ stays closed-loop — no external bank leg — so the shard boundary is the *only* boundary a transaction can't cover.

---

## Inputs, outputs, guarantees

**Inputs**
- `job.requested` events from the Kafka `jobs` topic, consumed by the `ledger-workers` consumer group.
- Wallet and ledger state from Postgres (read inside the posting transaction).

**Outputs**
- `transfers` rows (one per movement; `completed` or `failed`).
- `ledger_entries` rows (two per completed transfer: the debit and credit legs). **The financial source of truth.**
- `jobs.status` transition to `completed` or `failed`.
- `transfer.completed` / `transfer.failed` events written to the `events` outbox (the relay publishes them to the `notify` topic for the Webhook Worker).
- `dlq_entries` rows for poison messages that exhaust the retry budget.

**Guarantees**
- Every accepted job reaches a terminal state or is routed to the DLQ within bounded time (**I7**).
- Every completed transfer has exactly one debit and one credit leg of equal magnitude, written atomically (**I1**).
- An active wallet's derived balance never goes negative; the overdraft check runs under the source wallet's row lock (**I2**).
- A wallet's entries are totally ordered by `id`, and concurrent transfers on one wallet are serialized by its row lock — across any number of worker replicas, because the lock lives in the database, not in worker memory (**I4**). This is why the worker is the system's horizontal-scaling exemplar.
- Reprocessing a redelivered job produces no additional postings (`UNIQUE (transfer_id, leg)`).

**Non-guarantees**
- No throughput SLO. Under load, jobs queue; backpressure is consumer lag, not request rejection.
- No webhook delivery — only enqueueing the notify event. The Webhook Worker owns delivery.
- No cross-wallet linearizability. Transfers on different wallets may commit in any order relative to wall-clock time.

---

## The mechanism

### The posting transaction, in full

This is the entire core of the service. A transfer of `amount` from `from_wallet` to `to_wallet`, for `job_id`, with the deterministic `transfer_id` derived from the job:

```sql
BEGIN ISOLATION LEVEL SERIALIZABLE;

  -- 1. Lock both wallets in a deadlock-free order (by id), and read fresh state.
  SELECT id, merchant_id, currency, status
    FROM wallets
   WHERE id IN ($from_wallet, $to_wallet)
   ORDER BY id
     FOR UPDATE;

  -- 2. Validate under the lock (terminal errors abort here):
  --    both wallets exist, both active, currencies match the transfer,
  --    source is owned by the job's merchant.

  -- 3. Compute the source balance under the lock and check it.
  --    SELECT COALESCE(SUM(amount),0) FROM ledger_entries WHERE wallet_id = $from_wallet;
  --    if balance < amount -> abort with INSUFFICIENT_BALANCE.

  -- 4. Post both legs. UNIQUE(transfer_id, leg) makes a redelivery a no-op.
  INSERT INTO transfers (id, job_id, from_wallet, to_wallet, amount, currency, status)
       VALUES ($transfer_id, $job_id, $from_wallet, $to_wallet, $amount, $currency, 'completed');

  INSERT INTO ledger_entries (wallet_id, transfer_id, leg, amount, balance_after)
       VALUES ($from_wallet, $transfer_id, 'debit',  -$amount, $from_balance - $amount),
              ($to_wallet,   $transfer_id, 'credit', +$amount, $to_balance   + $amount);

  -- 5. Terminate the job and enqueue the notification — same transaction.
  UPDATE jobs SET status = 'completed', completed_at = NOW() WHERE id = $job_id;

  INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id,
                      correlation_id, payload, occurred_at, publish_topic)
       VALUES ($event_id, 'transfer.completed', 'transfer', $transfer_id,
               $job_id, $payload, NOW(), 'notify');

COMMIT;
```

Everything between `BEGIN` and `COMMIT` is one atomic unit. Read it for the properties:

- **No in-flight window.** The debit, the credit, the job status, and the outbox notify all become visible at the same instant, or none do. There is no observable state where the source was debited but the destination wasn't.
- **The lock is the whole concurrency story.** `FOR UPDATE` on the two wallet rows means any other transfer touching either wallet waits here. The overdraft check (step 3) is therefore safe: nothing can debit between the check and the insert. The lock is released automatically at `COMMIT` — no TTL to tune, no watchdog, no "lease expired mid-operation" edge case.
- **Deadlock-free.** Locking the wallets `ORDER BY id` means transfer A↔B and transfer B↔A both grab locks in the same order, so they queue instead of deadlocking.
- **The outbox makes the notification exactly as durable as the money.** Because the `events` row is written in the same transaction, you cannot have a posted transfer with a lost notification, or a notification for a transfer that didn't post. The relay publishes the row to Kafka afterward, at-least-once; the Webhook Worker is idempotent on `event_id`.

### A failed transfer is also one transaction

A transfer that can't proceed (insufficient balance, frozen or closed wallet, currency mismatch) is *not* an error to recover — it's a normal terminal outcome. The worker commits a `failed` record in one transaction and moves on:

```sql
BEGIN;
  INSERT INTO transfers (id, job_id, from_wallet, to_wallet, amount, currency, status, failure_reason)
       VALUES ($transfer_id, $job_id, ..., 'failed', $reason);
  UPDATE jobs SET status = 'failed', failure_reason = $reason, completed_at = NOW() WHERE id = $job_id;
  INSERT INTO events (..., 'transfer.failed', ..., publish_topic = 'notify');
COMMIT;
```

No ledger entries are written, so there is nothing to undo — conservation (I1) holds trivially because no money moved. The merchant gets a `transfer.failed` webhook with the reason.

### Bulk payout is a loop, not a saga

A bulk payout (`type = 'bulk_payout'`) is one merchant paying N recipients. The worker iterates the N legs and runs **each as its own independent posting transaction**, exactly like a single transfer, with `transfer_id = job_id:i`:

- One bad leg does **not** roll back the others. Leg 4,998 failing (closed recipient wallet) leaves the other 4,999 committed and final — there is nothing to compensate, because each was atomic on its own. This is the property a saga had to work hard for and a loop of transactions gets for free.
- Each leg is independently crash-safe and idempotent (its own `UNIQUE(transfer_id, leg)`).
- When all legs are terminal, the worker writes one `bulk_payout.completed` event with success/failure counts (publish_topic `notify`) and sets the job `completed`.

### Crash and redelivery: why there is almost nothing to do

The classic money-mover nightmare — "worker died between the debit and the credit" — **cannot happen**, because the debit and credit are in one transaction. If the worker dies mid-transaction, Postgres rolls it back; there is no partial posting anywhere.

Recovery is therefore just at-least-once redelivery:

1. The worker crashes before committing the Kafka offset for `job_42`.
2. Kafka detects the missing heartbeat and rebalances the partition to a surviving worker.
3. The new worker re-runs `job_42`. Two cases:
   - **The transaction had committed** before the crash (worker died after `COMMIT`, before the offset commit). The re-run's `INSERT … ledger_entries` hits `UNIQUE(transfer_id, leg)` and no-ops; the worker recognizes the job is already terminal and just commits the offset.
   - **The transaction had not committed.** It was rolled back; the re-run posts normally.

Both paths converge on "exactly one posting, then the offset is committed." The uniqueness constraint converts the unknown outcome of a crash into a definite known outcome — which is the entire recovery design.

### Retryable vs terminal errors

The worker classifies each failure:

- **Terminal** (won't change on retry): insufficient balance, frozen/closed wallet, currency mismatch, unknown wallet. → commit a `failed` transfer + job, done.
- **Retryable** (transient): the database is briefly unavailable, a serialization failure forces a retry, a lock wait times out. → roll back and retry the whole transaction with bounded backoff.

A retryable error that never clears makes the job a **poison message**. After the retry budget (default 5 minutes of attempts), the worker writes a `dlq_entries` row (source `ledger`) with the `job.requested` payload and commits the offset (**I8**). The job is now owned by the DLQ, observable to operators, and out of the live path (**I7**).

Misclassifying matters: a retryable error marked terminal causes spurious failures; a terminal error marked retryable wastes the budget on doomed work. The classifier lives in one small module mapping error types to one bucket or the other.

---

## Worker lifecycle

```mermaid
sequenceDiagram
    autonumber
    participant K as Kafka (jobs topic)
    participant W as Ledger Worker
    participant DB as Postgres (jobs + transfers + ledger + events)

    loop main consume loop
        W->>K: Poll jobs topic
        K-->>W: job.requested (or empty)
        loop per message
            W->>DB: BEGIN SERIALIZABLE
            W->>DB: SELECT wallets FOR UPDATE (ORDER BY id)
            alt valid + sufficient balance
                W->>DB: INSERT transfers (completed)
                W->>DB: INSERT ledger_entries (debit, credit)
                W->>DB: UPDATE jobs completed; INSERT events (transfer.completed, outbox)
            else terminal error
                W->>DB: INSERT transfers (failed); UPDATE jobs failed; INSERT events (transfer.failed)
            end
            W->>DB: COMMIT
            W->>K: Commit offset
        end
    end

    Note over W: SIGTERM (graceful shutdown)
    W->>W: stop polling, finish the in-flight transaction, exit
    Note over W: uncommitted offsets are redelivered to a peer; UNIQUE(transfer_id,leg) makes reprocessing safe
```

Notice there is no "load saga state on startup" step and no "resume from the last completed step" logic. There is no durable per-step state to resume, because there are no steps — there is one transaction. Graceful shutdown finishes the current transaction (or lets Postgres roll it back) and exits; a peer redelivers anything uncommitted.

---

## Failure walk-throughs

Each corresponds to at least one test.

### F1: Worker crashes mid-post
Covered above. The transaction rolls back; Kafka redelivers; the re-run either posts (rollback case) or no-ops on `UNIQUE(transfer_id, leg)` (committed case). The merchant sees at most a slightly later webhook. **No double debit is possible.**

### F2: Destination wallet is frozen
Caught under the lock in step 2. The transaction aborts before any leg is written and commits a `failed` transfer + `transfer.failed` event with reason `WALLET_FROZEN`. No money moved; I1 holds trivially.

### F3: Insufficient balance under concurrency
Two transfers of 60 from a wallet holding 100. Both try to `SELECT … FOR UPDATE` the source wallet; one wins the lock, debits, commits (balance 40); the second then acquires the lock, reads balance 40, fails the check, commits a `failed` transfer. Exactly one succeeds; the wallet never goes negative (**I2**).

### F4: Postgres commit returns an unknown outcome
A network blip at the moment of `COMMIT` — the worker doesn't know whether it committed. It treats this as retryable and re-runs the transaction. If the commit had landed, the re-run no-ops on `UNIQUE(transfer_id, leg)`; if not, it posts. The constraint resolves the unknown into a definite outcome (the local version of the [unknown-outcome problem](../00-OVERVIEW.md)).

### F5: Bulk payout, one leg fails
Leg `i` targets a closed wallet. That leg's transaction commits `failed`; the others are unaffected and already final. The job's `bulk_payout.completed` summary reports `N-1` succeeded, `1` failed. No compensation anywhere.

### F6: A job can never succeed (poison message)
A job trips a persistent serialization conflict or references state that keeps erroring. After the retry budget, it's written to `dlq_entries` and the offset is committed. An operator inspects it, fixes the cause, and replays it from the Admin Dashboard.

---

## Code skeleton (Go reference)

Skeletons are for comprehension, not copying. The shape communicates the architecture.

```go
// Package ledger implements the Ledger Worker: it posts each transfer as a
// single serializable transaction.
//
// Invariants upheld here:
//   I1 (conservation), I2 (no negative balance), I4 (per-wallet ordering),
//   I7 (job termination).

type Worker struct {
    db         *pgxpool.Pool
    kafka      *kafka.Reader   // jobs topic, group "ledger-workers"
    classifier ErrorClassifier // retryable vs terminal
    metrics    *Metrics
}

func (w *Worker) Run(ctx context.Context) error {
    for {
        msg, err := w.kafka.FetchMessage(ctx)
        if err != nil {
            return err
        }
        if err := w.handle(ctx, msg); err != nil {
            // Retryable, not yet exhausted: do NOT commit the offset; let it redeliver.
            continue
        }
        _ = w.kafka.CommitMessages(ctx, msg) // terminal (completed, failed, or DLQ'd)
    }
}

// post executes one transfer as a single serializable transaction.
// Returns Done if a redelivery found the work already committed.
func (w *Worker) post(ctx context.Context, tx pgx.Tx, j Job, t Transfer) (Outcome, error) {
    // Lock both wallets in id order; read fresh status + currency.
    rows, err := tx.Query(ctx, `
        SELECT id, status, currency FROM wallets
         WHERE id = ANY($1) ORDER BY id FOR UPDATE`,
        []string{t.From, t.To})
    if err != nil {
        return Retry, err
    }
    src, dst, err := scanWallets(rows, t)
    if err != nil {
        return Terminal, err // unknown/duplicate wallet
    }
    if src.Status != "active" || dst.Status != "active" {
        return Terminal, ErrWalletNotActive
    }
    if src.Currency != t.Currency || dst.Currency != t.Currency {
        return Terminal, ErrCurrencyMismatch
    }

    var srcBal int64
    if err := tx.QueryRow(ctx,
        `SELECT COALESCE(SUM(amount),0) FROM ledger_entries WHERE wallet_id=$1`,
        t.From).Scan(&srcBal); err != nil {
        return Retry, err
    }
    if srcBal < t.Amount {
        return Terminal, ErrInsufficientBalance
    }

    // Both legs, atomically. UNIQUE(transfer_id, leg) makes a redelivery a no-op.
    if _, err = tx.Exec(ctx, `
        INSERT INTO transfers (id, job_id, from_wallet, to_wallet, amount, currency, status)
        VALUES ($1,$2,$3,$4,$5,$6,'completed') ON CONFLICT (id) DO NOTHING`,
        t.ID, j.ID, t.From, t.To, t.Amount, t.Currency); err != nil {
        return Retry, err
    }
    ct, err := tx.Exec(ctx, `
        INSERT INTO ledger_entries (wallet_id, transfer_id, leg, amount, balance_after)
        VALUES ($1,$2,'debit',  $3, $4),
               ($5,$2,'credit', $6, $7)
        ON CONFLICT (transfer_id, leg) DO NOTHING`,
        t.From, t.ID, -t.Amount, srcBal-t.Amount,
        t.To, t.Amount, dstBalAfter)
    if err != nil {
        return Retry, err
    }
    if ct.RowsAffected() == 0 {
        return Done, nil // already posted by an earlier delivery
    }
    return Continue, nil
}
```

The full `handle` method opens a `SERIALIZABLE` transaction, calls `post`, and on `Continue`/`Done` also updates the job and writes the outbox event before `COMMIT`. There is no orchestrator, no step interface, no compensation method — the complexity that used to live in those is gone.

---

## Test plan

Organized by the invariant each test validates.

### Validates I1 (conservation)
- **`TestConservation_HappyPath`** — 1,000 transfers; assert net zero across all wallets and exactly one debit + one credit per `transfer_id`.
- **`TestConservation_FailedTransfersMoveNothing`** — transfers that fail at validation; assert zero ledger entries and unchanged balances.

### Validates I2 (no negative balance)
- **`TestBalance_RejectsOverdraft`** — transfer from balance 100 for 150; assert `failed` with `INSUFFICIENT_BALANCE`, no legs.
- **`TestBalance_ConcurrentCannotOverdraft`** — balance 100, two concurrent 60s; assert exactly one succeeds. The core row-lock test.

### Validates I4 (per-wallet ordering)
- **`TestOrdering_ManyReplicasOneWallet`** — many workers, one hot wallet; assert entries are totally ordered by `id` and the balance is correct.

### Validates I7 (job termination) and idempotent redelivery
- **`TestTermination_CrashRecovery`** — kill the worker mid-post; assert the job reaches a terminal state with no double posting.
- **`TestIdempotency_Redelivery`** — deliver the same `job.requested` twice; assert exactly one posting (`UNIQUE(transfer_id, leg)`).
- **`TestTermination_PoisonToDLQ`** — force every attempt to fail transiently; assert the job lands in `dlq_entries` after the budget.

### Validates locking / deadlock freedom
- **`TestLocking_OppositeDirectionNoDeadlock`** — transfer A→B and B→A concurrently; assert both complete (lock order by id prevents deadlock).

### Validates bulk payout
- **`TestBulk_OneLegFailsOthersStand`** — 5,000-leg payout with one closed recipient; assert 4,999 posted, 1 failed, no rollback of the rest.

---

## What this service depends on

- **Postgres** — the merchant's shard (one logical ledger per shard); the source of truth for `jobs`, `transfers`, `ledger_entries`, and the outbox. The most write-intensive backend.
- **Kafka** — the `jobs` topic it consumes (produced by the outbox relay on the gateway's behalf).
- **API Gateway** — writes the `jobs` rows and the `job.requested` outbox events this worker consumes.

## What depends on this service

- **Webhook Worker** — consumes the `transfer.completed`/`transfer.failed` notify events this worker enqueues.
- **Fraud Worker** — consumes the same `jobs` topic via its own consumer group.
- **Reconciliation** — replays the `ledger_entries` this worker writes.

---

## Where to read next

- How the single transaction scales horizontally → [`../03-SCALING-AND-AVAILABILITY.md`](../03-SCALING-AND-AVAILABILITY.md)
- The outbox and event log it writes to → [`../deep-dives/25-EVENT-STORE-AND-PROJECTIONS.md`](../deep-dives/25-EVENT-STORE-AND-PROJECTIONS.md)
- The webhook delivery it feeds → [`12-WEBHOOK-WORKER.md`](12-WEBHOOK-WORKER.md)
