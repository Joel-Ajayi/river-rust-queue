# 21: Sagas

> **What this is.** The deep dive on sagas: where the pattern comes from, the formal model behind it, why it's the right answer for cross-service transactions, and the implementation subtleties that catch first-time builders out. The most important deep-dive in the set.
>
> **Reading time.** ~30 minutes.
>
> **Prerequisites.** [`../services/11-SAGA-WORKER.md`](../services/11-SAGA-WORKER.md). The service doc covers the *what*; this doc covers the *why* and the *edge cases*.

---

## The original paper

The saga pattern was formally introduced in a 1987 paper by Hector Garcia-Molina and Kenneth Salem, titled simply ["Sagas"](https://www.cs.cornell.edu/andru/cs711/2002fa/reading/sagas.pdf). The motivating problem they identified is the same one RRQ faces forty years later:

> "A LLT [long-lived transaction] is a transaction that takes a long time to execute, of the order of hours, days, or even longer... If we use traditional database transaction management techniques, the system would have to lock the data accessed by [the LLT] for the duration of its execution. This is unacceptable."

Their answer: break the long transaction into a sequence of smaller transactions, each of which can commit independently. Pair each one with a **compensating transaction** that semantically undoes it. If the sequence fails partway through, run the compensations for the steps that did complete, in reverse. The result is not the strong ACID guarantees of a single transaction, but a weaker property they called **semantic atomicity**: either the whole logical operation succeeds, or the system ends up in a state that's equivalent to having never started.

Garcia-Molina and Salem were writing about database operations spanning hours within a single DBMS. The pattern transfers cleanly to distributed systems where the "long-lived" aspect is not duration but *scope*, operations that cross transaction boundaries because they cross machine boundaries. RRQ's Transfer saga takes milliseconds in wall time, but it crosses the boundary between "wallet A's database write" and "wallet B's database write" (and "external API call" in a fuller system), and that crossing is what makes the saga pattern necessary.

If you want one piece of context for an interview, it's this: **a saga is what you build when you cannot use a database transaction.** Every saga design decision flows from that constraint. If you could use a transaction, you would. You can't, so you build a saga.

---

## What a saga is, formally

A saga is a sequence `T₁, T₂, ..., Tₙ` of **forward operations**, each paired with a **compensating operation** `C₁, C₂, ..., Cₙ`. The compensation `Cᵢ` semantically undoes `Tᵢ`.

Two outcomes are valid:

**Complete success:**
```
T₁ → T₂ → ... → Tₙ
```
All forward operations succeed in order. The system is in the desired end state.

**Compensated failure at step k:**
```
T₁ → T₂ → ... → Tₖ₋₁ → Tₖ (fails) → Cₖ₋₁ → ... → C₂ → C₁
```
The saga makes it to step k, which fails. Compensations for steps 1 through k-1 run in reverse order. The system is in a state semantically equivalent to "the saga never happened."

Three properties matter:

1. **Each `Tᵢ` is itself a real transaction**, atomic, durable, isolated within its own scope. If `Tᵢ` is "insert a row into the ledger," that insert either commits or doesn't; there's no half-state.
2. **Each `Cᵢ` is the semantic inverse of `Tᵢ`**, running `Tᵢ` followed by `Cᵢ` (with nothing in between) leaves the system in its pre-`Tᵢ` state, or in a state equivalent to it.
3. **Compensations are pure undoes, not retries.** If `Tᵢ` failed, you don't run `Cᵢ`. You run `Cᵢ₋₁, Cᵢ₋₂, ..., C₁`, the compensations for the steps that *succeeded*.

The word "semantically" in property 2 is doing work. A real undo might not literally reverse the steps. If `T₃` was "send a confirmation email," `C₃` can't unsend the email, it can only send a follow-up "previous email was sent in error" message. The compensation is the *best available semantic restoration*, not a magic time-reversal.

This matters for RRQ. A `Debit` step is straightforwardly reversible: insert a compensating Credit, and the wallet balance is restored. But a hypothetical `NotifyMerchant` step is not, once we've POSTed to the merchant, we can't un-POST. The system has to be designed so genuinely-irreversible operations are *last* (after which compensation is no longer needed) or are guarded by reversible state changes earlier in the saga.

In RRQ's Transfer saga, the steps are ordered specifically so that the only genuinely-irreversible operation (`Notify`) is the last one, after the saga has reached a terminal state. By the time we notify, there's nothing left to compensate.

---

## Orchestration vs choreography

The 1987 paper assumed a single coordinator. The distributed-systems community later identified a second style: **choreography**, where each service reacts to events and emits new events without a central coordinator. The two are usually presented as opposing choices, but the real distinction is *where the saga's control flow lives*.

**Orchestration**: a single service (the orchestrator) owns the saga's control flow. It calls `T₁`, waits for the result, calls `T₂`, and so on. On failure, it calls compensations. The orchestrator has explicit knowledge of the saga's steps; the participating services are unaware they're part of a saga.

```
   ┌──────────────┐
   │ Orchestrator │ ──── T₁ ───▶ Service A
   │              │ ◀─── ok ────
   │              │ ──── T₂ ───▶ Service B
   │              │ ◀── err ────
   │              │ ──── C₁ ───▶ Service A
   │              │ ◀─── ok ────
   └──────────────┘
```

**Choreography**: no central coordinator. Each service knows its own role. When a transfer is requested, Service A debits and emits `Debited`. Service B subscribes to `Debited`, performs the credit, and emits `Credited`. On failure, Service B emits `CreditFailed`. Service A subscribes to `CreditFailed`, runs its own compensation, and emits `Compensated`. The "saga" is an emergent property of the event flow.

```
   ┌────────────┐  ────── Requested ────▶  ┌──────────┐
   │ Initiator  │                          │ Service A│
   └────────────┘  ◀───── Debited ──────   └────┬─────┘
                                                │
                          ┌─────────────────────┘
                          ▼
                     ┌──────────┐
                     │ Service B│  ──── Credited ────▶ ...
                     └──────────┘  ◀── CreditFailed ── (back to Service A for compensation)
```

The choice between them is real and consequential. Choreography is championed in some microservices literature on the grounds of decoupling, no service "knows about" the saga; each just reacts to events. Orchestration is championed in others on the grounds of clarity, the saga's logic lives in one place where you can read it.

**RRQ uses orchestration**, deliberately, for three reasons:

1. **Saga steps in RRQ have inter-step data dependencies.** The Redlock acquired in step 2 must be released by code that knows it's the same logical saga and has the lock token. In choreography this is awkward, the token has to be carried in events. In orchestration the token is just a local variable.

2. **The failure path is fundamentally different from the success path.** With orchestration, "on failure, run compensations" is a single method on the orchestrator. With choreography, every service has to listen for failure events from every later service and decide whether to compensate. The wiring is fragile and grows quadratically with steps.

3. **Sagas in RRQ are conceptually one thing.** A Transfer saga is "move money from A to B." That's an atomic logical operation. Putting its definition in one place matches how we think about it. Choreography distributes the definition across many services and makes "what does this saga do?" a much harder question to answer.

The tradeoff: orchestration creates a service (the Saga Worker) that has significant logic and becomes a target for being too big. We mitigate by keeping the orchestrator's job narrow, it executes step machines, persists state, handles compensation. The actual *work* of each step (the ledger insert, the lock acquisition) is delegated. The orchestrator is a coordinator, not a god class.

**A senior engineer's answer to "orchestration or choreography?" is "it depends, what's your dependency structure?"** If your steps are independent (each service does its thing and emits an event, downstream services just react), choreography is fine. If your steps need shared state, careful ordering, or significant inter-step logic, orchestration wins. RRQ has the latter, so orchestration. The answer that signals junior thinking is "I prefer choreography because it's more loosely coupled", that's a pattern preference, not a design analysis.

---

## The state machine, restated

RRQ's Transfer saga state machine appears in [`../services/11-SAGA-WORKER.md`](../services/11-SAGA-WORKER.md). Here we look at it through a different lens: what the state machine is *for*, in terms of crash recovery.

A state machine in a saga serves two purposes:

1. **Programmatic clarity.** Code that reads "if in state Debited, do Credit next" is easier to maintain than code that infers next-step from a sequence of database queries.

2. **Crash recovery semantics.** When a replacement worker reads `saga_state.current_state = 'Debited'`, it knows *exactly* what has been done and what hasn't. The replacement doesn't need to query "did the debit happen?", the state row already answers.

The second is the load-bearing one. The state row is a **promise to the future**: it asserts that the corresponding work has been done and is durable. Specifically:

| State | What's true |
| --- | --- |
| `Init` | The saga has been created. No ledger writes have happened yet. |
| `Valid` | Validation has passed. No ledger writes. |
| `Locked` | Redlocks are held on both wallets. No ledger writes. |
| `Debited` | Source wallet's debit ledger entry exists and the `DebitApplied` event exists. |
| `Credited` | Destination wallet's credit ledger entry exists and the `CreditApplied` event exists. |
| `Compensating` | The saga has decided to fail; compensation in progress. |
| `Compensated` | All necessary compensations have been applied. |
| `Completed` | Terminal: success. Notify enqueued. Locks released. |
| `Failed` | Terminal: failure. Notify enqueued. Locks released. |
| `DeadLettered` | Terminal: stuck. Operator intervention required. |

The state row and the corresponding event/ledger writes are **committed in the same database transaction**. This is the single most important implementation detail of the saga, and it's worth restating: when you commit the transition from `Locked → Debited`, the `INSERT INTO ledger_entries`, the `INSERT INTO events (debit_applied)`, and the `UPDATE saga_state SET current_state = 'Debited'` all commit together or all roll back together.

If they didn't, if the saga state could be updated independently of the work it asserts, then crash recovery would be impossible. Imagine a crash between "ledger insert committed" and "state row updated." The state row says `Locked` but the ledger has the debit. A replacement worker, reading `Locked`, would re-run the debit and insert a duplicate. The transactional bundling makes this case impossible.

This is also why the `UNIQUE(saga_id, step_name)` constraint exists on `ledger_entries`. Even if the bundling were somehow broken, the constraint catches the duplicate insert. Belt and suspenders.

---

## Crash recovery, the hard part, in painful detail

The high-level story of crash recovery, "replacement worker reads saga_state, resumes at the right step", hides a lot of important nuance. Here's the full picture.

### The lifecycle of a stream message

A `JobRequested` event lives in the Redis Streams `stream:jobs`. Its lifecycle:

1. **Produced** by the API Gateway via `XADD`. Has a stream ID like `1700000000-0`.
2. **Delivered** to a consumer in the `saga-workers` group via `XREADGROUP`. Now in the **pending list** for the consumer that received it.
3. **Processed** by the worker (saga execution).
4. **Acknowledged** via `XACK`. Removed from the pending list.

Between steps 2 and 4, the message is "claimed but not acknowledged." If the worker crashes here, the message stays in the pending list of the dead consumer. It's not redelivered automatically, Redis Streams trusts that consumers ACK things they've processed.

The mechanism for recovering messages from dead consumers is `XPENDING` (list pending messages, with idle time) and `XCLAIM` / `XAUTOCLAIM` (transfer ownership). RRQ uses `XAUTOCLAIM`:

```
XAUTOCLAIM stream:jobs saga-workers <my-consumer-id> 60000 0
```

This atomically transfers any messages in any consumer's pending list that have been idle for at least 60 seconds to the calling consumer. The new consumer is now responsible for processing them.

The 60-second threshold is tuned: too short and you'd steal messages from healthy slow workers (causing duplicate processing, which idempotency handles but with overhead); too long and recovery from a crashed worker takes too long. 60 seconds is comfortably longer than any healthy single-step duration in RRQ's sagas.

### What a replacement worker actually does

When `XAUTOCLAIM` returns a reclaimed message, the replacement worker's processing is *not* the same as for a fresh message. The differences:

**Fresh message:**
1. Read message, parse event.
2. Generate saga_id (or use the one in the event payload).
3. `INSERT INTO saga_state (current_state='Init', ...)`.
4. Execute saga steps in order.
5. ACK.

**Reclaimed message:**
1. Read message, parse event.
2. Look up existing saga_state by job_id.
3. **If found:** read current_state. Determine resume point. Execute from there.
4. **If not found:** the previous worker crashed *before* even inserting the saga_state row. Treat as fresh, go to fresh-message step 3.
5. ACK.

The "look up existing saga_state" step is critical. Without it, a reclaimed message would be processed from scratch, re-running steps that already succeeded. The idempotency constraints would catch the duplicates and produce `Done` outcomes, *correct* behavior, but wasteful, and it muddles the audit trail.

In code, the orchestrator's entry point handles both cases:

```
fn process_message(msg):
    job_id = msg.job_id
    
    state = SELECT * FROM saga_state WHERE job_id = ?
    
    if state is None:
        // Fresh saga.
        saga_id = ULID()
        INSERT saga_state (saga_id, job_id, current_state='Init', ...)
        state = (just-inserted row)
    else:
        // Resumed saga. State row already exists.
        // The orchestrator knows where to pick up.
        pass
    
    run_saga_from(state)
```

The lookup-by-job_id is what reconciles the two paths. The `job_id` is in the message; it's stable across redeliveries; it's the natural key for "have we seen this work before?"

### The resume-point algorithm

Given a `saga_state` row, the orchestrator needs to compute *which step to run next*. The algorithm:

```
fn resume_point(state) -> (step_index, mode):
    steps = step_sequence_for(state.saga_type)
    
    // Find the step matching state.last_completed_step.
    if state.last_completed_step is null:
        last_idx = -1  // before step 0
    else:
        last_idx = index_of(state.last_completed_step, steps)
    
    match state.current_state:
        case "Init", "Valid", "Locked", "Debited", "Credited":
            // Forward mode. Next step is last_idx + 1.
            return (last_idx + 1, ForwardMode)
        
        case "Compensating":
            // Compensation mode. Compensate from last_idx backwards.
            return (last_idx, CompensateMode)
        
        case "Completed", "Failed", "Compensated", "DeadLettered":
            // Terminal. Nothing to do; just ACK.
            return (None, NoMode)
```

The mapping from `current_state` to "forward or backward" is implicit in the state machine's design. States that name a *progress point* (Valid, Locked, Debited, Credited) mean forward; states that name a *decision* (Compensating) mean backward; states ending in `-ed`/`-d` final words mean terminal.

If the state names are inconsistent or the algorithm has to special-case each, the code becomes fragile. RRQ's state names are designed to make this algorithm trivial. The naming convention is part of the architecture.

### The re-validation question

The most subtle case in crash recovery: when a worker resumes a saga in state `Locked` or later, the Redlocks have likely expired during the gap (the lock TTL is 5 seconds; the XAUTOCLAIM threshold is 60 seconds). Another saga could have acquired the lock on the same wallet during the gap.

The replacement worker has to re-acquire the lock before proceeding. But should it just acquire and continue, or should it re-validate?

**Argument for "just acquire and continue":** the saga has already passed validation; re-validating is wasted work. Trust the persisted state.

**Argument for "re-validate":** during the lock gap, another saga could have completed and changed the wallet's balance, status, or relationship to other wallets. Specifically:
- A different saga could have debited the source wallet, leaving insufficient balance for the resuming saga's pending step.
- The fraud worker could have frozen the destination wallet.
- An operator could have manually frozen the source wallet via the Admin Dashboard.

In these cases, the original validation result is **stale**. Proceeding without rechecking could violate invariants, e.g., debit a wallet that should have been frozen, or credit a wallet that should be closed.

RRQ chose re-validation. Specifically, when resuming a saga in any non-Init state, the orchestrator:

1. Re-acquires Redlocks.
2. Re-runs the validation logic (status checks, balance check). This is a few queries, fast.
3. If validation still passes, proceeds with the next step.
4. If validation now fails, transitions to `Compensating` to undo any work already done.

The cost is a handful of database queries on resume. The benefit is correctness under genuinely-concurrent failure scenarios.

A weaker design would skip re-validation and accept that some sagas would proceed with stale assumptions. The argument for the weaker design: re-validation has cost, and the failure scenarios where it matters (a saga that crashed at exactly the wrong moment, *and* the wallet's state changed during the gap) are vanishingly rare. The argument against: rare bugs in payment systems compound; better to be paranoid.

RRQ chose paranoid. The cost is small; the worst case is bad.

### Edge cases that aren't obvious

A few more crash-recovery scenarios that show up in chaos tests:

**Crash after `INSERT events` but before `UPDATE saga_state`.** Impossible by construction, they're in the same transaction. Either both commit or neither does. If you saw this in production, you have a bug in transaction handling, not a saga-state-machine bug.

**Crash during compensation.** Same logic as forward crash. The state is `Compensating`; `last_completed_step` indicates which compensation completed last (the most recent compensation, since they run in reverse). Resume continues compensating backward from that point.

**Two workers reclaim the same message.** Possible in principle if both call `XAUTOCLAIM` at the same instant. Defense: the `SELECT ... FOR UPDATE` on the saga_state row when processing begins. Whichever worker wins the row lock proceeds; the other blocks, eventually finds the state has advanced, and either resumes (different state) or no-ops (terminal state).

**Worker reads stale saga_state from a read replica.** Not possible, the saga worker reads from the primary. Read replicas serve dashboard queries, not operational ones. Documented in the data model section.

**The saga's deadline_at expires mid-execution.** The deadline is for *observability*, not for *enforcement*. A saga that exceeds its deadline keeps running; the deadline is what makes "stuck saga" queryable in the Admin Dashboard. The saga doesn't auto-abort on deadline.

---

## The compensation idempotency story, in full

Compensations are idempotent. This is the most important property of the compensation design, and the mechanism that enforces it is worth understanding completely.

### Why compensation idempotency is necessary

Crash recovery during compensation: the worker is partway through running compensations (say, has finished `C₃` and is starting `C₂`) and dies. The replacement worker reads `saga_state.current_state = 'Compensating'`, `last_completed_step = 'compensation_for_step_3'`, and resumes. But "resume" here means *retry from where we left off*, and the previous worker may have completed `C₂` before crashing, even though `last_completed_step` doesn't reflect it (if the update of `last_completed_step` was the last thing that didn't commit).

The replacement runs `C₂` again. If `C₂` is not idempotent, this is a bug, it has the effect of running the compensation twice.

For a debit→credit compensation, "not idempotent" would mean: the compensation inserts a credit entry without checking whether one already exists. Two runs of the compensation = two credit entries = source wallet is now richer than it started.

### How the constraint makes it work

The mechanism: every ledger entry has `UNIQUE(saga_id, step_name)`. The compensation's insert is:

```sql
INSERT INTO ledger_entries (wallet_id, amount, ..., saga_id, step_name)
VALUES ('wal_A', +500000, ..., 'sg_42', 'compensation_credit')
```

If this row already exists (from a previous compensation attempt that committed before the crash), the insert fails with `ERROR: duplicate key value violates unique constraint`. The compensation step recognizes this specific error and treats it as "already done":

```
match insert_compensation_entry() {
    Ok(_) => /* compensation just ran */,
    Err(UniqueViolation) => /* already ran in a previous attempt; no-op */,
    Err(other) => return Retry,
}
```

After handling either case (insert succeeded or already-existed), the saga proceeds to the next compensation step. The state transitions to `last_completed_step = 'compensation_credit'`. The crash-resume cycle eventually terminates with all compensations applied exactly once.

### The general principle

This pattern generalizes to any side effect a saga step makes. The principle: **each saga step's side effect is keyed on `(saga_id, step_name)`, and the storage layer enforces uniqueness on that key.**

For ledger entries: `UNIQUE(saga_id, step_name)` on `ledger_entries`.

For events: events have their own `event_id` (a ULID) which is unique by construction, but for *replay*, the event store treats `(correlation_id, event_type, step_name)` as the natural dedup key. A reconciler that finds two `DebitApplied` events with the same `(saga_id, step_name)` knows one of them is a duplicate (and the constraint should have prevented it; if it didn't, that's a bug to investigate).

For Redis side effects (lock acquisition): the lock value is the saga_id, so trying to acquire an already-held lock is a no-op (we already own it).

For external side effects (a webhook POST to a merchant): handled by the merchant's idempotent processing of `event_id`, out of our direct control.

The pattern is: **make the side effect itself idempotent at the storage layer, not at the code layer.** Code-level idempotency (check-then-write) is racy; storage-level idempotency (unique constraint) is atomic. RRQ uses storage-level idempotency wherever possible.

---

## When compensation cannot succeed

The unhandled tail of saga design: what happens when a compensation step *itself* fails permanently?

Concrete scenario: the Transfer saga is `Debited`, advancing to `Credited`. The credit fails. Compensation begins: insert `compensation_credit` ledger entry on the source wallet. The database is unreachable. The compensation retries; the database is still unreachable. After N retries, the compensation gives up.

What now? The system is in an *inconsistent* state. The source wallet has been debited; the destination has not been credited; the compensation that should have restored the source is stuck.

RRQ's answer: **transition the saga to `DeadLettered`**. The state row indicates that the saga is unrecoverable by automated means. An operator must intervene.

The operator's tools:

1. The Admin Dashboard's saga detail view for `sg_42` reveals the full state, including which step failed, why, and the partial ledger entries.
2. The operator investigates the root cause. Often it's an environmental issue (database overload, transient network partition) that has since resolved.
3. The operator decides on a path: 
    - **Replay compensation manually.** If the underlying issue is fixed, the dashboard's retry-compensation action on `sg_42` re-runs the compensation. The idempotency constraints ensure it doesn't double-credit if it had partially succeeded.
    - **Manual adjustment.** If the compensation cannot be safely re-run (e.g., the saga's data has been corrupted), the operator inserts an adjustment event manually with full reasoning, restoring the wallet to its correct state.
    - **Mark resolved with note.** If the discrepancy is small and not worth investigating (rare; usually only in test environments).

The system **never auto-corrects** a `DeadLettered` saga. The reasoning is the same as for reconciliation alerts: auto-correction masks real bugs and reduces audit trail clarity. A `DeadLettered` saga is a problem for humans, not for algorithms.

This is the *only* failure mode in the system that requires manual intervention. Every other failure has an automated recovery path. The fact that this one doesn't is a recognition that some failures are genuinely outside the scope of automation.

---

## A note on Sagas and the SAGAS-original semantics

Worth flagging for completeness, even though it's an academic point: the 1987 paper's saga model assumed a property called **forward-recoverability**, where a sufficiently-determined system could retry a failed step indefinitely until it succeeded. The paper distinguished two saga variants:

- **Forward-recoverable sagas:** every step either succeeds eventually (with enough retry) or causes the whole saga to fail.
- **Compensable sagas:** every step is paired with a compensation; failure of any step triggers reverse compensation.

RRQ uses **compensable** sagas. The original paper would have called this the safer choice for systems where forward progress cannot be assumed.

For an interview, the relevant historical note is: this design is *forty years old*. The paper that introduced it discusses bank transactions explicitly as the canonical use case. RRQ is doing what the paper recommended for the use case the paper described.

---

## Implementation patterns: the saga step as code

A few approaches to encoding saga steps in code, with the tradeoffs.

### Pattern A: Interface + slice (the Go approach)

```go
type Step interface {
    Name() string
    Forward(ctx context.Context, sc *SagaContext) (StepOutcome, error)
    Compensate(ctx context.Context, sc *SagaContext) error
}

var transferSteps = []Step{
    &ValidateStep{},
    &AcquireLockStep{},
    &DebitStep{},
    &CreditStep{},
    &CompleteStep{},
    &NotifyStep{},
}

func runForward(steps []Step, sc *SagaContext) error {
    for i, step := range steps {
        outcome, err := step.Forward(sc.Ctx, sc)
        // ... handle outcome ...
    }
}

func runCompensation(steps []Step, startIdx int, sc *SagaContext) error {
    for i := startIdx; i >= 0; i-- {
        if err := steps[i].Compensate(sc.Ctx, sc); err != nil {
            return err
        }
    }
}
```

**Strengths:** simple, idiomatic Go. Easy to add new steps. The slice order makes the sequence obvious. Iterating forward and backward is mechanical.

**Weaknesses:** invalid transitions are runtime-checked, not compile-time. If the state machine adds a new state, code that switches on state must be manually updated everywhere. The compiler doesn't help.

**Where RRQ uses it:** the Go implementation. This is the dominant pattern in Go codebases handling state machines.

### Pattern B: Type-state encoding (the Rust approach)

```rust
struct Saga<S> {
    id: SagaId,
    job_id: JobId,
    data: SagaData,
    _state: PhantomData<S>,
}

struct Init;
struct Valid;
struct Locked;
struct Debited;
// ...

impl Saga<Init> {
    async fn validate(self) -> Result<Saga<Valid>, Failure> { ... }
}
impl Saga<Valid> {
    async fn acquire_lock(self) -> Result<Saga<Locked>, Failure> { ... }
}
impl Saga<Locked> {
    async fn debit(self) -> Result<Saga<Debited>, Failure> { ... }
}
impl Saga<Debited> {
    async fn credit(self) -> Result<Saga<Credited>, Failure> { ... }
}
```

**Strengths:** invalid transitions are compile errors. `saga_in_init.credit()` doesn't compile because `Saga<Init>` doesn't have a `credit()` method. Refactor safely: add a new state, the compiler tells you every place that needs to handle it.

**Weaknesses:** more code (one impl block per state). The crash-recovery boundary is unavoidably stringly-typed, reading `current_state = 'Debited'` from the database requires reconstructing `Saga<Debited>` via pattern-matching on a string. The type-state guarantee resumes *after* the reconstruction, not at it.

**Where RRQ uses it:** the Rust implementation. It's not gratuitous, it's a real correctness mechanism, and it's the single strongest Rust-specific talking point in the project.

### Pattern C: Explicit state machine with pattern match (an alternative neither uses)

Some codebases write the saga as a single function that switches on the current state:

```
fn run_saga(state) {
    loop {
        state = match state.current_state {
            "Init" => execute_validate(state),
            "Valid" => execute_acquire_lock(state),
            "Locked" => execute_debit(state),
            // ...
            "Completed" => break,
            "Failed" => break,
        }
    }
}
```

**Strengths:** the whole state machine is in one place. Easy to read top-to-bottom.

**Weaknesses:** the function grows unwieldy as the state machine grows. Hard to test individual transitions in isolation. The "shared data threading" (passing `sc` to each step) becomes verbose.

**Where RRQ uses it:** not as the main pattern in either language. The two pattern A/B variants both extract per-step logic into per-step structs/impls, which composes better.

### Which is "best"?

There isn't one. They're three points on a tradeoff curve between conciseness and compile-time safety. RRQ picks A for Go (idiom) and B for Rust (type-state strength) deliberately. A reviewer asking "why these specific choices?" gets a specific answer about idiom and tradeoff, not "because this is the One True Way."

---

## Performance considerations

Sagas have an inherent overhead that database transactions don't: the per-step state persistence. Each step transition is an additional database write (the `UPDATE saga_state`). For an N-step saga, that's N extra writes compared to a hypothetical single-transaction implementation.

For RRQ's Transfer saga with 6 steps, that's 6 state updates per saga. At 1,000 sagas/sec, that's 6,000 state writes/sec. Each write is small (a few hundred bytes) and indexed (`saga_state.saga_id` is the primary key). Modern SSDs handle this well, but it's a real cost.

Optimizations RRQ does not use, but could:

- **Bundle state update with the work transaction.** Already done. The state update and the work (ledger insert + event insert) are in the same transaction. One commit per step, not two.
- **Skip state updates for transient intermediate states.** Some saga implementations skip persisting "fast" states (Init → Valid is a few microseconds; do we really need to write Valid before moving to Locked?). RRQ doesn't skip, every state transition is durable. The cost is small; the recovery semantics are cleaner.
- **Batched saga state writes.** Multiple sagas advancing simultaneously could batch their state updates. Adds complexity, doesn't meaningfully help at our throughput.

Optimizations RRQ doesn't do because they break correctness:

- **Async state updates.** Updating saga_state in a different transaction than the work would let work commit before state, breaking crash recovery. Don't do this.
- **In-memory caching of saga state.** A worker holding saga state in memory and writing only at terminal would lose state on crash. Don't do this either.

The general principle: saga state is *the* source of truth for "where is this saga right now." Compromising its durability or freshness breaks the whole pattern.

---

## Patterns to avoid

A short tour of saga anti-patterns that show up in immature implementations:

**"Just retry the whole saga."** When a step fails, some implementations restart the saga from step 1. This is wrong because (a) some steps have already happened and re-running them would double-apply, and (b) you'd be re-running validation steps over and over, masking real bugs.

**"Save state to memory; persist at the end."** Tempting because it's fast. Broken because crashes lose the state. Sagas have to be durable across crashes; this is non-negotiable.

**"Long-lived database transactions across the whole saga."** This is the thing sagas exist to *avoid*. If you can hold a transaction open across all steps, you don't need a saga, just use the transaction. If you can't, holding a transaction open is wrong.

**"Compensations that are full inverses of forward operations."** A "credit" is not the inverse of a "debit" in a strict sense, it has its own audit trail, its own ledger entry, its own event. The compensation is *semantically* the inverse; it leaves the system in an equivalent state. Treating compensations as literal reverse-operations leads to confusion about what to record in audits.

**"Idempotent forward operations, non-idempotent compensations."** Both must be idempotent. The compensation is the more dangerous one, it runs in the failure path, often after a crash, and is far more likely to be replayed than the forward path. If only one is idempotent, make it the compensation.

**"Saga steps that call each other."** A step calling another step (with its own state transition) creates a nested saga that's not represented in the state machine. The result: crash recovery has to be aware of the nesting, which is much harder to get right. Keep steps atomic and linearly ordered.

**"Reading current state from events, not state row."** Events are append-only and immutable; computing current state by replaying events is O(events) per check. The state row exists precisely to avoid this. Use the state row for control flow; use events for audit.

---

## Saga design questions for a new saga

If you ever need to design a new saga, here's the checklist worth running through:

1. **What are the steps?** List them, in order. Each step must be a single coherent unit that can succeed or fail independently.
2. **What is each step's compensation?** Explicit. "There is no compensation for step N" is acceptable only if step N is the last step (after which compensation isn't needed).
3. **What state does the saga need to carry between steps?** Define the `state_data` schema. Keep it minimal.
4. **What are the deadlines?** Per-saga, in real terms. A Transfer saga should complete in seconds; a hypothetical Chargeback might take days.
5. **What are the idempotency constraints?** Which `(saga_id, step_name)` tuples need uniqueness in storage?
6. **Which step is the first irreversible one?** It must be the last step, or you have to redesign.
7. **What does a `DeadLettered` outcome look like?** What information would an operator need to recover?

These seven questions, answered upfront, lead to sagas that survive review. Sagas that don't have answers tend to develop them under fire during incidents, which is too late.

---

## Where to read next

- The service that runs all of this → [`../services/11-SAGA-WORKER.md`](../services/11-SAGA-WORKER.md)
- The locking that protects step atomicity → [`23-LOCKING.md`](23-LOCKING.md)
- The event store the saga writes to → [`25-EVENT-STORE.md`](25-EVENT-STORE.md)

---

*Pass 3 of the architecture series. Last updated pre-implementation.*
