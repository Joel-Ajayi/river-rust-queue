# 15 — Admin CLI

> **What this is.** The service document for the Admin CLI. The smallest service in RRQ — a single binary, no consumer loops, no state of its own. But operationally critical.
>
> **Reading time.** ~10 minutes.
>
> **Prerequisites.** Skim the other service docs. The CLI is a window into all of them.

---

## What it does

The Admin CLI is the **operator's interface** to the rest of the system. It's the tool an engineer uses when something has gone wrong and they need to investigate, diagnose, or intervene. It's how DLQ entries get replayed, how stuck sagas get inspected, how wallet freezes get applied or reversed, how consumer lag gets surfaced.

It is *not* an interactive shell or a dashboard. It's a Unix-style CLI with a fixed set of subcommands. Each subcommand does one operation, prints structured output (table or JSON), and exits. This makes it scriptable, log-friendly, and easy to test.

The CLI exists for two reasons:

1. **The DLQ is a feature, not a failure.** Some failures by design require human judgment — a webhook endpoint that's been broken for hours, a saga that exhausted compensation retries. The DLQ holds these for review. Without a way to inspect and replay DLQ entries, the DLQ is a black hole. The CLI is what makes it visible.

2. **Production incidents need operator levers.** When a wallet is showing anomalous activity at 3am, an operator needs a way to freeze it without redeploying code. When a transfer is stuck mid-saga and needs to be force-cancelled, an operator needs an escape valve. The CLI provides these levers with audit trails.

Every CLI action that mutates state writes an event to the event log, recording who did what and why. This is part of how the system stays auditable — operator actions are first-class citizens in the history, not out-of-band changes.

---

## Inputs, outputs, guarantees

**Inputs**
- Command-line arguments (subcommand + flags).
- Environment variables for connection strings and operator identity.
- Optional config file for default values.

**Outputs**
- Tabular or JSON output to stdout. Default is tabular; `--json` flag switches.
- Exit code: 0 on success, non-zero on error.
- Side effects (for mutating commands): inserts to the events table, updates to wallets/saga_state/dlq_entries.
- Audit events: every mutating action writes a corresponding `operator.action` event with operator identity, command run, and parameters.

**Guarantees**
- **Auditability.** Every mutation produces an event. The audit trail is complete; no operator action is invisible.
- **Idempotency where possible.** `freeze` on an already-frozen wallet is a no-op. `replay` on an already-replayed DLQ entry is rejected. The CLI defends against accidental double-action.
- **Confirmation for destructive operations.** Mutations require either `--yes` flag or an interactive y/n prompt.

**Non-guarantees**
- **Not a permissions system.** v1 assumes the operator running the CLI has access to the database and Redis (typically via VPN + bastion). v2 would add operator identity verification and per-command permissions; v1 logs the configured `OPERATOR_ID` but trusts it.
- **Not transactional across multiple commands.** Each subcommand is its own database transaction. Running `freeze wal_X` and then `freeze wal_Y` is two independent operations; if the second fails, the first is not rolled back.

---

## The mechanism

### Command surface

The CLI is structured as `rrq <subcommand> <action> [args] [flags]`. Subcommands group related operations:

```
rrq dlq      — dead letter queue operations
rrq saga     — saga state inspection and intervention
rrq stream   — Redis Streams diagnostics (consumer lag, pending)
rrq circuit  — circuit breaker state
rrq wallet   — wallet operations (freeze, unfreeze, balance)
rrq events   — event log queries
rrq reconcile — manual reconciliation runs and alert review
```

### The full command list

```
rrq dlq list [--source saga|webhook] [--status open|replayed|resolved] [--limit N] [--json]
rrq dlq show <entry_id>
rrq dlq replay <entry_id> [--yes]
rrq dlq resolve <entry_id> --note "<reason>" [--yes]

rrq saga show <saga_id>
rrq saga stuck [--older-than 1h] [--limit N]
rrq saga abort <saga_id> --reason "<text>" [--yes]
   # Forces a saga to terminal-failed state with operator note.
   # Does NOT trigger compensation — used only when compensation is also stuck.

rrq stream lag
rrq stream pending <stream> [--consumer <id>]
rrq stream peek <stream> [--count N]
   # Read messages without ACKing (for debugging).

rrq circuit status [--merchant <id>]
rrq circuit reset <merchant_id> [--yes]
   # Force a per-merchant breaker back to closed state.

rrq wallet show <wallet_id>
   # Balance, status, recent activity.
rrq wallet freeze <wallet_id> --reason "<text>" [--yes]
rrq wallet unfreeze <wallet_id> --reason "<text>" [--yes]
rrq wallet history <wallet_id> [--from <date>] [--to <date>] [--limit N]

rrq events tail [--type <event_type>] [--aggregate <id>]
   # Like `tail -f` for the event log.
rrq events search --type <type> [--from <date>] [--to <date>] [--limit N]

rrq reconcile run [--window 24h]
   # Manual one-off reconciliation. Acquires advisory lock; fails if one is running.
rrq reconcile alerts [--last 7d] [--limit N]
```

### Output examples

`rrq dlq list`:

```
ID                    SOURCE   ATTEMPTS  AGE     STATUS  ERROR
dlq_01HQX...          webhook  10        3h ago  open    HTTP 500 (last)
dlq_01HQY...          saga     5         12h ago open    compensation_failed
dlq_01HQZ...          webhook  10        2d ago  replayed (job_42)
```

`rrq saga show sg_42`:

```
Saga sg_42 (transfer)
  job_id:       job_42
  state:        Debited                ← stuck here
  last_step:    debit                  
  started:      14:23:01Z (5m 12s ago)
  deadline:     14:28:01Z (expired)
  state_data:
    from_wallet: wal_A
    to_wallet:   wal_B
    amount:      500000

Recent events:
  [14:23:01.123]  saga.validated
  [14:23:01.456]  ledger.debit_applied  (wal_A: -500000, balance 1500000)
  
Worker assignment:
  consumer: fraud-host-3-7
  message_id: 1700000000-0
  pending in: stream:jobs
  idle: 4m 12s
  
Actions:
  This saga has likely lost its worker. It should be reclaimed by XAUTOCLAIM
  within the next minute. If still stuck after 5 minutes, run:
      rrq saga abort sg_42 --reason "..."
```

`rrq stream lag`:

```
STREAM                   CONSUMER GROUP   LAG   PENDING
stream:jobs              saga-workers     12    3
stream:jobs              fraud-workers    12    1
stream:notify-0          webhook-workers  0     0
stream:notify-1          webhook-workers  847   12  ← unhealthy
stream:notify-2          webhook-workers  0     0
...
```

Each command does exactly one thing, prints exactly the information needed for the task, and exits.

### The audit-event pattern

Every mutating command writes an `operator.action` event to the event store. Example:

```json
{
  "event_type": "operator.action",
  "occurred_at": "2026-05-12T14:35:22Z",
  "data": {
    "operator_id": "ops-eng-3",
    "command": "wallet freeze",
    "args": { "wallet_id": "wal_X", "reason": "manual review - suspicious activity" },
    "outcome": "success",
    "before_state": { "status": "active" },
    "after_state":  { "status": "frozen" }
  }
}
```

This is in the same event store as everything else. Reconciliation reads it. Audits can query it. There's no separate "audit log" — the system's event log is the audit log.

---

## Walk-throughs

### Walk-through 1: investigating a DLQ entry

It's Monday morning. The on-call engineer sees `webhook_deliveries_dlq_total{merchant_id="m_X"}` has been climbing all weekend.

```
$ rrq dlq list --source webhook --status open
ID                    MERCHANT  ATTEMPTS  AGE     ERROR
dlq_01HQA...          m_X       10        72h     HTTP 500 (last)
dlq_01HQB...          m_X       10        72h     HTTP 500 (last)
dlq_01HQC...          m_X       10        71h     HTTP 500 (last)
... (847 rows total for m_X)
```

The engineer drills in:

```
$ rrq dlq show dlq_01HQA...
Entry dlq_01HQA...
  source:        webhook
  status:        open
  attempts:      10
  first_failed:  Friday 14:23 UTC
  last_failed:   Friday 17:12 UTC (after 10 attempts)
  last_error:    HTTP 500 - "Internal Server Error"
  
  original_payload:
    event_type: transfer.completed
    event_id:   ev_01HQ...
    merchant_id: m_X
    url:        https://m-x.example/webhooks/rrq
    payload:    { ... }
```

The engineer hypothesizes that m_X's endpoint was misconfigured. They check directly:

```
$ curl -X POST https://m-x.example/webhooks/rrq -d '{"test":true}'
HTTP/2 500
```

Yep, still broken. The engineer reaches out to m_X. m_X confirms the issue and fixes their endpoint by Tuesday afternoon. The engineer verifies:

```
$ curl -X POST https://m-x.example/webhooks/rrq -d '{"test":true}'
HTTP/2 200
```

Now they need to replay the backlog. First, check the circuit breaker:

```
$ rrq circuit status --merchant m_X
MERCHANT  STATE   CONSECUTIVE_FAILURES  LAST_CHANGE
m_X       open    5                     Friday 14:23 UTC
```

Reset it:

```
$ rrq circuit reset m_X
Force-close circuit breaker for m_X? [y/N] y
Breaker reset to closed.
Event written: operator.action (rec_01HQ...)
```

Now replay the DLQ entries. The CLI supports batch operations:

```
$ rrq dlq list --source webhook --status open --merchant m_X --json | \
    jq -r '.[].id' | \
    xargs -I {} rrq dlq replay {} --yes
Replayed dlq_01HQA... → new job_id: job_01HRA...
Replayed dlq_01HQB... → new job_id: job_01HRB...
...
```

Each replayed entry triggers a fresh saga, with a new job_id and idempotency key (derived from the DLQ entry id so the replay is itself idempotent). The webhook worker picks them up and delivers to the now-functioning endpoint. By Tuesday evening, the backlog has drained, the metric is flat, and the incident is closed.

The audit trail in the event log records every command run, who ran it, and the parameters. Compliance is satisfied; future operators can read the trail.

### Walk-through 2: a stuck saga

A merchant complains that a transfer they submitted an hour ago is still showing "pending." The engineer checks:

```
$ rrq saga show sg_99
Saga sg_99 (transfer)
  state:        Compensating         ← bad sign at this age
  last_step:    compensation_credit
  started:      13:24:45Z (62m ago)
  deadline:     13:34:45Z (expired 50m ago)
```

The saga is in the middle of compensation and has been for 50 minutes past its deadline. That's well outside normal.

```
$ rrq stream pending stream:jobs --consumer fraud-host-1
ID                CONSUMER       IDLE        DELIVERIES
1700000000-42     fraud-host-1   62m 14s     3
```

The message has been claimed by `fraud-host-1` and idle for an hour. The XAUTOCLAIM should have reclaimed it after 60 seconds. Why didn't it?

```
$ kubectl get pods -l app=saga-worker
NAME             STATUS    AGE
saga-worker-0    Running   62m
saga-worker-1    Running   62m
```

Both saga workers are running but not making progress on this message. The engineer checks the worker logs:

```
$ kubectl logs saga-worker-0 | grep sg_99
2026-05-12 13:34:45 WARN saga sg_99 compensation step 'compensation_credit' failed: pgxpool: timeout
2026-05-12 13:35:45 WARN saga sg_99 retrying (attempt 2/3)
2026-05-12 13:36:45 WARN saga sg_99 retrying (attempt 3/3)
2026-05-12 13:37:45 ERROR saga sg_99 compensation exhausted retries
```

The saga is genuinely stuck — compensation has been failing on a database timeout. The database has been recovered, but the saga is now in DLQ-equivalent state.

```
$ rrq saga abort sg_99 --reason "compensation stuck on db timeout; debit reversed manually via SQL adjustment"
Abort saga sg_99? This will mark it Failed without running compensation. [y/N] y
Saga sg_99 marked as Failed.
DLQ entry created: dlq_01HQX...
Event written: operator.action (rec_01HQX...)
```

The engineer also manually inserts an adjustment ledger entry that restores the source wallet (since the compensation that should have done this is now stuck). The adjustment writes its own event for audit. The system's state is consistent again, the merchant is notified that their transfer failed (via the webhook now triggering on the Failed state), and the engineer files a ticket to investigate why compensation hit a database timeout in the first place.

### Walk-through 3: routine reconciliation review

It's Wednesday morning. The engineer reviews the previous night's reconciliation alerts:

```
$ rrq reconcile alerts --last 24h
RUN_ID       WALLETS  DISCREPANCIES  TIME
rec_01HQX... 1247     0              2026-05-13T01:00:00Z (32.4s)
```

No alerts. The system is consistent. The engineer moves on with their day.

---

## Code skeleton (Go reference)

The Go version uses Cobra for command parsing:

```go
// Package admin implements the rrq CLI.
//
// Commands follow the pattern: subcommand → action → flags.
// Each action calls a corresponding function in the same package
// or in a sibling internal package.

func main() {
    var rootCmd = &cobra.Command{
        Use:   "rrq",
        Short: "RRQ operator CLI",
    }
    
    rootCmd.AddCommand(
        dlqCmd(),
        sagaCmd(),
        streamCmd(),
        circuitCmd(),
        walletCmd(),
        eventsCmd(),
        reconcileCmd(),
    )
    
    if err := rootCmd.Execute(); err != nil {
        os.Exit(1)
    }
}

func dlqCmd() *cobra.Command {
    cmd := &cobra.Command{Use: "dlq", Short: "Dead letter queue operations"}
    cmd.AddCommand(
        dlqListCmd(),
        dlqShowCmd(),
        dlqReplayCmd(),
        dlqResolveCmd(),
    )
    return cmd
}

func dlqReplayCmd() *cobra.Command {
    var assumeYes bool
    cmd := &cobra.Command{
        Use:   "replay <entry_id>",
        Short: "Re-enqueue a DLQ entry",
        Args:  cobra.ExactArgs(1),
        RunE: func(cmd *cobra.Command, args []string) error {
            entryID := args[0]
            
            ctx := context.Background()
            client := mustConnect(ctx)
            defer client.Close()
            
            // Fetch the entry, show summary.
            entry, err := client.DLQ.Get(ctx, entryID)
            if err != nil {
                return err
            }
            if entry.Status != "open" {
                return fmt.Errorf("entry status is %q; only 'open' entries can be replayed", entry.Status)
            }
            
            printEntry(entry)
            
            if !assumeYes && !confirm("Replay this entry?") {
                return errors.New("aborted")
            }
            
            newJobID, err := client.DLQ.Replay(ctx, entryID, getOperatorID())
            if err != nil {
                return err
            }
            
            fmt.Printf("Replayed %s → new job_id: %s\n", entryID, newJobID)
            return nil
        },
    }
    cmd.Flags().BoolVar(&assumeYes, "yes", false, "skip confirmation prompt")
    return cmd
}

// AdminClient is the high-level API the CLI uses. It hides the database
// and Redis details; the CLI just calls .DLQ.Replay() etc.
type AdminClient struct {
    DB    *pgxpool.Pool
    Redis *redis.Client
    DLQ   *DLQOps
    Saga  *SagaOps
    // ...
}

type DLQOps struct {
    db *pgxpool.Pool
}

func (d *DLQOps) Replay(ctx context.Context, entryID string, operatorID string) (string, error) {
    tx, err := d.db.Begin(ctx)
    if err != nil {
        return "", err
    }
    defer tx.Rollback(ctx)
    
    // Fetch entry under FOR UPDATE to prevent concurrent replays.
    var entry DLQEntry
    err = tx.QueryRow(ctx, `
        SELECT id, source, original_payload, status FROM dlq_entries
        WHERE id = $1 FOR UPDATE
    `, entryID).Scan(/* ... */)
    if err != nil {
        return "", err
    }
    if entry.Status != "open" {
        return "", fmt.Errorf("already %s", entry.Status)
    }
    
    // Generate new job_id with idempotency derived from DLQ entry id.
    newJobID := ulid.New()
    newIdempotencyKey := "dlq-replay-" + entryID
    
    // Re-emit to the appropriate stream.
    if entry.Source == "webhook" {
        // The original payload contains everything we need to re-emit.
        if err := d.reEmitWebhook(ctx, &entry, newJobID); err != nil {
            return "", err
        }
    } else if entry.Source == "saga" {
        if err := d.reEmitJob(ctx, &entry, newJobID, newIdempotencyKey); err != nil {
            return "", err
        }
    }
    
    // Mark DLQ entry as replayed.
    _, err = tx.Exec(ctx, `
        UPDATE dlq_entries
        SET status = 'replayed', replayed_at = NOW(), replayed_job_id = $2
        WHERE id = $1
    `, entryID, newJobID)
    if err != nil {
        return "", err
    }
    
    // Write audit event.
    _, err = tx.Exec(ctx, `
        INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, payload, occurred_at)
        VALUES ($1, 'operator.action', 'dlq_entry', $2, $3, NOW())
    `, ulid.New(), entryID, mustJSON(map[string]any{
        "operator_id": operatorID,
        "command":     "dlq replay",
        "args":        map[string]any{"entry_id": entryID},
        "outcome":     "success",
        "new_job_id":  newJobID,
    }))
    if err != nil {
        return "", err
    }
    
    return newJobID, tx.Commit(ctx)
}
```

The shape: each command parses flags, validates input, calls a function on the `AdminClient`, prints the result. The `AdminClient` functions are themselves transactional and write audit events.

---

## Code skeleton (Rust reference)

The Rust version uses `clap`:

```rust
//! RRQ operator CLI.

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "rrq")]
#[command(about = "RRQ operator CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Dlq(DlqArgs),
    Saga(SagaArgs),
    Stream(StreamArgs),
    Circuit(CircuitArgs),
    Wallet(WalletArgs),
    Events(EventsArgs),
    Reconcile(ReconcileArgs),
}

#[derive(clap::Args)]
struct DlqArgs {
    #[command(subcommand)]
    action: DlqAction,
}

#[derive(Subcommand)]
enum DlqAction {
    List { #[arg(long)] source: Option<String>, #[arg(long, default_value = "open")] status: String, #[arg(long, default_value = "20")] limit: u32 },
    Show { entry_id: String },
    Replay { entry_id: String, #[arg(long)] yes: bool },
    Resolve { entry_id: String, #[arg(long)] note: String, #[arg(long)] yes: bool },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let client = AdminClient::connect().await?;
    
    match cli.command {
        Commands::Dlq(args) => handle_dlq(&client, args).await,
        Commands::Saga(args) => handle_saga(&client, args).await,
        // ...
    }
}

async fn handle_dlq(client: &AdminClient, args: DlqArgs) -> Result<()> {
    match args.action {
        DlqAction::List { source, status, limit } => {
            let entries = client.dlq.list(source.as_deref(), &status, limit).await?;
            print_dlq_table(&entries);
            Ok(())
        }
        DlqAction::Replay { entry_id, yes } => {
            let entry = client.dlq.get(&entry_id).await?;
            print_entry(&entry);
            
            if !yes && !confirm("Replay this entry?")? {
                return Err(anyhow!("aborted"));
            }
            
            let new_job_id = client.dlq.replay(&entry_id, &get_operator_id()).await?;
            println!("Replayed {entry_id} → new job_id: {new_job_id}");
            Ok(())
        }
        // ...
    }
}
```

Rust's clap with the derive feature handles a lot of the boilerplate (help text, parsing, validation). The structure is otherwise parallel to the Go version.

---

## Test plan

The CLI has both unit tests (parsing, validation) and integration tests (full operations against real Postgres).

### Validates command structure

- **`TestCLI_HelpText`** — `rrq --help` and `rrq <subcommand> --help` produce sensible output.
- **`TestCLI_InvalidArgs`** — missing required arguments produce usage error and non-zero exit.

### Validates DLQ operations

- **`TestDLQReplay_HappyPath`** — seed a DLQ entry; replay; assert entry status is "replayed", new job appears in stream, audit event written.
- **`TestDLQReplay_AlreadyReplayed`** — replay an already-replayed entry; assert error, no second job created.
- **`TestDLQReplay_RaceProtected`** — two concurrent replay attempts on same entry; assert exactly one succeeds (the FOR UPDATE wins).

### Validates audit events

- **`TestAudit_EveryMutationWritesEvent`** — for each mutating command, run it; assert exactly one `operator.action` event was written with the right operator_id and parameters.
- **`TestAudit_ReadOnlyCommandsDontWriteEvents`** — for read-only commands (list, show), assert no audit event is written.

### Validates idempotency

- **`TestFreeze_AlreadyFrozen`** — freeze an already-frozen wallet; assert success (no-op) with informative message, no duplicate freeze event.

---

## FAQ — the questions interviewers actually ask

**Q: Why a CLI and not a web admin dashboard?**

Three reasons. First, simplicity: a CLI doesn't need an auth system, a frontend framework, a deployment surface, or its own observability. It runs on the operator's laptop. Second, scriptability: real ops work involves piping commands together (`rrq dlq list ... | jq ... | xargs rrq dlq replay`), which a dashboard doesn't support naturally. Third, audit-friendliness: a CLI command is one line in bash history; a dashboard action is harder to log and reproduce. For an early-stage system, the CLI is enough. A v2 could add a dashboard on top, reading the same data.

**Q: How do operators get the credentials to run this?**

In production: the CLI reads connection strings from environment variables that are populated by a credentials broker (Vault, AWS Secrets Manager, etc.) accessed via a bastion host. The operator SSHs to the bastion, the bastion injects credentials, the CLI runs. v1 documents this pattern but doesn't implement the broker — the credentials are read from a local config file or env vars. v2 would harden this.

**Q: What if an operator runs a dangerous command by accident?**

Two defenses. First, mutating commands require either `--yes` or interactive confirmation; you can't fat-finger `rrq saga abort` without acknowledging the prompt. Second, every action writes an audit event, so even if a mistake happens, it's traceable and reversible (most mutations have an inverse: freeze → unfreeze, replay → resolve, etc.). The system trusts operators not to be malicious; it defends against being careless.

**Q: Why does `saga abort` exist? Isn't that bypassing the saga's correctness guarantees?**

Yes, deliberately. It's the escape valve for situations where the saga's normal failure handling can't make progress — like a compensation step that's permanently broken because of an environmental issue. The operator force-marks the saga `Failed`, *and is responsible for cleaning up the ledger manually*. The CLI documents this in the abort command's help text: "This does not run compensation. You are responsible for ensuring ledger consistency. Recommended: insert a manual adjustment event with full reasoning."

It exists because the alternative — a saga stuck forever with no way out — is worse. Real systems have to have escape valves. The escape valve is logged and audit-trailed; that's what makes it safe to have one.

**Q: How do you know who an operator is? The `OPERATOR_ID` env var seems trust-based.**

It is, in v1. The CLI reads `OPERATOR_ID` from the environment and trusts it. Production would have a real identity system: SSO-based, with mTLS certificates for the CLI authenticating to the database. v1 documents this gap; the audit events capture whatever `OPERATOR_ID` was set, which in practice is set by the bastion host based on the SSO identity used to access it. So even in v1, the trust isn't unbounded — the bastion is the trust anchor.

**Q: Can the CLI be used remotely or only locally?**

Locally. The CLI connects directly to Postgres and Redis. To run it, you need network access to those services. In production, that means VPN + bastion. There's no "RRQ admin API" exposed publicly; the database is the API. This is intentional — the surface area is smaller, the audit trail is more direct, and there's no separate admin-API service to maintain.

**Q: What's the most-used command in practice?**

`rrq dlq list` and `rrq stream lag`, by a wide margin. The first is the "what needs human attention" view; the second is the "is the system healthy" view. The other commands are used during incidents, which are rare. The CLI's design optimizes for the read-heavy operational queries.

---

## What this service depends on

- **Postgres** — reads everything; writes audit events.
- **Redis** — reads streams, breaker state, lag.

## What depends on this service

- Operators (humans).
- Optionally: shell scripts wrapping common operational sequences.

---

## Where to read next

- The DLQ design (which the CLI exists to operate) → [`../deep-dives/24-RESILIENCE.md`](../deep-dives/24-RESILIENCE.md)
- The event store (where audit events land) → [`../deep-dives/25-EVENT-STORE.md`](../deep-dives/25-EVENT-STORE.md)

---

*Pass 2 of the architecture series. Last updated pre-implementation.*
