# 15: Admin Dashboard

> **What this is.** The service document for the Admin Dashboard. Replaces what was originally designed as a CLI. The dashboard is the operator's interface to the rest of the system *and* the demo/test surface for the project.
>
> **Reading time.** ~15 minutes.
>
> **Prerequisites.** Skim the other service docs. The dashboard is a window into all of them.

---

## What this does

The Admin Dashboard is a web application that lets a privileged operator inspect and manage the system. It's where DLQ entries get replayed, stuck sagas get investigated, wallets get frozen, merchants get onboarded, and the system gets demonstrated.

It serves two audiences:

1. **Operators (in production).** People running RRQ on behalf of a deployment. They use it for incident response, routine ops, merchant onboarding.
2. **Demo viewers (during evaluation).** Reviewers and interviewers exploring what RRQ does. They use it to drive transfers, observe behavior, see the system in motion.

These overlap heavily. The same actions an operator uses to recover from incidents (replay a DLQ entry) are useful for demo (show what DLQ replay looks like). Same code, same surface, same data, but with authentication gating it from public access.

The dashboard is **not the test suite**. Automated unit and integration tests run via `go test` in CI on every commit (and `cargo test` once the Rust comparison is built). The dashboard is for *manual* exercise of the system, exploratory testing, demos, ops. Both layers exist for different reasons.

---

## What you can do from the dashboard

Functional surface, organized by what an operator/demo viewer wants to do:

### Merchant management
- List all merchants with status and creation date.
- Create new merchant (form with name, webhook URL; returns API key once).
- View merchant details (wallets, recent activity, webhook config).
- Rotate API key.
- Rotate webhook signing secret.
- Suspend / unsuspend merchant.

### Wallet management
- List wallets (filterable by merchant, type, status).
- Create new wallet for a merchant.
- View wallet details: balance, recent ledger entries, status, audit history.
- Freeze / unfreeze wallet (with reason).
- Close wallet (requires zero balance).
- **Seed wallet** (dev/staging only, disabled in production via feature flag).

### Transfer operations
- Submit a transfer (single).
- Submit a bulk payout (multiple recipients in one operation).
- View transfer status, including saga state and current step.
- View transfer history (filterable by merchant, status, time window).

### Saga inspection
- List active sagas.
- List stuck sagas (deadline expired, not yet terminal).
- View saga detail: state machine position, step history, locked resources.
- Force-abort a saga (with reason, requires confirmation).
- Retry compensation on a dead-lettered saga.

### Webhook operations
- View recent webhook deliveries (per merchant).
- View circuit breaker state per merchant.
- Force-reset a breaker.
- Mock merchant endpoint configuration, toggle to return 200/500/timeout (used in demos to show retry/breaker behavior).

### DLQ operations
- List open DLQ entries.
- View entry detail (original payload, error history).
- Replay individual entry.
- Batch replay (select multiple).
- Mark resolved without replay (with note).

### Reconciliation
- View latest reconciliation run summary.
- View discrepancies (if any) with full context.
- Trigger manual reconciliation run.
- View historical run summaries (for trend tracking).

### System health
- Service status (which pods are running, which are unhealthy).
- Stream lag per consumer group.
- Database connections and query stats.
- Recent alerts.

### Demo controls (dev/staging only)
- "Run scenario" buttons that exercise specific behaviors:
  - Submit 100 transfers in parallel.
  - Simulate a worker crash mid-saga.
  - Trigger a webhook failure cascade.
  - Trigger a fraud freeze.
  - Drive a reconciliation discrepancy and observe detection.

The demo controls are the project's strongest interview asset. A reviewer clicks "Trigger fraud freeze," watches 51 transfers complete, sees the wallet freeze, sees the next transfer rejected, that's a memorable thirty seconds of demo.

---

## Architecture

The dashboard is structured as:

```
┌────────────────────────────────┐
│        Web Frontend            │
│  (React/Svelte/whatever you    │
│   choose, single-page app)    │
└──────────────┬─────────────────┘
               │ HTTPS (REST)
               ▼
┌────────────────────────────────┐
│       Admin API Service        │
│  (Go or Rust HTTP server)      │
│  Handles auth, RBAC,           │
│  business logic                │
└──────────────┬─────────────────┘
               │
               ├──► Postgres (reads + audit writes)
               ├──► Redis (reads + breaker reset writes)
               └──► Saga Worker / Webhook Worker
                    (via shared DB; no RPC)
```

The frontend is a single-page app. The backend is a small HTTP server (one of the languages, picking the one you're faster in for the implementation work).

**The backend uses the same database and Redis as the rest of the system**, not a separate one. It's not "calling into" services; it's reading and writing the same shared state. This works because RRQ is event-driven, the saga worker doesn't need to know the dashboard exists; it just reads from streams and writes to the database.

For mutating operations (freeze a wallet, replay a DLQ entry), the dashboard:
1. Validates the operator's permission for this action.
2. Performs the database write (in a transaction).
3. Writes an audit event (`operator.action`) in the same transaction.
4. If the operation needs propagation (e.g., DLQ replay enqueues a new job), writes to the appropriate stream.

This is structurally the same as what the original CLI would have done, same database, same effects, same audit trail. Different interface only.

---

## Authentication and authorization

**Simple auth.** A single operator account (or a small fixed list) with username/password. Authenticated session via JWT cookie. No SSO, no multi-tenant operator accounts.

This is deliberately minimal, and a hardened production deployment would want more: SSO integration (Google OAuth, GitHub OAuth, Okta), role-based permissions (some operators read but don't write; some write only to their assigned merchants), and 2FA for destructive operations. Those are out of scope here. For development, staging, and demos, the simple auth is sufficient. The discipline is to ensure the dashboard is *never* exposed publicly without auth in front of it.

The dashboard's authentication layer is the first line of defense. Beyond that, every mutating action requires confirmation (a prompt or modal asking "are you sure?"), and audit events make every operator action visible after the fact.

---

## Audit trail

Every mutating action writes an `operator.action` event. Same shape regardless of which UI element triggered it:

```json
{
  "event_type": "operator.action",
  "occurred_at": "2026-05-12T14:35:22Z",
  "data": {
    "operator_id": "ops-eng-3",
    "action": "wallet.freeze",
    "target_type": "wallet",
    "target_id": "wal_X",
    "params": { "reason": "manual review - suspicious activity" },
    "outcome": "success",
    "before_state": { "status": "active" },
    "after_state":  { "status": "frozen" }
  }
}
```

These events land in the `events` table like any other. Reconciliation reads them. Audit queries can filter for them. There's no separate "audit log", the system's event log is the audit log.

The audit trail is what makes operator actions reversible-in-principle. Every action has an inverse (freeze ↔ unfreeze, replay ↔ resolve, etc.), and every action is recorded with full context. Mistakes can be undone; their history is visible.

---

## The "test surface" capability

Since all production verification happens via the dashboard, the dashboard needs to be explicit about its testing capabilities.

**What the dashboard can do that automated tests can't:**

- **Manual scenario walking.** A reviewer wants to see what happens when a webhook endpoint fails. They click "Toggle Mock Merchant to 500." They click "Submit Transfer." They watch the dashboard show the retry being scheduled, the breaker opening, the DLQ entry appearing.
- **Long-running observation.** Watch a saga progress through its states over seconds. See locks acquired and released. See events written.
- **Demo-quality narrative.** A storyteller-friendly interface that walks through scenarios rather than just asserting "the test passed."

**What automated tests can do that the dashboard can't:**

- **Regression coverage.** The dashboard is for one operator at one moment; automated tests run on every commit.
- **Speed.** Tests run in seconds. Dashboard interactions are human-paced.
- **Reproducibility.** Tests are deterministic. Dashboard interactions involve a human and may be inconsistent.

Use both. They complement each other; they don't substitute.

### Demo scenarios

The dashboard has a section called "Scenarios" that drives specific demonstrations with one click. Each scenario:
1. Sets up necessary state (creates merchants, wallets, seeds funds).
2. Runs the scenario (submits transfers, simulates failures).
3. Displays the system's response in real time.
4. Tears down state at the end (optional, configurable).

Concrete scenarios to implement:

- **"Happy path transfer"**, single transfer, completes cleanly, webhook delivered.
- **"Failed saga"**, transfer where destination wallet is frozen; saga compensates.
- **"Worker crash recovery"**, kill the saga worker mid-saga; show replacement resumes.
- **"Webhook breaker"**, merchant endpoint returns 500s; show breaker opening and cooling.
- **"Bulk payout"**, 50 sub-transfers, some succeed, some fail.
- **"Fraud freeze"**, 51 transfers in 60 seconds from one wallet; wallet freezes; next transfer rejected.
- **"Reconciliation alert"**, inject a known discrepancy; trigger reconciliation; show alert detection.

These scenarios are the project's elevator pitch in interactive form. A reviewer who clicks through all seven understands what RRQ does in fifteen minutes.

---

## Implementation notes

The dashboard is a separate service from the application services. It's deployed alongside them in the same Kubernetes cluster but as its own Deployment + Service + Ingress.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rrq-dashboard
  namespace: rrq
spec:
  replicas: 1                       # single replica is fine; dashboard is low-traffic
  selector:
    matchLabels:
      app: rrq-dashboard
  template:
    metadata:
      labels:
        app: rrq-dashboard
    spec:
      containers:
      - name: dashboard
        image: rrq/dashboard:<version>
        env:
        - name: POSTGRES_URL
          valueFrom: { secretKeyRef: { name: rrq-db, key: url } }
        - name: REDIS_URL
          valueFrom: { secretKeyRef: { name: rrq-redis, key: url } }
        - name: ALLOW_WALLET_SEEDING
          value: "true"             # dev/staging only; production false
        ports:
        - containerPort: 8080
```

The dashboard's Ingress is behind the cluster's authentication layer, which here means the simple auth described above.

**Technology choice for the frontend.** Pick what you can ship fastest. The point isn't to demonstrate frontend skills; it's to have a working operator UI. Reasonable options:
- **React + Vite + Tailwind**: industry-standard, lots of examples.
- **Svelte + SvelteKit**: smaller, faster to develop in.
- **HTMX + plain HTML**: simplest, no frontend framework required.

For RRQ specifically, HTMX is probably the right answer. Operator dashboards don't need rich client-side interactivity; they need fast page loads and clear data display. HTMX delivers that with minimal JS. But pick what you're fastest in.

**Backend choice.** The dashboard backend can be either Go or Rust. It doesn't have to match the service implementation (Go first, with Rust as a comparison study). The dashboard is one piece; pick one language. Probably whichever has the better web framework story for your needs.

---

## Test plan

Tests for the dashboard itself, separate from the service tests:

- **`TestAuth_LoginFlow`**, submit valid credentials; receive JWT cookie; subsequent requests authenticated.
- **`TestAuth_InvalidCredentials`**, 401.
- **`TestAuth_ExpiredToken`**, 401, with redirect to login.
- **`TestRBAC_OperatorActionsRequireAuth`**, unauthenticated POST to mutating endpoint; 401.
- **`TestAudit_EveryMutationWritesEvent`**, for each mutating action, run it; assert `operator.action` event written with correct context.
- **`TestAudit_ReadActionsDoNotWriteEvents`**, for each read action, run it; assert no audit event written.
- **`TestSeeding_ProdFlagDisabled`**, in prod config, seed endpoint returns 403.
- **`TestSeeding_DevFlagEnabled`**, in dev config, seed endpoint succeeds, audit event written.
- **`TestScenario_HappyPathTransfer`**, invoke happy-path scenario; assert all expected state transitions happened.
- **`TestScenario_FraudFreeze`**, invoke fraud freeze scenario; assert wallet frozen after threshold.

The scenario tests are valuable because they're integration tests in disguise, exercising the system end-to-end through the same actions a demo would.

---

## Where to read next

- The merchant/wallet flows the dashboard drives → [`16-MERCHANT-WALLET-LIFECYCLE.md`](16-MERCHANT-WALLET-LIFECYCLE.md)
- The simulator that drives traffic and triggers the demo scenarios → [`17-SIMULATION-HARNESS.md`](17-SIMULATION-HARNESS.md)
- The funding model the dashboard implements → [`16-MERCHANT-WALLET-LIFECYCLE.md`](16-MERCHANT-WALLET-LIFECYCLE.md) (Funding)
- The operations docs for things the dashboard helps with → [`../deep-dives/28-OPERATIONS.md`](../deep-dives/28-OPERATIONS.md)

---

*Pass 5: replaces the original CLI design with a dashboard. Same capabilities, web UI instead of terminal.*
