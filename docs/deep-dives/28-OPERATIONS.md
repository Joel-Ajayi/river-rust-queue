# 28 — Operations

> **What this is.** The deep dive on running RRQ in production on Kubernetes. Schema migrations on a live system, backup and restore of stateful sets, rolling deploys with graceful shutdown, alerting runbooks, retention policies.
>
> **Reading time.** ~20 minutes.
>
> **Prerequisites.** [`../deferred/32-KUBERNETES.md`](../deferred/32-KUBERNETES.md) (the K8s deployment shape).

---

## Why this document exists

Everything else in the docs is *about the system*: what it does, why it's built that way, what guarantees it provides. This document is *about running it*. The two are different skills.

A system that ships without operational thinking eventually breaks in ways its design didn't anticipate. The classic failure mode: a migration runs at deploy time and locks the table for 20 minutes; the application is unavailable during the lock; alerts fire; everyone stares at dashboards. The system's design was fine; the operations were missing.

This doc is the operations design.

---

## Schema migrations on a live system

The hard part is not "how do I run a migration"; it's "how do I run a migration without taking the system down."

Three categories of migration, in increasing difficulty:

### Category 1: Additive, safe

Adding new tables. Adding new nullable columns. Adding new indexes (with `CONCURRENTLY`).

These are safe to run during normal operation. No exclusive locks. Application code keeps running while the migration applies.

Process:
1. Operator merges the migration to the main branch.
2. CI builds a migration job container image.
3. Operator deploys the new application version *with* a Kubernetes Job that runs the migration before the new application pods start.
4. Once the Job completes, the new pods come up (they depend on the migration's completion via an init container that polls).
5. Old pods drain via rolling update.

No downtime. The new column is unused by old code (it doesn't know about it) and used by new code. Both versions coexist during the rollout window.

### Category 2: Backward-compatible schema change

Renaming a column. Changing a column's type to a compatible one. Adding a NOT NULL constraint to an existing column.

These need careful sequencing. The classic pattern is "expand and contract":

**Expand:**
1. Add the new column alongside the old one (additive, safe).
2. Deploy application code that *writes to both* and *reads from both* (or reads from old, writes to both).
3. Backfill the new column from existing data.
4. Verify backfill is complete.

**Contract:**
1. Deploy application code that *writes to new only* and *reads from new only*.
2. Drop the old column.

This takes multiple deploys but causes no downtime. The window of "both columns active" is when the system is most vulnerable to bugs — you have two sources of truth and they can disagree if backfill has gaps. Tests must cover this.

For RRQ specifically: most schema changes are additive (new event types, new optional payload fields, new indexes). The category 2 path is rarely needed.

### Category 3: Destructive

Dropping a column, dropping a table, changing a column's type in incompatible ways, adding a constraint that existing data violates.

These cannot run on a live system in one step. They require:

1. Application code change to stop using the old structure.
2. Deploy and verify.
3. After observation window (often days), run the destructive migration in a maintenance window.

For RRQ, the destructive case we explicitly want to avoid: **modifying the events table**. The whole architecture depends on it being append-only. A migration that adds a NOT NULL constraint to an existing column on events would block on the rewrite of every row. Don't do this; design new columns to allow NULL for historical rows.

### How migrations actually run on K8s

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: rrq-migrate-v1-23
  namespace: rrq
spec:
  template:
    spec:
      containers:
      - name: migrate
        image: rrq/migrate:v1-23
        command: ["/migrate", "up"]
        env:
        - name: POSTGRES_URL
          valueFrom:
            secretKeyRef:
              name: rrq-db
              key: url
      restartPolicy: OnFailure
      backoffLimit: 3
```

The Job runs once. On success, the new application Deployments are applied. Application pods have an init container that waits for the migration's completion before starting the main container:

```yaml
initContainers:
- name: wait-for-migration
  image: rrq/wait-migrate
  args: ["--version", "v1.23"]
```

The wait-container queries a `migrations` metadata table. When it sees `v1.23` recorded as completed, it exits 0 and the main container starts. If the migration hasn't completed within the init-container's timeout, the pod fails to start; the rollout pauses; alerts fire.

This pattern decouples migration timing from application startup. The migration runs once cluster-wide, not once per pod.

---

## Backup and restore

The event store is the source of truth. Losing it loses everything. Backup strategy is the single most important operational decision in the system.

### What gets backed up

- **Postgres**: continuous WAL archiving + daily full backups. Tools: pgBackRest, wal-g, Barman. For v1 on K8s, wal-g is the simplest.
- **Redis**: AOF persistence to the StatefulSet's PV. Streams and stream consumer state survive Redis restarts. Daily snapshot of the AOF file to cold storage (S3, GCS, OCI Object Storage) for the case where the PV is lost.

### What does NOT get backed up

- Application service pods. They're stateless; on loss, K8s reschedules them.
- Observability data (Jaeger traces, Prometheus metrics). Loss of these is annoying but not catastrophic — historical traces and metrics are operational data, not business data.
- The dashboard. Same reasoning.

### Backup frequencies and retention

- **Postgres WAL**: every commit (continuous streaming).
- **Postgres full backup**: nightly.
- **Postgres retention**: 30 days of full backups, 7 days of WAL.
- **Redis snapshot to cold storage**: daily.
- **Redis retention**: 7 days.

The asymmetry: Postgres retention is longer because Postgres is the source of truth. Redis is more ephemeral (idempotency cache, streams, locks). Losing a day of Redis data is recoverable; losing a day of Postgres data is a serious incident.

### Restore drills

Every operational system needs to test its restore process. The risk otherwise: backups exist, but no one has tried restoring, and when an incident comes, the restore takes 4× longer than expected because of an undocumented step.

The discipline: **quarterly restore drill into a staging cluster**. Take the latest production backup. Restore to staging. Verify reconciliation runs cleanly (no discrepancies). Document the time it took.

For RRQ v1, this drill is a manual checklist in `docs/runbooks/restore-drill.md` (to be created during implementation).

### RTO and RPO targets

- **RPO (Recovery Point Objective)**: how much data are we willing to lose? With continuous WAL archiving, RPO is "the last committed transaction" — effectively zero for committed transactions.
- **RTO (Recovery Time Objective)**: how long does restoration take? Target: 1 hour for full restore from backup. Includes provisioning a new Postgres instance, restoring WAL up to point-in-time, verifying integrity, switching application traffic.

1-hour RTO is realistic for a v1 system on K8s. Larger production systems target much tighter (single-digit minutes). The mechanism: hot standby replicas that can be promoted instantly. v1 doesn't have replicas because cost; v2 would.

---

## Rolling deploys with graceful shutdown

The story sketched in the K8s deferred doc; here's the operational version.

When you deploy a new version of the saga worker:

1. K8s starts new pods with the new image.
2. New pods come up, pass readiness probes, join the consumer group.
3. K8s sends SIGTERM to old pods.
4. Old pods' preStop hook fires (`kill -USR1 1`).
5. Old pods' main process catches SIGUSR1, sets a "stop accepting new work" flag.
6. The consumer loop checks the flag, stops calling `XREADGROUP`, finishes any in-flight saga step, exits.
7. K8s sees the pod exit cleanly within `terminationGracePeriodSeconds` (60s). Pod removed.
8. New pods take over the consumer group's pending messages via `XAUTOCLAIM` after 60s of idle.

Without the preStop + SIGUSR1 dance, K8s would SIGKILL pods mid-step. The saga would be left unacked; another worker would reclaim it after the XAUTOCLAIM threshold. Correct, but disruptive — every deploy would trigger a wave of XAUTOCLAIM events and saga resumes. The graceful shutdown avoids the wave.

### Maximum surge and unavailable settings

```yaml
strategy:
  type: RollingUpdate
  rollingUpdate:
    maxSurge: 1        # one extra pod can exist briefly during rollout
    maxUnavailable: 0  # no pods can be unavailable
```

For RRQ's saga worker with 2 replicas, this means: 3 pods exist briefly (new + old + old), then 2 (new + old), then 2 (new + new). At no point do we have fewer than 2 consumers.

For the gateway with 3 replicas, similar pattern.

### Database migration ordering with rolling deploys

This is the subtle one. If the migration changes the schema in an incompatible way, you cannot have old and new pods running simultaneously against the new schema.

The discipline for RRQ: **schema changes are always backward-compatible**. New columns are nullable; new tables exist independently; old behaviors don't break. If a change genuinely cannot be made backward-compatible, use the expand-and-contract pattern described above.

This discipline is what makes rolling deploys safe. Violating it means scheduling a maintenance window, which is a v1-acceptable but v2-undesirable pattern.

---

## Alerting and runbooks

Alerts exist to wake humans up. Every alert needs a corresponding runbook telling the human what to do.

### Alert: `reconciliation_discrepancies_total > 0`

**What it means.** The nightly reconciliation found a discrepancy between the ledger and the event log. Some wallet's derived balance disagrees with its materialized balance.

**Severity.** High. This is the system's "data integrity is broken" signal.

**Runbook steps:**
1. Open the dashboard, navigate to Reconciliation → Latest Run.
2. Identify the affected wallet(s) and the deltas.
3. Pull the wallet's event history from the dashboard's Wallet Audit view.
4. Pull the wallet's ledger entries from the same view.
5. Identify which ledger entries don't have matching events (or vice versa).
6. Determine root cause. Most likely: a recent code change has a bug; recent ledger entries were inserted outside the expected event-and-ledger transaction.
7. Decide on remediation. Options:
    - Revert the offending change. The next reconciliation will still show the discrepancy until manually corrected.
    - Insert a manual adjustment event documenting the correction. Use the dashboard's "Manual Adjustment" action with full reasoning.
8. Document the incident.

### Alert: `webhook_deliveries_dlq_total{merchant_id="..."}` increasing rapidly

**What it means.** Webhooks to a specific merchant are failing in volume.

**Severity.** Medium. Affects one merchant.

**Runbook steps:**
1. Check circuit breaker state for the merchant (Dashboard → Webhooks → Breaker State).
2. If breaker is open, the failure has been sustained.
3. From the DLQ entries, identify the failure mode: 5xx, timeout, connection refused, DNS failure.
4. Test the merchant's endpoint manually (curl from the operator's machine).
5. If endpoint is broken: contact the merchant. Cannot remediate from our side until they fix it.
6. If endpoint is working: the failures may have been transient. Reset the breaker via the dashboard.
7. Replay DLQ entries once the underlying issue is resolved. Use the dashboard's batch DLQ replay action.

### Alert: `rrq_saga_dead_lettered_total` increases

**What it means.** A saga reached the DeadLettered state. Compensation itself failed; the saga is in an inconsistent state.

**Severity.** High. Data is in an unknown state.

**Runbook steps:**
1. From the dashboard's Saga Detail view, get the full event history of the DeadLettered saga.
2. Identify which step failed and what the error was.
3. The wallet(s) affected are visible in the saga's state_data.
4. Inspect the ledger entries for those wallets. Identify what's there that shouldn't be, and vice versa.
5. If the issue is recoverable (e.g., the database had a transient error and is now healthy):
    - Use the dashboard's "Retry Compensation" action.
6. If not recoverable:
    - Insert a manual adjustment event with reasoning.
    - Use the dashboard's "Mark Saga Resolved" action with the resolution note.
7. Document the incident.

### Alert: `rrq_stream_lag{stream="stream:jobs"}` > 1000

**What it means.** The saga workers are falling behind the rate of incoming jobs.

**Severity.** Medium initially; High if sustained.

**Runbook steps:**
1. Check current saga worker replica count (Dashboard → Services → Saga Worker).
2. Check HPA status. Is it scaling up? Has it hit max replicas?
3. If at max replicas, the bottleneck is downstream — Postgres, Redis, or actual saga work.
4. Check Postgres metrics for slow queries.
5. Check if any specific saga is hung (Dashboard → Sagas → Stuck).
6. Remediate by increasing maxReplicas in the HPA spec (requires a deploy) or by addressing the bottleneck.
7. If the lag is from a known burst (e.g., end-of-month settlement batch), wait for it to drain.

### Alert: `kube_pod_status_ready{namespace="rrq"} == 0`

**What it means.** All pods of a service are not-ready.

**Severity.** Critical. Service is down.

**Runbook steps:**
1. Identify the affected service from the alert labels.
2. Check pod events: `kubectl describe pod -n rrq`.
3. Check pod logs: `kubectl logs -n rrq <pod> --tail=200`.
4. Most likely causes: failed init container (migration not complete), config error, dependency unavailable (Postgres down, Redis down).
5. Remediate based on cause.

### Alert: `postgres_up == 0`

**What it means.** Postgres is unreachable.

**Severity.** Critical. The entire system is degraded; new transfers will fail.

**Runbook steps:**
1. Check Postgres pod status.
2. Check the PV is mounted correctly.
3. If the pod is crash-looping, capture logs and roll back any recent Postgres-touching deploy.
4. If the pod is gone and won't reschedule, this is a node-level issue. Check cluster health.
5. If the data is lost (PV destroyed), invoke restore procedure (see Backup and Restore).

---

## Retention policies

How long does data stay around?

| Data | Retention | Rationale |
| --- | --- | --- |
| `events` (the event store) | Forever (v1); archived after 7 years (v2) | Source of truth; required for compliance |
| `ledger_entries` | Forever (v1); archived with events | Audit/regulatory |
| `saga_state` (terminal sagas) | 90 days then archived | Operational history; not source of truth |
| `webhook_deliveries` (delivered) | 30 days | Operational history; events have the canonical record |
| `webhook_deliveries` (DLQ-ed) | Until resolved or explicitly purged | These need human action |
| `dlq_entries` | Until resolved | Same |
| Audit `operator.action` events | Forever | Compliance |
| Jaeger traces | 7 days | Investigation window |
| Prometheus metrics | 30 days | Trend analysis |
| Application logs | 14 days | Debugging window |

Retention policies are enforced by:
- Postgres: scheduled cleanup jobs (a K8s CronJob runs nightly with `DELETE FROM webhook_deliveries WHERE delivered_at < NOW() - INTERVAL '30 days' AND status = 'delivered'`).
- Observability tooling: native retention configs (Prometheus `--storage.tsdb.retention.time=30d`, etc.).

The Postgres cleanup is *not* destructive to source-of-truth data. It only deletes derived/projection rows. The events table is never touched by cleanup.

---

## Capacity planning

How big should the cluster be?

For v1's target (1,000 TPS):

- **Saga workers**: 2-4 replicas, 0.5 CPU + 512MB each.
- **API Gateway**: 2-3 replicas, 0.5 CPU + 256MB each.
- **Webhook worker**: 2 replicas, 0.5 CPU + 512MB each.
- **Reconciliation**: 1 instance during run, 2 CPU + 2GB. Idle otherwise (CronJob).
- **Admin dashboard + API**: 1 replica, 0.5 CPU + 256MB.
- **Postgres**: 2 CPU + 4GB + 100GB PV (grows over time).
- **Redis**: 1 CPU + 2GB + 10GB PV.
- **Observability stack**: 1-2 CPU + 2GB across Jaeger, Prometheus, Grafana.

Cluster total: ~10 CPU, ~14GB RAM, ~120GB storage.

A 3-node cluster of small nodes (4 CPU, 8GB each = 12 CPU, 24GB total) accommodates this with headroom. On DigitalOcean, that's about $36-50/month; on Oracle Always Free, fits in the 4 oCPU + 24GB allocation.

---

## Pre-flight checklist for production deploy

Before deploying RRQ to production for the first time, verify:

- [ ] Postgres has continuous WAL archiving configured.
- [ ] Daily Postgres backups are running.
- [ ] Restore from backup has been tested in staging.
- [ ] All migrations have been tested against a production-shape dataset.
- [ ] All Kubernetes secrets are managed (External Secrets Operator or equivalent).
- [ ] Resource requests and limits are set on every container.
- [ ] Liveness and readiness probes are configured.
- [ ] preStop hooks are in place for saga, webhook, fraud workers.
- [ ] HPAs are configured with appropriate metrics.
- [ ] Network policies restrict pod-to-pod traffic to expected paths.
- [ ] All metrics are exposed and scraped by Prometheus.
- [ ] All alerts have corresponding runbooks (see above).
- [ ] Reconciliation CronJob is scheduled.
- [ ] Wallet seeding is *disabled* via feature flag.
- [ ] At least one operator has dashboard credentials.

Without these in place, you're deploying a system you can't operate. The checklist exists to surface them.

---

## Where to read next

- The K8s deployment design → [`../deferred/32-KUBERNETES.md`](../deferred/32-KUBERNETES.md)
- The observability stack → [`26-OBSERVABILITY.md`](26-OBSERVABILITY.md)
- The admin dashboard that drives many of these operations → [`../services/15-ADMIN-DASHBOARD.md`](../services/15-ADMIN-DASHBOARD.md)

---

*Pass 5 addition. The "running this thing" doc.*
