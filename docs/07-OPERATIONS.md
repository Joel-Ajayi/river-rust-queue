# 07: Operations

Deployment topology, GitOps delivery, Kubernetes configuration, health probes, graceful shutdown, autoscaling, and operational runbooks.

---

## Kubernetes topology

RRQ runs the same Kustomize manifests in two environments:

- **Development:** a local `kind` cluster with a dev overlay. Every stateless component runs at least 2 replicas (same as production), just with smaller resource requests. Data backends (Postgres, Redis, Kafka) are identical — self-hosted in-cluster — so failover behaviour is exercisable on a laptop.
- **Production:** DigitalOcean Kubernetes (DOKS) with a managed control plane and node pool. The same operators (CloudNativePG, Strimzi, Redis Sentinel) manage the stateful backends. A DigitalOcean LoadBalancer fronts Kong at the edge.

### Stateless services

| Service | Kubernetes resource | Replicas |
|---------|-------------------|----------|
| Kong (edge gateway) | Deployment | 2 |
| API Gateway | Deployment, HPA | 3+ on Kafka lag |
| Outbox Relay | Deployment | 2, leader-elected |
| Ledger Worker | Deployment, HPA | 2+ on Kafka lag |
| Webhook Worker | Deployment, HPA | 2+ on Kafka lag |
| Fraud Worker | Deployment | 2+ |
| Reconciliation | CronJob | leader-elected |
| Admin Dashboard | Deployment | 2 |

### Stateful backends

All self-hosted in-cluster:

- **Postgres** via CloudNativePG operator — one `Cluster` per shard (primary + hot standby, synchronous replication), exposed via `postgres-rw` (writes) and `postgres-ro` (reads) Services.
- **Redis** via a primary + replica StatefulSet with Sentinel failover. Holds only velocity counters and breaker state — not correctness-critical.
- **Kafka** via the Strimzi operator. Topics: `jobs` (consumed by Ledger and Fraud workers) and `notify` (partitioned by `merchant_id`).

---

## GitOps delivery

Production is pull-based. Argo CD runs in the DOKS cluster, watches this repository, and reconciles live state toward `k8s/overlays/prod`. A deploy is a git commit: CI builds and pushes container images to GHCR and bumps the image tag in the overlay; Argo CD notices and syncs. No `kubectl apply` from a laptop into production.

Local development uses Skaffold for fast inner-loop iteration (build, `kind load`, apply `overlays/dev`, re-apply on change).

### Database migrations

Migrations run as an Argo CD PreSync hook — a Kubernetes Job that runs to completion before the new pods roll, so the schema is in place before anything depends on it. Locally the same migration Job runs as part of the `make dev` target.

---

## Health probes

Every Deployment specifies separate liveness and readiness probes:

- **API Gateway** — liveness on `GET /health` (process alive); readiness on `GET /ready` (Postgres and Redis connections healthy, returning 503 if either is down).
- **Workers (Ledger, Webhook, Fraud)** — liveness via an internal heartbeat timestamp (checked < 30s old to detect deadlocked workers); readiness checks Postgres + Kafka connectivity.
- **Reconciliation** — no probes (CronJob runs once and exits).

### Graceful shutdown

On SIGTERM, a `preStop` hook signals the Kafka consumer to stop claiming new messages before the termination grace period counts down. In-flight work finishes normally (or rolls back if interrupted — Postgres rolls back uncommitted transactions, Kafka redelivers unacked messages, and the handler is idempotent via `UNIQUE(transfer_id, leg)`).

The rolling update strategy uses `maxSurge: 1, maxUnavailable: 0` — with 2+ replicas, there are never fewer than 2 live consumers during a deploy.

---

## Autoscaling

Workers scale on Kafka consumer-group lag via the Horizontal Pod Autoscaler. Minimum replicas is 2 (redundancy); maximum is 10 (beyond which the bottleneck is the Postgres primary, not worker count). The lag metric is exposed via the Prometheus Adapter.

---

## Secrets management

Secrets are committed as SealedSecrets: encrypt a plain Kubernetes `Secret` with the cluster's public key via `kubeseal`, commit the encrypted `SealedSecret`, and the in-cluster controller decrypts it at apply time. Argo CD only ever sees encrypted objects. Each environment seals against its own cluster key.

---

## Sync between Postgres and Kong

When a merchant is created or updated, the Admin Dashboard (or a background operator) creates Kubernetes Custom Resources that the Kong Ingress Controller watches:

- **`KongConsumer`** — represents the merchant.
- **`KongPlugin`** — per-merchant rate-limiting configuration.
- **`KongCredential`** — the JWT public key for RS256 signature validation.

The Kong Ingress Controller syncs these into Kong's data plane automatically, keeping the edge gateway in sync with the Postgres source of truth without manual Admin API calls.

---

## Reconciliation

The nightly reconciliation job (CronJob) re-derives every wallet's balance from the `ledger_entries` table and cross-checks against the `wallet_balance_cache`. Any discrepancy produces an alert and a `reconciliation.discrepancy` event. The job is leader-elected (advisory lock), idempotent, and re-runnable.

---

## Key implementation locations

| Area | File/Directory |
|------|---------------|
| Kustomize base manifests | `k8s/base/` |
| Dev overlay | `k8s/overlays/dev/` |
| Prod overlay | `k8s/overlays/prod/` |
| CI/CD workflows | `.github/workflows/` |
| Database migration Jobs | `services/go-services/internal/platform/migrations/` |
| Dashboard ConfigMaps | `rrq-gitops/rrq/base/observability/dashboards/` |
| Alert rules | `rrq-gitops/rrq/base/observability/prometheusrule.yaml` |
| Edge configuration (Kong) | `rrq-gitops/rrq/base/edge/kong/` |
