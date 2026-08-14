# RRQ — River Rust Queue Payment Processing Core

[![Application CI](https://github.com/Joel-Ajayi/river-rust-queue/actions/workflows/app-ci.yml/badge.svg)](https://github.com/Joel-Ajayi/river-rust-queue/actions/workflows/app-ci.yml)
[![Go Version](https://img.shields.io/github/go-mod/go-version/Joel-Ajayi/river-rust-queue?filename=services%2Fgo-services%2Fgo.mod)](https://github.com/Joel-Ajayi/river-rust-queue/tree/main/services/go-services)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

A high-performance, fault-tolerant payment processing engine built using **Go** and **Rust**.

RRQ is designed as a **closed-loop ledger core**: money moves exclusively between wallets inside the system. By scoping transfers to a closed loop, intra-shard transfers execute in **one serializable PostgreSQL transaction**, eliminating distributed sagas and locks for common payments while utilizing a 2-phase clearing account saga for cross-shard transfers.

---

## Documentation Quick Links

- [System Architecture Specification](docs/ARCHITECTURE.md) — Comprehensive component-by-component architectural reference.
- [System Correctness Invariants (I1–I9)](docs/INVARIANTS.md) — Formal guarantees enforced in code and database constraints.
- [Operational Runbooks & Incident Response](docs/RUNBOOKS.md) — Step-by-step procedures for on-call SREs and administrators.
- [GitOps & Infrastructure Repository](https://github.com/Joel-Ajayi/rrq-gitops) — Declarative Kubernetes manifests, operators, and capacity engine.

---

## Architecture Overview

```mermaid
graph TD
  merchant["Merchant Client"]
  kong["Kong / Envoy Gateway<br/>(TLS · Edge Auth · Rate Limiting)"]
  gateway["API Gateway<br/>(JWT Claim · Idempotency Anchor)"]

  merchant -->|HTTPS POST /v1/transfers| kong
  kong -->|Validated Ingress| gateway

  subgraph Database Shard Cluster
    shardA[("Postgres Shard A<br/>(SERIALIZABLE Tx)")]
    shardB[("Postgres Shard B<br/>(SERIALIZABLE Tx)")]
  end

  gateway -->|"INSERT jobs + outbox event"| shardA
  gateway --> shardB

  relay["Outbox Relay"]
  relay -->|"Poll outbox events"| shardA
  relay --> shardB

  subgraph Event Messaging Backbone
    jobsTopic{{"Kafka Topic: jobs"}}
    notifyTopic{{"Kafka Topic: notify"}}
  end

  relay -->|job.requested| jobsTopic
  relay -->|transfer.completed| notifyTopic

  subgraph Async Worker Fleet
    ledgerWorker["Ledger Worker<br/>(Double-Entry Postings)"]
    fraudWorker["Fraud Worker<br/>(Velocity Check)"]
    webhookWorker["Webhook Worker<br/>(HMAC Delivery & Breakers)"]
  end

  jobsTopic --> ledgerWorker
  jobsTopic --> fraudWorker
  notifyTopic --> webhookWorker

  ledgerWorker -->|"Post debit/credit legs"| shardA
  ledgerWorker --> shardB
```

### End-to-End Transaction Flow

```mermaid
sequenceDiagram
    autonumber
    participant M as Merchant Client
    participant API as API Gateway
    participant DB as Postgres Shard Primary
    participant RL as Outbox Relay
    participant K as Kafka Backbone
    participant LW as Ledger Worker
    participant WW as Webhook Worker

    M->>API: POST /v1/transfers (Idempotency-Key)
    rect rgb(240, 240, 240)
        note over API,DB: Atomic Local Transaction
        API->>DB: BEGIN SERIALIZABLE
        API->>DB: INSERT INTO jobs ON CONFLICT DO NOTHING
        API->>DB: INSERT INTO events (outbox job.requested)
        API->>DB: COMMIT
    end
    API-->>M: HTTP 202 Accepted (job_id)

    RL->>DB: Poll unpublished outbox events
    RL->>K: Produce job.requested → jobs topic

    LW->>K: Consume job.requested
    rect rgb(240, 240, 240)
        note over LW,DB: Single Double-Entry Transaction
        LW->>DB: BEGIN SERIALIZABLE
        LW->>DB: SELECT wallets FOR UPDATE (ordered)
        LW->>DB: INSERT INTO ledger_entries (debit + credit)
        LW->>DB: UPDATE jobs (status = 'completed')
        LW->>DB: INSERT INTO events (outbox transfer.completed)
        LW->>DB: COMMIT
    end
    LW->>K: Commit message offset

    RL->>K: Produce transfer.completed → notify topic
    WW->>K: Consume notify topic
    WW->>M: POST signed HMAC webhook
    WW->>DB: Record delivery status
```

---

## Deployment & Development

### 1. Production Kubernetes Deployment (GitOps)

RRQ uses a pull-based declarative GitOps pipeline managed by **Argo CD** via the [`rrq-gitops`](https://github.com/Joel-Ajayi/rrq-gitops) repository.

1. **Apply Root App-of-Apps Manifest**:
   Apply the root Argo CD Application to your production Kubernetes cluster:
   ```bash
   kubectl apply -f https://raw.githubusercontent.com/Joel-Ajayi/rrq-gitops/main/apps/root-app.yaml
   ```
2. **Automated Operator & Service Provisioning**:
   Argo CD automatically discovers and reconciles all infrastructure operators (CloudNativePG, Strimzi KRaft Kafka, KEDA, Envoy Gateway, OTel Operator) and RRQ microservices declared in `rrq/overlays/prod/` across sync waves `-2` through `2`.
3. **Continuous Deployment (CD Pipeline)**:
   - Pushing commits to `main` on this repository triggers **App CI** to lint code, run tests, and build Docker images tagged with the Git commit SHA.
   - The `gitops-promote` job automatically updates `rrq/overlays/prod/kustomization.yaml` in `rrq-gitops` with the new commit SHA tags.
   - Argo CD detects the repository change and executes a zero-downtime rolling update on live cluster deployments.

---

### 2. Local Development Quickstart

For local development and testing, run the local cluster stack using Kind and Skaffold:

#### Prerequisites
- **Docker Engine**
- **Kubernetes Cluster** (`kind` or `minikube`)
- **Skaffold** & **Go 1.26+**

#### Step 1: Bootstrap Platform Infrastructure
Clone the GitOps sibling repository to `../rrq-gitops` and provision the local infrastructure:
```bash
git clone https://github.com/Joel-Ajayi/rrq-gitops.git ../rrq-gitops
cd ../rrq-gitops
make cluster-up
make argocd
make bootstrap-dev
```

#### Step 2: Launch Live Hot-Reloading Loop
Return to this repository and launch Skaffold:
```bash
cd ../river-rust-queue
make dev
```

#### Step 3: Run Automated Test Suite
Execute unit and integration tests:
```bash
cd services/go-services
go test ./...
```

---

## Tech Stack

- **Languages:** Go 1.26+ (`pgx/v5`, `kafka-go`, OpenTelemetry), Rust.
- **Contracts:** Protocol Buffers (Protobuf).
- **Databases:** PostgreSQL 17 (Sharded via CloudNativePG operator with 3-instance HA).
- **Messaging:** Kafka (Managed by Strimzi, driving Transactional Outbox pattern).
- **Caching & Rate Limiting:** Redis.
- **Edge Proxy:** Kong Gateway & Envoy Gateway.
- **GitOps & Deployment:** Argo CD, Kustomize, KEDA, Skaffold.

---

## License

[MIT](LICENSE).
