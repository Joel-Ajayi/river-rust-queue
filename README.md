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

## Tech Stack

- **Languages:** Go 1.26+ (`pgx/v5`, `kafka-go`, OpenTelemetry), Rust.
- **Contracts:** Protocol Buffers (Protobuf).
- **Databases:** PostgreSQL 17 (Sharded via CloudNativePG operator with 3-instance HA).
- **Messaging:** Kafka (Managed by Strimzi, driving Transactional Outbox pattern).
- **Caching & Rate Limiting:** Redis.
- **Edge Proxy:** Kong Gateway & Envoy Gateway.
- **GitOps & Deployment:** Argo CD, Kustomize, KEDA, Skaffold.

---

## Local Development Quickstart

### Prerequisites

Ensure you have the following installed:
- **Docker**
- **Kubernetes Cluster** (`kind` or `minikube`)
- **Skaffold** & **Go 1.26+**

> **Note:** Clone the GitOps sibling repository (`rrq-gitops`) to `../rrq-gitops` prior to running local development loops:
> ```bash
> git clone https://github.com/Joel-Ajayi/rrq-gitops.git ../rrq-gitops
> ```

### 1. Bootstrap Platform Infrastructure

```bash
cd ../rrq-gitops
make bootstrap-dev
```

### 2. Launch Local Application Loop

Return to this repository and launch live hot-reloading:

```bash
cd ../river-rust-queue
make dev
```

### 3. Run Automated Tests

Execute the full unit and integration test suite:

```bash
cd services/go-services
go test ./...
```

---

## License

[MIT](LICENSE).
