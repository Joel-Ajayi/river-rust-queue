# Technical Interview Q&A: Infrastructure & GitOps Architecture

This document contains deep-dive interview questions, operational design rationales, and infrastructure trade-off analyses covering RRQ's GitOps operating model, Argo CD setup, Kubernetes operators, and Gateway API choices.

---

## Q1: Why separate the GitOps infrastructure repository (`rrq-gitops`) from the application repository (`river-rust-queue`)?

### Answer:
Decoupling application code from declarative infrastructure state enforces clear operational boundaries:

1. **Separation of Concerns & Access Control**: Application developers push code changes to `river-rust-queue`. Platform SREs manage cluster manifests, scaling limits, and security policies in `rrq-gitops`. Merge rules and RBAC policies can be enforced independently.
2. **CI/CD Pipeline Decoupling**: App CI runs unit tests, linters, and image builds. GitOps CI validates Kustomize syntax. Operations teams can update HPA thresholds or Prometheus alerts without triggering application image rebuilds.
3. **Clean Audit Log**: `git log` in `rrq-gitops` provides an immutable history strictly reflecting cluster state modifications.

---

## Q2: Why use Argo CD's "App of Apps" pattern instead of manual `kubectl apply` commands in CI?

### Answer:
Manual `kubectl apply` in CI pipelines uses a **push-based deployment model**, which has severe security and reliability drawbacks:

```
[ Push Model (CI -> Cluster) ]
CI Runner holds Admin K8s Credentials --> Applies YAML --> Security Risk & Flaky Network

[ Pull Model (Argo CD inside Cluster) ]
Argo CD Controller watches Git Repo --> Synchronizes Cluster --> Self-Healing & Secure
```

1. **Pull-Based Security**: Argo CD runs inside the Kubernetes cluster. The CI pipeline never requires cluster admin credentials; it only updates image tags in Git.
2. **Declarative Hierarchy**: The root `infrastructure.yaml` Application manages all operator sub-applications (CloudNativePG, Strimzi, KEDA, Envoy Gateway). Adding an operator means committing a manifest, not running manual commands.
3. **Automated Self-Healing**: If an operator manually edits a deployment in the cluster (`kubectl edit`), Argo CD detects drift and overwrites the live state to match Git.

---

## Q3: Why choose CloudNativePG (CNPG) over Patroni or Zalando Postgres Operator?

### Answer:
CloudNativePG was selected for managing production PostgreSQL clusters (`merchants-db`, `shard-a`, `shard-b`) based on strict architectural evaluation:

| Evaluation Metric | CloudNativePG (RRQ Choice) | Patroni + Etcd | Zalando Postgres Operator |
|---|---|---|---|
| **DCS Dependency** | **None** (Uses native K8s CRDs) | Requires external Etcd/Consul cluster | Uses ConfigMaps/Etcd |
| **Backup Integration** | Native Barman-cloud S3 integration in CRD | External pgBackRest sidecar setup | External Spilo sidecar setup |
| **Kubernetes Integration** | Native `Cluster` CRD | Custom Patroni wrapper scripts | Custom `postgresql` CRD |
| **Update Safety** | `supervised` primary switchover strategy | Automated failover | Automated failover |

**Key Rationale**: Patroni requires managing an independent Etcd cluster for Distributed Consensus Store (DCS). If Etcd experiences network partition, Patroni cannot manage failover. CNPG uses Kubernetes API primitives natively, removing Etcd operational overhead.

---

## Q4: Why deploy Kafka with Strimzi in KRaft mode instead of ZooKeeper?

### Answer:
Strimzi manages Kafka brokers using modern **KRaft (Kafka Raft Metadata)** mode (Kafka 3.9+):

1. **Zero ZooKeeper Overhead**: Replaces the ZooKeeper cluster with internal Kafka Raft controller quorum nodes, eliminating ZooKeeper pod management, dual-cluster backup complexity, and synchronization bugs.
2. **Faster Leader Election**: KRaft metadata is stored in-memory across controller nodes. Partition leader re-elections complete in $<100\text{ms}$ during broker restarts, compared to multi-second ZooKeeper metadata syncs.
3. **Simplified Storage & Scaling**: Strimzi `KafkaNodePool` allows combined `controller` and `broker` roles in unified node pools for development and dedicated node pools for production.

---

## Q5: Why Envoy Gateway (Gateway API) over traditional NGINX Ingress Controller?

### Answer:
Envoy Gateway implements the Kubernetes **Gateway API** standard (`gateway.networking.k8s.io`), replacing legacy annotation-heavy Ingress resources:

1. **Role-Oriented CRDs**: Standardizes infrastructure responsibilities (`GatewayClass` for platform team, `Gateway` for cluster ops, `HTTPRoute` for application developers).
2. **Edge JWT Validation**: Envoy Gateway handles JWT signature verification natively at the edge via `SecurityPolicy` CRDs:
   ```yaml
   spec:
     jwt:
       providers:
         - name: rrq-core-api
           issuer: rrq-core-api
           remoteJWKS:
             uri: http://core-api.rrq.svc.cluster.local:8080/.well-known/jwks.json
           claimToHeaders:
             - claim: sub
               header: X-Merchant-Id
   ```
   Invalid requests are rejected in $<1\text{ms}$ at the proxy edge before ever reaching `core-api` pods.
3. **C++ Performance**: Envoy proxy handles high-concurrency connection multiplexing with sub-millisecond latency.

---

## Q6: How do Argo CD Sync Waves prevent service dependency crashes during cluster bootstrapping?

### Answer:
Sync waves enforce strict sequential rollout order during cluster provisioning:

```
Wave -2: Sealed Secrets Controller
   │
   ▼
Wave -1: Operators (CloudNativePG, Strimzi, KEDA, Envoy Gateway)
   │
   ▼
Wave  0: Stateful Clusters (Postgres HA, Kafka Brokers, Redis)
   │
   ▼
Wave  1: DB Schema Migrations & Seeding Jobs
   │
   ▼
Wave  2: Microservice Deployments (core-api, ledger-worker, etc.)
```

**Init Container Safety**: Microservice deployments also incorporate `busybox` init containers that probe backend endpoints via `nc -z` prior to launching main application containers. If Postgres or Kafka is restarting, the init container blocks, preventing pod `CrashLoopBackOff` spirals.
