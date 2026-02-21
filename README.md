# 🚀 RRQ: River Rust Queue

### _A Parallel-Stack Implementation in Rust & NestJS_

**Domain:** Enterprise Financial Data & Task Orchestration  
**Infrastructure:** DigitalOcean K8s (DOKS), Redis Streams, PostgreSQL  
**Architecture:** Event-Driven Architecture (EDA) with CQRS & Saga Patterns

---

## 📖 Overview

RRQ is a high-performance, fault-tolerant job execution platform designed to handle complex business logic and heavy data processing. The core innovation of this project is the **Parallel Stack Implementation**: solving identical distributed systems challenges in both **Rust (Tokio)** and **NestJS (Node.js)** to compare performance, concurrency models, and developer velocity.

---

## 🏗️ System Architecture

### 1. The High-Level Flow

1.  **Ingress:** Traefik (L7) terminates SSL and routes traffic to the API Gateways.
2.  **Producer:** NestJS/Rust APIs validate requests and emit `JobRequested` events.
3.  **Broker:** Redis Streams acts as the immutable "Truth" for all active jobs.
4.  **Consumer:** Distributed Workers (Rust/NestJS) react to events, processing data or financial ledgers.
5.  **Persistence:** PostgreSQL stores the audited "Event Store" and final job states.

### 2. Parallel Stack Comparison

| Feature           | Rust Implementation           | NestJS Implementation        |
| :---------------- | :---------------------------- | :--------------------------- |
| **Concurrency**   | Multi-threaded (Tokio)        | Single-threaded (Event Loop) |
| **Safety**        | Compile-time (Borrow Checker) | Runtime (TypeScript/DTOs)    |
| **Communication** | gRPC / Protobuf / RESP        | BullMQ / REST / gRPC         |
| **Best For**      | CPU-Intensive / Low Latency   | I/O-Intensive / Rapid Logic  |

---

## 🛠️ Deep-Dive: The 16 "Systems" Pillars

### Phase 1: Architectural Patterns

- **EDA:** Utilizing Redis Streams for decoupled asynchronous event flows.
- **Idempotency:** Unique `request_id` tracking to prevent double-charging/double-processing.
- **CAP Theorem:** Prioritizing **Availability** for job submission and **Consistency** for ledger writes.
- **Saga Pattern:** Implementing **Compensating Transactions** for failed multi-step transfers.
- **CQRS & Event Sourcing:** Separating command logic from dashboard read-models via an immutable event log.

### Phase 2: Communication & Networking

- **Infrastructure:** TLS termination at Traefik with internal mTLS for service-to-service security.
- **L4/L7 Balancing:** Path-based routing for APIs (L7) and direct TCP balancing for Redis/DB (L4).
- **gRPC & Protobuf:** Binary contracts for high-speed internal service synchronization.
- **API Gateway:** Edge-level Rate Limiting, JWT Validation, and Request Shaping.

### Phase 3: Execution & The Engine

- **Queues Deep-Dive:** Implementing **Bounded Queues** for backpressure and **Dead Letter Queues (DLQ)** for poison-pill jobs.
- **Concurrency:** Handling shared state via `Arc/Mutex` in Rust and Worker Threads in NestJS.
- **Distributed Locking:** Using **Redlock** to ensure mutual exclusion across the cluster.
- **Data Sharing:** Event-carried state transfer to minimize "N+1" database queries.

### Phase 4: Resilience & Monitoring

- **Caching:** Implementing **Cache-Aside** for user data and **Write-Through** for job metadata.
- **Circuit Breakers:** Preventing cascade failures during external API (Brevo/Bank) outages.
- **Bulkheading:** Dedicated K8s namespaces and resource quotas for High vs. Low priority tasks.
- **Distributed Tracing:** Full-stack observability using **OpenTelemetry (OTel)** and Jaeger.

---
