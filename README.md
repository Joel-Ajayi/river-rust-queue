# Yotstack-Queue: Distributed Fault-Tolerant Job Engine

A high-performance, asynchronous job processing system built in **Rust**, designed for reliability and horizontal scalability within a **Kubernetes** ecosystem.

## 🚀 Overview

Yotstack-Queue is a distributed task orchestration engine that decouples heavy computational workloads from user-facing APIs. Built with a "Reliability-First" approach, it ensures that no task is lost due to network partitions, worker crashes, or infrastructure restarts.

## 🏗️ Architecture & Systems Design

The system implements a **Producer-Consumer** pattern using **Redis Streams** as the durable message broker.

### Core Components

- **Producer API (Rust/Axum):** A high-throughput REST entry point that validates payloads and ingests jobs into the broker.
- **Worker Node (Rust/Tokio):** A multi-threaded consumer that pulls tasks, manages local resource constraints, and handles execution logic (e.g., video transcoding).
- **Durable Broker (Redis):** Configured as a **StatefulSet** with AOF (Append-Only File) persistence to ensure queue data survives hardware failures.
- **Shared Protocol Library:** A type-safe contract crate that enforces schema consistency across the entire distributed system.

## 🛠️ Key Engineering Features

- **Durable Persistence:** Integrated Redis AOF persistence to prevent data loss during broker restarts.
- **Consumer Groups & Scalability:** Utilizes Redis Consumer Groups to allow multiple worker replicas to process the queue in parallel without double-processing tasks.
- **Fault Tolerance & Retries:** Automatic job recovery mechanisms and dead-letter queue (DLQ) logic for handling intermittent processing failures.
- **Graceful Shutdown:** Workers implement SIGTERM handling to finish active tasks and safely "NACK" (negative-acknowledge) incomplete jobs back to the queue before termination.
- **Backpressure Management:** Bounded buffers and rate-limiting at the API layer to prevent system saturation during traffic spikes.
- **Observability:** Instrumental logging and health checks designed for Prometheus/Grafana monitoring of consumer lag and worker throughput.

## 🧰 Tech Stack

| Layer              | Technology                    |
| :----------------- | :---------------------------- |
| **Language**       | Rust (1.75+)                  |
| **Runtime**        | Tokio (Async I/O)             |
| **Broker**         | Redis 7.0+ (Streams)          |
| **Infrastructure** | Kubernetes (K8s/DigitalOcean) |
| **Deployment**     | Kluctl (GitOps) / Docker      |
| **CI/CD**          | GitHub Actions                |

## 📐 Project Structure

```text
.
├── crates/
│   ├── shared-protocol/  # Shared data models and serialization logic
│   ├── producer-api/     # Job ingestion service
│   └── worker-node/      # Background processing engine
├── k8s/                  # Kubernetes manifests (StatefulSets, Deployments)
├── docker/               # Optimized multi-stage Dockerfiles
└── .github/              # Automated CI/CD pipelines
```
