# Technical Interview Q&A: Observability Architecture & SRE Practice

This document contains deep-dive interview questions, telemetry design rationales, and SRE operational explanations covering RRQ's 3-tier OpenTelemetry collector topology, RED/USE metrics, trace-log correlation, and Prometheus alert rules.

---

## Q1: Why adopt a 3-Tier OpenTelemetry Collector topology instead of a single-agent collector?

### Answer:
RRQ deploys OpenTelemetry across three distinct tiers to balance resource efficiency, data reliability, and metadata enrichment:

```
┌─────────────────────────────────────────────────────────────┐
| TIER 1: Application SDKs (Go / Rust)                       │
| Emits OTLP traces & canonical log events                    │
└──────────────────────────────┬──────────────────────────────┘
                               │ (OTLP gRPC local socket)
                               v
┌─────────────────────────────────────────────────────────────┐
| TIER 2: OTel Agent DaemonSet (1 Pod per K8s Node)          │
| Receives OTLP, collects hostmetrics & container logs        │
└──────────────────────────────┬──────────────────────────────┘
                               │ (OTLP gRPC batched)
                               v
┌─────────────────────────────────────────────────────────────┐
| TIER 3: OTel Gateway Deployment (HA In-Cluster Collector)   │
| k8sattributes enrichment, spanmetrics RED connector, fanout  │
└──────┬───────────────────────┬──────────────────────┬───────┘
       │                       │                      │
       v                       v                      v
┌──────────────┐        ┌──────────────┐       ┌──────────────┐
|  Jaeger v2   |        |Elasticsearch |       |  Prometheus  |
|  (Traces)    |        | (Log Events) |       |  (Metrics)   |
└──────────────┘        └──────────────┘       └──────────────┘
```

1. **Local Node Buffering**: Tier 2 DaemonSet agents buffer telemetry locally per node. If the central OTel Gateway is restarting or experiencing backpressure, node agents queue spans in memory without dropping application telemetry or causing Go SDK memory leaks.
2. **Cluster Metadata Enrichment**: Tier 3 Gateway Deployment executes `k8sattributes` processors, injecting pod labels, container IDs, and namespace names into spans centrally rather than wasting CPU on every node.
3. **Derived RED Metrics**: The OTel Gateway uses the `spanmetrics` connector to automatically derive Rate, Errors, and Duration metrics directly from trace spans, providing zero-code RED metrics for every endpoint.

---

## Q2: How does RED Method differ from USE Method across RRQ's dashboard taxonomy?

### Answer:
SRE observability requires matching telemetry methodologies to resource types:

- **RED Method (Rate, Errors, Duration)**: Applied to **stateless microservices** (`core-api`, `ledger-worker`, `webhook-worker`):
  - **Rate**: Request QPS or Kafka consumption rate.
  - **Errors**: HTTP 5xx rates or failed transaction ratios.
  - **Duration**: P50, P95, and P99 processing latency percentiles.
- **USE Method (Utilization, Saturation, Errors)**: Applied to **stateful middleware & compute infrastructure** (PostgreSQL, Kafka, Redis, K8s Nodes):
  - **Utilization**: CPU core usage %, Postgres buffer cache hit ratio, memory fill ratio.
  - **Saturation**: PostgreSQL connection pool wait queues, Kafka consumer group lag, CPU throttling.
  - **Errors**: Disk write failures, deadlocks, OOMKilled events.

---

## Q3: How is Trace-Log Correlation implemented in Go microservices?

### Answer:
When debugging an incident, an engineer must move seamlessly between logs and distributed traces without guessing timestamps:

1. **Context Propagation**: OpenTelemetry trace context (`trace_id` and `span_id`) is extracted from incoming HTTP headers or Kafka message headers and attached to `context.Context`.
2. **Logger Integration**: Custom `zap` logger core wrappers retrieve `trace_id` and `span_id` from `ctx` on every log call:
   ```json
   {
     "level": "error",
     "ts": "2026-08-14T13:30:00Z",
     "logger": "ledger-worker",
     "msg": "insufficient funds",
     "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
     "span_id": "00f067aa0ba902b7",
     "merchant_id": "m_live_101",
     "job_id": "job_998231"
   }
   ```
3. **UI Drilldown**: In Grafana / Kibana, clicking a log line parses `trace_id` and immediately opens the exact Jaeger trace spanning all microservices.

---

## Q4: What is the 5-Tier Grafana Dashboard Taxonomy in RRQ?

### Answer:
To prevent dashboard sprawl, RRQ organizes Grafana dashboards into a persona-driven 5-tier structure:

1. **Tier 1 (Business & SLOs)**: High-level GTV (Gross Transaction Value), total transfers, Transfer Success Rate (TSR %), and business invariants.
2. **Tier 2 (User Journeys & Flows)**: Asynchronous data flow, cross-shard saga completion rates, DLQ churn, and idempotency conflicts.
3. **Tier 3 (Service Health - RED)**: Single dynamic dashboard (`tier3-service-health`) with `$service_name` dropdown covering throughput, error rate, P99 latency, and circuit breaker states for all microservices.
4. **Tier 4 (Middleware & Data Stores - USE)**: PostgreSQL connection starvation, autovacuum dead tuples, Strimzi Kafka partition lag, and Redis memory fragmentation.
5. **Tier 5 (Compute & Infrastructure - USE)**: Kubernetes node pressure, pod restart loops, PVC storage saturation, and network drop rates.
