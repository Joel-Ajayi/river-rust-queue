# 26: Observability

> **What this is.** The deep dive on observability in RRQ: distributed tracing, metrics, structured logs, and what "observable" actually means. Less algorithmically deep than other deep-dives, but operationally essential.
>
> **Reading time.** ~18 minutes.
>
> **Prerequisites.** None specific. Reading the service docs first helps because they reference the metrics and traces this doc designs.

---

## What observability is and isn't

Observability is the property that lets you answer questions about your system's behavior from outside. Not "is it up" (that's monitoring); not "what does the code do" (that's documentation); but "what *did* it do, right now, for this specific request?"

The test of observability is concrete: **given a merchant complaint, "my transfer at 14:23 failed and I don't know why," can the operator answer within 5 minutes using only what the system emits?** If yes, the system is observable. If the operator has to SSH into a box and grep log files, it isn't.

The bar matters because the cost of inadequate observability is paid during incidents, when stakes are high and time is short. Building observability after the fact is much harder than building it from the start, you have to retrofit every service, and you discover you didn't capture the data that would have explained the incident.

Observability has three pillars: **traces, metrics, logs**. Each addresses a different question.

- **Traces** answer "what happened to this specific request?" A trace is a record of one request as it flowed through the system, including timing and outcomes at each step.
- **Metrics** answer "what's the system doing in aggregate?" Metrics are time series, counts and durations sampled over time.
- **Logs** answer "what did the code think it was doing?" Logs are events emitted by application code, structured for machine query.

Each pillar has a role. Traces are best for incident diagnosis; metrics are best for monitoring and alerts; logs are best for forensics. Together, they cover the questions you ask in practice.

---

## Distributed tracing

### What a trace is

When a merchant sends a request, RRQ starts a trace. The trace has a unique ID (`trace_id`). Every operation in the system that processes this request emits a **span**, identified by `span_id`, with a parent pointer to the calling span.

A span represents one unit of work: an HTTP request received, an idempotency check, an XADD to Redis, a saga step, a database query, a webhook delivery. Each span records:

- Start and end times (and therefore duration).
- Attributes (merchant_id, job_id, saga_id, step_name, attempt_count).
- Status (OK or ERROR with an error message).
- Events (point-in-time annotations within the span).

When all the spans for one trace are collected and assembled, you get a complete picture: every operation, in order, with how long each took, what its parent was, and what happened.

```
Trace 0xabc123 (request from merchant m_M)
├── HTTP POST /v1/transfers           [42ms]
│   ├── jwt.verify                    [1ms]
│   ├── idempotency.check             [3ms]
│   │   └── redis.set_nx              [1ms]
│   └── stream.publish                [8ms]
│       └── redis.xadd                [2ms]
├── saga.execute (sg_99)              [380ms]
│   ├── validate                      [12ms]
│   │   └── postgres.select_wallets   [8ms]
│   ├── acquire_lock                  [22ms]
│   │   └── redlock.acquire           [18ms]
│   ├── debit                         [110ms]
│   │   ├── postgres.insert_ledger    [40ms]
│   │   └── postgres.insert_event     [42ms]
│   ├── credit                        [105ms]
│   │   ├── postgres.insert_ledger    [38ms]
│   │   └── postgres.insert_event     [41ms]
│   └── complete                      [85ms]
│       ├── postgres.insert_event     [44ms]
│       └── stream.publish_notify     [25ms]
└── webhook.deliver                   [167ms]
    ├── postgres.lookup_merchant      [12ms]
    ├── hmac.sign                     [1ms]
    └── http.post                     [148ms]
```

The diagram above is what an operator sees in Jaeger or any other trace viewer. At a glance: where the time went, what happened in order, where the errors (if any) occurred.

### Why traces matter for RRQ

Three concrete uses:

**Debugging a specific transfer.** A merchant complains. The operator pulls the trace by `job_id`. The trace shows exactly which step failed, with the error attached. Often the cause is obvious from the trace alone.

**Finding the bottleneck.** Performance is poor for some subset of transfers. Group traces by status, by merchant, by saga type. Compare durations. The slow group's trace shows which step is consuming the time. No guessing.

**Understanding async causality.** RRQ is event-driven; a single API request triggers a chain of async work (saga, webhook). Without tracing, the relationship between the request and the eventual webhook would be invisible, they'd be separate log lines with no thread to connect them. With tracing, the trace_id propagates through the system, tying them together.

This last property is what makes distributed tracing distinctive. Logs alone could capture the same information per-service, but assembling the cross-service story requires a shared identifier. The trace_id is that identifier.

### How tracing is wired

RRQ uses OpenTelemetry. The choice: an open standard with broad ecosystem support; the alternative (vendor-specific tracing) would lock us in.

Components:

- **Application code** uses OTel SDK to start spans, set attributes, record events. Every service does this.
- **OTel Collector** receives spans from applications, batches them, forwards to backends. Runs as a sidecar or standalone process; configured via YAML.
- **Jaeger** (or Tempo, or Honeycomb, or Datadog APM, or any OTel-compatible backend) stores traces and provides the query UI.

For local development, the all-in-one Jaeger runs in the `kind` cluster (deployed by the dev overlay). Production runs a more scalable backend in the DOKS cluster.

### Propagating trace context

A trace_id starts somewhere, for RRQ, at the API gateway when a merchant request arrives. From there, it has to propagate through the system: across HTTP calls (the gateway → saga via Redis), across async boundaries (event passed through a stream), across database round-trips.

OpenTelemetry standardizes this propagation:

- **HTTP**: the trace context is carried in headers (`traceparent`, `tracestate`). The gateway accepts an incoming trace context if the merchant sent one (rarely; most merchants don't); otherwise it starts a new trace.
- **Redis Streams**: we include `traceparent` as a field in the message. The saga worker, when consuming, extracts the field and continues the trace.
- **Database**: each query is a span with the saga's trace_id as parent. No special propagation needed; the application code threads the context.

The end result: a trace that starts at HTTP and follows the work through Redis, the saga worker, Postgres, back to Redis, and finally to the merchant's webhook endpoint. The merchant's endpoint, if it understands OTel, could continue the trace into their system. (None of them do, but the trace is there if they want.)

### What attributes to put on spans

Span attributes are searchable. The Jaeger UI lets you filter for "all spans with merchant_id = m_X and status = ERROR." For this to be useful, the attributes have to be present and consistent.

RRQ's convention:

- **`merchant_id`** on every span involving merchant context.
- **`job_id`** on every span downstream of a merchant request.
- **`saga_id`** on every span within a saga.
- **`wallet_id`** on every span affecting a wallet.
- **`step_name`** on saga step spans.
- **`attempt`** on retry-aware spans (webhook deliveries, saga step retries).
- **`error.type`** and **`error.message`** on spans with status=ERROR.

Standard OTel attributes (`http.method`, `http.status_code`, `db.statement`, etc.) are also included automatically by the SDK's instrumentation.

The discipline is: every attribute that an operator might filter or group by is set. The cost is small (a few bytes per span); the value at debugging time is high.

### What not to put on spans

Spans aren't logs. Don't put unbounded data on them:

- **Don't put full request bodies.** A transfer payload might be small; a bulk payout payload might be megabytes. Spans should be small and consistent.
- **Don't put PII.** Even if the payload is small, customer data shouldn't end up in trace storage. The merchant_id is fine; the customer's name isn't.
- **Don't put secrets.** API keys, signing secrets, never in spans.

Spans should be enough to *find* what you need; the actual data is in the database where it belongs.

### Sampling

In production with high throughput, recording every span for every request is expensive. The standard mitigation is sampling: record N% of traces fully, discard the rest.

RRQ records 100% of traces (sample rate = 1.0). Justifiable at our scale (1,000 TPS is manageable storage-wise). For higher scale, the sampling strategy would be:

- **Head-based sampling**: decide at the start of the trace whether to record it. Simple but loses some interesting traces.
- **Tail-based sampling**: record everything in memory, decide at the end whether to keep. Better for "always keep errors" but requires the collector to buffer.

RRQ doesn't optimize this; it's a known path for higher scale.

---

## Metrics

### What metrics are

Metrics are time-series data: a value, a name, and a set of labels, sampled at intervals. Examples:

- `rrq_transfers_total{status="success"}`, counter of successful transfers.
- `rrq_saga_duration_seconds{step="debit"}`, histogram of step durations.
- `rrq_active_sagas`, gauge of currently-running sagas.

Metrics are not for individual requests (that's what traces are for). They're for aggregate behavior, counts, rates, percentiles, over time.

### The four golden signals

Google's SRE book identifies four metrics that define a service's health:

1. **Latency**, how long requests take. Report p50, p95, p99. Never report only the average; averages hide tails.
2. **Traffic**, how many requests per second. The basic load measurement.
3. **Errors**, what fraction of requests fail. Error rate is what alerting fires on.
4. **Saturation**, how full the system is. Different services have different measures: queue depth, consumer lag, CPU utilization, memory pressure.

These four metrics per service give you a dashboard that tells you whether the system is healthy at a glance.

### RRQ's metrics

Per service, the implementation captures (at minimum):

**API Gateway:**
- `rrq_gateway_requests_total{endpoint, status}`, counter
- `rrq_gateway_request_duration_seconds{endpoint}`, histogram
- `rrq_gateway_idempotency_check_duration_seconds`, histogram
- `rrq_gateway_idempotency_hits_total{outcome}`, counter (outcome: new, in_flight, cached)

**Saga Worker:**
- `rrq_saga_total{saga_type, outcome}`, counter (outcome: completed, failed, dead_lettered)
- `rrq_saga_duration_seconds{saga_type, outcome}`, histogram
- `rrq_saga_step_duration_seconds{step}`, histogram
- `rrq_saga_active`, gauge
- `rrq_saga_state_transitions_total{from, to}`, counter (for state machine debugging)

**Webhook Worker:**
- `rrq_webhook_deliveries_total{outcome}`, counter (outcome: delivered, failed_retry, failed_dlq)
- `rrq_webhook_delivery_duration_seconds`, histogram
- `rrq_webhook_retries_total{attempt}`, counter
- `rrq_circuit_breaker_state{merchant_id}`, gauge (encoded as 0=closed, 1=open, 2=half_open)

**Fraud Worker:**
- `rrq_fraud_events_processed_total`, counter
- `rrq_fraud_signals_emitted_total{rule}`, counter
- `rrq_fraud_active_tasks`, gauge
- `rrq_fraud_task_panic_total`, counter

**Reconciliation:**
- `rrq_reconciliation_run_duration_seconds`, histogram (sparse, once per day)
- `rrq_reconciliation_discrepancies_total`, counter, *the most important business-level metric*
- `rrq_reconciliation_wallets_checked`, gauge

**Stream consumer lag** (across all consumers, scraped from Redis):
- `rrq_stream_lag{stream, consumer_group}`, gauge

These metrics are exposed by each service on its `/metrics` endpoint (Prometheus format). Prometheus scrapes them every 15 seconds. Grafana renders dashboards.

### Counters vs gauges vs histograms

The three types and when to use each:

**Counter.** Monotonically increasing number. Resets on service restart. Examples: `requests_total`, `errors_total`. Rate-of-change is calculated by Prometheus (`rate()` function). Counters never decrease.

**Gauge.** Arbitrary value, can go up or down. Examples: `active_sagas`, `queue_depth`, `cpu_percent`. Read directly.

**Histogram.** Distribution of values. Records observations into buckets; computes percentiles. Examples: `request_duration_seconds`, `saga_step_duration_seconds`. Used for latency reporting.

Histogram bucket choices matter. RRQ uses Prometheus defaults for HTTP request durations and saga durations: 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10 seconds. These cover the range from sub-millisecond fast paths to multi-second slow paths.

If most of the observations fall outside the bucket range, percentile estimates degrade. We watch for this in benchmarks; the defaults are tuned to RRQ's actual latency distribution.

### Why p99 matters more than average

A common mistake: report average latency. "Our average response time is 50ms." This is meaningless because it averages over fast and slow requests indiscriminately.

The reality is usually more like:
- 95% of requests: 20-40ms
- 4% of requests: 100-200ms
- 1% of requests: 500ms-2s

The average might be 60ms. The user experience for 1% of requests is dramatically worse. That 1% is users encountering GC pauses, network jitter, or saturated downstream services. Hiding that 1% in the average hides the experience that matters.

The standard reporting is p50/p95/p99:
- **p50**: median. Half of requests are faster.
- **p95**: 95% of requests are faster. The "typical bad day" experience.
- **p99**: 99% of requests are faster. The tail. Where GC pauses, network issues, contention show up.

RRQ targets p99 latency under 500ms for transfer requests. Average alone could be 30ms; p99 reveals the bad cases. The benchmark methodology requires p50/p95/p99 reporting; averages are forbidden.

### Alerts on metrics

Metrics drive alerts. The discipline: alert on symptoms, not causes.

Good alerts:
- `rrq_reconciliation_discrepancies_total` > 0. The system found ledger drift; humans need to investigate.
- `rrq_webhook_deliveries_dlq_total` increases by more than N per hour. Many webhooks are terminally failing.
- Error rate (`rate(rrq_gateway_requests_total{status="5xx"}[5m])`) > 1%. Something is broken.
- `rrq_saga_dead_lettered_total` increases. A saga reached unrecoverable state.
- `rrq_stream_lag{...}` > 1000. Consumer is falling behind producer.

Bad alerts:
- CPU > 80%. CPU being high isn't a problem unless it causes user-visible latency. Alert on the latency.
- Memory > 90%. Same logic. The OOM killer is the alert.
- Worker restart count > N. Restarts are normal; the question is whether the system is healthy after them.

The hierarchy: alert on user-visible problems (errors, latency, drift). Investigate causes when alerted. Don't alert on causes preemptively; you'll alert-fatigue your operators.

---

## Structured logs

### What structured logging is

Log lines are emitted as JSON, not free-form strings. Every log line has a stable schema:

```json
{
  "timestamp": "2026-05-12T14:23:01.123Z",
  "level": "INFO",
  "service": "saga-worker",
  "trace_id": "abc123...",
  "span_id": "def456...",
  "merchant_id": "m_M",
  "saga_id": "sg_99",
  "message": "saga step completed",
  "step": "debit",
  "duration_ms": 110
}
```

Properties:
- **Machine-queryable.** Logging aggregators (Loki, Elasticsearch, Datadog Logs) index the JSON fields. Operators can search by trace_id, merchant_id, etc.
- **Joinable to traces.** Every log line carries trace_id/span_id, so you can pivot from a trace to its logs and back.
- **Stable.** Adding a new field is fine. Changing the meaning of an existing field is not. Schema discipline matters.

The alternative, `printf`-style logs like "saga sg_99 completed step debit in 110ms", works for humans reading individual lines but breaks down at scale. You can't grep for "all log lines for merchant m_M" when the merchant_id is embedded inconsistently in the prose. Structured logs solve this by making the fields explicit.

### Log levels with discipline

The standard levels:

- **DEBUG**: detailed information for development. Off in production.
- **INFO**: significant state transitions. Used sparingly.
- **WARN**: handled errors, unusual conditions, retries.
- **ERROR**: unhandled errors. Signals a bug that needs investigation.

RRQ's discipline:

- DEBUG is off in production. Don't log every database query or HTTP call, those are span data, captured by tracing.
- INFO is for transitions worth recording: "saga sg_99 reached state Completed", "consumer reclaimed N stuck messages on startup". Not for "received message", which would flood logs.
- WARN is for handled errors: "webhook delivery failed, scheduling retry", "circuit breaker opened for merchant m_X".
- ERROR is rare and significant: "saga step panicked", "could not connect to Postgres on startup". Every ERROR is a bug to be investigated.

The volume hierarchy: ERROR << WARN < INFO < DEBUG. If ERROR is firing frequently, something is wrong. If DEBUG is on in production, you have a noise problem.

### What NOT to log

Three categories of mistake:

**Logging on every operation.** Don't log "received HTTP request" for every request, that's what request metrics are for. Don't log "started database query", that's what spans are for. Logs are for *unusual* or *significant* events, not for everything.

**Logging PII.** Customer names, account numbers, personal details. Log the merchant_id (an opaque identifier), not the merchant's name. Log the wallet_id, not the customer's email.

**Logging secrets.** API keys, signing secrets, JWT contents. The standard defense: a redacting logger that masks fields matching configured patterns. RRQ uses one in both Go (`zap` with field redaction) and Rust (`tracing-subscriber` with custom layers).

---

## How the three pillars work together

A real incident:

1. **Alert fires.** `rrq_webhook_deliveries_dlq_total{merchant_id="m_X"}` increased by 50 in the last hour.
2. **Operator opens Grafana dashboard.** The graph shows when the increase started, 14:00 UTC. Other panels show error rate for m_X spiked at the same time.
3. **Operator queries Loki for warnings/errors related to m_X.** Sees logs like "webhook delivery failed: HTTP 500" repeating.
4. **Operator picks one failure and follows the trace.** The trace shows: HTTP POST to merchant endpoint took 8 seconds (timeout), returned 500. Status: ERROR.
5. **Operator concludes**: the merchant's endpoint started returning 500s at 14:00. Reaches out to m_X.
6. **Eventually**: m_X confirms their issue, fixes it. Operator uses the Admin Dashboard to reset the circuit breaker and replay DLQ entries.

Each pillar contributed:
- The metric told the operator something was wrong, with merchant scope.
- The trace told them *what* was wrong (timeout, HTTP 500).
- The logs gave structured detail and could be cross-referenced.

A system with only metrics: knows something is wrong, doesn't know what.
A system with only traces: hard to know what to look for; traces are per-request.
A system with only logs: can find the failures but can't measure the rate or scope.

All three together: the incident is diagnosed in minutes.

---

## Designing for observability from the start

A failure mode in many projects: observability is added last. The result: traces don't propagate properly, metrics are scattered and inconsistent, logs are unstructured. Retrofitting is expensive because every code path that emits telemetry has to be touched.

RRQ avoids this by treating observability as part of the implementation requirement from each service's first day. Specifically:

- **Every HTTP handler** wraps in tracing middleware.
- **Every database call** is wrapped to emit a span.
- **Every Redis call** is wrapped to emit a span.
- **Every saga step** emits a span explicitly.
- **Every service initializes structured logging at startup.**
- **Every service exposes `/metrics`.**

These aren't options; they're baseline expectations. A service without them is incomplete.

The good news: the cost is mostly upfront and template-y. Once the patterns are established (one example of each), the rest is mechanical. Shared crates/packages (`v-go/shared/observability`, `v-rust/shared/src/observability.rs`) reduce duplication.

---

## What "observability" looks like at scale

RRQ's scale (1,000 TPS) is small enough that we can do everything: 100% sampling, all metrics, all logs. The collection and storage costs are negligible.

At higher scale, the tradeoffs change:

- **Trace sampling** becomes necessary. Tail-based sampling (keep errors, sample successes) is the usual choice.
- **Metric cardinality** becomes a real concern. Labels like `merchant_id` can produce millions of distinct time series; storage explodes. The mitigation: per-merchant metrics are exposed selectively (only top-N merchants or aggregated bands).
- **Log volume** dwarfs everything else. Structured logs at TB/day are expensive. Sampling and aggregation are essential.

RRQ doesn't optimize for any of these. The patterns scale, but the volumes don't yet need to.

---

## The benchmark scenarios benefit specifically from observability

Notable: the benchmark methodology (in `docs/appendices/43-BENCHMARK-METHODOLOGY.md`) relies heavily on traces and metrics. Comparing Go and Rust isn't just "this one was faster"; it's "Rust's p99 was 2.5ms while Go's was 3.8ms, and the trace shows Go's tail comes from GC pauses during reconciliation, visible as 50ms gaps in span durations." The conclusion is supported by trace data, not assertions.

Without observability, benchmarks produce numbers without explanations. With it, benchmarks produce *insights*. That's the value at the project level.

---

## Implementation notes

A few concrete details on how observability is wired:

**Go stack:** OTel SDK + Prometheus client + Zap logger. Initialization in `v-go/shared/observability/observability.go`. Each service calls `observability.Setup("api-gateway")` at startup, which:
- Configures OTel trace exporter pointing at OTel Collector.
- Registers Prometheus collectors.
- Initializes the global Zap logger with redaction.

**Rust stack:** `tracing` crate + `tracing-opentelemetry` + Prometheus exporter + `tracing-subscriber`. Initialization in `v-rust/shared/src/observability.rs`. Setup is more verbose due to layer composition; the pattern is the same.

**Trace context in Redis Streams:** the message includes a `traceparent` field. The consumer extracts it and uses `Context::extract` to start its span as a child of the producer's span.

**Trace context in Postgres:** OTel automatic instrumentation (`pgx-otel` for Go, `sqlx` with `tracing` for Rust) wraps every database call in a span. The span propagates through the application's context object.

**Local development:** Jaeger and Prometheus run in the `kind` cluster (the dev overlay deploys them, kube-prometheus-stack for metrics). The metric endpoints are scraped automatically. Traces appear in Jaeger within seconds of being emitted.

The setup is well-trodden territory; the value is in *using* it consistently, not in inventing new patterns.

---

## Where to read next

- The benchmark methodology that uses traces and metrics → [`../appendices/43-BENCHMARK-METHODOLOGY.md`](../appendices/43-BENCHMARK-METHODOLOGY.md)
- OpenTelemetry concepts: <https://opentelemetry.io/docs/concepts/>
- Google's SRE book on the four golden signals: <https://sre.google/sre-book/monitoring-distributed-systems/>

---

*Pass 3 of the architecture series. Last updated pre-implementation.*
