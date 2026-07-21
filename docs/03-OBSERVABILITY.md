# 03: Observability

RRQ's observability strategy combines distributed tracing, structured logging, business and infrastructure metrics, and tiered dashboards to support both real-time operations and post-mortem analysis.

---

## Traces

All Go services emit OpenTelemetry traces via a hybrid model:

- **eBPF auto-instrumentation** captures HTTP and database boundaries at the kernel level, deployed as a Kubernetes `Instrumentation` custom resource in the `rrq` namespace.
- **Manual SDK bridges** propagate trace context across async Kafka boundaries (producer span in the outbox relay → consumer span in the ledger, fraud, or webhook worker), implemented via the shared `internal/platform/otel.go` utilities.

Key trace points:
- Inbound HTTP requests at the API Gateway
- Kafka message produce/consume pairs across the outbox relay and all workers
- Database transactions in each adapter

Every span carries `service_name`, `merchant_id`, `job_id`, `transfer_id`, and `wallet_id` attributes for correlation. See `internal/platform/otel.go` and the per-service `observability/` decorator packages.

---

## Logs

Logging uses structured JSON via `go.uber.org/zap` with a rigid schema. The `internal/platform/logger.go` package provides a `NewLogger` function that initializes a JSON encoder configured for the service.

### Canonical log events

At the end of every significant transaction lifecycle (transfer posted, saga completed, webhook delivered, velocity check failed), the relevant service emits a single JSON log line via `LogCanonicalEvent`. Every line includes `trace_id`, `span_id`, `service_name`, `merchant_id`, `job_id`, and `wallet_id` so a single Kibana query reconstructs the full context.

The event name and status fields follow a controlled vocabulary defined in `internal/platform/logger.go`:
- Event names: `transfer.completed`, `transfer.failed`, `saga.initiated`, `saga.completed`, `saga.compensated`, `webhook.delivered`, `webhook.failed`, `velocity.check`
- Statuses: `success`, `failed`, `retry`, `pending`
- Error codes: `saga_failed`, `velocity_limit_exceeded`

### PII masking

Payloads containing sensitive fields (PAN, card number, CVV, passwords, API keys, tokens, secrets) are automatically redacted by the `internal/platform/pii/mask.go` utility before logging. Sensitive keys are fully replaced with `***MASKED***`; email and phone fields are partially obfuscated to preserve debugging value.

---

## Metrics

All metrics use the `rrq_` Prometheus namespace. Implementation is concentrated in `internal/platform/metrics.go`, with per-service decorator packages (`internal/adapter/outbound/observability/`) that wrap each service's port interfaces to record metrics at every operation boundary.

### Infrastructure metrics (15 instruments)

These cover circuit breaker state, DLQ ingestion, outbox relay health, idempotency conflicts, consumer processing duration, backoff, and commits. Every worker exposes `rrq_circuit_breaker_state` (gauge), `rrq_dlq_ingestion_rate` (counter), `rrq_infrastructure_errors_total` (counter), `rrq_consumer_message_duration_seconds` (histogram), and `rrq_consumer_commits_total` (counter).

### Business metrics (8 instruments)

Emitted by the ledger worker at transaction boundaries, defined in `internal/platform/business_metrics.go`:

- `rrq_business_gtv_total{currency}` — Gross Transaction Value (counter)
- `rrq_business_transfers_total{status,shard_type}` — transfer volume by outcome and shard type (counter)
- `rrq_business_declines_total{reason}` — decline events by reason (counter)
- `rrq_business_saga_initiated_total`, `rrq_business_saga_completed_total` — saga lifecycle counters
- `rrq_business_saga_duration_seconds` — saga clearing time (histogram)
- `rrq_business_refunds_total` — refund volume (counter)
- `rrq_business_disputes_total` — dispute/chargeback volume (counter)

---

## Dashboards

Nine dashboards are provisioned as Kubernetes ConfigMaps with the `grafana_dashboard: "1"` label in the `observability` namespace. They follow a four-tier strategy, each tuned to a specific audience:

| Tier | Dashboard | UID | Audience |
|------|-----------|-----|----------|
| 1 | Executive NOC | `rrq-noc-executive` | Leadership, on-call |
| 2 | Service-specific (6) | `rrq-svc-{service}` | Service owners |
| 3 | Platform Infrastructure | `rrq-platform-infra` | Platform engineers |
| 4 | Business Operations | `rrq-business-ops` | Business ops |

Configuration lives in `rrq-gitops/rrq/base/observability/dashboards/`. Each dashboard queries the actual `rrq_*` metrics and `up`/`kube_*` metrics.

---

## Alerting

Twenty-eight alert rules are defined in `rrq-gitops/rrq/base/observability/prometheusrule.yaml`, grouped by service. Covering:

- **Financial invariants**: ledger imbalance, hanging cross-shard sagas, reconciliation failures (P0)
- **Infrastructure health**: circuit breaker open, DLQ spike, backoff spike, processing latency high, outbox relay stuck, panic rate (P1)
- **Gateway health**: HTTP 5xx rate, idempotency conflict spike, processing latency (P1)
- **Operational**: crash loop backoff, consumer group lag, startup failures (P2)

---

## Trace-Log Correlation

The bridge between traces and logs is the `trace_id` and `span_id` fields present in every canonical log line. An engineer observing a slow or failed transaction in Jaeger copies the `trace_id` and pastes it into Kibana to see the exact log lines emitted during that request, without time-range guessing. The correlation is established by the `internal/platform/otel.go` context helpers that extract the active span from a Go `context.Context` and inject it into the log fields.

---

## Key implementation locations

| Area | File |
|------|------|
| Trace initialization and context helpers | `internal/platform/otel.go` |
| Zap logger setup and canonical log line | `internal/platform/logger.go` |
| PII masking utility | `internal/platform/pii/mask.go` |
| Infrastructure metrics registration | `internal/platform/metrics.go` |
| Business metrics registration | `internal/platform/business_metrics.go` |
| Per-service metric decorators | Each service's `internal/adapter/outbound/observability/` |
| Dashboards as ConfigMaps | `rrq-gitops/rrq/base/observability/dashboards/` |
| Alert rules | `rrq-gitops/rrq/base/observability/prometheusrule.yaml` |
| OTel auto-instrumentation CRD | `rrq-gitops/rrq/base/observability/instrumentation.yaml` |
