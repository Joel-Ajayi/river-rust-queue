# Dead Letter Queue (DLQ) Architecture — river-rust-queue

> **Zero-tolerance for data loss.** Every message represents real money. DLQs are not trash bins — they are critical operational state requiring automated triage, circuit breaker integration, PII masking, and auditable replay.
>
> [!CAUTION]
> **Work In Progress**: This document serves as an **aspirational design document**, not a reflection of the current codebase reality. Many features described here (e.g. webhook DLQ, fraud worker DLQ, admin API) are currently unbuilt stubs. See [STATUS.md](STATUS.md) for actual implementation status.

---

## 1. System Overview

### Messaging Platform
- **Apache Kafka** (via Strimzi operator) with segmentio/kafka-go
- **PostgreSQL** (CloudNativePG) for durable DLQ storage (`dlq_entries` table)
- **Redis** for ephemeral state (fraud velocity, webhook breakers)

### Services Producing DLQ Entries

| Service | Kafka Topic(s) | Failure Modes | DLQ Sink |
|---------|---------------|---------------|----------|
| **ledger-worker** | `jobs`, `xshard.<shard>` | Poison protobuf, terminal business errors (B8/B9), infra failures, panics | `dlq_entries` (source=`ledger`) |
| **outbox-relay** | `events` table → `jobs`, `notify`, `xshard.*` | Corrupted JSON, oversized messages, invalid notify schema, batch panics | `dlq_entries` (source=`ledger`) |
| **webhook-worker** | `notify` (partitioned by merchant_id) | HTTP 4xx/5xx, timeout, HMAC failure, merchant endpoint down | `webhook_deliveries` table (status=`dlq`) + `dlq_entries` for poison |
| **fraud-worker** | `jobs` (separate consumer group) | Velocity check failures, rule engine panics, Redis down | `dlq_entries` (source=`fraud`) |

---

## 2. Error Classification & Triage (The "Why")

Every DLQ entry **must** be classified at write time. Classification drives automated playbook.

We use **exactly four categories**:

```go
type ErrorClassification string

const (
    ClassificationPoison       ErrorClassification = "poison"       // corrupted payload, unmarshal failure
    ClassificationTransient    ErrorClassification = "transient"    // retryable: timeout, rate limit, deadlock
    ClassificationTerminal     ErrorClassification = "terminal"     // business rule: insufficient balance, frozen wallet
    ClassificationInfrastructure ErrorClassification = "infrastructure" // DB down, Kafka down, network partition
)
```

| Category | Examples | Auto-Action | Retry Policy |
|----------|----------|-------------|--------------|
| **Transient** | Kafka `LeaderNotAvailable`, `RequestTimedOut`; PG `40P01` deadlock, `40001` serialization; HTTP 429, 503, timeout | Exponential backoff + jitter | 5 attempts: 1s, 2s, 4s, 8s, 15s (full jitter) |
| **Terminal** | Protobuf unmarshal failure, JSON schema invalid, `ErrInsufficientBalance`, `ErrWalletFrozen`, `ErrCurrencyMismatch`, invalid HMAC, 400/401/403/404 from merchant | **Immediate DLQ** — no retry | 0 attempts |
| **Infrastructure** | PG `08006` connection lost, `ECONNREFUSED`, Kafka `BrokerNotAvailable`, network partition | Circuit breaker opens → immediate DLQ | CB handles (fast-fail) |
| **Poison** | Corrupted protobuf/JSON, oversized message, invalid schema for `notify` topic | **Immediate DLQ** — no retry | 0 attempts |

### Classification Logic (Single Source of Truth)

```go
// internal/platform/errors.go
func ClassifyError(err error) ErrorClassification {
    // 1. Check for poison payload first (fast path)
    if isPoisonError(err) {
        return ClassificationPoison
    }
    
    // 2. Check for terminal business errors
    if domain.IsTerminalError(err) {
        return ClassificationTerminal
    }
    
    // 3. Check for transient infrastructure errors
    if IsTransientError(err) {
        return ClassificationTransient
    }
    
    // 4. Check for infrastructure failures
    if IsInfrastructureError(err) {
        return ClassificationInfrastructure
    }
    
    // 5. Default: transient — limited retries then DLQ
    return ClassificationTransient
}

func isPoisonError(err error) bool {
    // Protobuf/JSON unmarshal failures
    var syntaxErr *json.SyntaxError
    if errors.As(err, &syntaxErr) {
        return true
    }
    var unmarshalErr *json.UnmarshalTypeError
    if errors.As(err, &unmarshalErr) {
        return true
    }
    // Protobuf errors would be checked similarly
    // Oversized message
    var maxBytesErr *http.MaxBytesError
    if errors.As(err, &maxBytesErr) {
        return true
    }
    return false
}
```

---

## 3. Retry & Circuit Breaker Integration

### Retry Boundary Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ Layer 1: Consumer/Daemon Loop (owns retry)                      │
│   - attempt counter + sleepBackoff (exponential + full jitter)  │
│   - offset NOT committed on failure                             │
│   - ClassificationTransient → retry                             │
│   - ClassificationTerminal/Poison → DLQ + commit offset         │
│   - ClassificationInfrastructure → fast-fail, CB handles        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ Layer 3: Circuit Breaker (gobreaker) — pure shield              │
│   - dbIsSuccessfulPolicy: terminal + transient = success        │
│   - Only infra failures (08xxx, ECONNREFUSED) trip breaker      │
│   - CB Open → fast-fail → Layer 1 sees ErrOpenState             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ DLQ Write Path (Layer 1)                                        │
│   - Write to dlq_entries BEFORE committing Kafka offset         │
│   - DLQ write failure → abort batch, backoff, retry DLQ write   │
│   - DLQ write failure after max retries → crash process         │
└─────────────────────────────────────────────────────────────────┘
```

### Circuit Breaker → DLQ Flow

```
PG Connection Refused (ECONNREFUSED)
    │
    ▼
IsInfrastructureError = true
    │
    ▼
Circuit Breaker counts failure (consecutive failures ≥ 3)
    │
    ▼
CB State: CLOSED → OPEN
    │
    ▼
All subsequent calls fast-fail with ErrOpenState (µs)
    │
    ▼
Layer 1 (consumer) sees ErrOpenState → ClassificationInfrastructure
    │
    ▼
DLQ entry: error="circuit breaker open", classification="infrastructure"
    │
    ▼
Offset committed (message parked safely)
    │
    ▼
CB Half-Open probe → PG recovers → CB CLOSED → processing resumes
```

---

## 4. Unified DLQ Schema (PostgreSQL)

### Base Migration (deploy/db/migrations/shard/000007_create_dlq_entries.up.sql)

```sql
-- dlq_entries: terminal failures awaiting human attention.
CREATE TABLE dlq_entries (
    id                TEXT PRIMARY KEY,
    source            TEXT NOT NULL CHECK (source IN ('ledger', 'webhook', 'fraud', 'outbox-relay')),
    original_payload  JSONB NOT NULL,
    error_message     TEXT NOT NULL,
    error_classification TEXT NOT NULL CHECK (error_classification IN ('poison', 'transient', 'terminal', 'infrastructure')),
    attempt_count     INT NOT NULL,
    first_failed_at   TIMESTAMPTZ NOT NULL,
    last_failed_at    TIMESTAMPTZ NOT NULL,
    status            TEXT NOT NULL DEFAULT 'open'
                      CHECK (status IN ('open', 'replayed', 'resolved')),
    replayed_at       TIMESTAMPTZ,
    replayed_job_id   TEXT,
    resolved_at       TIMESTAMPTZ,
    resolved_by       TEXT,
    resolution_note   TEXT,
    trace_id          TEXT,
    span_id           TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- "open entries, newest first" — dominant operator query
CREATE INDEX dlq_entries_open_idx   ON dlq_entries (created_at DESC) WHERE status = 'open';
CREATE INDEX dlq_entries_source_idx ON dlq_entries (source, status);
CREATE INDEX dlq_entries_trace_idx  ON dlq_entries (trace_id) WHERE trace_id IS NOT NULL;
```

### Enhancement Migration (000012_add_dlq_enhanced_columns.up.sql)

```sql
-- dlq_entries: add error_classification, trace_id, span_id columns for enhanced DLQ observability
ALTER TABLE dlq_entries
    ADD COLUMN IF NOT EXISTS error_classification TEXT
        CHECK (error_classification IN ('poison', 'transient', 'terminal', 'infrastructure'));

ALTER TABLE dlq_entries
    ADD COLUMN IF NOT EXISTS trace_id TEXT;

ALTER TABLE dlq_entries
    ADD COLUMN IF NOT EXISTS span_id TEXT;

-- Index for trace correlation
CREATE INDEX IF NOT EXISTS dlq_entries_trace_idx ON dlq_entries (trace_id) WHERE trace_id IS NOT NULL;

-- Update existing rows with default classification based on source
UPDATE dlq_entries
SET error_classification = 'terminal'
WHERE error_classification IS NULL;
```

### Column Rationale

| Column | Purpose |
|--------|---------|
| `error_classification` | Drives automated replay eligibility |
| `trace_id` / `span_id` | Correlate with Jaeger traces for root cause |
| `replayed_job_id` | Links replay to new job for idempotency |
| `resolution_note` | Audit trail for PCI-DSS/SOC2 |
| `attempt_count` | Distinguish "first failure" from "exhausted retries" |

---

## 5. Service-Specific DLQ Write Paths

### 5.1 Ledger Worker (`ledger-worker/internal/adapter/inbound/kafka/consumer.go`)

```go
func (m *ConsumerManager) routeToDLQ(ctx context.Context, msg kafka.Message, reason string) error {
    classification := platform.ClassifyError(errors.New(reason))
    
    // Extract trace context from Kafka headers
    var traceID, spanID string
    for _, h := range msg.Headers {
        if h.Key == "traceparent" {
            parts := strings.Split(string(h.Value), "-")
            if len(parts) >= 3 {
                traceID, spanID = parts[1], parts[2]
            }
            break
        }
    }

    // Determine shard for DLQ write
    shardID := m.dlqFallbackShard
    if env := extractMerchantID(msg.Value); env != "" {
        if s, err := m.directory.ShardFor(ctx, env); err == nil {
            shardID = s
        }
    }

    // PII mask the payload before storage
    maskedPayload := pii.Mask(msg.Value)

    entry := domain.DLQEntry{
        ID:                    string(msg.Key),
        Source:                domain.DLQSourceLedger,
        OriginalPayload:       maskedPayload,
        ErrorMessage:          fmt.Sprintf("[%s] %s", classification, reason),
        ErrorClassification:   classification,
        AttemptCount:          currentAttempt, // from consumer loop
        FirstFailedAt:         msg.Time,
        LastFailedAt:          time.Now(),
        Status:                domain.DLQStatusOpen,
        TraceID:               traceID,
        SpanID:                spanID,
    }

    if entry.ID == "" {
        entry.ID = platform.NewEventID()
    }

    // Retry DLQ write with backoff (max 3 attempts)
    var lastErr error
    for i := 0; i < platform.ConsumerDLQMaxRetries; i++ {
        if err := m.dlqStore.WriteDLQEntry(ctx, shardID, entry); err != nil {
            lastErr = err
            delay := platform.CalculateJitterBackoff(i, platform.ConsumerDLQRetryBaseDelay, platform.ConsumerDLQMaxBackoff)
            select {
            case <-ctx.Done(): return ctx.Err()
            case <-time.After(delay):
            }
            continue
        }
        return nil
    }

    // DLQ write exhausted — crash to prevent silent data loss
    m.logger.Fatal("DLQ write failed after max retries", zap.Error(lastErr))
    return lastErr // unreachable
}
```

### 5.2 Outbox Relay (`outbox-relay/internal/adapter/outbound/postgres/event_store.go`)

```go
func (e *EventStore) RouteToDLQ(ctx context.Context, shardID string, event domain.Event, errorMsg string) error {
    classification := platform.ClassifyError(errors.New(errorMsg))
    
    maskedPayload := pii.Mask(event.Payload)

    _, err := pool.Exec(ctx, `
        INSERT INTO dlq_entries (id, source, original_payload, error_message, error_classification, attempt_count, first_failed_at, last_failed_at, status, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW(), 'open', NOW())
        ON CONFLICT (id) DO UPDATE SET
            error_message = EXCLUDED.error_message,
            attempt_count = EXCLUDED.attempt_count,
            last_failed_at = EXCLUDED.last_failed_at,
            status = EXCLUDED.status
    `, event.ID, "outbox-relay", maskedPayload, fmt.Sprintf("[%s] %s", classification, errorMsg), classification, 0)

    if err != nil {
        return err
    }

    platform.RecordDLQIngestion(ctx, "outbox-relay")
    return nil
}
```

### 5.3 Webhook Worker (Reconcile with `webhook_deliveries`)

The `webhook_deliveries` table IS the DLQ for webhooks (status=`dlq`). Also mirror to `dlq_entries` for unified dashboard:

```go
// In webhook delivery failure path:
if delivery.AttemptCount >= maxAttempts {
    // 1. Update webhook_deliveries
    _, _ = pool.Exec(ctx, `
        UPDATE webhook_deliveries 
        SET status='dlq', last_error=$1, last_attempt_at=NOW()
        WHERE id=$2
    `, errorMsg, deliveryID)

    // 2. Mirror to unified dlq_entries
    maskedPayload := pii.Mask(delivery.Payload)
    _, _ = pool.Exec(ctx, `
        INSERT INTO dlq_entries (id, source, original_payload, error_message, error_classification, attempt_count, first_failed_at, last_failed_at, status, trace_id, span_id, created_at)
        VALUES ($1, 'webhook', $2, $3, $4, $5, $6, NOW(), 'open', $7, $8, NOW())
    `, deliveryID, maskedPayload, errorMsg, classification, delivery.AttemptCount, delivery.CreatedAt, traceID, spanID)
}
```

### 5.4 Fraud Worker (New Implementation)

```go
// fraud-worker/internal/adapter/inbound/kafka/consumer.go
// Mirror ledger-worker pattern with source="fraud"
```

---

## 6. PII Masking (PCI-DSS / SOC2)

```go
// internal/platform/pii/mask.go
package pii

import (
	"encoding/json"
	"regexp"
	"strings"
)

var (
	panRegex      = regexp.MustCompile(`\b\d{13,19}\b`)                           // PAN
	cvvRegex      = regexp.MustCompile(`\b\d{3,4}\b`)                             // CVV
	emailRegex    = regexp.MustCompile(`[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-z]{2,}`)
	phoneRegex    = regexp.MustCompile(`\b\+?\d{1,3}[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b`)
)

func Mask(payload []byte) []byte {
	var obj map[string]interface{}
	if err := json.Unmarshal(payload, &obj); err != nil {
		return []byte(`{"masked": true, "error": "non-json payload"}`)
	}
	maskObject(obj)
	masked, _ := json.Marshal(obj)
	return masked
}

func maskObject(obj map[string]interface{}) {
	for k, v := range obj {
		switch k {
		case "pan", "card_number", "account_number", "credit_card":
			if s, ok := v.(string); ok {
				obj[k] = maskPAN(s)
			}
		case "cvv", "cvc", "security_code":
			obj[k] = "***"
		case "email":
			if s, ok := v.(string); ok {
				obj[k] = maskEmail(s)
			}
		case "phone", "phone_number", "mobile":
			if s, ok := v.(string); ok {
				obj[k] = maskPhone(s)
			}
		default:
			if nested, ok := v.(map[string]interface{}); ok {
				maskObject(nested)
			} else if arr, ok := v.([]interface{}); ok {
				for _, item := range arr {
					if nested, ok := item.(map[string]interface{}); ok {
						maskObject(nested)
					}
				}
			}
		}
	}
}

func maskPAN(s string) string {
	if len(s) < 8 { return "****" }
	return s[:4] + strings.Repeat("*", len(s)-8) + s[len(s)-4:]
}

func maskEmail(s string) string {
	parts := strings.Split(s, "@")
	if len(parts) != 2 { return "***@***" }
	return maskString(parts[0], 2) + "@" + maskString(parts[1], 2)
}

func maskString(s string, visible int) string {
	if len(s) <= visible { return strings.Repeat("*", len(s)) }
	return s[:visible] + strings.Repeat("*", len(s)-visible)
}
```

---

## 7. Replay Mechanics (Admin Dashboard + CLI)

### Replay Eligibility Rules

| Classification | Replay Allowed | Conditions |
|----------------|----------------|------------|
| `poison` | **Yes** | After payload fix |
| `terminal` | **Yes** | After business rule fix or root cause resolution |
| `infrastructure` | **Yes** | After infra recovery (usually auto-retried by CB half-open) |
| `transient` | **Auto-retry** | Usually handled by consumer before DLQ |

### Replay Flow

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Operator   │────▶│ Admin API   │────▶│  DLQ Table  │────▶│ Kafka Topic │
│  (Dashboard)│     │ /dlq/replay │     │  (update)   │     │  (produce)  │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                           │                    │                    │
                           ▼                    ▼                    ▼
                      Validate:           status: open          Inject with:
                      - entry exists         → replayed           - X-Retry-Count: 0
                      - classification       replayed_job_id:     - X-Is-Replay: true
                        replayable           new ULID             - traceparent (new)
                      - payload valid                                 - idempotency_key preserved
```

### Admin API Endpoints

```go
// POST /admin/dlq/replay
type ReplayRequest struct {
    DLQEntryIDs   []string `json:"dlq_entry_ids"`   // or single ID
    TimeRange     *TimeRange `json:"time_range,omitempty"` // replay all in range
    SourceFilter  []string  `json:"source_filter,omitempty"` // "ledger", "webhook", "fraud"
    ClassificationFilter []string `json:"classification_filter,omitempty"` // "terminal", "poison", "infrastructure"
    ModifiedPayload map[string]json.RawMessage `json:"modified_payload,omitempty"` // optional patch
}

// Response
type ReplayResponse struct {
    ReplayedCount int      `json:"replayed_count"`
    FailedIDs     []string `json:"failed_ids"`
    NewJobIDs     []string `json:"new_job_ids"` // for tracking
}

// GET /admin/dlq?status=open&source=ledger&limit=100
// GET /admin/dlq/{id} — full entry with masked payload
// PATCH /admin/dlq/{id} — update payload (for poison error fix)
```

### Idempotency on Replay

- Original `idempotency_key` **preserved** in replayed message
- New Kafka headers: `X-Retry-Count: 0`, `X-Is-Replay: true`
- Ledger worker `ON CONFLICT (idempotency_key) DO NOTHING` handles dedup
- Webhook worker uses `event_id` + `merchant_id` for dedup

---

## 8. Monitoring & Alerting (PrometheusRules)

```yaml
groups:
- name: dlq-alerts
  rules:
  # Critical: DLQ ingestion spike (derivative, not absolute)
  - alert: DLQIngestionRateSpike
    expr: |
      rate(rrq_dlq_ingestion_rate[5m]) > 10
      and
      rate(rrq_dlq_ingestion_rate[5m]) > 3 * rate(rrq_dlq_ingestion_rate[1h])
    for: 2m
    labels:
      severity: critical
    annotations:
      summary: "DLQ ingestion rate spike — possible bad deploy or upstream outage"
      runbook_url: "https://runbooks.example.com/dlq-spike"

  # Warning: DLQ depth growing
  - alert: DLQDepthGrowing
    expr: |
      rrq_dlq_depth{status="open"} > 1000
      and
      increase(rrq_dlq_depth{status="open"}[15m]) > 100
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "DLQ depth > 1000 and growing — backlog not draining"

  # Critical: DLQ write failure (process will crash)
  - alert: DLQWriteFailure
    expr: increase(rrq_infrastructure_errors_total{component="dlq_store"}[5m]) > 0
    for: 0m
    labels:
      severity: critical
    annotations:
      summary: "DLQ write failed — process may crash, investigate immediately"

  # Critical: Circuit breaker open → messages routing to DLQ
  - alert: CircuitBreakerOpenRoutingToDLQ
    expr: rrq_circuit_breaker_state == 2
    for: 1m
    labels:
      severity: critical
    annotations:
      summary: "CB {{ $labels.circuit_breaker }} OPEN — messages routing to DLQ"

  # P0: Reconciliation discrepancy (ledger drift)
  - alert: LedgerDiscrepancy
    expr: rrq_reconciliation_discrepancies_total > 0
    for: 0m
    labels:
      severity: critical
    annotations:
      summary: "Reconciliation found ledger drift — ALL HANDS"
```

### Required Metrics (in `internal/platform/metrics.go`)

```go
var (
    DLQIngestionRate = metric.NewInt64Counter("rrq_dlq_ingestion_rate", 
        metric.WithDescription("DLQ entries ingested per second"),
        metric.WithAttributes("source", "classification"))
    
    DLQDepth = metric.NewInt64Gauge("rrq_dlq_depth",
        metric.WithDescription("Current DLQ entries by status"),
        metric.WithAttributes("status", "source"))
    
    DLQReplayRate = metric.NewInt64Counter("rrq_dlq_replay_rate",
        metric.WithDescription("Manual replay attempts"),
        metric.WithAttributes("source", "outcome")) // success/failed
    
    DLQOldestMessageAge = metric.NewFloat64Gauge("rrq_dlq_oldest_message_age_seconds",
        metric.WithDescription("Age of oldest open DLQ entry"),
        metric.WithAttributes("source"))
)
```

---

## 9. Implementation Checklist

| Task | Status | Notes |
|------|--------|-------|
| **Migration**: `000012_add_dlq_enhanced_columns` with `error_classification`, `trace_id`, `span_id` | ⬜ | Run on all shards |
| **Ledger Worker**: Update `routeToDLQ` with classification + PII masking + trace extraction | ⬜ | |
| **Outbox Relay**: Update `RouteToDLQ` to match migration + classification | ⬜ | |
| **Webhook Worker**: Mirror to `dlq_entries` + PII masking | ⬜ | |
| **Fraud Worker**: New DLQ implementation | ⬜ | |
| **PII Masking Package**: `internal/platform/pii/mask.go` | ⬜ | |
| **Error Classification**: `internal/platform/errors.go:ClassifyError` | ⬜ | |
| **Admin API**: `/admin/dlq/*` endpoints | ⬜ | |
| **Replay Tooling**: Admin dashboard + CLI | ⬜ | |
| **PrometheusRules**: Deploy alerts | ⬜ | |
| **Grafana Dashboard**: DLQ panels (ingestion rate, depth, oldest age, replay success) | ⬜ | |
| **Integration Tests**: Poison message → DLQ → replay → success | ⬜ | |

---

## 10. References

- [Stripe: Idempotency & API Design](https://stripe.com/blog/idempotency)
- [AWS: DLQ Patterns](https://aws.amazon.com/blogs/architecture/amazon-sqs-dead-letter-queues/)
- [Kafka: Reliable Message Processing](https://www.redpanda.com/blog/reliable-message-processing-with-dead-letter-queue)
- [Netflix: Circuit Breaker](https://github.com/Netflix/Hystrix/wiki/How-it-Works)
- [AWS Builders' Library: Timeouts, Retries, Backoff](https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/)
- [PCI-DSS: Logging Cardholder Data](https://www.pcisecuritystandards.org/documents/PCI_DSS_v3-2-1.pdf)
- [Karafka: DLQ](https://karafka.io/docs/Consumer-Groups-Dead-Letter-Queue/)
- [Uber: DOMA](https://www.uber.com/blog/uber-payments-platform-under-the-hood/)
- [Shopify: Flash Sales](https://shopify.engineering/surviving-flashes-of-high-write-traffic-part-1)

---

*Last Updated: 2026-07-13*  
*Next Review: After DLQ migration deployment*