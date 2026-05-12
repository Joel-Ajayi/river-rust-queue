# 42 — API Reference

> **What this is.** Reference for the merchant-facing HTTP API. Endpoints, request/response shapes, error codes, authentication.
>
> **Format.** Look-up reference. The full OpenAPI spec will live in `proto/openapi.yaml` (not yet generated); this doc is human-readable.

---

## Base URL

```
https://api.rrq.example/v1
```

The version path segment (`v1`) is part of the URL. Breaking changes go to `v2` paths; v1 paths remain stable.

---

## Authentication

All endpoints (except `/health`) require an `Authorization` header carrying a JWT:

```
Authorization: Bearer <jwt>
```

The JWT is HS256-signed (v1) with the platform secret. Tokens are issued out of band (the merchant registration / management API, out of scope for v1).

The JWT claims include:
- `sub` — the merchant_id
- `iat`, `exp` — issued at, expires at (typically 1 hour validity)
- `tier` — merchant tier (basic, premium, etc.; for rate-limit decisions in v2)

A missing or invalid JWT returns 401:

```json
{
  "error": "UNAUTHORIZED",
  "message": "missing or invalid token"
}
```

---

## Idempotency

All POST endpoints require an `Idempotency-Key` header. The merchant generates this (typically UUIDv4):

```
Idempotency-Key: 8e3f1c4a-9b2d-4f81-a7c5-d3b6e9f2a1c0
```

Missing key returns 400. Same key with same body returns the cached response. Same key with different body returns 422 with `IDEMPOTENCY_KEY_REUSED_WITH_DIFFERENT_BODY`. Concurrent same-key returns 409 with `IN_FLIGHT`.

Cached responses persist for 24 hours. See [`../deep-dives/20-IDEMPOTENCY.md`](../deep-dives/20-IDEMPOTENCY.md) for details.

---

## `POST /v1/transfers`

Initiate a single transfer between two wallets.

**Request:**
```http
POST /v1/transfers HTTP/1.1
Authorization: Bearer <jwt>
Idempotency-Key: <uuid>
Content-Type: application/json

{
  "from_wallet": "wal_...",
  "to_wallet": "wal_...",
  "amount": 500000,
  "currency": "NGN",
  "reference": "merchant-internal-id-9182"
}
```

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `from_wallet` | string | yes | ULID with `wal_` prefix; merchant must own. |
| `to_wallet` | string | yes | ULID with `wal_` prefix. |
| `amount` | integer | yes | In the smallest currency unit. Positive. |
| `currency` | string | yes | ISO 4217. Must match both wallets' currencies in v1. |
| `reference` | string | no | Merchant-supplied. Stored in the saga. |

**Successful response:**
```http
HTTP/1.1 202 Accepted
Content-Type: application/json

{
  "job_id": "job_...",
  "status": "pending",
  "_links": {
    "self": "/v1/jobs/job_..."
  }
}
```

The transfer has been accepted for processing. The actual transfer happens asynchronously; the merchant learns the outcome via webhook or by polling `/v1/jobs/{id}`.

**Error responses:**

| Status | Code | When |
| --- | --- | --- |
| 400 | `MISSING_IDEMPOTENCY_KEY` | Header not provided |
| 400 | `INVALID_BODY` | JSON parse error |
| 401 | `UNAUTHORIZED` | JWT missing/invalid/expired |
| 403 | `MERCHANT_FROZEN` | Merchant account is not active |
| 403 | `WALLET_NOT_OWNED` | `from_wallet` doesn't belong to this merchant |
| 409 | `IN_FLIGHT` | Same idempotency key currently processing |
| 422 | `VALIDATION_FAILED` | Field-level error (amount negative, currency unknown, etc.) |
| 422 | `IDEMPOTENCY_KEY_REUSED_WITH_DIFFERENT_BODY` | Key matches; body differs |
| 503 | `STREAM_UNAVAILABLE` | Internal: Redis unreachable. Merchant retries. |

Error response body shape:
```json
{
  "error": "VALIDATION_FAILED",
  "message": "amount must be positive",
  "field": "amount"  // when applicable
}
```

---

## `POST /v1/payouts`

Initiate a bulk payout: one transfer source, many recipients.

**Request:**
```http
POST /v1/payouts HTTP/1.1
Authorization: Bearer <jwt>
Idempotency-Key: <uuid>
Content-Type: application/json

{
  "from_wallet": "wal_...",
  "recipients": [
    {
      "to_wallet": "wal_...",
      "amount": 1000,
      "reference": "..."
    },
    ...
  ]
}
```

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `from_wallet` | string | yes | The source for all sub-transfers. |
| `recipients` | array | yes | Up to 10,000 entries per payout. |
| `recipients[].to_wallet` | string | yes | |
| `recipients[].amount` | integer | yes | Positive. |
| `recipients[].reference` | string | no | Per-recipient reference. |

**Response:** same shape as `/v1/transfers`. The `job_id` represents the parent BulkPayoutSaga; sub-transfers each have their own `saga_id`. Per-recipient status is available via `/v1/jobs/{id}`.

**Notable error:**

| Status | Code | When |
| --- | --- | --- |
| 422 | `TOO_MANY_RECIPIENTS` | More than 10,000 in one payout |
| 422 | `INSUFFICIENT_BALANCE` | Source can't fund the sum (calculated upfront) |

---

## `GET /v1/jobs/{id}`

Query the status of a job. Read directly from the event store; strongly consistent.

**Request:**
```http
GET /v1/jobs/job_... HTTP/1.1
Authorization: Bearer <jwt>
```

**Response (transfer, in-progress):**
```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "job_id": "job_...",
  "type": "transfer",
  "status": "pending",
  "created_at": "2026-05-12T14:23:01Z",
  "saga": {
    "saga_id": "sg_...",
    "current_state": "Credited"
  },
  "data": {
    "from_wallet": "wal_...",
    "to_wallet": "wal_...",
    "amount": 500000,
    "currency": "NGN"
  }
}
```

**Response (transfer, completed):**
```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "job_id": "job_...",
  "type": "transfer",
  "status": "completed",
  "created_at": "2026-05-12T14:23:01Z",
  "completed_at": "2026-05-12T14:23:02Z",
  "saga": {
    "saga_id": "sg_...",
    "current_state": "Completed"
  },
  "data": { ... }
}
```

**Response (transfer, failed):**
```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "job_id": "job_...",
  "type": "transfer",
  "status": "failed",
  "created_at": "...",
  "completed_at": "...",
  "failure": {
    "reason": "INSUFFICIENT_BALANCE",
    "detail": "wallet balance 100000 < required 500000"
  },
  "data": { ... }
}
```

**Response (bulk payout, in progress):**
```http
HTTP/1.1 200 OK
{
  "job_id": "job_...",
  "type": "bulk_payout",
  "status": "pending",
  "summary": {
    "total_recipients": 5000,
    "completed": 3247,
    "failed": 12,
    "pending": 1741
  },
  "data": { ... }
}
```

**Errors:**

| Status | Code | When |
| --- | --- | --- |
| 404 | `JOB_NOT_FOUND` | No such job_id, or job belongs to another merchant |
| 401 | `UNAUTHORIZED` | JWT issue |

The `/v1/jobs/{id}` endpoint is the **strongly consistent read** path. Merchants who need to verify an operation completed should poll this rather than rely on the dashboard (which is eventually consistent).

---

## Webhooks (outbound)

When events occur, RRQ POSTs to the merchant's configured webhook URL with the signed payload.

**Webhook request:**
```http
POST /merchant-webhook-url HTTP/1.1
Host: merchant.example
Content-Type: application/json
X-RRQ-Event-Id: ev_...
X-RRQ-Signature: sha256=<hex>
X-RRQ-Delivery-Attempt: 1
User-Agent: rrq-webhook/1.0

{
  "event_id": "ev_...",
  "event_type": "transfer.completed",
  "occurred_at": "2026-05-12T14:23:45.123Z",
  "delivery_attempt": 1,
  "data": {
    "job_id": "job_...",
    "from_wallet": "wal_...",
    "to_wallet": "wal_...",
    "amount": 500000,
    "currency": "NGN"
  }
}
```

**Expected response:**
- `2xx`: success. We mark the delivery as delivered.
- `5xx` / timeout / connection error: retryable. We schedule a retry with exponential backoff.
- `4xx` other than 408/425: terminal. We move the delivery to DLQ.

**Signature verification (merchant side):**

```python
import hmac, hashlib, json

def verify(body_bytes, signature_header, secret):
    canonical = json.dumps(json.loads(body_bytes), sort_keys=True, separators=(',', ':'))
    expected = hmac.new(secret, canonical.encode(), hashlib.sha256).hexdigest()
    provided = signature_header.removeprefix("sha256=")
    return hmac.compare_digest(expected, provided)
```

Use constant-time comparison. The canonical form is JSON with sorted keys and no whitespace (RFC 8785 JCS).

**Idempotent handling (merchant side):**

The merchant *will* receive duplicate webhooks (network blips, RRQ retries after successful but unACKed deliveries). Defense: track processed `event_id`s and ignore duplicates.

```python
def handle(event):
    if redis.exists(f"processed:{event['event_id']}"):
        return  # duplicate; no-op
    redis.set(f"processed:{event['event_id']}", "1", ex=86400 * 7)  # remember for 7 days
    # process the event...
```

---

## Webhook event types

The events delivered as webhooks (subset of the internal event types):

| event_type | When |
| --- | --- |
| `transfer.completed` | A single transfer succeeded |
| `transfer.failed` | A single transfer failed |
| `bulk_payout.completed` | All sub-transfers in a bulk payout have terminated (some may have failed individually) |
| `wallet.frozen` | A wallet was frozen (rare; informational) |
| `dispute.initiated` | (v2) A chargeback was initiated against the merchant |
| `dispute.resolved` | (v2) A chargeback was resolved |

---

## Rate limiting

v1 does not enforce per-merchant rate limits. The system can be overwhelmed by a single merchant submitting at very high rates.

v2 will implement token-bucket rate limiting per merchant_id, exposed in headers:

```http
X-RateLimit-Limit: 1000
X-RateLimit-Remaining: 947
X-RateLimit-Reset: 1715520000
```

Exceeding the rate limit returns 429 Too Many Requests with `Retry-After` header.

---

## Health endpoints

Not part of the merchant API; used by infrastructure.

| Path | Purpose |
| --- | --- |
| `GET /health` | Liveness probe. 200 if the process is alive. |
| `GET /ready` | Readiness probe. 200 if Redis and Postgres are reachable. |
| `GET /metrics` | Prometheus exposition. Internal access only. |

---

## Versioning policy

v1 paths (`/v1/*`) remain stable. Breaking changes go to a new version path (`/v2/*`). The old version is supported for at least 12 months after the new version is announced.

Non-breaking additions (new optional fields, new error codes) can happen within a version. Clients should ignore unknown fields.

---

*Pass 4 of the architecture series. Last updated pre-implementation. Full OpenAPI spec at `proto/openapi.yaml` (to be generated).*
