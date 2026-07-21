# 05: HTTP API Reference

The merchant-facing HTTP API: endpoints, request parameters, response structure, error codes, and authentication. The authoritative OpenAPI specification lives in `proto/openapi.yaml`; this document is the human-readable version.

---

## Base URL

`https://api.rrq.example/v1`

The version path segment (`v1`) is part of the URL. Breaking changes introduce `v2` paths; v1 paths remain stable.

---

## Authentication

All endpoints except `/health` require a JWT in the `Authorization` header. The JWT is RS256-signed with the platform's active private RSA key, identified by the `kid` header.

**Flow:**
- Merchants receive an API key at onboarding (operator action via the Admin Dashboard).
- `POST /v1/auth/token` exchanges the API key for a short-lived JWT (1-hour validity).
- Kong validates the JWT signature and enforces rate limits at the edge before requests reach the API Gateway.

**JWT claims:** `sub` (merchant_id), `iat`, `exp`, `tier` (carried for Kong rate-limit decisions).

---

## Idempotency

All work-submitting POST endpoints require an `Idempotency-Key` header (typically a UUIDv4). The guarantee:

- First request with a given `(merchant_id, idempotency_key)` is accepted and executed.
- Same key + same body → replay returns the cached response.
- Same key + different body → 422 `IDEMPOTENCY_KEY_REUSED_WITH_DIFFERENT_BODY`.
- Concurrent same-key requests → 409 `IN_FLIGHT`.

Cached responses persist for 24 hours.

---

## `POST /v1/transfers`

Initiate a single transfer between two wallets.

**Request fields:** `from_wallet` (required, ULID, must be owned by the authenticated merchant), `to_wallet` (required, ULID), `amount` (required, positive integer in smallest currency unit), `currency` (required, ISO 4217, must match both wallets' currencies), `reference` (optional, merchant-supplied).

**Success:** 202 Accepted. Body contains `job_id`, `status: "pending"`, and a `_links.self` URL for polling.

**Error codes:** 400 for missing idempotency key or invalid body; 401 for missing/invalid JWT; 403 for frozen merchant or unowned wallet; 409 for in-flight idempotency key; 422 for validation failure or idempotency key reuse with different body; 503 when Postgres is unreachable.

---

## `POST /v1/payouts`

Initiate a bulk payout: one source wallet, many recipients.

**Request fields:** `from_wallet` (required), `recipients` (required array, up to 10,000 entries), each with `to_wallet`, `amount`, and optional `reference`.

**Success:** 202 Accepted. The `job_id` represents the entire payout; each recipient is posted as its own transfer. Per-recipient status is available via `GET /v1/jobs/{id}`.

**Error codes:** same as transfers, plus 422 for too many recipients or insufficient balance.

---

## `GET /v1/jobs/{id}`

Query the status of a job. Reads directly from the event store — strongly consistent.

**Response:** 200 OK. Body contains `job_id`, `type` (transfer or payout), `status` (pending/completed/failed), timestamps, and job-type-specific data. Jobs belonging to other merchants return 404.

---

## Refunds and reversals

There is no dedicated refund endpoint. A merchant-initiated refund is expressed as an ordinary transfer in the opposite direction — `POST /v1/transfers` from the original destination back to the original source — carrying the merchant's own reference to link the two. The system treats it identically to any other transfer, with the same idempotency and invariants.

A failed transfer (insufficient balance, frozen wallet, currency mismatch) moves no money — the Ledger Worker writes no ledger entries. The merchant receives a `transfer.failed` webhook.

---

## Health endpoints

- `GET /health` — liveness probe. Returns 200 if the process is alive.
- `GET /ready` — readiness probe. Returns 200 only if Postgres and Redis connections are healthy. Used by Kubernetes to drain pods.

---

## Key implementation locations

| Area | File/Directory |
|------|---------------|
| API Gateway HTTP handlers | `services/go-services/cmd/api-gateway/internal/adapter/inbound/http/` |
| Request validation | `services/go-services/internal/domain/validation/` |
| OpenAPI specification | `proto/openapi.yaml` |
| JWT verification (Kong) | `rrq-gitops/rrq/base/edge/kong/` |
