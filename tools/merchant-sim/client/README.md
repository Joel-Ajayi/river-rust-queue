# client/ (placeholder)

> Designed, not built. See [`docs/services/17-SIMULATION-HARNESS.md`](../../../docs/services/17-SIMULATION-HARNESS.md), section "Merchant identity and auth".

The merchant API client. Will hold the API key, exchange it for a JWT via
`POST /v1/auth/token`, refresh the JWT before expiry, and post transfers and
payouts with a fresh `Idempotency-Key` on every call. This exercises the real
auth path rather than a shortcut. No code yet.
