# receiver/ (placeholder)

> Designed, not built. See [`docs/services/17-SIMULATION-HARNESS.md`](../../../docs/services/17-SIMULATION-HARNESS.md), section "Webhook receiver".

The HTTP endpoint RRQ's webhook worker delivers to. Will verify the
`X-RRQ-Signature` HMAC-SHA256, deduplicate on `X-RRQ-Event-Id`, and record every
delivery so the simulator can check that it was notified about every transfer it
started, in per-merchant order (the consumer-side mirror of invariant I5). Has a
control knob to return 500, time out, or go offline for a window, which is how
the `webhook-outage` scenario trips the breaker and fills the DLQ on demand. No
code yet.
