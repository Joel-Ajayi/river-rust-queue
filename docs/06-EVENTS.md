# 06: Event Catalog

Every event type in RRQ: the event name, the aggregate it belongs to, which service emits it, which services consume it, and when it fires. Events are business facts, not money movements — the money lives in `ledger_entries`, and events point at a `transfer_id` without re-recording the amounts. The authoritative schema definitions live in `proto/events/events.proto`; this document is the human-readable catalog.

---

## Schema convention

Every event is written to the `events` table with: `event_type` (dotted name like `transfer.completed`), `aggregate_type` / `aggregate_id` (what the event is about), `correlation_id` (the `job_id` linking every event from one merchant submission), `payload` (JSONB conforming to a specific Protobuf message), `occurred_at` (application time), and `publish_topic` (the Kafka topic the outbox relay must publish it to, or NULL for audit-only events).

---

## Job lifecycle

### `job.requested`

A merchant submitted a job and the API Gateway accepted it durably.

- **Aggregate:** `job` / `job_id`
- **publish_topic:** `jobs`
- **Emitted by:** API Gateway
- **Consumed by:** Ledger Worker, Fraud Worker (separate consumer groups)

---

## Transfer outcomes

These events the merchant cares about — each triggers a webhook. Written by the Ledger Worker in the same transaction as the posting.

### `transfer.completed`

A transfer posted successfully — debit and credit legs committed in `ledger_entries`.

- **Aggregate:** `transfer` / `transfer_id`
- **publish_topic:** `notify`
- **Emitted by:** Ledger Worker
- **Consumed by:** Webhook Worker

### `transfer.failed`

A transfer could not be posted for a terminal business reason. No ledger entries were written — no money moved.

- **Aggregate:** `transfer` / `transfer_id`
- **publish_topic:** `notify`
- **Emitted by:** Ledger Worker
- **Consumed by:** Webhook Worker

### `bulk_payout.completed`

A bulk payout finished; all its child transfers reached a terminal state.

- **Aggregate:** `job` / `job_id`
- **publish_topic:** `notify`
- **Emitted by:** Ledger Worker
- **Consumed by:** Webhook Worker

---

## Saga lifecycle (cross-shard transfers)

### `xshard.transfer.requested`

A cross-shard transfer has been initiated — the source shard debited the source wallet and credited the clearing account.

- **Aggregate:** `transfer` / `transfer_id`
- **publish_topic:** `jobs` (routed to destination shard)
- **Emitted by:** Ledger Worker (source shard)
- **Consumed by:** Ledger Worker (destination shard)

### `xshard.transfer.confirmed`

The destination shard posted the credit to the destination wallet.

- **Aggregate:** `transfer` / `transfer_id`
- **Emitted by:** Ledger Worker (destination shard)
- **Consumed by:** Ledger Worker (source shard, to settle)

### `xshard.transfer.rejected`

The destination shard could not post the credit. The source shard compensates by reversing the debit.

- **Aggregate:** `transfer` / `transfer_id`
- **Emitted by:** Ledger Worker (destination shard)
- **Consumed by:** Ledger Worker (source shard, to compensate)

---

## Wallet events

### `wallet.frozen`

A wallet's status changed to frozen, either automatically by the Fraud Worker (velocity threshold exceeded) or manually by an operator.

- **Aggregate:** `wallet` / `wallet_id`
- **Emitted by:** Fraud Worker or Admin Dashboard
- **Consumed by:** none directly (Ledger Worker reads `wallets.status` under row lock)

### `wallet.unfrozen`

A frozen wallet was restored to active by an operator.

- **Aggregate:** `wallet` / `wallet_id`
- **Emitted by:** Admin Dashboard

### `wallet.created`

A new wallet was provisioned for a merchant.

- **Aggregate:** `wallet` / `wallet_id`
- **Emitted by:** Admin Dashboard

---

## Fraud events

### `fraud.suspected`

A velocity threshold was exceeded; the system flagged or auto-froze the wallet.

- **Aggregate:** `wallet` / `wallet_id`
- **Emitted by:** Fraud Worker
- **Consumed by:** none directly (alerting subscribes externally)

---

## Webhook events

### `webhook.delivered`

A webhook was successfully delivered to a merchant.

- **Aggregate:** `webhook` / `delivery_id`
- **Emitted by:** Webhook Worker
- **Consumed by:** none (audit only)

### `webhook.failed`

A webhook delivery exhausted its retry budget and moved to the DLQ.

- **Aggregate:** `webhook` / `delivery_id`
- **Emitted by:** Webhook Worker
- **Consumed by:** none (audit; DLQ row created in the same transaction)

---

## Merchant events

### `merchant.created`

A merchant was onboarded by an operator. The raw API key is never recorded.

- **Aggregate:** `merchant` / `merchant_id`
- **Emitted by:** Admin Dashboard

### `merchant.api_key_rotated`

A merchant's API key was rotated.

- **Aggregate:** `merchant` / `merchant_id`
- **Emitted by:** Admin Dashboard

---

## Reconciliation events

### `reconciliation.completed`

A nightly reconciliation run finished. All balances matched, or discrepancies were recorded.

- **Aggregate:** `reconciliation` / `run_id`
- **Emitted by:** Reconciliation Worker

### `reconciliation.discrepancy`

A balance mismatch was detected during reconciliation.

- **Aggregate:** `reconciliation` / `run_id`
- **Emitted by:** Reconciliation Worker

---

## Key implementation locations

| Area | File/Directory |
|------|---------------|
| Protobuf event definitions | `proto/events/events.proto` |
| Events table schema | `services/go-services/internal/platform/schema/` |
| Outbox relay | `services/go-services/cmd/outbox-relay/` |
