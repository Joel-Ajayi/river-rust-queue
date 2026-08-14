# Operational Runbooks & Incident Response Procedures

This document provides step-by-step operational procedures for incident response, troubleshooting, and administrative maintenance of the RRQ platform.

---

## Index of Runbooks

- [RB-01: DLQ Inspection & Replay Procedure](#rb-01-dlq-inspection--replay-procedure)
- [RB-02: Circuit Breaker Recovery](#rb-02-circuit-breaker-recovery)
- [RB-03: Outbox Relay Lag & Kafka Egress Stalls](#rb-03-outbox-relay-lag--kafka-egress-stalls)
- [RB-04: Retry Budget Exhaustion Response](#rb-04-retry-budget-exhaustion-response)
- [RB-05: Manual Wallet Freeze / Unfreeze Procedure](#rb-05-manual-wallet-freeze--unfreeze-procedure)
- [RB-06: Executing Capacity Planning Engine](#rb-06-executing-capacity-planning-engine)

---

## RB-01: DLQ Inspection & Replay Procedure

### Symptom / Alert
- Prometheus Alert: `LedgerWorkerDLQSpike`, `WebhookWorkerDLQSpike`, or `OutboxDLQIngestion`.
- Grafana Panel: Tier 2 Dashboard shows unhandled poison pill events entering `dlq_entries`.

### Step-by-step Resolution
1. **Query open DLQ entries**:
   Connect to the target PostgreSQL shard or global DB and list open DLQ records:
   ```sql
   SELECT id, source, error_classification, error_message, attempt_count, created_at 
   FROM dlq_entries 
   WHERE status = 'open' 
   ORDER BY created_at DESC 
   LIMIT 10;
   ```

2. **Inspect payload details**:
   ```sql
   SELECT original_payload, trace_id 
   FROM dlq_entries 
   WHERE id = '<DLQ_ENTRY_ID>';
   ```

3. **Determine Root Cause**:
   - `poison`: Schema violation or invalid payload. **Do not replay directly** until upstream code fix is deployed.
   - `transient` / `infrastructure`: Downstream timeout or DB disconnect. Safe to replay once downstream recovers.

4. **Trigger Replay via Admin API**:
   ```bash
   curl -X POST https://api.rrq.yotstack.tech/v1/admin/dlq/<DLQ_ENTRY_ID>/replay \
     -H "Authorization: Bearer <ADMIN_JWT>"
   ```

5. **Verify Resolution**:
   Confirm that the `dlq_entries` status flipped to `replayed` and the job executed cleanly.

---

## RB-02: Circuit Breaker Recovery

### Symptom / Alert
- Prometheus Alert: `LedgerWorkerCBOpen`, `WebhookWorkerCBOpen`, or `OutboxRelayShardCBOpen`.
- Grafana Panel: Tier 3 Service Health shows Circuit Breaker state $= 2$ (Open).

### Step-by-step Resolution
1. **Identify the Open Breaker**:
   Check Prometheus metrics to isolate which breaker tripped:
   ```promql
   rrq_circuit_breaker_state == 2
   ```

2. **DB Shard Breaker Open**:
   - Check CloudNativePG cluster health:
     ```bash
     kubectl cnpg status shard-a -n rrq
     ```
   - If primary pod crashed, verify automatic failover promoted a standby.
   - Once database connectivity is restored, the circuit breaker will enter `HALF-OPEN` (1) after cooldown (`10s`) and auto-close after 5 consecutive successful probe requests.

3. **Merchant Webhook Breaker Open**:
   - The breaker tripped due to 5 consecutive failed deliveries to a specific merchant URL.
   - Contact the merchant or check merchant endpoint health.
   - The breaker will automatically probe the endpoint every `30s` until 5 consecutive HTTP 2xx responses are received.

---

## RB-03: Outbox Relay Lag & Kafka Egress Stalls

### Symptom / Alert
- Prometheus Alert: `OutboxLagCritical` (> 10s) or `OutboxKafkaBufferFull`.
- Grafana Panel: Tier 4 Middleware Dashboard shows growing unpublished event count in PostgreSQL `events`.

### Step-by-step Resolution
1. **Check Outbox Lag Metric**:
   ```promql
   rrq_outbox_lag_seconds{service_name="outbox-relay"}
   ```

2. **Inspect Strimzi Kafka Cluster**:
   ```bash
   kubectl get kafka -n rrq
   kubectl get pods -l strimzi.io/cluster=rrq-kafka -n rrq
   ```

3. **Verify Outbox Relay Pod Logs**:
   ```bash
   kubectl logs -l app=outbox-relay -n rrq --tail=100
   ```

4. **Mitigation**:
   - If Kafka brokers are starved for disk or memory, scale broker resources or clean expired segments.
   - If outbox relay is CPU-constrained by high transaction volume, increase `outbox-relay` pod replicas.

---

## RB-04: Retry Budget Exhaustion Response

### Symptom / Alert
- Prometheus Alert: `RetryBudgetExhaustedHigh`.
- Log entries showing `ErrRetryBudgetExhausted` ("retry budget exhausted").

### Step-by-step Resolution
1. **Understanding the Mechanism**:
   Token Bucket Retry Budgets limit retries to a percentage (e.g. 10%) of total successful volume. Under severe downstream outages, token buckets drain to zero, forcing retries to fail fast to DLQ to prevent thundering herd overload.

2. **Identify Distressed Downstream**:
   Check Tier 3 RED metrics to see which database shard or downstream HTTP service is failing.

3. **Resolve Root Outage**:
   Focus resources on repairing the primary database or downstream service. Do NOT manually bypass retry budgets during an active outage, as doing so will amplify traffic load by $10\times$.

4. **Post-Recovery Token Refill**:
   Once the downstream service recovers and emits successful responses, `RecordSuccess()` will automatically deposit tokens back into the bucket up to `RetryBudgetMaxTokens`.

---

## RB-05: Manual Wallet Freeze / Unfreeze Procedure

### Symptom / Operator Action
- Fraud velocity alert (`FraudVelocityLimitExceeded`) or security request requiring emergency freeze of a merchant wallet.

### Step-by-step Procedure
1. **Freeze Wallet**:
   Connect to the merchant's home database shard:
   ```sql
   UPDATE wallets 
   SET status = 'frozen', updated_at = NOW() 
   WHERE id = '<WALLET_ID>' AND merchant_id = '<MERCHANT_ID>';
   ```

2. **Verify Freeze Effect**:
   Subsequent transfer requests sourcing from `<WALLET_ID>` will be aborted by `ledger-worker` with `ErrWalletFrozen`.

3. **Unfreeze Wallet**:
   ```sql
   UPDATE wallets 
   SET status = 'active', updated_at = NOW() 
   WHERE id = '<WALLET_ID>' AND merchant_id = '<MERCHANT_ID>';
   ```

---

## RB-06: Executing Capacity Planning Engine

### Operator Action
- Updating service SLO parameters, changing replica counts, or recalculating infrastructure ceilings.

### Step-by-step Procedure
1. **Navigate to Capacity Directory**:
   ```bash
   cd rrq-gitops/capacity
   ```

2. **Edit Inputs (`slo-input.yaml`)**:
   Modify target QPS, latency SLOs, or infrastructure limits in `slo-input.yaml`.

3. **Execute Capacity Engine**:
   ```bash
   go run . slo-input.yaml
   ```

4. **Verify Generated ConfigMaps**:
   Confirm generated manifests in `rrq-gitops/rrq/base/config/` and inspect `capacity-output.yaml`.
