# 24 — Resilience

> **What this is.** The deep dive on resilience patterns in RRQ: circuit breakers, exponential backoff with jitter, and dead letter queues. Each addresses a different failure mode; together they form the system's defense against external dependency unreliability.
>
> **Reading time.** ~18 minutes.
>
> **Prerequisites.** [`../services/12-WEBHOOK-WORKER.md`](../services/12-WEBHOOK-WORKER.md).

---

## What resilience means

Resilience is the property of a system that **degrades predictably** when its dependencies misbehave. The opposite is brittleness: when a dependency fails, the system either fails catastrophically (cascading failures, total outage) or pretends nothing is wrong (silent dropping, retries until heat death).

The three failure modes RRQ handles via resilience patterns:

- **A merchant's webhook endpoint is briefly slow or returning 5xx errors.** The system should retry, eventually succeed, and not give up after one try.
- **A merchant's webhook endpoint is broken for a sustained period.** The system should stop wasting resources hammering it, and route the affected messages to a place where humans can deal with them.
- **A sudden burst of failures causes a thundering herd of retries.** The retries should spread out, not pile up on the recovering endpoint and re-crush it.

Three patterns address these three failure modes: dead letter queues, circuit breakers, and exponential backoff with jitter. They compose; each addresses a different aspect of the same underlying problem.

---

## Exponential backoff with full jitter

### The thundering herd problem

A merchant's load balancer fails for 30 seconds. During the outage, the webhook worker tries to deliver 1,000 webhooks; all fail. Without backoff, the worker immediately retries all 1,000. The load balancer is just coming back online; it gets crushed by 1,000 simultaneous requests. It fails again.

This is the thundering herd. Retries make recovery harder, not easier.

The standard fix is **exponential backoff**: each retry waits longer than the previous. After the first failure, wait 1 second. After the second, 2 seconds. After the third, 4. After the Nth, `base * 2^(N-1)`, capped at some maximum.

Exponential backoff alone is *better* than no backoff, but it has a subtle problem: the retries are still correlated. All 1,000 webhooks that failed at the same moment retry at the same intervals — 1s, 2s, 4s — and hit the merchant in simultaneous bursts. The herd is slower but still herd-shaped.

### Why jitter

The fix: add randomness. Each retry's wait time is randomized within a window. Now retries spread out across time rather than clustering at discrete intervals.

Different jitter strategies exist:

**No jitter:** `delay = base * 2^attempt`. Pure exponential. Correlated retries.

**Equal jitter:** `delay = base * 2^attempt / 2 + random(0, base * 2^attempt / 2)`. Half fixed, half random. Partially decorrelates.

**Full jitter:** `delay = random(0, base * 2^attempt)`. Entirely random within the exponential window. Maximally decorrelates.

[AWS's architecture blog](https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/) analyzed these and concluded full jitter is the best for high-contention scenarios. The math:

- For a fixed retry interval, all retries collide.
- For exponential without jitter, retries collide at each interval.
- For equal jitter, retries are spread across half the window but still cluster.
- For full jitter, retries are uniform across the window.

RRQ uses full jitter:

```
delay = random_between(0, min(cap, base * 2^attempt))
next_retry_at = NOW() + delay
```

With `base = 1s`, `cap = 5min`, `max_attempts = 10`:

| Attempt | Window | Average | Worst-case wait |
| --- | --- | --- | --- |
| 1 | 0–2s | 1s | 2s |
| 2 | 0–4s | 2s | 4s |
| 3 | 0–8s | 4s | 8s |
| 4 | 0–16s | 8s | 16s |
| 5 | 0–32s | 16s | 32s |
| 6 | 0–64s | 32s | 64s |
| 7 | 0–128s | 64s | 128s |
| 8 | 0–256s (capped 300s) | 128s | 256s |
| 9 | 0–300s (capped) | 150s | 300s |
| 10 | 0–300s (capped) | 150s | 300s |

Worst-case total wait across all retries: ~22 minutes. Average total: ~9 minutes. After attempt 10, the delivery moves to the DLQ.

### Why these numbers

The choices `base = 1s`, `cap = 5min`, `max_attempts = 10` are calibration, not theorems:

- **Base = 1s.** A merchant whose endpoint had a transient blip deserves a fast first retry. Too long and routine flakes feel slow; too short and the herd doesn't disperse. 1 second is a working compromise.
- **Cap = 5min.** A long-broken endpoint shouldn't have its retry interval grow unboundedly. 5 minutes is short enough that recovery is detected within reasonable time; long enough that we're not hammering.
- **Max attempts = 10.** Bounded by the total retry budget. With the formulas above, 10 attempts span ~45 minutes — enough for most transient failures to resolve, not so long that the DLQ never receives anything.

A production deployment might tune these per-merchant — premium merchants get more attempts, free-tier merchants get fewer. v1 uses one set of values for all merchants.

### Implementation details

The retry is scheduled by writing `next_retry_at` to `webhook_deliveries`. A separate scheduler loop polls for due retries. The schedule is durable; it survives worker restarts.

```
fn schedule_retry(delivery, attempt, error):
    delay_seconds = random.uniform(0, min(cap_seconds, base_seconds * 2 ** (attempt - 1)))
    next_retry_at = now() + delay_seconds
    
    UPDATE webhook_deliveries
    SET attempt_count = $1,
        last_attempt_at = NOW(),
        last_error = $2,
        next_retry_at = $3,
        status = 'pending'
    WHERE id = $4
```

The full-jitter formula is two lines. The clever part is the choice; the implementation is trivial.

---

## Circuit breakers

### The problem with retries alone

Exponential backoff handles a *temporarily* broken endpoint well. It does not handle a *permanently* broken one.

If a merchant's endpoint has been returning 500 for the last 6 hours, the webhook worker has been retrying every delivery for that merchant, eating up resources on doomed requests, and clogging the retry scheduler. Worse, every new event for that merchant adds to the backlog and starts its own retry cycle.

The circuit breaker addresses this. The idea, borrowed from electrical engineering: a fuse that trips when too much current flows, preventing damage downstream. In software, a fuse that trips when too many failures occur, preventing wasted attempts.

### The state machine

A circuit breaker has three states:

```
                  ┌──────────┐
                  │  Closed  │  Normal. Requests flow. Failures counted.
                  └────┬─────┘
                       │ N consecutive failures
                       ▼
                  ┌──────────┐
                  │   Open   │  All requests fast-fail. Cooldown timer running.
                  └────┬─────┘
                       │ cooldown expires
                       ▼
                  ┌──────────┐
                  │ Half-Open│  Single trial request allowed.
                  └─┬──────┬─┘
        success     │      │   failure
                    ▼      ▼
              ┌──────────┐ ┌──────────┐
              │  Closed  │ │   Open   │ (cooldown restarts)
              └──────────┘ └──────────┘
```

**Closed**: the breaker is "off" (the circuit is complete; current flows). Requests proceed normally. Failures are counted. If consecutive failures reach the threshold, transition to **Open**.

**Open**: the breaker is "tripped" (the circuit is broken; current is stopped). New requests fail-fast without attempting the network call. A cooldown timer is running. After the timer expires, transition to **Half-Open**.

**Half-Open**: one trial request is allowed through. If it succeeds, the breaker transitions back to **Closed** (the endpoint has recovered). If it fails, back to **Open** (cooldown restarts).

The state names are confusing — "Closed" means "working normally" and "Open" means "tripped" — because they come from electrical engineering, where a closed circuit conducts and an open circuit doesn't. Software engineers get used to it.

### Why this works

The breaker turns a permanently-broken endpoint into a fast-failing endpoint after the threshold trips. Subsequent deliveries take microseconds (fail-fast) instead of seconds (HTTP timeout). The downstream pressure on the retry scheduler is much lower; resources are freed for healthy work.

The half-open state is the recovery mechanism. Instead of staying open forever (which would mean a permanently-failed endpoint never recovers), the breaker periodically probes the endpoint with a single request. If the endpoint has recovered, the probe succeeds and the breaker closes; normal operation resumes. If not, the probe fails and the breaker re-opens with a fresh cooldown.

The total number of network attempts during a long outage drops from "one per delivery per attempt" (potentially thousands) to "one per cooldown" (handfuls). Operationally significant.

### Tuning

The two knobs:

**Failure threshold.** How many consecutive failures before tripping? RRQ uses 5. Too low (say 2): transient blips trip the breaker unnecessarily, hurting recovery time. Too high (say 50): the breaker takes too long to trip when an endpoint is genuinely broken.

**Cooldown period.** How long to wait before probing? RRQ uses 30 seconds. Too short: the probe hits an endpoint that's still recovering, fails, and resets the timer. Too long: a recovered endpoint waits longer than necessary.

Both are configuration. Production tuning depends on observed merchant endpoint behavior; the defaults are a reasonable starting point.

### Per-merchant scoping

The circuit breaker is keyed *per merchant*, not globally. A failing merchant trips their own breaker without affecting other merchants.

If the breaker were global, one broken merchant could effectively DoS the system — webhook delivery would halt for everyone while the global breaker was tripped. That's a denial-of-service vector and an operational disaster.

Per-merchant scoping isolates damage. The state is stored in Redis with keys like `breaker:webhook:m_X`. Reading the state on each delivery is a single Redis GET; updating is a single SET. Negligible overhead per request.

### Interaction with retries

The breaker and the retry scheduler both observe a delivery attempt. The interaction:

1. Retry scheduler picks up a due retry.
2. Before attempting the network call, check the breaker state for the merchant.
3. If **Open**: don't attempt; schedule the retry for after the cooldown ends.
4. If **Closed** or **Half-Open**: attempt the call.
5. Record the outcome (success or failure) into the breaker.

Code shape (simplified):

```
fn attempt_delivery(merchant, payload):
    state = breaker.get_state(merchant.id)
    
    if state == Open:
        next_check = breaker.cooldown_end(merchant.id)
        schedule_retry_at(delivery, next_check)
        return DeferredDueToBreaker
    
    result = http_post(merchant.url, payload)
    
    breaker.record_result(merchant.id, result)
    
    if result.is_success:
        return Success
    else:
        schedule_retry_with_jitter(delivery, current_attempt + 1)
        return Failure
```

The breaker state read and write are bracketed around the actual delivery. The retry scheduling continues to work normally; the breaker just suppresses the actual network call when the breaker is open.

### What the breaker doesn't catch

The breaker fires on consecutive failures. It does not fire on intermittent failures (50% failure rate, alternating successes and failures, never N in a row). For an intermittent merchant, the breaker stays closed and retries continue.

This is intentional. Intermittent failures are part of the normal operating envelope; full jitter handles them. The breaker is for endpoints that are *clearly* broken, not flaky.

A more sophisticated breaker would track failure *rate* over a window and trip if the rate exceeds a threshold. RRQ doesn't implement this; the consecutive-failure approach is simpler and works for the failure modes we actually see.

---

## Dead letter queues

### The problem

After backoff and breaker have done their work, some deliveries still fail. The endpoint may have been deconfigured. The merchant may have stopped using webhooks. The payload may trigger an edge case in the merchant's parser that they never fixed.

For these terminal failures, the system needs a destination that:

- Persists the failed message so it's not lost.
- Surfaces it for human inspection.
- Allows replay if the cause is fixed.
- Allows resolution-without-replay if the message is no longer relevant.

The DLQ — dead letter queue — is that destination.

### What lands in the DLQ

Two paths feed the DLQ in RRQ:

1. **Webhook deliveries** that exhaust their retry budget (10 attempts) or exceed their total time budget (24 hours from first attempt).
2. **Saga compensations** that themselves fail terminally (the `DeadLettered` saga state).

Both write to the same `dlq_entries` table, distinguished by the `source` column.

### What's in a DLQ entry

A DLQ entry preserves enough context that an operator can understand what was attempted and decide what to do:

```
dlq_entries:
  id              UUID
  source          'saga' | 'webhook'
  original_payload JSONB   -- enough to replay
  error_message   TEXT
  attempt_count   INT
  first_failed_at TIMESTAMPTZ
  last_failed_at  TIMESTAMPTZ
  status          'open' | 'replayed' | 'resolved'
  replayed_at     TIMESTAMPTZ
  replayed_job_id TEXT
  resolved_at     TIMESTAMPTZ
  resolved_by     TEXT
  resolution_note TEXT
```

The `original_payload` is the most important field. It includes everything needed to reproduce the failed work: the merchant_id, the URL, the signed payload, the headers — for a webhook. The saga_id, the failed step, the saga state — for a saga.

The `status` field tracks the lifecycle: an entry starts as `open`, transitions to `replayed` if an operator replays it (with the new job_id recorded), or to `resolved` if an operator decides no action is needed (with a note).

### Why a database table, not a Redis stream

The DLQ is operational, not high-throughput. The reads are: "show me all open entries for merchant X," "show me entries older than 7 days," "find entries with this error message." These are indexed queries, not stream consumption patterns.

A Redis stream would optimize for the wrong access pattern. A Postgres table with appropriate indexes is the right fit:

- `dlq_entries(status, created_at)` — operator queries.
- `dlq_entries(source, status)` — filter by saga vs webhook.

The DLQ also benefits from being transactional. When the operator replays an entry, the replay marks the entry as `replayed` *and* enqueues the new work in one atomic operation. With Postgres that's a single transaction; with a Redis stream it would be more delicate.

### The replay flow

Replay re-creates the failed work as a fresh job:

1. Operator runs `rrq dlq replay dlq_entry_id`.
2. The CLI reads the entry, validates that status is `open`, and presents the original payload for confirmation.
3. On confirmation: generate a new job_id, with idempotency key derived from the entry ID (`dlq-replay-<entry_id>`).
4. Re-emit the original payload to the appropriate stream (job stream for saga failures, notify stream for webhook failures).
5. Update the entry: `status='replayed'`, `replayed_at=NOW()`, `replayed_job_id=<new>`.
6. Write an audit event: `operator.action`.

The replay's idempotency key prevents accidental double-replay. If the same entry is replayed twice, the second replay sees the entry status is already `replayed` and refuses. And even if that check failed, the idempotency key would catch the duplicate at the API gateway level.

### The resolve flow

Sometimes an entry shouldn't be replayed. The merchant has decommissioned the endpoint, the relevance window has passed, the operator has made an out-of-band decision. The CLI's `resolve` command marks the entry without replaying:

```
rrq dlq resolve dlq_entry_id --note "merchant endpoint decommissioned 2026-05-20"
```

The entry transitions to `status='resolved'`. The note explains why. Future operators reviewing the DLQ history see the disposition.

Resolved entries are not deleted. They're archived in the table indefinitely. The DLQ becomes a record of operational decisions over time, queryable for audits.

### Operational hygiene

A DLQ that no one watches is just data accumulation. The system's resilience depends on the DLQ being a real workflow, not a black hole.

Concrete practices:

- Prometheus metric `webhook_deliveries_dlq_total{merchant_id}` increments on each new entry; alerts fire when the count crosses a threshold.
- Daily report (cron-driven): list of open DLQ entries by age, grouped by source. Sent to ops channel.
- SLO: open DLQ entries older than 7 days are an incident.

These practices are not part of the code — they're operational discipline. v1's docs describe them; production deployment implements them.

---

## How these patterns compose

The three patterns are not independent. They interact:

```
First attempt → fails
   ↓
Exponential backoff schedules retry
   ↓
Retry attempted → fails again
   ↓
Failures accumulate → circuit breaker trips
   ↓
Subsequent attempts fast-fail (breaker open)
   ↓
Cooldown expires → breaker half-open
   ↓
Trial attempt
   ↓
   ├── Success → breaker closes, normal operation resumes
   └── Failure → breaker re-opens
   ↓
Eventually: attempts exhausted (10) or time budget exceeded (24h)
   ↓
DLQ entry written, status=open
   ↓
   ├── Operator replays → new job, new attempt sequence starts
   └── Operator resolves → entry archived
```

Each pattern handles one part of the lifecycle:

- **Backoff** handles the gap between transient failure and recovery (seconds to minutes).
- **Circuit breaker** handles sustained failure (minutes to hours).
- **DLQ** handles terminal failure (when retry is no longer worth attempting).

A system with all three has graceful degradation across all failure timescales. A system missing one has a gap: too-aggressive retries during sustained failure (no breaker), wasted resources on doomed retries forever (no DLQ), or thundering herds on recovery (no jitter).

---

## What about preventative retries?

Some systems retry *speculatively*: send the request to two endpoints in parallel, use whichever responds first. This trades cost (2× the requests) for latency (slowest response is hidden).

RRQ doesn't do this for webhooks because:

- Merchants generally have one endpoint, not redundant ones.
- The bandwidth cost matters.
- The deduplication burden falls on the merchant — they receive double traffic and must drop duplicates.

For internal service-to-service calls (where we control both ends and the endpoints are redundant), speculative retry is a reasonable optimization. v1 doesn't have any such cases.

---

## What about timeouts?

Every HTTP request from the webhook worker has a hard timeout (10 seconds). This is technically a resilience pattern too — without it, a hanging connection would tie up a worker thread indefinitely.

The timeout is set at the request level (not the connection level): if the merchant's endpoint responds with headers but stops streaming the body, the connection is closed after 10 seconds total. This protects against slowloris-style attacks and slow-but-not-broken endpoints.

10 seconds is generous. Most merchants respond in under a second. A 10-second response is unusual; a 30-second response is a problem to solve, not a normal case to accommodate.

The timeout interacts with the breaker: a timeout is a failure, counted by the breaker. A merchant whose endpoint hangs consistently will trip the breaker quickly. Good.

---

## What about retries for non-network failures?

The patterns above describe retries for *network-flavored* failures: timeouts, 5xx responses, connection errors. What about other failures?

**Authentication failures (401, 403).** Not retryable. The merchant's webhook secret has changed (or the system's signing is wrong). Move to DLQ immediately. Operator action required.

**Payload-shape failures (400).** Not retryable. The payload format is wrong; retrying with the same payload will fail the same way. Move to DLQ. (Note: 400 from a merchant's endpoint is unusual — usually they accept any payload and ignore unknown fields.)

**Rate limit failures (429).** Retryable, but with extra respect: use the `Retry-After` header if present, otherwise default backoff.

**Redirects (3xx).** Don't follow automatically. Webhooks are POST to a specific URL; following a redirect could land at an unrelated endpoint.

The classifier — which errors are retryable, which terminal — is a small module the webhook worker uses to decide what to do with each response. Getting the classifier right is part of the implementation work.

---

## A note on testing

Testing resilience patterns is hard because the patterns are about behavior over time. Unit tests against pure functions only get you partway.

What RRQ's test suite does:

**Backoff and jitter.** Unit-test the jitter formula by running it many times and checking the distribution is approximately uniform.

**Circuit breaker.** Integration test against a controllable mock endpoint. Trip the breaker by returning 500s; verify state transitions. Recover with successful trial response.

**DLQ.** Integration test against a mock endpoint that always fails. Verify the entry appears in `dlq_entries` after the retry budget is exhausted, with the right payload preserved.

**Replay.** Integration test that replays a DLQ entry against a now-healthy endpoint. Verify success.

**Chaos.** Use turmoil (Rust) to simulate network partitions during retry scheduling. Verify the system recovers gracefully when the partition heals.

The tests exercise the patterns, not the patterns' theoretical properties. Whether the breaker actually saves resources at scale, whether the DLQ surfaces the right work to operators, whether the jitter prevents thundering herd in production — these are operational questions, not unit-test questions. The patterns are textbook; the practical value is measured in production.

---

## Where to read next

- The webhook worker that implements these patterns → [`../services/12-WEBHOOK-WORKER.md`](../services/12-WEBHOOK-WORKER.md)
- The admin CLI for operating the DLQ → [`../services/15-ADMIN-CLI.md`](../services/15-ADMIN-CLI.md)
- AWS's analysis of backoff strategies: <https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/>
- Martin Fowler's circuit breaker write-up: <https://martinfowler.com/bliki/CircuitBreaker.html>

---

*Pass 3 of the architecture series. Last updated pre-implementation.*
