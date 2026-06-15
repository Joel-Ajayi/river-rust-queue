# 43: Benchmark Methodology

> **What this is.** Reference for the benchmark suite that compares the Go and Rust implementations of RRQ. The fairness rules, the scenarios, the reporting format.
>
> **Format.** Look-up reference. Read the section for the scenario you're investigating, or the rules section to verify a methodology choice.
>
> **Sequencing.** Go is the primary implementation and is benchmarked first; the published numbers are the Go numbers. The Rust implementation is a comparison study built against the working Go reference, and the head-to-head comparison runs once it exists. Until then, "both implementations" in this doc describes the methodology the comparison will follow, not work happening in parallel.

---

## Why this document matters

A benchmark that favors one implementation due to unfair test conditions is worse than no benchmark; it is *misinformation*. If we report numbers, those numbers must be defensible. The methodology has to be transparent enough that anyone can reproduce, and rigorous enough that anyone can trust the results.

The audience for benchmarks is twofold:

1. **Future me, designing optimizations.** If I see "scenario C: Go p99 12ms, Rust p99 3ms," I want to know why. Stable methodology means I can rerun the benchmark and compare apples to apples over time.
2. **Reviewers and interviewers.** A reviewer who asks "did Go's GC cost you on this workload?" deserves a real answer with traces backing it up. The benchmark methodology produces the data needed to answer.

The discipline below is what makes the data trustworthy.

---

## Fairness rules

These apply to every scenario. Violations invalidate the run.

**Rule 1: Identical infrastructure.**
Both implementations run in identical Docker containers, against the same Postgres instance, the same Redis instance, on the same host machine. The only difference is which binary is running.

In practice, this means running one implementation, capturing results, stopping it, starting the other, capturing results. Not running both simultaneously (they'd compete for resources).

**Rule 2: Warm-up before measurement.**
The first N requests of any scenario are *warmup* and excluded from measurements. N = 1000 by default. Reasons:

- JIT-like compilation in Go (PGO, escape analysis stabilization).
- Connection pool fill (no cold-start connection overhead).
- Page cache warming for Postgres.
- Redis AOF / RDB stability.

Without warmup, the first few hundred requests carry artifacts that have nothing to do with the implementation's steady-state performance.

**Rule 3: Three runs, report median.**
Each scenario runs three times. Report the median, not the best run. Best-of-N is a known way to deceive yourself; it captures lucky moments rather than typical behavior.

If the three runs disagree significantly (>20% variance), that itself is a finding worth investigating before reporting.

**Rule 4: Environment recorded.**
Every benchmark run records:
- CPU model and core count.
- RAM size.
- Docker version.
- Host OS / kernel.
- Postgres version.
- Redis version.
- Commit SHAs for both implementations.
- Date and time.

Without this metadata, the numbers are not reproducible. Two runs three months apart on different machines compare nothing.

**Rule 5: RSS memory, not virtual memory.**
For memory metrics, report Resident Set Size (RSS), actual physical memory in use. Don't report Virtual Memory Size; Go's runtime reserves a large virtual address space that doesn't reflect actual usage. Read RSS from `/proc/<pid>/status` or platform-equivalent.

**Rule 6: No runtime tuning.**
Go runs with `GOGC=100` (the default). Rust runs in `--release` mode with default optimization profile. We do NOT tune the GC, change tracing levels, or apply implementation-specific tricks to make one implementation look better. The benchmark measures what a default deployment would look like.

If we ever do explore tuning, it's a separate experiment with separate reporting; we don't mix tuned and untuned numbers.

**Rule 7: Honest reporting.**
If Go wins a scenario, we say so. If Rust wins, we say so. If they tie, we say so. We do not report only the scenarios where one implementation looks favorable. The benchmark report includes all six scenarios, with the raw numbers, regardless of who wins which.

---

## Scenarios A through F

### Scenario A, Sustained transfer throughput

**Question:** What's the steady-state request rate for single transfers?

**Setup:**
- 100 virtual users (k6 VUs).
- Each VU sends sequential transfers (one at a time per VU, no concurrency within a VU).
- Random source/destination wallet pairs from a pre-seeded pool of 1,000 wallets.
- Each VU's idempotency key is fresh per request (no retry simulation here).
- Duration: 60 seconds after warmup.

**Measured:**
- Requests per second (throughput).
- p50, p95, p99 latency (API response time, not full saga duration).
- Error rate (should be ~0).

**What we're learning:** the runtime cost of the HTTP request path: parse, validate, JWT verify, idempotency check, XADD, response. This is mostly I/O-bound (Redis writes), so the two implementations should be close, but not identical, due to different request-routing overhead, allocation patterns, and serialization costs.

### Scenario B, Bulk payout stress

**Question:** How does the system handle fan-out workloads?

**Setup:**
- 10 VUs.
- Each VU sends bulk payouts of 100 recipients per request.
- Duration: 60 seconds after warmup.

**Measured:**
- Throughput in payouts/sec.
- p50/p95/p99 latency for the parent payout API response (returns 202 quickly; doesn't wait for sub-transfers).
- p50/p95/p99 latency for sub-transfers (saga duration, observed via traces).
- Consumer lag on `stream:jobs` during the run.

**What we're learning:** the Saga Worker's throughput under fan-out load. The bounded-concurrency semaphore should keep the system responsive; if it's misconfigured, sub-transfers queue up.

### Scenario C, Memory under load

**Question:** What's the memory profile under sustained traffic, including GC behavior?

**Setup:**
- Ramp from 0 → 500 VUs over 30 seconds.
- Sustain 500 VUs for 60 seconds.
- Ramp down to 0 over 30 seconds.
- Idle for 30 seconds.

**Measured:**
- RSS memory at peak load (during sustain).
- RSS memory 60 seconds after load ends (GC reclamation).
- p99 latency during sustain (to detect GC pause spikes).

**What we're learning:** how memory grows under load and whether it returns to baseline after. Rust should have flatter memory; Go should grow more and then reclaim. The interesting metric is the gap.

### Scenario D, Fraud throughput with per-wallet ordering

**Question:** How fast can the fraud worker process events with the per-wallet ordering constraint?

**Setup:**
- Pre-seeded 100 wallets.
- Generate 10,000 `TransferCompleted` events distributed across the 100 wallets (~100 events per wallet).
- Push them all to `stream:jobs` simultaneously.
- The Fraud Worker is the only consumer (Saga Worker disabled for this scenario).

**Measured:**
- Total processing time (first event ACKed to last event ACKed).
- Per-wallet ordering violations (must be 0; verified by recording the order of processing per wallet and asserting it matches input order).
- Memory during processing.
- Active task count peak.

**What we're learning:** the cost of two-level dispatch and the relative efficiency of Go's RWMutex vs Rust's DashMap. This is one of the most direct concurrency-model comparisons in the suite.

### Scenario E, Circuit breaker behavior

**Question:** Does the circuit breaker behave as designed?

**Setup:**
- Mock merchant endpoint that returns 200 normally; can be toggled to return 500.
- Webhook Worker delivering at sustained rate.
- Toggle the endpoint to fail.
- Measure: time until breaker opens (should be ~immediately after the 5th consecutive failure).
- Measure: requests during open state (should be near-zero, they fail-fast without HTTP calls).
- Toggle endpoint back to healthy.
- Measure: time until breaker half-opens (cooldown duration: 30s).
- Measure: time until breaker closes (1 successful half-open attempt).
- Measure: requests during this transition.

**What we're learning:** the breaker's state machine behaves correctly, and the resource savings during open state are real (no wasted HTTP attempts).

This is more of a *correctness* benchmark than a performance one. The implementation is correct or it isn't; the timing should match the configured cooldowns.

### Scenario F, Reconciliation at scale

**Question:** How fast can reconciliation process a large event log?

**Setup:**
- Seed the event store with 1,000,000 events distributed across 10,000 wallets.
- Ledger entries materialized correctly (so reconciliation should find zero discrepancies).
- Run reconciliation over the full window.

**Measured:**
- Total run duration.
- CPU utilization during run.
- Peak memory.
- Events processed per second.

**What we're learning:** this is the headline CPU-bound benchmark. Reconciliation is parallel (one task per wallet, distributed across cores) and event-heavy (deserializing protobuf payloads, summing amounts). Rust's `par_iter` versus Go's goroutine-with-channel-fanout. The two runtimes' allocation patterns and parallelism efficiency differ here.

If there's one number worth quoting in the project's README, it's this one. Both implementations doing the same work; the difference is the runtime.

---

## Reporting format

The benchmark report is a markdown table plus appendix CSVs.

### Table

| Scenario | Metric | Go | Rust | Difference |
| --- | --- | --- | --- | --- |
| A | Throughput (req/s) | 4,200 | 4,500 | +7% Rust |
| A | p50 latency (ms) | 12 | 11 | -8% Rust |
| A | p95 latency (ms) | 24 | 22 | -8% Rust |
| A | p99 latency (ms) | 48 | 28 | -42% Rust |
| B | Throughput (payouts/s) | 38 | 42 | +11% Rust |
| ... | ... | ... | ... | ... |
| F | Total time (s) | 38.2 | 19.7 | -48% Rust |
| F | Events/sec | 26,000 | 51,000 | +96% Rust |

(Numbers are illustrative; actual values depend on the implementation and infrastructure.)

### Appendix CSVs

For each scenario, a CSV with the raw per-request data:

```
timestamp,scenario,vu,implementation,request_id,latency_ms,status
2026-05-12T14:00:01.123Z,A,1,go,req_001,11,200
2026-05-12T14:00:01.124Z,A,2,go,req_002,12,200
...
```

These let anyone reproduce the percentile calculations or compute additional statistics.

### Narrative

Each scenario gets a paragraph explaining *what the numbers mean*. Example:

> **Scenario C, Memory under load.** Go's RSS peaks at 1.8 GB; Rust at 480 MB. After load ends, Go reclaims down to 600 MB (the runtime holds heap for future allocation); Rust returns to 180 MB. p99 latency during sustain shows Go's GC pauses as 50ms spikes every ~10s; Rust shows no equivalent pattern. The 50ms p99 latency is the price Go pays for managed memory; the absolute number is acceptable for our SLOs, but worth noting.

The narrative is where the benchmark says something useful. Numbers alone are just numbers.

---

## What the benchmark does NOT measure

Being explicit:

- **Real-world production performance.** Benchmarks are controlled; production is not. Mileage will vary with hardware, network, and workload mix.
- **Developer productivity.** Building the same system in Go and Rust takes different time; that's a real cost that doesn't appear in the benchmark.
- **Code maintainability.** The Go version may be more approachable; the Rust version may have stronger correctness guarantees. These trade off against the runtime numbers.
- **Operational complexity.** Different deployment patterns, observability stacks, debugging experiences. Not benchmarked.

The benchmark is one input to decision-making, not the deciding factor.

---

## When to rerun

Rerun the full suite when:

- A significant change lands in either implementation (e.g., a substantial refactor of the saga worker).
- A new scenario is added.
- The infrastructure changes (new Postgres major version, new Redis version, different host hardware).
- Quarterly, for trend tracking.

Don't rerun ad-hoc to "see what happens"; that's how cherry-picking creeps in. Each run is a formal data point with environment recorded.

---

## Tooling

- **k6** for load generation. JavaScript test scripts in `scripts/benchmark/`.
- **Prometheus** for metric collection during runs. Snapshot at end of each scenario.
- **Jaeger** for trace inspection (optional, useful for the narrative).
- **pprof** (Go) and **flamegraph** (Rust) for profiling when investigating specific scenarios.
- **/proc/<pid>/status** for RSS readings during the run.

The Makefile target `make bench` will run all scenarios and produce the report. Not yet implemented; design is complete.

---

*Pass 4 of the architecture series. Last updated pre-implementation.*
