# Technical Interview Q&A: Resilience Engineering & Capacity Sizing

This document contains deep-dive interview questions, mathematical derivations, and resilience engineering explanations covering RRQ's failsafe-go primitives, token bucket retry budgets, backoff algorithms, and capacity engine formulas.

---

## Q1: Why does pure exponential backoff fail during database outages, and how does Full Jitter solve it?

### Answer:
During a backend database or network outage, hundreds of clients fail simultaneously. If retries use pure exponential backoff ($T_{\text{base}} \cdot 2^{n-1}$), all clients sleep for identical durations and retry at the **exact same millisecond**:

```
[ Pure Exponential Backoff ]
Client 1: ---- [Retry 1] -------- [Retry 2] ------------------------ [Retry 3]
Client 2: ---- [Retry 1] -------- [Retry 2] ------------------------ [Retry 3]
Client 3: ---- [Retry 1] -------- [Retry 2] ------------------------ [Retry 3]
               ▲                  ▲                                  ▲
            SPIKE 1            SPIKE 2                            SPIKE 3
```

This creates **retry storms** (thundering herd waves) that repeatedly crush the recovering database.

**The Full Jitter Solution** (AWS Builder's Library specification):
$$
t_{\text{exponential}}(n) = \min \left( T_{\text{max\_backoff}}, \; T_{\text{base}} \cdot 2^{n-1} \right)
$$
$$
t_{\text{sleep}}(n) = \text{UniformRandom}\left( 0, \; t_{\text{exponential}}(n) \right)
$$

By picking a uniform random delay between $0$ and the exponential cap, Full Jitter spreads retry attempts uniformly over time. The expected delay is $E[t_{\text{sleep}}] = \frac{1}{2} t_{\text{exponential}}$, while retry collision probability drops to near zero ($P(\text{collision}) \approx 0$).

---

## Q2: How does the Token Bucket Retry Budget prevent load amplification during systemic outages?

### Answer:
Naive retry policies execute $N$ retries for every failed request. Under a 100% outage, a 3-retry policy multiplies traffic by **$4\times$** ($1 \text{ original} + 3 \text{ retries}$), ensuring a struggling system can never recover.

RRQ implements a volume-derived **Token Bucket Retry Budget** (`platform.RetryBudget`):
1. **Token Refill**: Successful operations deposit tokens into the bucket (e.g. 1 token per 10 successes for a 10% budget fraction).
2. **Token Spend**: Every retry attempt must acquire a token via `TryAcquire()`.
3. **Fail Fast to DLQ**: When an outage occurs, retry tokens drain rapidly. Once the bucket hits `0`, `TryAcquire()` returns `false`, causing the worker to fail fast and route the failed payload directly to `dlq_entries` without attempting network retries.

---

## Q3: How is Bulkhead Semaphore capacity derived using Little's Law?

### Answer:
A Bulkhead limits maximum concurrent requests ($L_{\text{inflight}}$) executing in a service handler to prevent thread pool and memory exhaustion.

**Little's Law Equation**:
$$
L = \lambda \cdot W
$$
Where:
- $\lambda$ = Peak target QPS ($5,000\text{ TPS}$)
- $W$ = Target p99 processing latency ($0.05\text{s} = 50\text{ms}$)

$$
L_{\text{nominal\_peak}} = 5,000 \cdot 0.05 = 250 \text{ concurrent requests}
$$

Applying a $1.0$ micro-burst headroom coefficient ($\alpha_{\text{burst}} = 1.0 \implies 100\%$ extra capacity):
$$
B_{\text{limit}} = L_{\text{nominal\_peak}} \cdot (1 + \alpha_{\text{burst}}) = 250 \cdot 2.0 = 500 \text{ concurrent in-flight requests}
$$

If concurrent requests exceed 500, the 501st request is shed immediately with `HTTP 429 Too Many Requests` in $<1\text{ms}$, protecting memory buffers.

---

## Q4: How does Kingman's Formula model queueing delay under non-Poisson traffic variance in the capacity engine?

### Answer:
Standard M/M/1 queueing models assume Poisson arrivals and exponential service times ($C_a^2 = 1, C_s^2 = 1$). Real payment traffic exhibits bursty arrival variance ($C_a^2 > 1$) and multi-modal SQL execution variance ($C_s^2 > 1$).

The capacity engine uses **Kingman's Formula Approximation** to calculate expected queue waiting time $W_q$:

$$
W_q \approx \left( \frac{\rho}{1 - \rho} \right) \cdot \left( \frac{C_a^2 + C_s^2}{2} \right) \cdot \tau
$$

Where:
- $\rho$ = Target CPU utilization (e.g. 0.70 for 70%)
- $C_a^2$ = Coefficient of variation of inter-arrival times
- $C_s^2$ = Coefficient of variation of service times
- $\tau$ = Mean service processing time

This formula allows the capacity engine to accurately predict queue latency spikes under high utilization and automatically size `REQUEST_TIMEOUT_MS` and HPA replica bounds.

---

## Q5: How does Deadline Propagation prevent "zombie transactions"?

### Answer:
In a multi-layer architecture (`Gateway` $\rightarrow$ `Core API` $\rightarrow$ `Postgres`), setting fixed independent timeouts at each layer creates **zombie transactions**—where an upstream caller times out and returns an error to the client, but downstream database queries keep executing for seconds, wasting CPU and lock capacity.

**Golden Inequality for Timeout Alignment**:
$$
T_{\text{Postgres Query}} (500\text{ms}) < T_{\text{Core API}} (2.0\text{s}) < T_{\text{Envoy Gateway}} (2.5\text{s}) < T_{\text{Client}} (5.0\text{s})
$$

**Deadline Propagation Implementation**:
1. Gateway creates a root context deadline $t_{\text{deadline}} = t_{\text{start}} + T_{\text{budget}}$.
2. The remaining timeout $T_{\text{remaining}} = t_{\text{deadline}} - t_{\text{current}} - \sigma_{\text{slack}}$ is passed downstream in headers/contexts.
3. If $T_{\text{remaining}} \le 0$, downstream handlers cancel database execution immediately.
