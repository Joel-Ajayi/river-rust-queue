# 20: Idempotency

> **What this is.** The deep dive on idempotency keys: the canonical pattern, why it works, the variations that don't, and the production-grade details that distinguish a real implementation from a textbook one.
>
> **Reading time.** ~20 minutes.
>
> **Prerequisites.** [`../services/10-API-GATEWAY.md`](../services/10-API-GATEWAY.md). The service doc shows the implementation; this doc shows the reasoning and the edge cases.

---

## The problem, restated

A merchant submits a request. The system processes it. The response is sent. Somewhere on the wire, the response is lost. The merchant times out, doesn't know whether their request succeeded, and retries.

Without idempotency, the system has no way to know the retry is "the same logical request." It treats it as new. The transfer is executed twice. The merchant's customer is paid twice.

This is the **unknown-outcome problem** from [`../01-PROBLEM.md`](../01-PROBLEM.md) applied to a specific failure mode. The merchant cannot distinguish "request didn't arrive" from "request was processed but response lost." From their side, both look identical, and retry is the rational response to both. The system has to make retry *safe*, to ensure that no matter how many times a logical request is submitted, the underlying operation happens at most once.

This is invariant **I3** in [`../02-INVARIANTS.md`](../02-INVARIANTS.md): *for each `(merchant_id, idempotency_key)` pair where the API has accepted a request, exactly one `JobRequested` event exists in the job stream.*

The mechanism that enforces this invariant is the topic of this document.

---

## The pattern

Stripe popularized the modern form of this pattern in [their 2017 blog post on idempotency](https://stripe.com/blog/idempotency). The shape is simple:

1. Every state-changing API request carries an `Idempotency-Key` HTTP header. The merchant generates this key (typically a UUIDv4).
2. The server uses the key as a **distributed mutex**: the first request with key K wins; subsequent requests with key K either wait, see the in-flight state, or get the cached result of the first.
3. The cache survives across server restarts (it's in a durable store, not process memory).
4. The cache expires after some retention window (24 hours is common).

The merchant's mental model: "I generate a UUID for this logical operation. As long as I use that UUID, I can retry as many times as I want without worrying about double-charging. If I want to perform the operation again, I use a different UUID."

Three things about the API design worth noticing:

**The merchant generates the key, not the server.** This is what makes retry safe, the merchant uses the same key on retry, which is what tells the server "this is the same logical request." If the server generated the key, the merchant wouldn't know what to send on retry.

**The key is in a header, not the URL.** URL-based keys (like `/transfers/{client-id}`) work but require URL design choices that constrain the API. Headers are more flexible.

**The key carries no semantics.** The server doesn't parse the key for information; it treats it as an opaque string. Merchants are free to use UUIDs, sequence numbers, or any other unique scheme.

---

## The SETNX dance

```mermaid
flowchart TD
    req(["POST with Idempotency-Key K"]) --> setnx{"SET idemp:m:K 'processing:hash' NX EX 86400"}
    setnx -->|"OK (new key)"| xadd["XADD JobRequested"]
    xadd --> ok["202 Accepted (job_id)<br/>cache response async"]
    setnx -->|"nil (key exists)"| get["GET idemp:m:K"]
    get --> cmp{"compare body hash"}
    cmp -->|"hash matches, still processing"| inflight["409 Conflict (in-flight)"]
    cmp -->|"hash matches, completed"| cached["202 with cached response"]
    cmp -->|"hash differs"| reuse["422 key reused with different body"]
```

The mechanism that enforces the at-most-once property is a single atomic Redis operation: `SET key value NX EX 86400`. Decoded:

- `SET`, store a value.
- `NX`, only if the key doesn't already exist. Atomically check-and-set. This is the critical bit.
- `EX 86400`, expire in 24 hours.

Redis serializes commands; only one client can win the race for a given key. Every other client's `SET ... NX` returns nil (the key exists) and they fall through to the duplicate-handling path.

The full pattern, in pseudocode:

```
on_request(merchant_id, idempotency_key, body):
    body_hash = SHA256(canonical_json(body))
    redis_key = "idemp:" + merchant_id + ":" + idempotency_key
    placeholder = "processing:" + body_hash
    
    set_result = SET redis_key placeholder NX EX 86400
    
    if set_result == OK:
        // New request. Proceed with normal processing.
        try:
            response = process_request(body)
            SET redis_key (body_hash + ":" + json(response)) EX 86400
            return response
        except err:
            // Critical: release the claim on failure.
            DEL redis_key
            raise err
    
    else:
        // Duplicate. Inspect existing value.
        existing = GET redis_key
        
        if existing.startswith("processing:"):
            existing_hash = existing.split(":", 1)[1]
            if existing_hash == body_hash:
                return error(409, "in_flight")
            else:
                return error(422, "key_reused_with_different_body")
        
        else:
            existing_hash, cached_response = existing.split(":", 1)
            if existing_hash == body_hash:
                return cached_response
            else:
                return error(422, "key_reused_with_different_body")
```

The key insight: **the entire correctness of the pattern hinges on the atomicity of `SET ... NX`.** If the check ("does key K exist?") and the set ("create key K with value V") were two separate operations, two concurrent requests could both observe "K doesn't exist" between each other's check and set, and both would proceed. Atomic SETNX prevents this.

The Redis documentation guarantees `SET ... NX` is atomic at the level of a single Redis instance. For multi-node Redis Cluster, the same guarantee holds for keys hashing to the same slot, which RRQ's idempotency keys do (they don't span slots).

---

## Why each part exists

The pattern looks simple, but each design choice solves a specific problem.

### Why the body hash

Consider a merchant who reuses an idempotency key with a different request body:

- Request 1: `transfer 5000 NGN from A to B`, key `K`.
- Request 2: `transfer 10000 NGN from A to C`, key `K`.

Without body checking, Request 2 would either return Request 1's cached response (wrong, the merchant might think their second transfer happened) or be silently rejected as a duplicate (worse, they don't know it didn't go through).

The body hash makes the answer explicit. The stored value is `body_hash + cached_response`. On a duplicate request, the server compares hashes:

- Same hash → genuine retry; return cached response.
- Different hash → key reuse with different intent; return 422 with a specific error.

The 422 response includes a clear error code (`IDEMPOTENCY_KEY_REUSED_WITH_DIFFERENT_BODY`) and a documentation link. Some merchants do this accidentally (a bug in their retry logic generates a new request body but keeps the old key); the clear error helps them debug.

**Why SHA-256?** Cryptographic collision resistance. A merchant who could find a SHA-256 collision could double-charge by sending two semantically-different requests that hash to the same value. SHA-256 collisions are not a practical concern; finding one is computationally infeasible. CRC32 or MD5 would not be safe; they have practical collisions.

**Why hash the canonical form?** JSON has cosmetic flexibility: `{"a":1,"b":2}` and `{"b": 2, "a": 1}` are semantically equivalent but byte-different. A merchant might serialize differently on retry (different library, different key ordering). Hashing canonical form means semantically-equivalent bodies produce identical hashes. The canonical form is defined as: sorted keys, no whitespace, RFC 8785 JCS.

### Why the placeholder

The `"processing:..."` placeholder distinguishes "request is in flight" from "request completed with this response." Without it, two states would be conflated:

- Request started, response not yet computed.
- Request completed, response cached.

If a second request comes in during the first scenario, the cache value is empty (or doesn't exist) and the second request could proceed. With the placeholder, the second request sees `processing:` and knows to return 409.

The placeholder also includes the body hash, so even during in-flight processing, body-mismatch detection works.

### Why merchant-scoped keys

The idempotency key is `idemp:{merchant_id}:{key}`, not just `idemp:{key}`. Reason: UUIDs aren't unique across merchants. Two merchants could independently generate the same UUID. Scoping prevents collisions.

A naive implementation without scoping would have a merchant's UUID collide with another merchant's UUID for an unrelated request, rejecting the second request as a "duplicate." That's a silent bug, the merchant would see 409 errors they can't explain.

### Why a 24-hour TTL

Long enough to cover any realistic retry window. Short enough to bound storage costs.

The retention is documented in the merchant-facing API. After 24 hours, the same key behaves as a brand-new request. Merchants whose retry strategies span longer (rare) must use different keys for retries beyond the window.

Stripe uses 24 hours. Some systems use 7 days. Either is defensible; RRQ matches Stripe because the precedent is well-understood.

### Why the explicit DEL on failure

This is the subtlest part of the pattern. If `process_request(body)` fails *after* the SETNX succeeded, say, the saga worker rejected the message because Redis Streams was down, the idempotency cache shows `"processing:hash"` indefinitely (until TTL).

Without the DEL: the merchant retries (because they got a 5xx). The second attempt sees `"processing:hash"`, returns 409, and the merchant gives up. The transfer never happens, even though the system would happily accept it. The user experience is "I tried twice, both times got an error, but the system thinks I'm in the middle of something."

With the DEL: the failed first attempt clears the claim. The retry sees no key, gets to start fresh. The merchant's experience is "I tried, got an error, tried again, succeeded." Much better.

**What if the DEL also fails?** Then the cache holds `"processing:"` for up to 24 hours, and retries during that window return 409. This is the worst-case failure mode. Mitigation: 5xx errors that happen *during* the processing window are very rare in practice (Redis is generally up), and if Redis is fully down, the second DEL attempt would also fail. The system isn't optimizing for "Redis is completely dead", that's an operational incident requiring intervention.

---

## Concurrent duplicates and the race window

The most-tested-but-least-considered scenario: two requests with the same idempotency key arriving at the *exact same instant*.

Without atomic SETNX, both requests would observe "key doesn't exist" simultaneously, both would proceed, both would execute. Atomic SETNX makes one of them win and the other see `nil`.

The race window in the *correct* implementation:

```
Request A: SET key NX  ── ok (A wins)
Request B: SET key NX  ── nil (loses)
Request A: process saga
Request B: GET key, return "processing:..." → 409
```

Time between A's SET and B's SET is irrelevant, Redis serializes them. Even at perfect simultaneity from the client's perspective, Redis applies one before the other.

The interesting question is what B sees in GET. Three sub-cases:

**Sub-case 1: B's GET happens before A finishes.** B sees `"processing:hash"`. Returns 409 Conflict. The merchant retries later, gets the cached response.

**Sub-case 2: B's GET happens after A finishes successfully.** B sees `"hash:response_json"`. Compares hashes. Matches. Returns 202 with A's cached job_id. From the merchant's view: both requests succeeded with the same job_id, which is correct (only one job was created).

**Sub-case 3: B's GET happens after A fails (and DELs the key).** B sees `nil` (key was deleted). Wait, does it retry the SETNX? Or fall through to an error path?

This is an edge case worth thinking through. The flow would be:

```
A: SETNX ok
A: process_request → error
A: DEL key
B: SETNX nil (between A's SETNX and A's DEL)
B: GET key → nil (the DEL just happened)
```

B's GET returns nil. The pseudocode above doesn't handle this case explicitly; it would fall through to some "shouldn't happen" branch.

The fix: in the duplicate path, if GET returns nil, retry the whole flow from the start (since the previous claimer's failure has cleared the way). RRQ's implementation does this with a bounded retry (max 3 iterations) to prevent an unbounded loop if some other failure mode keeps clearing the key.

```
on_request_with_retry(merchant_id, key, body, attempts=3):
    for i in range(attempts):
        result = on_request(merchant_id, key, body)
        if result != KEY_CLEARED_RETRY:
            return result
    return error(503, "could not process request after retries")
```

In practice, this code path almost never executes. But "almost never" isn't "never," and a correct implementation handles it.

---

## What happens after the saga

The flow above stops at "send 202 to merchant." But there's an asynchronous postscript: after the response is sent, the API gateway needs to update the cache from `"processing:..."` to `"hash:cached_response"` so future retries get the cached response.

This is **fire-and-forget**. If it fails, the cache stays at `"processing:"` until TTL. Subsequent retries return 409 (not the cached response) until the original processing completes and updates the cache. The downstream saga still runs exactly once (because we only XADDed one event); the merchant just gets less helpful responses during a brief window.

Why fire-and-forget? Because the 202 has already gone out. The merchant has their answer. Updating the cache is a courtesy for future retries, not a correctness requirement.

A more careful implementation would update the cache *before* sending the 202. But that adds a database write to the request path, which we've worked hard to avoid. The cost is small (a few milliseconds), but multiplied across high throughput it adds up.

For RRQ, the choice is to keep the request path fast and tolerate the minor caching gap. This is documented in the API gateway service doc and surfaces in tests.

---

## What is NOT idempotent

Worth stating explicitly: idempotency keys protect the *API acceptance* of a request. They don't protect anything downstream from being executed twice via other failure modes.

Specifically:

**Stream message redelivery.** Once the saga worker claims a message, it might process it twice if the worker crashes between processing and ACK. The idempotency key was already consumed at the gateway level; it can't protect the saga from re-processing the same `JobRequested`. The saga's *own* idempotency (via `UNIQUE(saga_id, step_name)`) handles this.

**Manual replay from DLQ.** If an operator replays a DLQ entry, it creates a new job with a new idempotency key (`dlq-replay-<entry_id>`). The original key's cache is untouched. The replay is a deliberate human action, not a merchant retry; it's accounted for in the audit log.

**Cross-merchant duplicates.** If two merchants accidentally use the same key, they don't collide (merchant-scoped). If one merchant logs into two systems using the same merchant_id and submits the same key from both, those are two different requests in different sessions, both go to the same logical cache. The cache scopes per-merchant, not per-session.

**Reconciliation adjustments.** If reconciliation discovers a discrepancy and an operator manually inserts an adjustment event, the adjustment doesn't use the idempotency cache. Adjustments are operator-driven, not merchant-driven.

The principle: **the idempotency key is a contract between the merchant and the API gateway.** It protects that boundary. Other boundaries have their own idempotency mechanisms (UNIQUE constraints, stream ACK semantics, operator audit trails).

---

## The merchant's side of the contract

Worth being explicit about what RRQ asks of merchants:

1. **Generate a unique key per logical operation.** Don't reuse keys across operations. Don't generate them from operation parameters (e.g., `hash(from_wallet, to_wallet, amount)`), that creates accidental dedup for genuinely-distinct operations like "send the same amount to the same wallet twice."

2. **Use the same key on every retry of the same operation.** If your HTTP client times out and you retry, use the original key. If you generate a new key on retry, you've lost the idempotency guarantee.

3. **Choose a TTL on your side that fits inside 24 hours.** If your retry strategy spans days, you need to be aware that the server's cache expires.

4. **Don't reuse keys across different request bodies.** If you generate a key K and use it for a transfer request, don't reuse K for a different transfer. (The server enforces this via the body hash; we reject with 422.)

5. **Treat 409 as "wait and retry."** A 409 means "your request is in flight; try again in a moment." Implement exponential backoff on 409s.

These are documented in the merchant-facing API documentation. RRQ enforces what it can (body hash check, atomic SETNX) and trusts the merchant for the rest. A merchant who violates rule 1 (reusing keys) gets confusing 422 errors; a merchant who violates rule 2 (different keys on retry) loses idempotency protection but the system doesn't otherwise misbehave.

---

## Stripe's approach, in detail

Since Stripe is the canonical reference, worth noting where RRQ's design matches and where it differs.

**Matches:**
- Header-based key (`Idempotency-Key`).
- Merchant-scoped storage.
- 24-hour retention.
- Body hash verification.
- 200/202 with cached response on retry.
- 409 for in-flight.
- 422 for key reuse with different body.

**Differences:**
- Stripe also caches the response *body* in their cache; RRQ caches the entire response including headers. Small difference.
- Stripe's failure path is slightly different, they discuss caching errors as well, so that retries get the same error. RRQ doesn't cache errors; a failed request clears the key, and the retry is processed fresh. The Stripe choice handles "the underlying operation failed deterministically; retrying will fail the same way" better; the RRQ choice handles "the failure was transient" better. Both are defensible.
- Stripe uses a TTL of 24 hours from *first request*; RRQ uses 24 hours from *most recent update*. The difference: in RRQ, if a retry comes 23 hours after the original, the TTL extends. In Stripe, it doesn't. Minor.

These differences are calibration, not architectural. The pattern is the same.

---

## A few things this pattern doesn't solve

**It doesn't make operations naturally idempotent.** If the underlying operation (a transfer) cannot tolerate being executed twice, the idempotency key isn't a substitute for designing the operation idempotently. The key ensures the operation runs *at most once* from the API's perspective. If the saga (downstream) has a bug that causes the same job_id to be processed twice, the key doesn't help. That's why the saga has its own idempotency at the storage layer.

**It doesn't protect against malicious replay attacks.** A man-in-the-middle who captures the merchant's request can replay it as long as the merchant's auth token is valid. The idempotency key would prevent the underlying operation from running twice, but it wouldn't prevent the attacker from observing the response. Defense against MITM is the job of HTTPS, not idempotency.

**It doesn't help with cross-request consistency.** If a merchant submits two unrelated requests in quick succession, the idempotency cache doesn't coordinate them. They're independent operations with independent idempotency guarantees.

**It doesn't expire on operation completion.** Even after a transfer completes, the cache holds the response for 24 hours. This is a feature, the merchant might retry late, but it also means the cache holds completed-and-acknowledged operations longer than strictly necessary. Storage cost is small; not worth optimizing.

---

## The retry-on-status-code pattern, briefly

Worth a small detour: what status codes should a merchant retry?

The conventional wisdom, which RRQ follows:

- **2xx**: success. Don't retry.
- **4xx**: client error. The request will fail the same way if retried; usually don't retry. Exceptions: 408 Request Timeout, 425 Too Early.
- **429 Too Many Requests**: rate limited. Retry after the `Retry-After` header.
- **5xx**: server error. Retry with exponential backoff and jitter (same shape as webhook retries, see [`24-RESILIENCE.md`](24-RESILIENCE.md)).
- **409**: in-flight idempotency claim. Retry after a short delay.

The merchant's idempotency story works because every retry uses the same key. RRQ on its side handles the dedup. The merchant just needs to not generate a new key on retry.

---

## A note on horizontal scaling

The atomic SETNX guarantee works for a single Redis instance. For RRQ's scale (1,000 TPS, ~5,000 idempotency operations/sec), one Redis instance is plenty.

For larger scale (millions of TPS), the idempotency cache might need to be partitioned. Redis Cluster supports this natively: keys are hashed to slots, slots are distributed across nodes. As long as the same idempotency key always hashes to the same slot, atomic SETNX still works for that slot.

The non-trivial scaling concern is *not* the atomic guarantee; it's the cache size. With 24-hour retention and 1,000 TPS, the cache holds ~86M keys at any time. At ~200 bytes per entry, that's ~17 GB of memory. Comfortable for a modern Redis instance, but worth watching.

For larger volumes, the cache could be sharded by `hash(merchant_id)`, with each shard on a different Redis instance. RRQ doesn't need this at its target scale; the scaling path is documented.

---

## Implementation gotchas

A list of subtle things that catch first-time implementers:

**Reading the body twice.** The HTTP body is a single-use stream by default. Idempotency middleware needs to read it to hash, but the handler needs to read it to deserialize. The fix: read once into a buffer; wrap the body for the handler's read.

```go
body, _ := io.ReadAll(r.Body)
r.Body = io.NopCloser(bytes.NewReader(body))
hash := sha256.Sum256(body)
// handler reads from r.Body
```

**Middleware order.** Idempotency depends on the merchant_id being known. It must run *after* authentication, not before. Reversing the order means you're caching unauthenticated requests, which is a bug (an attacker could submit a request, get a 401, then a legitimate merchant uses the same key and gets the attacker's cached 401 response).

**Caching errors.** If you cache 5xx responses, retries see the same 5xx and never recover from transient failures. RRQ doesn't cache errors, the DEL on failure clears the cache so retries are processed fresh.

**TTL on the cache vs. the saga state.** The idempotency cache TTL is 24 hours. The saga state lives forever (until explicit archival). They're decoupled. A merchant who retries 25 hours after the original gets treated as a fresh request, even though the saga state from the original still exists, the API gateway doesn't see it. This is by design.

**The "what if the merchant submits the same body but with a new key" scenario.** RRQ accepts this. Without an idempotency key, every request is independent. The merchant is responsible for not generating new keys for the same logical operation. If they do, that's a bug in their code, and the consequence is a duplicate transfer.

**Body encoding.** If two requests have semantically-identical bodies but different byte encodings (`{"a":1}` vs `{"a": 1}`, note the space), naive byte-hashing would treat them as different. RRQ canonicalizes the body before hashing. The canonical form is well-defined (RFC 8785 JCS) and reproducible by any client.

**Clock skew between merchants and servers.** Doesn't matter for idempotency. The TTL is measured server-side; the merchant's clock doesn't enter the picture. (It matters for some auth schemes, but that's a separate concern.)

---

## Where to read next

- The API gateway that implements all of this → [`../services/10-API-GATEWAY.md`](../services/10-API-GATEWAY.md)
- The saga's own idempotency mechanism (at the storage layer) → [`21-SAGAS.md`](21-SAGAS.md)
- The merchant-facing API guide → [`../appendices/42-API-REFERENCE.md`](../appendices/42-API-REFERENCE.md)

---

*Pass 3 of the architecture series. Last updated pre-implementation.*
