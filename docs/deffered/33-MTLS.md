# 33 — mTLS Between Services

> **What this is.** The mutual-TLS design for inter-service communication in RRQ, using Linkerd. Designed for v2; not deployed in v1.
>
> **Reading time.** ~8 minutes.
>
> **Status.** Designed, not deployed. See [`../../STATUS.md`](../../STATUS.md).

---

## Why mTLS

Standard TLS proves the *server's* identity to the client (the certificate authority signs a cert tying the public key to the hostname). The client is anonymous; the server doesn't know who's calling.

Mutual TLS (mTLS) goes both ways. Both sides present certificates. Both sides verify each other's identity.

For RRQ's internal services:
- The API Gateway calling the Saga Worker (via Redis Streams, indirectly): no need for direct mTLS, since the communication is through Redis.
- The Webhook Worker calling Postgres: standard TLS to authenticate Postgres; the worker authenticates with a Postgres username/password.
- Inter-service gRPC calls (if any): mTLS protects the channel.
- The Admin CLI calling internal RPC: mTLS verifies the operator's identity.

For v1, the operational simplicity of skipping mTLS wins. Internal services run in the same Docker Compose network or the same K8s cluster, and we trust the network. For v2 in real production, mTLS is the right default.

---

## The threat model

What does mTLS protect against?

**Pod-to-pod impersonation within the cluster.** Without mTLS, a compromised pod (say, a vulnerability in the Webhook Worker) can call the Saga Worker's internal API pretending to be the API Gateway. With mTLS, the Saga Worker rejects the connection because the calling pod doesn't have a Gateway certificate.

**Network-level eavesdropping inside the cluster.** Modern K8s networks generally encrypt pod-to-pod traffic at the network layer (CNI plugins like Cilium support this). But explicit application-layer encryption is defense-in-depth.

**Service-account confusion in audit logs.** With mTLS, every authenticated call carries an identity. Audit logs can record "this call came from the API Gateway service account," which is more meaningful than "this call came from some pod."

What mTLS does NOT protect against:

- A compromise that exfiltrates a certificate. If an attacker steals the API Gateway's cert, they can impersonate the Gateway.
- Application-layer vulnerabilities. mTLS protects the channel; the application can still be exploited.
- External traffic (merchant → API Gateway). mTLS is mutual; we don't expect merchants to present client certs. External traffic uses standard TLS.

---

## Why Linkerd

Three options to implement mTLS:

**Option 1: Application-level mTLS.** Every service has cert-loading code, custom tls.Config, certificate refresh logic. Maximum control; significant work to maintain across services.

**Option 2: Sidecar mTLS via Linkerd (or Istio).** Linkerd injects a proxy sidecar into every pod. The proxy intercepts all network traffic, terminates outgoing TLS, and authenticates incoming TLS. The application sees plain HTTP; the wire sees mTLS.

**Option 3: CNI-level encryption.** Cilium or similar can encrypt pod-to-pod traffic transparently. Easy to enable, but operates below the application — you don't get identity-based authentication, just encryption.

RRQ's design chooses **Option 2 with Linkerd**. Linkerd is simpler than Istio (smaller surface area, lighter resource cost, better defaults). The sidecar pattern means the application doesn't need to know about mTLS — Linkerd handles certificate issuance, rotation, and verification.

Application code:
```
// No TLS code. The application makes plain HTTP/gRPC calls.
response := httpClient.Post("http://saga-worker:8080/internal/...")
```

Linkerd proxy in front of the application receives the request, wraps it in mTLS, sends it to the saga-worker's Linkerd proxy. The receiving proxy verifies the client cert, unwraps the TLS, and forwards plain HTTP to the application.

Both proxies are transparent. The application code doesn't change.

---

## Certificate management

Linkerd's identity service issues certificates per service account. Certs are short-lived (24-hour default); rotation happens automatically.

The trust anchor is a root cert at install time. Each service's cert is derived from this root. Operators don't manage individual certs; they manage the root and the service accounts.

For RRQ:
- Install Linkerd in the cluster.
- Annotate each Deployment with `linkerd.io/inject: enabled` so the proxy is added.
- Define service accounts per service (`api-gateway-sa`, `saga-worker-sa`, etc.).
- Set up authorization policies that allow specific service accounts to call specific endpoints.

Example authorization policy:
```yaml
apiVersion: policy.linkerd.io/v1beta1
kind: AuthorizationPolicy
metadata:
  name: saga-worker-allow-gateway
  namespace: rrq
spec:
  targetRef:
    group: policy.linkerd.io
    kind: Server
    name: saga-worker
  requiredAuthenticationRefs:
  - kind: ServiceAccount
    name: api-gateway-sa
```

This says: "the saga-worker only accepts connections from pods running as the api-gateway service account." Any other caller is rejected at the Linkerd proxy.

---

## Performance impact

mTLS adds latency to every internal call: TLS handshake (~5ms first call, ~0.1ms cached connection), proxy hop (~0.5ms each direction). For a typical service call, the additional latency is ~1-2ms.

For RRQ's throughput targets (1,000 TPS), this is negligible. For systems where every microsecond matters (high-frequency trading), it's prohibitive. RRQ is not in the prohibitive zone.

---

## Why this is deferred

For v1:
- Most "internal" calls are via Redis Streams (which has its own auth) or Postgres (which has its own auth). Direct service-to-service RPC is minimal.
- Local development uses docker-compose, where mTLS adds complexity without benefit.
- The threat model concerns are addressable at the application layer (proper auth on internal endpoints) without mTLS.

For v2:
- Production deployment on K8s with multiple services.
- Larger attack surface (more services, more potential for compromise).
- Compliance requirements (PCI-DSS often requires encryption in transit, even for internal traffic).

v2 with Linkerd is a quick add — install Linkerd, annotate deployments, define authorization policies. The application code doesn't change. The complexity is operational, not architectural.

---

## What an interviewer asks

**"Why didn't you implement mTLS in v1?"** Answer: scope. v1's threat model is local development plus a single-cluster deployment where the network is trusted. mTLS is the right default for production at scale; the design is in place for when v2 deploys to a hostile network.

**"What's wrong with just running TLS, not mTLS?"** Answer: TLS protects the channel but not the caller's identity. An attacker in the same network can present any client they want; the server has no way to verify. mTLS verifies both sides. For internal services, both sides being verified is exactly the property we want.

**"Why Linkerd over Istio?"** Answer: Linkerd is smaller and operationally simpler. Istio offers more features (traffic shaping, fault injection, multi-cluster) at significantly higher operational cost. For RRQ's needs (mutual auth and observability), Linkerd is sufficient.

---

## Where to read next

- The Kubernetes design where this fits → [`32-KUBERNETES.md`](32-KUBERNETES.md)
- Linkerd documentation: <https://linkerd.io/2/getting-started/>

---

*Pass 4 of the architecture series. Deferred feature; not deployed in v1.*
