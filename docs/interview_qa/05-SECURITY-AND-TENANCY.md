# Technical Interview Q&A: Security & Tenant Isolation Architecture

This document contains deep-dive interview questions, cryptographic design rationales, and security explanations covering RRQ's API key hashing, JWT authentication model, edge security policies, and tenant data isolation.

---

## Q1: Why use Argon2id for API key storage instead of bcrypt or SHA-256?

### Answer:
API keys (`rrq_live_<base58>`) are long-lived secrets presented during authentication. Storing them safely requires specific cryptographic properties:

1. **SHA-256 is Insecure for Secrets**: Fast cryptographic hash functions (SHA-256, MD5) can execute billions of hashes per second on GPUs, making brute-force dictionary attacks trivial if the hash database is leaked.
2. **Argon2id Memory-Hardness**: Argon2id is the winner of the Password Hashing Competition (PHC). It enforces memory-hardness ($m=64\text{MB}$, $t=1$, $p=4$). An attacker cannot utilize parallel GPU registers to brute-force hashes because each evaluation requires 64MB of physical RAM.
3. **Argon2id vs Bcrypt**: Bcrypt has a 72-byte input truncation limit and fixed memory consumption. Argon2id supports arbitrary key lengths and provides resistance against side-channel timing attacks.

---

## Q2: Why use asymmetric Ed25519 JWTs with 15-minute expiration instead of symmetric HMAC?

### Answer:
1. **Asymmetric Verification (Ed25519 vs HMAC)**:
   - **HMAC (HS256)** requires both the signing service (`core-api`) and verifying proxy (`Envoy Gateway`) to share the same secret key. If the gateway proxy is compromised, an attacker gains the private key and can forge arbitrary merchant tokens.
   - **Ed25519 (EdDSA)** uses asymmetric key pairs. `core-api` holds the private key to sign tokens; Envoy Gateway fetches public keys from `.well-known/jwks.json` to verify tokens.
2. **Short-Lived 15-Minute Expiration**: If a JWT is stolen in transit, the exposure window is strictly capped at 15 minutes. Merchants must re-authenticate using their Argon2id-backed API key to obtain a fresh JWT.

---

## Q3: How is Tenant Isolation (Invariant I9) enforced across multi-tenant database shards?

### Answer:
Cross-tenant access—where Merchant A reads or debits Merchant B's wallet—is the highest-severity failure in a payment core. RRQ enforces isolation at two defense layers:

1. **Layer 1: Gateway Claim Extraction**: Envoy Gateway verifies the JWT at the edge, extracts `claim: sub`, and injects `X-Merchant-Id: m_live_101` as a trusted downstream header.
2. **Layer 2: Mandatory Query Scoping**:
   ```sql
   -- API Gateway checks wallet ownership before inserting job
   SELECT id FROM wallets 
   WHERE id = $1 AND merchant_id = $2;
   ```
   If `$2` (`X-Merchant-Id`) does not match the wallet's owner, the query returns 0 rows.
3. **HTTP 404 vs 403 Response Strategy**:
   When a caller requests a wallet or transfer ID belonging to another merchant, RRQ returns `HTTP 404 Not Found` rather than `HTTP 403 Forbidden`. Returning `403` leaks the existence of the resource ID to unauthorized callers. `404` prevents resource enumeration attacks.

---

## Q4: How does PII log masking prevent sensitive financial data leakage?

### Answer:
Payment systems must comply with PCI-DSS and privacy standards by ensuring credentials, authorization headers, and card data never reach stdout or Elasticsearch logs.

1. **Zap Logger Masking Core**: Application loggers wrap Zap fields with a PII masking layer (`internal/platform/pii`).
2. **Sensitive Key Detection**: Fields containing sensitive keys (`pan`, `card_number`, `secret`, `authorization`, `api_key`, `cvv`) are intercepted and replaced with redacted placeholders (`[REDACTED]`).
3. **Structured Field Enforcement**: Free-form un-structured string logging is prohibited by code linter rules; all logs must use structured Zap key-value pairs to guarantee masking filter evaluation.
