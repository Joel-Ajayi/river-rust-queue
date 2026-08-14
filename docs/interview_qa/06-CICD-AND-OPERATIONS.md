# Technical Interview Q&A: CI/CD & Operations Pipeline

This document contains deep-dive interview questions, deployment pipeline rationales, and operational explanations covering RRQ's two-repository CI/CD workflow, image promotion, DLQ replay procedures, and disaster recovery.

---

## Q1: How does the cross-repository CI/CD promotion flow work between `river-rust-queue` and `rrq-gitops`?

### Answer:
The two-repository model separates application code integration from GitOps cluster state promotion:

```
┌─────────────────────────────────────────────────────────────┐
| 1. Developer pushes commit to river-rust-queue/main         |
└──────────────────────────────┬──────────────────────────────┘
                               │
                               v
┌─────────────────────────────────────────────────────────────┐
| 2. App CI (app-ci.yml)                                      |
|    - Runs Go, Rust, and Buf Protobuf checks in parallel     |
|    - Builds Docker images tagged with git commit SHA        |
|    - Pushes images to GitHub Container Registry (GHCR)      |
└──────────────────────────────┬──────────────────────────────┘
                               │
                               v
┌─────────────────────────────────────────────────────────────┐
| 3. Promote Step (gitops-promote)                            |
|    - App CI updates kustomization.yaml image tags in        |
|      rrq-gitops overlay using yq                             |
|    - Commits and pushes to rrq-gitops/main                  |
└──────────────────────────────┬──────────────────────────────┘
                               │
                               v
┌─────────────────────────────────────────────────────────────┐
| 4. Argo CD Sync                                             |
|    - Argo CD controller detects git commit in rrq-gitops     |
|    - Synchronizes cluster manifests via rolling updates     |
└─────────────────────────────────────────────────────────────┘
```

1. **Commit SHA Tagging**: Container images are tagged with the full 40-character Git commit SHA (`ghcr.io/joel-ajayi/core-api-go:abc123...`). Tags are immutable; no `latest` tags are used in production.
2. **Kustomize Image Overrides**: The `gitops-promote` job uses `yq` to update the `newTag` field in `rrq/overlays/prod/kustomization.yaml`.
3. **Auditability**: Every deployment commit in `rrq-gitops` explicitly links to the application commit SHA that generated the container build.

---

## Q2: Why run Go, Rust, and Protobuf linting/testing in parallel during CI?

### Answer:
1. **Zero Cross-Dependency**: Go service code, Rust prototype code, and Protobuf schemas are structurally decoupled. Running `go test`, `cargo test`, and `buf lint` in parallel reduces overall App CI duration from ~12 minutes down to ~3 minutes.
2. **Buf Breaking Change Detection**: Protobuf changes are evaluated against `main` using `buf breaking --against '.git#branch=main'`. This prevents developers from pushing breaking schema changes to event payloads or gRPC interfaces.
3. **Sequential Promotion Gate**: The `images` build job requires `needs: [go, rust, buf]`. If any linters, unit tests, or Protobuf breaking checks fail, image building and GitOps promotion are immediately halted.

---

## Q3: How does automated DLQ Replay work without creating double-posting risks?

### Answer:
When a transaction or webhook exhausts its retry budget, it is persisted to `dlq_entries`. An operator triggers replay via the Admin API (`POST /v1/admin/dlq/{id}/replay`).

**Idempotency Safety During Replay**:
1. **Ledger DLQ Replay**: The DLQ replay fetches the `original_payload` and re-injects the job. Because the job retains its original `transfer_id`, the database constraint `UNIQUE (transfer_id, leg)` on `ledger_entries` guarantees that even if the original transaction completed late, the replayed transaction cannot insert duplicate debit or credit legs.
2. **Webhook DLQ Replay**: Webhook deliveries are idempotent on `(merchant_id, delivery_id)`. Replaying a failed webhook re-uses the original event payload and HMAC signature.

---

## Q4: What is the emergency rollback strategy if a faulty deployment reaches production?

### Answer:
1. **Standard GitOps Rollback (Preferred)**:
   Revert the last commit in `rrq-gitops`:
   ```bash
   cd rrq-gitops
   git revert HEAD
   git push origin main
   ```
   Argo CD immediately detects the revert commit and performs a rolling update to restore the previous container image tags.
2. **Emergency Hot-Patch (Bypass GitOps temporarily)**:
   If Git access is unavailable during an active outage, run `kubectl set image`:
   ```bash
   kubectl set image deployment/core-api core-api=ghcr.io/joel-ajayi/core-api-go:<PREVIOUS_GOOD_SHA> -n rrq
   ```
   *Note*: Argo CD self-healing should be briefly paused (`Auto-Sync: Disabled`) during emergency manual overrides to prevent Argo CD from overwriting the manual fix before Git is updated.
