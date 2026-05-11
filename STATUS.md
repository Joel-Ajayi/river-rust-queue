# STATUS

> What's been done, what's been designed, what's not started. Updated as the project moves.
>
> This file exists because "in-progress" projects often blur the line between *designed* and *built*. RRQ does not. If something has been built, this file says so. If only designed, this file says that. If neither, that too.

**Last updated:** Pre-implementation, design phase complete.

---

## Phase status

| Phase | Status |
| --- | --- |
| Design — system, services, invariants | **Complete** (see `docs/`) |
| Design — deep-dives (sagas, idempotency, ordering, locking, resilience) | In progress |
| Design — deferred features (chargebacks, FX, K8s, mTLS) | Not started |
| Implementation — scaffold (proto, migrations, docker-compose, CI) | Complete |
| Implementation — API Gateway (Go + Rust) | Not started |
| Implementation — Saga Worker (Go + Rust) | Not started |
| Implementation — Webhook Worker (Go + Rust) | Not started |
| Implementation — Fraud Worker (Go + Rust) | Not started |
| Implementation — Reconciliation (Go + Rust) | Not started |
| Implementation — Admin CLI (Go + Rust) | Not started |
| Chaos tests (turmoil in Rust, testcontainers in Go) | Not started |
| Benchmarks (k6 scenarios A–F) | Not started |
| Deployment (Fly.io, both implementations) | Not started |

---

## What exists right now

**A complete design specification.** See `docs/00-OVERVIEW.md` for the system in one read. The design covers six services, the invariants they uphold, the failure modes they handle, and the data model. Read this before reading anything else.

**Repository scaffolding.** Monorepo structure, protobuf event/service definitions, SQL migrations for the seven tables in the data model, `docker-compose.yml` for local development infrastructure, GitHub Actions CI configured for both Go and Rust toolchains. None of this is fake — every file describes something that will exist; no folder is empty padding.

**Workspace files.** `v-rust/Cargo.toml` declares the Rust workspace. `v-go/go.mod` declares the Go module. Both compile cleanly with no service binaries yet, because there are no service binaries yet.

---

## What does not exist yet

**Service implementations.** No service has a working binary in either language. The `v-go/*-worker/` and `v-rust/*-worker/` directories contain a single README each pointing to the relevant design doc. They are not stubs that pretend to be code; they are placeholders for code that hasn't been written.

**Tests.** No unit tests, no integration tests, no chaos tests. These exist in the design (`docs/02-INVARIANTS.md` enumerates which tests will validate which invariants) but not in code.

**Benchmarks.** No k6 scripts, no benchmark results, no comparison numbers. The methodology is designed (`docs/appendix/43-BENCHMARK-METHODOLOGY.md` — coming) but no measurement has been taken.

**Deployment.** No Fly.io deploy, no Kubernetes cluster, no Linkerd. The K8s manifests will eventually live in `k8s/` as documentation of how the system would be deployed; no live deployment exists.

---

## Why this file exists

Honesty about project state is, by itself, a positive signal in a project of this scope. Most in-progress repos do one of two things: (1) pretend they're further along than they are, with bullets like "implementing X" and "building Y," or (2) say nothing, leaving the reader to guess. Both hurt the project.

This file is the third option: state plainly what's done, what's not, and what's next. A reviewer who reads it knows exactly what they're looking at. There are no surprises in either direction.

---

## Next implementation milestones (rough order)

1. API Gateway in Go — HTTP server, idempotency middleware, JWT validation, write `JobRequested` to Redis Streams.
2. API Gateway in Rust — same behavior, structurally parallel.
3. Integration tests for the idempotency invariant (I3) in both languages.
4. Saga Worker happy path in both languages — Transfer saga, no compensation yet.
5. Saga Worker failure handling and compensation in both.
6. Chaos tests for saga crash-resumability (turmoil in Rust, process-kill in Go).

Each milestone updates this file when complete.
