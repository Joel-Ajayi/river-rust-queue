# STATUS

> What's been done, what's been designed, what's not started. Updated as the project moves.
>
> This file exists because "in-progress" projects often blur the line between _designed_ and _built_. RRQ does not. If something has been built, this file says so. If only designed, this file says that. If neither, that too.

**Last updated:** Pre-implementation. Design phase complete; Go build is next.

---

## Phase status

| Phase                                                        | Status                       |
| ------------------------------------------------------------ | ---------------------------- |
| Design, system, services, invariants                        | Complete (see `docs/`)       |
| Design, deep-dives                                           | In progress                  |
| Design, simulation harness and merchant-sim                  | Complete (see doc 17)        |
| Design, chargebacks and Kubernetes deployment               | Complete (docs 18, 29)       |
| Implementation, scaffold (proto, migrations, k8s overlays, CI) | Not started (dirs are placeholders) |
| Implementation (Go), API Gateway                             | Not started                  |
| Implementation (Go), Saga Worker                             | Not started                  |
| Implementation (Go), Webhook Worker                          | Not started                  |
| Implementation (Go), Fraud Worker                            | Not started                  |
| Implementation (Go), Reconciliation                          | Not started                  |
| Implementation (Go), Admin Dashboard                         | Not started                  |
| Implementation (Go), merchant-sim (sim, receiver, scenarios) | Not started                  |
| Scenario suite passing in CI (proves invariants end to end)  | Not started                  |
| Benchmarks (k6 scenarios, Go)                                | Not started                  |
| Deployment (Kubernetes, Go, public demo URL)                 | Not started                  |
| Rust comparison implementation (language study)              | Not started                  |

---

## What exists right now

**A complete design specification.** See `docs/00-OVERVIEW.md` for the system in one read. The design covers six services, the invariants they uphold, the failure modes they handle, and the data model. Read this before reading anything else.

**A simulation harness design.** `docs/services/17-SIMULATION-HARNESS.md` specifies `merchant-sim`, the simulated merchant and end-user population that lets the whole pipeline run without real integrators. This is the part that turns a set of services into a system you can watch work. Designed, not yet built.

**Repository tree.** The monorepo layout exists (`proto/`, `migrations/`, `v-go/`, `v-rust/`, `tools/merchant-sim/`, `k8s/`, `scripts/`, `benchmarks/`), and each directory names a real part of the system. Being honest about state: most of these directories are empty placeholders. The protobuf schemas, SQL migrations, Kubernetes manifests (Kustomize base plus `dev`/`prod` overlays and the Argo CD Application), and CI are designed in the docs but **not written yet**. The one exception is `v-rust/`, which has its Cargo workspace manifests.

**Workspace files.** `v-rust/Cargo.toml` declares the Rust workspace (for the comparison study) and its crate manifests are in place. `v-go/` does not yet contain a `go.mod`; the Go tree is empty pending the first service.

---

## What does not exist yet

**Service implementations.** No service has a working binary. The `v-go/` tree is empty, awaiting the first service. The Rust tree (`v-rust/`) is the comparison study: its Cargo workspace is scaffolded but no service is built. Neither tree contains stubs that pretend to be code.

**merchant-sim.** The simulated outside world is designed (doc 17) but not built. Until it exists, the services have nothing driving them end to end, which is why building it sits early in the milestone list.

**Tests.** No unit tests, no integration tests, no scenario suite. These exist in the design (`docs/02-INVARIANTS.md` enumerates which tests will validate which invariants, and doc 17 defines the end-to-end scenarios) but not in code.

**Benchmarks.** No k6 results, no comparison numbers. The methodology is designed (`docs/appendices/43-BENCHMARK-METHODOLOGY.md`) but no measurement has been taken.

**Deployment.** No deploy, no public demo URL. RRQ deploys to Kubernetes (design in `docs/deep-dives/29-KUBERNETES.md`): a local `kind` cluster for development and DigitalOcean Kubernetes for production, with Kong at the edge and Argo CD syncing the `prod` overlay. The Kustomize overlays in `k8s/` and the Argo CD Application are not written yet and nothing has been applied to a cluster.

**Dispute operations tooling.** Chargebacks are designed (`docs/services/18-CHARGEBACKS.md`), but only the dispute *engine* is. The operator surface is missing: the Admin Dashboard cannot yet inspect or override a dispute, and the playbook for "merchant successfully appeals after a default refund" is not written. This is the one part of the pipeline whose service design is ahead of its operator design.

---

## Why this file exists

Honesty about project state is, by itself, a positive signal in a project of this scope. Most in-progress repos do one of two things: pretend they're further along than they are, with bullets like "implementing X" and "building Y," or say nothing and leave the reader to guess. Both hurt the project.

This file is the third option. State plainly what's done, what's not, and what's next. A reviewer who reads it knows exactly what they're looking at. There are no surprises in either direction.

---

## Next implementation milestones (rough order)

Go first, driven to a deployed and demonstrable state before the Rust port begins.

1. API Gateway in Go: HTTP server, idempotency middleware, JWT validation, write `JobRequested` to Redis Streams.
2. Saga Worker happy path in Go: Transfer saga, no compensation yet.
3. Saga Worker failure handling and compensation in Go.
4. Webhook Worker, Fraud Worker, and Reconciliation in Go.
5. Admin Dashboard in Go.
6. merchant-sim: merchant client, webhook receiver, end-user population, traffic driver, scenario engine.
7. Scenario suite green in CI, proving each invariant end to end through the public API.
8. Deploy to Kubernetes with a public demo URL, merchant-sim running in steady mode so the system looks alive.
9. k6 benchmarks for the Go implementation.
10. Rust port, service by service, against the working Go reference.

Each milestone updates this file when complete.
