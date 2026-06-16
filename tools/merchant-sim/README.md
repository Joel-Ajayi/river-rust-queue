# merchant-sim

> **Status: designed, not built.** This tree is a set of placeholders for code that
> has not been written yet. There are no fake stubs here; each directory holds a
> README pointing at the design it will implement. See
> [`STATUS.md`](../../STATUS.md) for the precise project state.

`merchant-sim` is the simulated outside world for RRQ: simulated merchants, a
synthetic end-user population that lives only inside this process, a webhook
receiver, a traffic driver, and a scenario engine. It talks to RRQ only over the
same public HTTP API a real merchant would use, so RRQ stays unaware that the
merchant on either side of it is simulated.

The full design is [`docs/services/17-SIMULATION-HARNESS.md`](../../docs/services/17-SIMULATION-HARNESS.md).
Read that first. This README only maps the design onto the directory layout.

## Boundary (the thing to get right)

RRQ stores no end-user identity. A `customer` wallet carries one opaque
`external_ref` and nothing else. The end-user model (names, ids, wallet mapping)
lives entirely in this simulator, in [`store/`](store/) and [`users/`](users/).
If a column that belongs to the simulator ever shows up in RRQ, the boundary has
leaked.

## Directory map

| Directory | What it will hold |
| --- | --- |
| [`cmd/`](cmd/) | Entry point: wires the parts together and starts the chosen mode. |
| [`client/`](client/) | Merchant API client: API-key to JWT exchange, transfers, payouts, idempotency keys. |
| [`receiver/`](receiver/) | Webhook receiver: HMAC verify, dedup on `X-RRQ-Event-Id`, misbehavior modes. |
| [`users/`](users/) | Synthetic end-user population and the user-to-wallet mapping. |
| [`driver/`](driver/) | Steady-mode traffic loop that keeps the system looking alive. |
| [`scenarios/`](scenarios/) | Named scenarios shared by the dashboard, CI, and the command line. |
| [`store/`](store/) | The simulator's own datastore (SQLite or a separate Postgres schema). |

## How it will run (not yet wired)

- `make sim` will run `merchant-sim` in steady mode against the local stack. The
  target exists in the root [`Makefile`](../../Makefile) but is marked
  not-yet-functional until the code lands.
- A `merchant-sim` entry in the dev overlay (`k8s/overlays/dev`) will bring it up
  alongside RRQ in the local `kind` cluster, so `make dev` shows a live system.
  Those manifests do not exist in the repo yet; see the project-level status notes.

It sits under `tools/`, not under the service directories, because it is part of
the project but not part of the product.
