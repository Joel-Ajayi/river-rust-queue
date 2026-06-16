# store/ (placeholder)

> Designed, not built. See [`docs/services/17-SIMULATION-HARNESS.md`](../../../docs/services/17-SIMULATION-HARNESS.md), section "The boundary decision: where end-users live".

The simulator's own datastore, kept away from RRQ's database on purpose (a SQLite
file or a separate Postgres schema). Holds the synthetic users and the
user-to-wallet mapping owned by [`users/`](../users/), plus the simulator's record
of which transfers it started and which webhooks it received. None of this is ever
visible to RRQ. No code yet.
