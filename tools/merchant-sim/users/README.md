# users/ (placeholder)

> Designed, not built. See [`docs/services/17-SIMULATION-HARNESS.md`](../../../docs/services/17-SIMULATION-HARNESS.md), section "End-user population and wallet mapping".

The synthetic end-user population. Each user has an id, a display name, and the
`external_ref` of the RRQ `customer` wallet that represents them. The mapping is
one-directional and lives only here: given a user, find their wallet. RRQ cannot
go the other way, and that is correct. End-user identity is never modeled in RRQ.
Backed by [`store/`](../store/). No code yet.
