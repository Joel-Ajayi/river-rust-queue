# scenarios/ (placeholder)

> Designed, not built. See [`docs/services/17-SIMULATION-HARNESS.md`](../../../docs/services/17-SIMULATION-HARNESS.md), section "Scenario engine".

Named, scripted sequences that drive RRQ into a specific failure mode and assert
the documented recovery. One implementation, three surfaces: a dashboard button,
`go test` in CI, and the command line. Planned scenarios: `retry-storm`,
`fraud-freeze`, `webhook-outage`, `crash-recovery`, `recon-drift`. These are the
end-to-end realization of the per-service test plans, run against the whole
system through the real API. No code yet.
