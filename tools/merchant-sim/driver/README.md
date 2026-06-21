# driver/ (placeholder)

> Designed, not built. See [`docs/services/17-SIMULATION-HARNESS.md`](../../../docs/services/17-SIMULATION-HARNESS.md), section "Traffic driver".

The steady-mode traffic loop. A light, in-process loop that posts a few transfers
per second between random end-user wallets, with the occasional payout, so a
visitor to the dashboard sees balances moving, transfers posting, and webhooks
arriving in real time. Heavy load is driven separately by the k6 scripts in
`scripts/`, not from here. No code yet.
